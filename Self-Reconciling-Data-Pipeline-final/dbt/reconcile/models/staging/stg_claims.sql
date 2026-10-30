{#
  Staging for billing claims.
  - One row per claim per business date (submitted_ts, facility day).
  - claims has no updated_at, so the latest version wins by source snapshot time, then batch.
  - Rebuilds business_date and the (lookback_days - 1) days before it, so claims
    reversed after loading are picked up when those days are re-extracted.
  - patient_id goes through approved MPI remaps (ops.patient_remaps) — Phase 6 pkg 4.
#}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'business_date', 'data_type': 'date'},
    cluster_by = ['claim_id'],
    full_refresh = false,
    on_schema_change = 'append_new_columns',
    meta = {'source_system': 'BILLING', 'criticality': 'high', 'phi': true, 'owner': 'revenue-cycle'}
) }}

with src as (
    select *
    from {{ source('raw', 'claims') }}
    {% if is_incremental() %}
      {% if bd is none %}
        {{ exceptions.raise_compiler_error("stg_claims: pass --vars '{business_date: YYYY-MM-DD}' for incremental runs") }}
      {% endif %}
    where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day)
                            and date('{{ bd }}')
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by business_date, claim_id
            order by _source_snapshot_ts desc, _batch_id desc
        ) as _rn
    from src
),

-- Phase 6 pkg 4: approved MPI merges (ops.patient_remaps, written by the engine after a data owner approved)
remaps as (
    select retired_id, any_value(surviving_id) as surviving_id
    from {{ source('engine_maps', 'patient_remaps') }}
    group by retired_id
),

mapped as (
    select r.* replace (coalesce(m.surviving_id, r.patient_id) as patient_id)
    from ranked r
    left join remaps m on m.retired_id = r.patient_id
)

select
    * except (_rn),
    status = 'reversed' as is_reversed
from mapped
where _rn = 1
{%- if inject('staging_duplicates') %}
-- failure suite (test only): a release breaks the dedupe, 3 claims of the business date appear twice
union all
(select * except (_rn), status = 'reversed' as is_reversed
 from mapped
 where _rn = 1 and business_date = date('{{ bd }}')
 order by claim_id
 limit 3)
{%- endif %}
