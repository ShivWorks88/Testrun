{#
  Staging for billing claims.
  - One row per claim per business date (submitted_ts, facility day).
  - claims has no updated_at, so the latest version wins by source snapshot time, then batch.
  - Rebuilds business_date and the (lookback_days - 1) days before it, so claims
    reversed after loading are picked up when those days are re-extracted.
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
)

select
    * except (_rn),
    status = 'reversed' as is_reversed
from ranked
where _rn = 1