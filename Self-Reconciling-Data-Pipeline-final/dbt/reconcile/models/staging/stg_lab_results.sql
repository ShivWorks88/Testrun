{#
  Staging for lab results. One row per result per business date (resulted_ts, facility day).
  Critical fields (result_value, result_unit, collected_at) pass through unchanged; the
  value-level check (models/value_recon) compares them with raw as KMS-protected fingerprints.

  Test-only injector (T6): --vars '{business_date: YYYY-MM-DD, inject_value_truncation: true}', or the failure-
  suite switch `value_truncation` (dbt_build conf dbt_inject, ALLOW_INJECTIONS=true only), simulates a buggy
  release that rounds 14 results of that business date to 2 decimal places.
#}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'business_date', 'data_type': 'date'},
    cluster_by = ['lab_result_id'],
    full_refresh = false,
    on_schema_change = 'append_new_columns',
    meta = {'source_system': 'LIS', 'criticality': 'high', 'phi': true, 'owner': 'laboratory'}
) }}

with src as (
    select *
    from {{ source('raw', 'lab_results') }}
    {% if is_incremental() %}
      {% if bd is none %}
        {{ exceptions.raise_compiler_error("stg_lab_results: pass --vars '{business_date: YYYY-MM-DD}' for incremental runs") }}
      {% endif %}
    where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day)
                            and date('{{ bd }}')
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by business_date, lab_result_id
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

select * except (_rn)
{% if (var('inject_value_truncation', false) or inject('value_truncation')) and bd is not none %}
    -- T6 injector (test only): a buggy release rounds 14 results of the business date to 2 dp.
    replace (
        case when lab_result_id in (
            select lab_result_id from ranked
            where _rn = 1 and business_date = date('{{ bd }}')
              and result_value is not null and result_value != round(result_value, 2)
            order by farm_fingerprint(lab_result_id)
            limit 14)
        then round(result_value, 2) else result_value end as result_value
    )
{% endif %}
from mapped
where _rn = 1