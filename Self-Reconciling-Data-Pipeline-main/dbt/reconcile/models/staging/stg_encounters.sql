{#
  Staging for EHR encounters.
  - One row per encounter per business date (dedupe: latest updated_at, then latest batch).
  - Incremental: rebuilds the business_date var and the (lookback_days - 1) days before it.
  - full_refresh: false, so `dbt build --full-refresh` can never wipe history by accident.
#}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'business_date', 'data_type': 'date'},
    cluster_by = ['encounter_id'],
    full_refresh = false,
    on_schema_change = 'append_new_columns',
    meta = {'source_system': 'EHR', 'criticality': 'high', 'phi': true, 'owner': 'data-engineering'}
) }}

with src as (
    select *
    from {{ source('raw', 'encounters') }}
    {% if is_incremental() %}
      {% if bd is none %}
        {{ exceptions.raise_compiler_error("stg_encounters: pass --vars '{business_date: YYYY-MM-DD}' for incremental runs") }}
      {% endif %}
    where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day)
                            and date('{{ bd }}')
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by business_date, encounter_id
            order by updated_at desc, _batch_id desc
        ) as _rn
    from src
)

select * except (_rn)
from ranked
where _rn = 1