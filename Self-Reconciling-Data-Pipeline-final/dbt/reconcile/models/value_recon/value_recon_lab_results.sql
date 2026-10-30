{#
  Value-level reconciliation for lab results (T6).
  Compares result_value, result_unit and collected_at between raw and stg_lab_results as
  deterministic-encryption fingerprints (key wrapped by KMS, usable only by sa-valuerecon).
  Output: one row per record and field that differs — record key and field name ONLY,
  never values or fingerprints. Runs only on the `valuerecon` target.
#}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}
{% set kms = env_var('KMS_RECON_KEY', '') %}

{% set parts = [] %}
{% if bd is not none %}
  {% set end = modules.datetime.date.fromisoformat(bd | string) %}
  {% for i in range(lookback) %}
    {% do parts.append("date('" ~ (end - modules.datetime.timedelta(days=i)).isoformat() ~ "')") %}
  {% endfor %}
{% endif %}

{{ config(
    enabled = (target.name == 'valuerecon'),
    schema = 'evidence_restricted',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'business_date', 'data_type': 'date'},
    partitions = (parts if parts else none),
    full_refresh = false,
    tags = ['valuerecon'],
    meta = {'source_system': 'LIS', 'criticality': 'high', 'phi': false, 'owner': 'laboratory'}
) }}

{% set ks = '' %}
{% if execute %}
  {% set q %}select to_base64(wrapped_keyset) from `{{ target.project }}.evidence_restricted.recon_keyset` where key_version = 'v1'{% endset %}
  {% set rows = run_query(q).columns[0].values() %}
  {% if rows | length == 0 %}
    {{ exceptions.raise_compiler_error("No recon keyset found: run `dbt run-operation create_recon_keyset --target valuerecon` first") }}
  {% endif %}
  {% set ks = rows[0] %}
{% endif %}
{% set chain %}keys.keyset_chain('{{ kms }}', from_base64('{{ ks }}')){% endset %}

{% set window %}
  {% if bd is not none %}
  where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day) and date('{{ bd }}')
  {% endif %}
{% endset %}

with raw_latest as (
    select business_date, lab_result_id, result_value, result_unit, collected_at
    from {{ source('raw', 'lab_results') }}
    {{ window }}
    qualify row_number() over (partition by business_date, lab_result_id
                               order by _source_snapshot_ts desc, _batch_id desc) = 1
),

stg as (
    select business_date, lab_result_id, result_value, result_unit, collected_at
    from {{ ref('stg_lab_results') }}
    {{ window }}
),

-- Canonical text for each critical field, identical rules on both sides.
canon as (
    select 'raw' as side, business_date, lab_result_id,
           ifnull(format('%.6f', cast(result_value as float64)), 'NULL')                   as c_value,
           ifnull(trim(result_unit), 'NULL')                                                as c_unit,
           ifnull(format_timestamp('%Y-%m-%dT%H:%M:%E6S', collected_at, 'UTC'), 'NULL')     as c_collected
    from raw_latest
    union all
    select 'stg', business_date, lab_result_id,
           ifnull(format('%.6f', cast(result_value as float64)), 'NULL'),
           ifnull(trim(result_unit), 'NULL'),
           ifnull(format_timestamp('%Y-%m-%dT%H:%M:%E6S', collected_at, 'UTC'), 'NULL')
    from stg
),

-- Fingerprints. The field name is bound in as associated data, so equal text in two fields differs.
fp as (
    select side, business_date, lab_result_id,
           deterministic_encrypt({{ chain }}, c_value,     'lab_results.result_value') as f_value,
           deterministic_encrypt({{ chain }}, c_unit,      'lab_results.result_unit')  as f_unit,
           deterministic_encrypt({{ chain }}, c_collected, 'lab_results.collected_at') as f_collected
    from canon
),

paired as (
    select business_date, lab_result_id,
           r.f_value as r_value, r.f_unit as r_unit, r.f_collected as r_collected,
           s.f_value as s_value, s.f_unit as s_unit, s.f_collected as s_collected
    from (select * from fp where side = 'raw') r
    full join (select * from fp where side = 'stg') s using (business_date, lab_result_id)
)

select business_date, lab_result_id as record_key, f.field_name, 'value_differs' as issue,
       'v1' as key_version, current_timestamp() as checked_at
from paired
cross join unnest([
    struct('result_value' as field_name, r_value != s_value as differs),
    struct('result_unit',  r_unit  != s_unit),
    struct('collected_at', r_collected != s_collected)
]) f
where r_value is not null and s_value is not null and f.differs

union all

select business_date, lab_result_id, '*',
       if(s_value is null, 'missing_in_staging', 'missing_in_raw'),
       'v1', current_timestamp()
from paired
where r_value is null or s_value is null