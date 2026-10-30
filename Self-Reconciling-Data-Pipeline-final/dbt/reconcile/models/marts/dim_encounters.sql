{#
  One row per encounter: its latest version across all business dates.
#}
{{ config(materialized = 'table', cluster_by = ['encounter_id'],
          meta = {'source_system': 'EHR', 'criticality': 'high', 'phi': true, 'owner': 'data-engineering'}) }}

with latest as (
    select *
    from {{ ref('stg_encounters') }}
    qualify row_number() over (partition by encounter_id order by business_date desc, updated_at desc) = 1
)

select
    encounter_id,
    patient_id,
    facility_id,
    encounter_type,
    ward,
    status,
    admit_ts,
    discharge_ts,
    business_date                                                     as admit_business_date,
    discharge_ts is null                                              as is_open,
    round(timestamp_diff(discharge_ts, admit_ts, minute) / 60.0, 2)   as length_of_stay_hours
{%- if inject('build_error') %}
    -- failure suite (test only): a release ships SQL that fails at run time
    , cast(concat('not-a-number-', cast(encounter_id as string)) as int64) as injected_build_error
{%- endif %}
from latest
