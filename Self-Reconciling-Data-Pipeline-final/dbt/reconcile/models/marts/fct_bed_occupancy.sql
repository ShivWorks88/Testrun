{#
  Inpatient beds per ward per facility day (Asia/Kolkata):
  admissions, discharges, and beds still occupied at midnight (end of the day).
#}
{% set tz = var('facility_tz') %}
{{ config(materialized = 'table', partition_by = {'field': 'business_date', 'data_type': 'date'},
          meta = {'source_system': 'EHR', 'criticality': 'high', 'phi': false, 'owner': 'operations'}) }}

with days as (
    select distinct business_date from {{ ref('stg_encounters') }}
),

inpatients as (
    select * from {{ ref('dim_encounters') }} where encounter_type = 'inpatient'
)

select
    d.business_date,
    coalesce(i.ward, 'UNASSIGNED')                                                   as ward,
    concat(cast(d.business_date as string), '|', coalesce(i.ward, 'UNASSIGNED'))     as occupancy_key,
    countif(date(i.admit_ts, '{{ tz }}') = d.business_date)                          as admissions,
    countif(date(i.discharge_ts, '{{ tz }}') = d.business_date)                      as discharges,
    countif(i.discharge_ts is null
            or i.discharge_ts >= timestamp(date_add(d.business_date, interval 1 day), '{{ tz }}'))
                                                                                     as occupied_at_midnight
from days d
join inpatients i
  on i.admit_ts < timestamp(date_add(d.business_date, interval 1 day), '{{ tz }}')
group by 1, 2, 3