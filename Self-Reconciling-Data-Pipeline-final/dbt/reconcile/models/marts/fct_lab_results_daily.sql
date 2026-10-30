{#
  Lab results per business date and test: how many, how many outside the reference range, average value.
  Phase 6 package 2: the first report built from LIS, so a lab-value problem (T6) or an LIS outage holds a
  report users actually see. Built from stg_lab_results only (counts and averages, no patient identifiers).
#}
{{ config(materialized = 'table', partition_by = {'field': 'business_date', 'data_type': 'date'},
          meta = {'source_system': 'LIS', 'criticality': 'high', 'phi': false, 'owner': 'laboratory'}) }}

select
    business_date,
    test_code,
    concat(cast(business_date as string), '|', test_code)                          as lab_day_key,
    any_value(result_unit)                                                          as result_unit,
    count(*)                                                                        as results,
    countif(result_value < reference_low or result_value > reference_high)          as abnormal_results,
    round(avg(result_value), 3)                                                     as avg_value
from {{ ref('stg_lab_results') }}
group by business_date, test_code
