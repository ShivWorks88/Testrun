{#
  T11: every lab result must use its test's expected unit and sit inside the plausible range.
  Counts stay the same when the lab system switches glucose to mmol/L, so receipts cannot see it;
  this test can. Returns one row per day, test and problem. Zero rows = PASS.
#}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

with results as (
    select r.business_date, r.test_code, r.result_unit, r.result_value,
           ref.expected_unit, ref.min_plausible, ref.max_plausible
    from {{ ref('stg_lab_results') }} r
    left join {{ ref('lab_reference_ranges') }} ref using (test_code)
    {% if bd is not none %}
    where r.business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day) and date('{{ bd }}')
    {% endif %}
)

select business_date, test_code, 'unknown test code' as problem, count(*) as rows_affected
from results where expected_unit is null
group by 1, 2

union all

select business_date, test_code,
       concat('unit ', coalesce(result_unit, 'NULL'), ' instead of ', expected_unit), count(*)
from results
where expected_unit is not null and coalesce(result_unit, '') != expected_unit
group by 1, 2, 3

union all

select business_date, test_code, 'value outside plausible range', count(*)
from results
where expected_unit is not null and result_value is not null
  and (result_value < min_plausible or result_value > max_plausible)
group by 1, 2