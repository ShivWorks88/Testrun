{#
  Every batch that landed in the model must have EXACTLY ONE receipt in ops.control_totals.
  Catches a missing receipt (0) and a double-written receipt (2+).
  Returns one row per bad batch. Zero rows = PASS.
#}
{% test control_totals_present(model, dataset) %}

{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

with batches as (
    select distinct business_date, _batch_id as batch_id
    from {{ model }}
    {% if bd is not none %}
    where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day) and date('{{ bd }}')
    {% endif %}
),

receipts as (
    select business_date, batch_id, count(*) as receipts
    from {{ source('ops', 'control_totals') }}
    where dataset = '{{ dataset }}'
    group by business_date, batch_id
)

select b.business_date, b.batch_id, coalesce(r.receipts, 0) as receipts
from batches b
left join receipts r using (business_date, batch_id)
where coalesce(r.receipts, 0) != 1

{% endtest %}