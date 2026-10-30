{#
  Reconciliation test: a staging model must match its source receipt in ops.control_totals.
  For each business date, take the LATEST receipt (= the batch currently in raw) and compare:
    rows landed  vs row_count
    distinct keys vs distinct_keys
    null amounts  vs null_amount_count   (only if amount_column is given)
    amount total  vs amount_total        (only if amount_column is given)
  Returns one row per mismatching business date. Zero rows = PASS.
#}
{% test reconcile_to_control_totals(model, dataset, key_column, amount_column=none) %}

{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

with receipt as (
    select business_date, batch_id, row_count, distinct_keys, null_amount_count, amount_total
    from {{ source('ops', 'control_totals') }}
    where dataset = '{{ dataset }}'
    {% if bd is not none %}
      and business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day) and date('{{ bd }}')
    {% endif %}
    qualify row_number() over (partition by business_date order by extracted_at desc) = 1
),

landed as (
    select
        business_date,
        count(*)                              as rows_landed,
        count(distinct {{ key_column }})      as distinct_landed
        {% if amount_column %}
        , countif({{ amount_column }} is null)  as null_amount_landed
        , sum({{ amount_column }})              as amount_landed
        {% endif %}
    from {{ model }}
    group by business_date
)

select
    r.business_date,
    r.batch_id,
    r.row_count,          l.rows_landed,
    r.distinct_keys,      l.distinct_landed
    {% if amount_column %}
    , r.null_amount_count, l.null_amount_landed
    , r.amount_total,      l.amount_landed
    {% endif %}
from receipt r
left join landed l using (business_date)
where l.business_date is null                       -- receipt exists but nothing landed
   or l.rows_landed     != r.row_count
   or l.distinct_landed != r.distinct_keys
   {% if amount_column %}
   or l.null_amount_landed != r.null_amount_count
   or coalesce(l.amount_landed, 0) != coalesce(r.amount_total, 0)
   {% endif %}

{% endtest %}