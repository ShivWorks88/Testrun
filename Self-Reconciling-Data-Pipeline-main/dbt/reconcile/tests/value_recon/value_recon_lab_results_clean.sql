-- T6: raw and staging must hold identical critical lab values. Any row = blocking failure.
{{ config(enabled = (target.name == 'valuerecon'), tags = ['valuerecon'],
          meta = {'policy_outcome': 'blocking'}) }}
{% set bd = var('business_date') %}
{% set lookback = var('lookback_days') | int %}

select business_date, field_name, issue, count(*) as records
from {{ ref('value_recon_lab_results') }}
{% if bd is not none %}
where business_date between date_sub(date('{{ bd }}'), interval {{ lookback - 1 }} day) and date('{{ bd }}')
{% endif %}
group by 1, 2, 3