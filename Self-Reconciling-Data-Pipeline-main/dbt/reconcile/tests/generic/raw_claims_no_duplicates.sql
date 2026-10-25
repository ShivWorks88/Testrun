{{ config(severity = 'warn', meta = {'policy_outcome': 'warning'}) }}

-- T4: a retried batch can load the same claim twice into raw. Staging dedupes it,
-- but raw duplicates are reported as a WARNING so they stay visible.
select business_date, claim_id, count(*) as copies
from {{ source('raw', 'claims') }}
{% if var('business_date') is not none %}
where business_date between date_sub(date('{{ var("business_date") }}'), interval {{ (var('lookback_days') | int) - 1 }} day)
                        and date('{{ var("business_date") }}')
{% endif %}
group by business_date, claim_id
having count(*) > 1