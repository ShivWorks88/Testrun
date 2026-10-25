-- The report table must carry exactly the claims and ₹ of staging, day by day.
-- A join or filter bug in the mart would show up here.
with mart as (
    select business_date, sum(claims) as claims, sum(gross_amount) as gross
    from {{ ref('fct_daily_claims') }}
    group by 1
),

stg as (
    select business_date, count(*) as claims, sum(claim_amount) as gross
    from {{ ref('stg_claims') }}
    group by 1
)

select
    s.business_date,
    s.claims as stg_claims, m.claims as mart_claims,
    s.gross  as stg_gross,  m.gross  as mart_gross
from stg s
left join mart m using (business_date)
where m.business_date is null
   or m.claims != s.claims
   or m.gross  != s.gross