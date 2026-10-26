{#
  Daily claims by payer and encounter type. Net amount excludes reversed claims.
  Joins dim_encounters, so a held encounters day also holds this mart (Phase 3).
#}
{{ config(materialized = 'table', partition_by = {'field': 'business_date', 'data_type': 'date'},
          meta = {'source_system': 'BILLING', 'criticality': 'high', 'phi': false, 'owner': 'revenue-cycle'}) }}

select
    c.business_date,
    c.payer,
    coalesce(e.encounter_type, 'unknown')                          as encounter_type,
    concat(cast(c.business_date as string), '|', c.payer, '|',
           coalesce(e.encounter_type, 'unknown'))                  as claim_day_key,
    count(*)                                                       as claims,
    countif(c.is_reversed)                                         as reversed_claims,
    sum(c.claim_amount)                                            as gross_amount,
    sum(if(c.is_reversed, 0, c.claim_amount))                      as net_amount,
    countif(e.encounter_id is null)                                as claims_without_encounter
{%- if inject('schema_drift') %}
    -- failure suite (test only): the source renamed claim_amount, this model still uses the new name
    , sum(c.claim_amount_inr)                                      as injected_schema_drift
{%- endif %}
from {{ ref('stg_claims') }} c
left join (
    select * from {{ ref('dim_encounters') }}
    {%- if inject('join_fanout') %}
    -- failure suite (test only): a join key that is not unique doubles 25 encounters
    union all
    select * from {{ ref('dim_encounters') }}
    where encounter_id in (
        select encounter_id from {{ ref('stg_claims') }}
        where business_date = date('{{ var("business_date") }}') and encounter_id is not null
        order by encounter_id limit 25)
    {%- endif %}
) e using (encounter_id)
group by 1, 2, 3, 4
