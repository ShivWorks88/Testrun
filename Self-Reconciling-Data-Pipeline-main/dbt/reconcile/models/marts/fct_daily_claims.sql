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
from {{ ref('stg_claims') }} c
left join {{ ref('dim_encounters') }} e using (encounter_id)
group by 1, 2, 3, 4