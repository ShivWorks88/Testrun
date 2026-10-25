-- Exit gate: evidence_restricted must never hold clinical values or fingerprints —
-- only record keys, field names, issue, key version and time.
-- depends_on: {{ ref('value_recon_lab_results') }}
{{ config(enabled = (target.name == 'valuerecon'), tags = ['valuerecon'],
          meta = {'policy_outcome': 'blocking'}) }}

select table_name, column_name, data_type
from `{{ target.project }}.evidence_restricted.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'value_recon_lab_results'
  and column_name not in ('business_date', 'record_key', 'field_name', 'issue', 'key_version', 'checked_at')