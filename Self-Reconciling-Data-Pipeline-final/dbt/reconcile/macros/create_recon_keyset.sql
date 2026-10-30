{#
  One-off: create the deterministic-encryption keyset for value-level reconciliation,
  wrapped by the KMS key, and store it in evidence_restricted.recon_keyset.
  Run as sa-valuerecon:  dbt run-operation create_recon_keyset --target valuerecon --profiles-dir .
  Safe to re-run: it only creates key_version v1 if it does not exist yet.
#}
{% macro create_recon_keyset() %}
  {% set kms = env_var('KMS_RECON_KEY') %}
  {% set table = target.project ~ '.evidence_restricted.recon_keyset' %}
  {% set sql %}
    create table if not exists `{{ table }}` (
      key_version    string    not null,
      wrapped_keyset bytes     not null,
      created_at     timestamp not null
    );
    insert into `{{ table }}` (key_version, wrapped_keyset, created_at)
    select 'v1', keys.new_wrapped_keyset('{{ kms }}', 'DETERMINISTIC_AEAD_AES_SIV_CMAC_256'), current_timestamp()
    from (select 1)
    where not exists (select 1 from `{{ table }}` where key_version = 'v1');
  {% endset %}
  {% do run_query(sql) %}
  {{ log("Recon keyset v1 is ready in " ~ table, info=True) }}
{% endmacro %}