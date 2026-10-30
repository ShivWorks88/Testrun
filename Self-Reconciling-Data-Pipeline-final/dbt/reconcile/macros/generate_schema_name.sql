{#
  Models build in the target dataset (audit) unless they set a custom schema, which is used
  exactly as written: quarantine, evidence_restricted. No "audit_quarantine" prefixes, so the
  dataset-level IAM in infra/iam.tf applies as designed.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
