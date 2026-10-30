{#
  Failure-suite switches (test project only). True only when ALL hold:
    - the switch is listed in --vars '{inject: [name, ...]}' (dbt_build conf "dbt_inject"),
    - a business_date is set,
    - the environment says ALLOW_INJECTIONS=true (set only in the test project).
  A normal nightly or engine-triggered build passes no switches, so it can never inject.
#}
{% macro inject(name) -%}
  {{ return(var('business_date') is not none
            and name in (var('inject', []) or [])
            and env_var('ALLOW_INJECTIONS', 'false') | lower == 'true') }}
{%- endmacro %}
