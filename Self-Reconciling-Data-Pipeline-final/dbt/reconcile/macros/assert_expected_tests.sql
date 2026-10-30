{#
  Expected-test-count assertion (runbook T7): a skipped test is a failure.
  Runs at the end of every dbt run/build. For every model built in this run,
  every test attached to it must also have run. Otherwise the run fails.
  Escape hatch for local experiments only: --vars '{enforce_test_count: false}'
#}
{% macro assert_expected_tests(results) %}
  {% if execute and var('enforce_test_count', true) %}
    {% set built_models = [] %}
    {% set ran_tests = [] %}
    {% for r in results %}
      {% if r.node.resource_type == 'model' %}{% do built_models.append(r.node.unique_id) %}{% endif %}
      {% if r.node.resource_type == 'test' %}{% do ran_tests.append(r.node.unique_id) %}{% endif %}
    {% endfor %}

    {% set skipped = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'test' and node.unique_id not in ran_tests %}
        {% for parent in node.depends_on.nodes %}
          {% if parent in built_models and node.name not in skipped %}
            {% do skipped.append(node.name) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}

    {% if skipped | length > 0 %}
      {{ exceptions.raise_compiler_error(
           "Expected-test-count check FAILED: " ~ (skipped | length) ~
           " test(s) on built models did not run: " ~ (skipped | join(', '))) }}
    {% else %}
      {{ log("Expected-test-count check passed: every test on the " ~ (built_models | length) ~ " built model(s) ran.", info=True) }}
    {% endif %}
  {% endif %}
  select 1
{% endmacro %}