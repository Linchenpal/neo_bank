{% macro record_source(model_ref=None) %}
  {% if model_ref is none %}
    '{{ this.schema }}.{{ this.name }}'
  {% else %}
    {% if target.name == 'test_vault' %}
      {% set schema = 'test_synthetic_staging' %}
    {% else %}
      {% set schema = this.schema %}
    {% endif %}
    '{{ schema }}.{{ model_ref }}'
  {% endif %}
{% endmacro %}
