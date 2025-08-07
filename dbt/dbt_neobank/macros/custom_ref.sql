{% macro custom_ref(model_name) %}
    {% if target.name == 'test_vault' %}
        {{ return(adapter.dispatch('custom_ref', 'dbt_neobank')(model_name)) }}
    {% else %}
        {{ return(ref(model_name)) }}
    {% endif %}
{% endmacro %}

{% macro dbt_neobank__custom_ref(model_name) %}
    {% set test_dataset = 'test_synthetic_staging' %}
    {{ return(test_dataset ~ '.' ~ model_name) }}
{% endmacro %}

{% macro default__custom_ref(model_name) %}
    {{ ref(model_name) }}
{% endmacro %}
