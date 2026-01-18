{% macro label_encode(column) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("label_encode: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {{ return(adapter.dispatch('label_encode', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__label_encode(column)  %}

    dense_rank() over (order by {{ column }}) - 1

{% endmacro %}