{% macro missing_encode(column) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("missing_encode: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {{ return(adapter.dispatch('missing_encode', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__missing_encode(column) %}

    case
        when {{ column }} is null then 1
        else 0
    end

{% endmacro %}
