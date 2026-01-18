{% macro log_transform(column, base=10, offset=0) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("log_transform: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if base is none %}
        {{ exceptions.raise_compiler_error("log_transform: 'base' parameter cannot be None.") }}
    {% endif %}

    {% if base <= 0 or base == 1 %}
        {{ exceptions.raise_compiler_error("log_transform: 'base' parameter must be positive and not equal to 1. Got: " ~ base ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('log_transform', 'dbt_ml_inline_preprocessing')(column, base, offset)) }}
{% endmacro %}

{% macro default__log_transform(column, base, offset)  %}

    case
        when {{ column }} is null or {{ column }} + {{ offset }} <= 0 then null
        else log({{ base }}, {{ column }} + {{ offset }})
    end

{% endmacro %}
