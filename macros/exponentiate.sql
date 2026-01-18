{% macro exponentiate(column, base=2.71) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("exponentiate: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if base is none %}
        {{ exceptions.raise_compiler_error("exponentiate: 'base' parameter cannot be None.") }}
    {% endif %}

    {% if base <= 0 %}
        {{ exceptions.raise_compiler_error("exponentiate: 'base' parameter must be positive. Got: " ~ base ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('exponentiate', 'dbt_ml_inline_preprocessing')(column, base)) }}
{% endmacro %}

{% macro default__exponentiate(column, base)  %}

    {% if base == 2.71 %}
        EXP({{ column }})
    {% else %}
        POW({{ base }}, {{ column }})
    {% endif %}

{% endmacro %}
