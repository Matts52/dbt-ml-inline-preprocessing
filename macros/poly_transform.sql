{% macro poly_transform(column, degree) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("poly_transform: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if degree is none %}
        {{ exceptions.raise_compiler_error("poly_transform: 'degree' parameter is required.") }}
    {% endif %}

    {{ return(adapter.dispatch('poly_transform', 'dbt_ml_inline_preprocessing')(column, degree)) }}
{% endmacro %}

{% macro default__poly_transform(column, degree)  %}

    POW({{ column }}, {{ degree }})

{% endmacro %}
