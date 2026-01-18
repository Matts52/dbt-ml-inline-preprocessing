{% macro rare_category_encode(column, cutoff=0.05) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("rare_category_encode: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if cutoff is none %}
        {{ exceptions.raise_compiler_error("rare_category_encode: 'cutoff' parameter cannot be None.") }}
    {% endif %}

    {% if cutoff < 0 or cutoff > 1 %}
        {{ exceptions.raise_compiler_error("rare_category_encode: 'cutoff' parameter must be between 0 and 1. Got: " ~ cutoff ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('rare_category_encode', 'dbt_ml_inline_preprocessing')(column, cutoff)) }}
{% endmacro %}

{% macro default__rare_category_encode(column, cutoff)  %}

    case
        when {{ column }} is null then null
        when (COUNT(*) over (partition by {{ column }}) / (COUNT(*) over ())::FLOAT) < {{ cutoff }} then 'Other'
        else {{ column }}
    end

{% endmacro %}
