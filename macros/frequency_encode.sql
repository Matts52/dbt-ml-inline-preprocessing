{% macro frequency_encode(column) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("frequency_encode: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {{ return(adapter.dispatch('frequency_encode', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__frequency_encode(column) %}

    case
        when {{ column }} is null then null
        else COUNT(*) over (partition by {{ column }}) / COUNT(*) over ()::FLOAT
    end

{% endmacro %}

{% macro snowflake__frequency_encode(column) %}

    case
        when {{ column }} is null then null
        else COUNT(*) over (partition by {{ column }}) / CAST(COUNT(*) over () AS FLOAT)
    end

{% endmacro %}
