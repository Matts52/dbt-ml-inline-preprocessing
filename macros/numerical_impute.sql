{% macro numerical_impute(column, measure='mean', percentile=0.5, source_relation='') %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("numerical_impute: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% set valid_measures = ['mean', 'median', 'percentile'] %}
    {% if measure not in valid_measures %}
        {{ exceptions.raise_compiler_error("numerical_impute: Invalid measure '" ~ measure ~ "'. Valid options are: " ~ valid_measures | join(", ") ~ ".") }}
    {% endif %}

    {% if measure == 'percentile' or measure == 'median' %}
        {% if percentile < 0 or percentile > 1 %}
            {{ exceptions.raise_compiler_error("numerical_impute: 'percentile' parameter must be between 0 and 1. Got: " ~ percentile ~ ".") }}
        {% endif %}
    {% endif %}

    {{ return(adapter.dispatch('numerical_impute', 'dbt_ml_inline_preprocessing')(column, measure, percentile, source_relation)) }}
{% endmacro %}

{% macro default__numerical_impute(column, measure, percentile, source_relation)  %}

    {% if source_relation == '' and measure != 'mean' %}
        {{ exceptions.raise_compiler_error("numerical_impute: 'source_relation' parameter is required when using '" ~ measure ~ "' measure. Please provide a ref() or source().") }}
    {% endif %}

    {% if measure != 'mean' %}
        {% set percentile_query %}
            select percentile_cont({{ percentile }}) within group (order by {{ column }} ) from {{ source_relation }}
        {% endset %}

        {% set result = dbt_utils.get_single_value(percentile_query) %}
    {% endif %}

    {% if measure == 'mean' %}
        coalesce({{ column }}, avg({{ column }}) OVER ())
    {% elif measure == 'median' %}
        coalesce({{ column }}, {{ result }})
    {% elif measure == 'percentile' %}
        coalesce({{ column }}, {{ result }})
    {% endif %}

{% endmacro %}

{% macro snowflake__numerical_impute(column, measure, percentile, source_relation)  %}

    {% if measure == 'mean' %}
        coalesce({{ column }}, avg({{ column }}) over ())
    {% elif measure == 'median' %}
        coalesce({{ column }}, percentile_cont(0.5) within group (order by {{ column }}) over ())
    {% elif measure == 'percentile' %}
        coalesce({{ column }}, percentile_cont({{ percentile }}) within group (order by {{ column }}) over ())
    {% endif %}

{% endmacro %}
