{% macro categorical_impute(column, measure='mode', source_relation='') %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("categorical_impute: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if measure != 'mode' %}
        {{ exceptions.raise_compiler_error("categorical_impute: Unsupported measure '" ~ measure ~ "'. Only 'mode' is currently supported.") }}
    {% endif %}

    {{ return(adapter.dispatch('categorical_impute', 'dbt_ml_inline_preprocessing')(column, measure, source_relation)) }}
{% endmacro %}

{% macro default__categorical_impute(column, measure, source_relation)  %}

    coalesce({{ column }}, mode({{ column }}) OVER ())

{% endmacro %}

{% macro postgres__categorical_impute(column, measure, source_relation)  %}

    {% if source_relation == '' %}
        {{ exceptions.raise_compiler_error("categorical_impute: 'source_relation' parameter is required for PostgreSQL. Please provide a ref() or source().") }}
    {% endif %}

    {% set mode_query %}
        select mode() within group (order by {{ column }} ) as mode from {{ source_relation }}
    {% endset %}

    {% set result = dbt_utils.get_single_value(mode_query) %}

    coalesce({{ column }}, '{{ result }}')

{% endmacro %}
