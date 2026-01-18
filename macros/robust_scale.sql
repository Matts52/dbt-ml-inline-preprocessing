{% macro robust_scale(column, iqr=0.5, source_relation='') %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("robust_scale: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if iqr is none %}
        {{ exceptions.raise_compiler_error("robust_scale: 'iqr' parameter cannot be None.") }}
    {% endif %}

    {% if iqr <= 0 or iqr > 1 %}
        {{ exceptions.raise_compiler_error("robust_scale: 'iqr' parameter must be between 0 (exclusive) and 1 (inclusive). Got: " ~ iqr ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('robust_scale', 'dbt_ml_inline_preprocessing')(column, iqr, source_relation)) }}
{% endmacro %}

{% macro default__robust_scale(column, iqr, source_relation)  %}

    {% if source_relation == '' %}
        {{ exceptions.raise_compiler_error("robust_scale: 'source_relation' parameter is required for PostgreSQL/DuckDB. Please provide a ref() or source().") }}
    {% endif %}

    {% set median_query %}
        select percentile_cont(0.5) within group (order by {{ column }} ) from {{ source_relation }}
    {% endset %}
    {% set iqr_minus_query %}
        select percentile_cont(0.5 - {{ iqr }}/2) within group (order by {{ column }} ) from {{ source_relation }}
    {% endset %}
    {% set iqr_plus_query %}
        select percentile_cont(0.5 + {{ iqr }}/2) within group (order by {{ column }} ) from {{ source_relation }}
    {% endset %}

    {% set median = dbt_utils.get_single_value(median_query) %}
    {% set iqr_minus = dbt_utils.get_single_value(iqr_minus_query) %}
    {% set iqr_plus = dbt_utils.get_single_value(iqr_plus_query) %}

    (
        {{ column }} - {{ median }}
    )
    /
    nullif(
        {{ iqr_plus }} - {{ iqr_minus }},
        0
    )

{% endmacro %}

{% macro snowflake__robust_scale(column, iqr, source_relation)  %}

    {#
        (value - median) / (IQR_plus - IQR_minus)
    #}

    (
        {{ column }} - (percentile_cont(0.5) within group (order by {{ column }}) over ())
    )
    /
    nullif(
        percentile_cont(0.5 + {{ iqr }}/2) within group (order by {{ column }}) over ()
        -
        percentile_cont(0.5 - {{ iqr }}/2) within group (order by {{ column }}) over (),
        0
    )

{% endmacro %}
