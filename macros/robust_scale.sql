{% macro robust_scale(column, iqr=0.5, source_relation='') %}
    {{ return(adapter.dispatch('robust_scale', 'dbt_ml_inline_preprocessing')(column, iqr, source_relation)) }}
{% endmacro %}

{% macro default__robust_scale(column, iqr, source_relation)  %}

    {% if source_relation == '' %}
        {% do exceptions.warn('Source relation is required for percentile impute in Postgresql 9.4+') %}
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
    (
        {{ iqr_plus }} - {{ iqr_minus }}
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
    (
        percentile_cont(0.5 + {{ iqr }}/2) within group (order by {{ column }}) over ()
        -
        percentile_cont(0.5 - {{ iqr }}/2) within group (order by {{ column }}) over ()
    )

{% endmacro %}
