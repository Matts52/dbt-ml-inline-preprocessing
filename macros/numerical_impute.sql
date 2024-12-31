{% macro numerical_impute(column, measure='mean', percentile=0.5, source_relation='') %}
    {{ return(adapter.dispatch('numerical_impute', 'dbt_ml_inline_preprocessing')(column, measure, percentile, source_relation)) }}
{% endmacro %}

{% macro default__numerical_impute(column, measure, percentile, source_relation)  %}

    {% if source_relation == '' and measure != 'mean' %}
        {% do exceptions.warn('Source relation is required for percentile impute in Postgresql 9.4+') %}
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
    {% else %}
        {% do exceptions.warn('Unsupported measure. Please use "mean", "median", or "percentile"') %}
    {% endif %}

{% endmacro %}

{% macro snowflake__numerical_impute(column, measure, percentile, source_relation)  %}

    {% if measure == 'mean' %}
        coalesce({{ column }}, avg({{ column }}) over ())
    {% elif measure == 'median' %}
        coalesce({{ column }}, percentile_cont(0.5) within group (order by {{ column }}) over ())
    {% elif measure == 'percentile' %}
        coalesce({{ column }}, percentile_cont({{ percentile }}) within group (order by {{ column }}) over ())
    {% else %}
        {% do exceptions.warn('Unsupported measure. Please use "mean", "median", or "percentile"') %}
    {% endif %}

{% endmacro %}
