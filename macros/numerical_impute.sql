{% macro numerical_impute(column, measure='mean', percentile=0.5) %}
    {{ return(adapter.dispatch('numerical_impute', 'dbt_ml_inline_preprocessing')(column, measure, percentile)) }}
{% endmacro %}

{% macro default__numerical_impute(column, measure, percentile)  %}

    {% if measure == 'mean' %}
        coalesce({{ column }}, avg({{ column }}) OVER ())
    {% elif measure == 'median' %}
        coalesce({{ column }}, percentile_cont(0.5) within group (order by {{ column }}) over ())
    {% elif measure == 'median' %}
        coalesce({{ column }}, percentile_cont({{ percentile }}) within group (order by {{ column }}) over ())
    {% else %}
        {% do exceptions.raise('Unsupported measure. Please use "mean", "median", or "percentile"') %}
    {% endif %}

{% endmacro %}
