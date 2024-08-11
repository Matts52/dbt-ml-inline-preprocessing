{% macro categorical_impute(column, measure='mode') %}
    {{ return(adapter.dispatch('categorical_impute', 'dbt_ml_inline_preprocessing')(column, measure)) }}
{% endmacro %}

{% macro default__categorical_impute(column, measure)  %}

    {% if measure == 'mode' %}
        coalesce({{ column }}, mode({{ column }}) OVER ())
    {% else %}
        {% do exceptions.raise('Unsupported measure. Please use "mode"') %}
    {% endif %}

{% endmacro %}
