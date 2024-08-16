{% macro categorical_impute(column, measure='mode', source_relation='') %}
    {{ return(adapter.dispatch('categorical_impute', 'dbt_ml_inline_preprocessing')(column, measure, source_relation)) }}
{% endmacro %}

{% macro default__categorical_impute(column, measure, source_relation)  %}

    {% if measure == 'mode' %}
        coalesce({{ column }}, mode({{ column }}) OVER ())
    {% else %}
        {% do exceptions.raise('Unsupported measure. Please use "mode"') %}
    {% endif %}

{% endmacro %}

{% macro postgres__categorical_impute(column, measure, source_relation)  %}

    {% if source_relation == '' %}
        {% do exceptions.warn('Source relation is required for categorical impute in Postgresql 9.4+') %}
    {% endif %}

    {% set mode_query %}
        select mode() within group (order by {{ column }} ) as mode from {{ source_relation }}
    {% endset %}

    {% set result = dbt_utils.get_single_value(mode_query) %}

    {% if measure == 'mode' %}
        coalesce(input, '{{ result }}')
    {% else %}
        {% do exceptions.warn('Unsupported measure. Please use "mode"') %}
    {% endif %}

{% endmacro %}
