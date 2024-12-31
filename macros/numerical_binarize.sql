{% macro numerical_binarize(column, cutoff, strategy='percentile', direction='>=', source_relation='') %}
    {{ return(adapter.dispatch('numerical_binarize', 'dbt_ml_inline_preprocessing')(column, cutoff, strategy, direction, source_relation)) }}
{% endmacro %}

{% macro default__numerical_binarize(column, cutoff, strategy, direction, source_relation)  %}

    {% if source_relation == '' and strategy == 'percentile' %}
        {% do exceptions.warn('Source relation is required for percentile impute in Postgresql 9.4+') %}
    {% endif %}

    {% if strategy == 'percentile' %}
        {% set percentile_query %}
            select percentile_cont({{ cutoff }}) within group (order by {{ column }} ) from {{ source_relation }}
        {% endset %}

        {% set result = dbt_utils.get_single_value(percentile_query) %}
    {% endif %}

    case
        when {{ column }} {{ direction }}
            {% if strategy == 'percentile' %}
                {{ result }}
            {% else %}
                {{ cutoff }}
            {% endif %}
            then 1
        else 0
    end

{% endmacro %}

{% macro snowflake__numerical_binarize(column, cutoff, strategy, direction, source_relation)  %}

    case
        when {{ column }} {{ direction }} 
            {% if strategy == 'percentile' %}
                percentile_cont({{ percentile }}) within group (order by {{ column }}) over ()
            {% else %}
                {{ cutoff }}
            {% endif %}
            then 1
        else 0
    end

{% endmacro %}
