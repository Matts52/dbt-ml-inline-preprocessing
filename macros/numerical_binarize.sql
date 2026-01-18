{% macro numerical_binarize(column, cutoff, strategy='percentile', direction='>=', source_relation='') %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("numerical_binarize: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if cutoff is none %}
        {{ exceptions.raise_compiler_error("numerical_binarize: 'cutoff' parameter is required.") }}
    {% endif %}

    {% set valid_strategies = ['percentile', 'value'] %}
    {% if strategy not in valid_strategies %}
        {{ exceptions.raise_compiler_error("numerical_binarize: Invalid strategy '" ~ strategy ~ "'. Valid options are: " ~ valid_strategies | join(", ") ~ ".") }}
    {% endif %}

    {% set valid_directions = ['>=', '>', '<=', '<'] %}
    {% if direction not in valid_directions %}
        {{ exceptions.raise_compiler_error("numerical_binarize: Invalid direction '" ~ direction ~ "'. Valid options are: " ~ valid_directions | join(", ") ~ ".") }}
    {% endif %}

    {% if strategy == 'percentile' %}
        {% if cutoff < 0 or cutoff > 1 %}
            {{ exceptions.raise_compiler_error("numerical_binarize: When using 'percentile' strategy, cutoff must be between 0 and 1. Got: " ~ cutoff ~ ".") }}
        {% endif %}
    {% endif %}

    {{ return(adapter.dispatch('numerical_binarize', 'dbt_ml_inline_preprocessing')(column, cutoff, strategy, direction, source_relation)) }}
{% endmacro %}

{% macro default__numerical_binarize(column, cutoff, strategy, direction, source_relation)  %}

    {% if source_relation == '' and strategy == 'percentile' %}
        {{ exceptions.raise_compiler_error("numerical_binarize: 'source_relation' parameter is required when using 'percentile' strategy. Please provide a ref() or source().") }}
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
                percentile_cont({{ cutoff }}) within group (order by {{ column }}) over ()
            {% else %}
                {{ cutoff }}
            {% endif %}
            then 1
        else 0
    end

{% endmacro %}
