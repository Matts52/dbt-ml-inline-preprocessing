{% macro k_bins_discretize(column, k, strategy='quantile') %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("k_bins_discretize: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if k is none %}
        {{ exceptions.raise_compiler_error("k_bins_discretize: 'k' parameter is required.") }}
    {% endif %}

    {% if k < 2 %}
        {{ exceptions.raise_compiler_error("k_bins_discretize: 'k' parameter must be at least 2. Got: " ~ k ~ ".") }}
    {% endif %}

    {% set valid_strategies = ['quantile', 'uniform'] %}
    {% if strategy not in valid_strategies %}
        {{ exceptions.raise_compiler_error("k_bins_discretize: Invalid strategy '" ~ strategy ~ "'. Valid options are: " ~ valid_strategies | join(", ") ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('k_bins_discretize', 'dbt_ml_inline_preprocessing')(column, k, strategy)) }}
{% endmacro %}

{% macro default__k_bins_discretize(column, k, strategy)  %}

    {% if strategy == 'quantile' %}
        NTILE({{ k }}) over (order by {{ column }})
    {% elif strategy == 'uniform' %}
        {#
            (floor of (value - min)) / ((max - min) / k) + 1
        #}
        floor(
        (
            {{ column }} - (min({{ column }}) over ())
        )
        /
        nullif(
            ((max({{ column }}) over ()) - (min({{ column }}) over ())) / {{ k - 1 }}::float,
            0
        )
        ) + 1
    {% endif %}

{% endmacro %}
