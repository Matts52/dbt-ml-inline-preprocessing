{% macro k_bins_discretize(column, k, strategy='quantile') %}
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
        (
            ((max({{ column }}) over ()) - (min({{ column }}) over ())) / {{ k - 1 }}::float
        )
        ) + 1
    {% else %}
        {% do exceptions.warn('Unsupported strategy. Please use "quantile" or "uniform"') %}
    {% endif %}

{% endmacro %}
