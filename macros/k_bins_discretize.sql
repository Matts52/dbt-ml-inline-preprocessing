{% macro k_bins_discretize(column, k, strategy='quantile') %}
    {{ return(adapter.dispatch('k_bins_discretize', 'dbt_ml_inline_preprocessing')(column, k)) }}
{% endmacro %}

{% macro default__k_bins_discretize(column, k)  %}

    {% if strategy='quantile' %}
        NTILE({{ k }}) over (order by {{ column }})
    {% elif strategy='uniform' %}
        /*
            floor of (value - min) / ((max - min) / k) + 1
        */
        (
            {{ column }} - (min({{ column }} over ()))
        )
        /
        (
            ((min({{ column }} over ()) - (min({{ column }} over ())))
            / 
            {{ k }}
        )
        )::int + 1 AS bin_number
    {% else %}
        {% do exceptions.warn('Unsupported strategy. Please use "quantile" or "uniform"') %}
    {% endif %}

{% endmacro %}