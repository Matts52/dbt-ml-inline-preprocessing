{%- macro zscore_outlier(column, threshold=3) -%}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("zscore_outlier: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if threshold is none %}
        {{ exceptions.raise_compiler_error("zscore_outlier: 'threshold' parameter cannot be None.") }}
    {% endif %}

    {% if threshold <= 0 %}
        {{ exceptions.raise_compiler_error("zscore_outlier: 'threshold' parameter must be positive. Got: " ~ threshold ~ ".") }}
    {% endif %}

    {{ adapter.dispatch('zscore_outlier', 'dbt_ml_inline_preprocessing')(column, threshold) }}
{%- endmacro -%}

{%- macro default__zscore_outlier(column, threshold) -%}
    case
        when abs(({{ column }} - avg({{ column }}) over ()) / nullif(stddev({{ column }}) over (), 0)) > {{ threshold }} then 1
        else 0
    end
{%- endmacro -%}
