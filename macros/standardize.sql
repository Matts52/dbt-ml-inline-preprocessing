{% macro standardize(column, target_mean=0, target_stddev=1) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("standardize: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if target_stddev is none %}
        {{ exceptions.raise_compiler_error("standardize: 'target_stddev' parameter cannot be None.") }}
    {% endif %}

    {% if target_stddev < 0 %}
        {{ exceptions.raise_compiler_error("standardize: 'target_stddev' parameter must be non-negative. Got: " ~ target_stddev ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('standardize', 'dbt_ml_inline_preprocessing')(column, target_mean, target_stddev)) }}
{% endmacro %}

{% macro default__standardize(column, target_mean, target_stddev)  %}

    {#
        ((value - mu_sample)/sigma_sample) * sigma_target + mu_target
    #}
    (
        ({{ column }} - avg({{ column }}) over ())
        /
        nullif(stddev({{ column }}) over (), 0)::FLOAT
    )
    *
    {{ target_stddev }}
    +
    {{ target_mean }}

{% endmacro %}
