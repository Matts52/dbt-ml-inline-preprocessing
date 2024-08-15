{% macro standardize(column, target_mean=0, target_stddev=1) %}
    {{ return(adapter.dispatch('standardize', 'dbt_ml_inline_preprocessing')(column, target_mean, target_stddev)) }}
{% endmacro %}

{% macro default__standardize(column, target_mean, target_stddev)  %}

    {#
        ((value - mu_sample)/sigma_sample) * sigma_target + mu_target
    #}
    (
        ({{ column }} - avg({{ column }}) over ())
        /
        (stddev({{ column }}) over ())::FLOAT
    )
    *
    {{ target_stddev }}
    +
    {{ target_mean }}

{% endmacro %}
