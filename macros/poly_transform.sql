{% macro poly_transform(column, exponent) %}
    {{ return(adapter.dispatch('poly_transform', 'dbt_ml_inline_preprocessing')(column, exponent)) }}
{% endmacro %}

{% macro default__poly_transform(column, exponent)  %}

    POW({{ column }}, {{ exponent }})

{% endmacro %}
