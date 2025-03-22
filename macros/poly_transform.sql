{% macro poly_transform(column, degree) %}
    {{ return(adapter.dispatch('poly_transform', 'dbt_ml_inline_preprocessing')(column, degree)) }}
{% endmacro %}

{% macro default__poly_transform(column, degree)  %}

    POW({{ column }}, {{ degree }})

{% endmacro %}
