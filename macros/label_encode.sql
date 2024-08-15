{% macro label_encode(column) %}
    {{ return(adapter.dispatch('label_encode', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__label_encode(column)  %}

    dense_rank() over (order by {{ column }}) - 1

{% endmacro %}