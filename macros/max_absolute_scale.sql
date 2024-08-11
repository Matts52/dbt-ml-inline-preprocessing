{% macro max_absolute_scale(column) %}
    {{ return(adapter.dispatch('max_absolute_scale', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__max_absolute_scale(column)  %}

    ({{ column }}) / (max(abs({{ column }})) over ())::FLOAT

{% endmacro %}
