{% macro max_absolute_scale(column) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("max_absolute_scale: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {{ return(adapter.dispatch('max_absolute_scale', 'dbt_ml_inline_preprocessing')(column)) }}
{% endmacro %}

{% macro default__max_absolute_scale(column)  %}

    ({{ column }}) / nullif(max(abs({{ column }})) over (), 0)::FLOAT

{% endmacro %}
