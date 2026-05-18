{% macro log_transform(column, offset=0) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("log_transform: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if offset is none %}
        {{ exceptions.raise_compiler_error("log_transform: 'offset' parameter cannot be None.") }}
    {% endif %}

    {{ return(dbt_ml_inline_preprocessing.power_transform(column, lambda=0, offset=offset)) }}
{% endmacro %}
