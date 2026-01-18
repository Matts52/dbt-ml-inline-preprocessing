{%- macro bool_to_one_hot(columns, null_as_one=false, prefix='is_') -%}
    {# Validate inputs #}
    {% if columns is none or columns | length == 0 %}
        {{ exceptions.raise_compiler_error("bool_to_one_hot: 'columns' parameter is required and cannot be empty. Please provide a list of boolean column names.") }}
    {% endif %}

    {% if prefix is none %}
        {{ exceptions.raise_compiler_error("bool_to_one_hot: 'prefix' parameter cannot be None.") }}
    {% endif %}

    {{ adapter.dispatch('bool_to_one_hot', 'dbt_ml_inline_preprocessing')(columns, null_as_one, prefix) }}
{%- endmacro -%}

{%- macro default__bool_to_one_hot(columns, null_as_one, prefix) -%}
    {% for column in columns %}
        case
            when {{ column }} is null then {{ 1 if null_as_one else 0 }}
            when {{ column }} then 1
            else 0
        end as {{ prefix }}{{ column }}
        {%- if not loop.last %},{% endif -%}
    {% endfor %}
{%- endmacro -%}
