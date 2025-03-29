{%- macro bool_to_one_hot(columns, null_as_one=false, prefix='is_') -%}
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