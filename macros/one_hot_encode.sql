{% macro one_hot_encode(column, source_relation, condition) %}
    {{ return(adapter.dispatch('one_hot_encode', 'dbt_ml_inline_preprocessing')(column, source_relation, condition)) }}
{% endmacro %}

{% macro default__one_hot_encode(column, source_relation, condition)  %}

    {# Get the unique values that exist in the column to be encoded #}
    {% set category_values = dbt_utils.get_column_values(
        table=source_relation,
        column=column,
        order_by=column,
        where=
            column
            + " is not null"
            + " and "
            + condition
        ) or []    
    %}

    {% for category in category_values %}

        case
            when {{ column }} = '{{ category }}' then 1
            else 0
        end as is_{{ column }}__{{ dbt_utils.slugify(category | string) }},

    {% endfor %}

    case
        when {{ column }} is null then 1
        else 0
    end as is_{{ column }}__

{% endmacro %}
