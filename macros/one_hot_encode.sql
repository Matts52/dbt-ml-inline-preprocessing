{% macro one_hot_encode(source_relation, source_column) %}
    {{ return(adapter.dispatch('one_hot_encode', 'dbt_ml_inline_preprocessing')(source_relation, source_column)) }}
{% endmacro %}

{% macro default__one_hot_encode(source_relation, source_column)  %}

    /* Get the unique values that exist in the column to be encoded */
    {% set category_values = dbt_utils.get_column_values(
        table=source_relation,
        column=source_column,
        order_by=source_column,
        where=source_column + " is not null") or []    
    %}

    {% for category in category_values %}

        case
            when {{ source_column }} = '{{ category }}' then 1
            else 0
        end as is_{{ source_column }}__{{ dbt_utils.slugify(category) }},

    {% endfor %}

    case
        when {{ source_column }} = '' or {{ source_column }} is null then 1
        else 0
    end as is_{{ source_column }}__

{% endmacro %}
