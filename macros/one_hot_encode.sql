{% macro one_hot_encode(column, source_relation='', source_condition='true', categories=[]) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("one_hot_encode: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if categories | length == 0 and source_relation == '' %}
        {{ exceptions.raise_compiler_error("one_hot_encode: Either 'source_relation' or 'categories' must be provided. Please specify at least one.") }}
    {% endif %}

    {{ return(adapter.dispatch('one_hot_encode', 'dbt_ml_inline_preprocessing')(column, source_relation, source_condition, categories)) }}
{% endmacro %}

{% macro default__one_hot_encode(column, source_relation, source_condition, categories)  %}

    {# Get the unique values that exist in the column to be encoded #}

    {% if categories != [] %}
        {% set category_values = categories %}
    {% else %}
        {% set category_values = dbt_utils.get_column_values(
            table=source_relation,
            column=column,
            order_by=column,
            where=
                column
                + ' is not null '
                + ' and '
                + source_condition
            ) or []
        %}
    {% endif %}

    {# Generate a CASE statement for each unique value #}

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
