{% macro random_impute(column, source_relation, consider_distribution=true) %}
    {{ return(adapter.dispatch('random_impute', 'dbt_ml_inline_preprocessing')(column, source_relation, consider_distribution)) }}
{% endmacro %}

{% macro postgres__random_impute(column, source_relation, consider_distribution)  %}

    {% if consider_distribution == false %}
        /* Get unique value from the column */
        {% set column_values = dbt_utils.get_column_values(
            table=source_relation,
            column=column,
            order_by=column,
            where=column + " is not null") or []    
        %}
    {% else %}
        /* Get all values from the column */
        {% set non_null_values_query %}
            select {{ column }} as val from {{ source_relation }} where {{ column }} is not null
        {% endset %}

        {%- set column_values = dbt_utils.get_query_results_as_dict(non_null_values_query)['val'] -%}
    {% endif %}

    /* query to get count of distinct values */
    {% set non_null_length_query %}
        select count(distinct {{ column }}) from {{ source_relation }} where {{ column }} is not null
    {% endset %}

    {% set non_null_length = dbt_utils.get_single_value(non_null_length_query) %}

    /* Generate SQL to replace null values with a random selection from column_values */
    case
        {% for column_value in column_values %}
            /* When the value is null and the remainder of the row number divided by sitinct value count is equal to the loop index */
            when {{ column }} is null and mod(row_number() over (), {{ non_null_length }}) = {{ loop.index0 }}
                then '{{ column_value }}'
        {% endfor %}
        else {{ column }}
    end

{% endmacro %}