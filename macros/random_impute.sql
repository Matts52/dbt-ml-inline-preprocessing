{% macro random_impute(column, source_relation) %}
    {{ return(adapter.dispatch('random_impute', 'dbt_ml_inline_preprocessing')(column, source_relation)) }}
{% endmacro %}

{% macro postgres__random_impute(column, source_relation)  %}

    {% set column_values = dbt_utils.get_column_values(
        table=source_relation,
        column=column,
        order_by=column,
        where=column + " is not null") or []    
    %}

    {% set non_null_length_query %}
        select count(distinct {{ column }}) from {{ source_relation }} where {{ column }} is not null
    {% endset %}

    {% set non_null_length = dbt_utils.get_single_value(non_null_length_query) %}

    /* Generate SQL to replace null values with a random selection from column_values */
    /* TODO: Make this more efficient with dbt_utils.get_column_values call first and randomly pick from list */
    case
        {% for column_value in column_values %}
        when {{ column }} is null and mod(row_number() over (), {{ non_null_length }}) = {{ loop.index0 }}
            then '{{ column_value }}'
        {% endfor %}
        else {{ column }} 
    end

{% endmacro %}