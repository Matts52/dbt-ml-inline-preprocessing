{% macro random_impute(column, source_relation, data_type, consider_distribution=true) %}
    {{ return(adapter.dispatch('random_impute', 'dbt_ml_inline_preprocessing')(column, source_relation, data_type, consider_distribution)) }}
{% endmacro %}

{% macro default__random_impute(column, source_relation, data_type, consider_distribution)  %}

    {% if consider_distribution == false %}
        {# Get unique value from the column #}
        {% set column_values = dbt_utils.get_column_values(
            table=source_relation,
            column=column,
            order_by=column,
            where=column + " is not null") or []    
        %}

        {# query to get count of distinct values #}
        {% set non_null_length_query %}
            select count(distinct {{ column }}) from {{ source_relation }} where {{ column }} is not null
        {% endset %}

        {% set non_null_length = dbt_utils.get_single_value(non_null_length_query) %}
    {% else %}
        {# Get all values from the column #}
        {% set non_null_values_query %}
            select {{ column }} as val from {{ source_relation }} where {{ column }} is not null
        {% endset %}

        {%- set column_values = dbt_utils.get_query_results_as_dict(non_null_values_query)['val'] -%}
        
        {# query to get count of values #}
        {% set non_null_length_query %}
            select count(*) from {{ source_relation }} where {{ column }} is not null
        {% endset %}

        {% set non_null_length = dbt_utils.get_single_value(non_null_length_query) %}    
    {% endif %}

    {# Generate SQL to replace null values with a random selection from column_values #}
    case
        {# grab a random number between 1 and 100 #}
        {% set random_num = range(0, 100) | random %}
        {% for column_value in column_values %}
            {# When the value is null and the remainder of the row number divided by the value count is equal to the loop index #}
            when {{ column }} is null
                and mod((row_number() over ()) + {{ random_num }}, {{ non_null_length }}) = {{ loop.index0 }}
                    then {% if data_type == 'numerical' %} {{ column_value }} {% else %} '{{ column_value }}' {% endif %}
        {% endfor %}
        else {{ column }}
    end

{% endmacro %}