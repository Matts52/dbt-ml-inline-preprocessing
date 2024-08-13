{% macro rare_category_encode(column, cutoff=0.05) %}
    {{ return(adapter.dispatch('rare_category_encode', 'dbt_ml_inline_preprocessing')(column, cutoff)) }}
{% endmacro %}

{% macro default__rare_category_encode(column, cutoff)  %}

    case
        when {{ column }} is null then null
        when (COUNT(*) over (partition by {{ column }}) / (COUNT(*) over ())::FLOAT) < {{ cutoff }} then 'Other'
        else {{ column }}
    end

{% endmacro %}
