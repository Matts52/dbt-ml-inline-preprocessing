{% macro log_transform(column, base=10, offset=0) %}
    {{ return(adapter.dispatch('log_transform', 'dbt_ml_inline_preprocessing')(column, base, offset)) }}
{% endmacro %}

{% macro default__log_transform(column, base, offset)  %}

    case
        when {{ column }} is null or {{ column }} + {{ offset }} <= 0 then null
        else log({{ base }}, {{ column }} + {{ offset }})
    end

{% endmacro %}
