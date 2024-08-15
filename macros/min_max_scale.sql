{% macro min_max_scale(column, new_min=0, new_max=1) %}
    {{ return(adapter.dispatch('min_max_scale', 'dbt_ml_inline_preprocessing')(column, new_min, new_max)) }}
{% endmacro %}

{% macro default__min_max_scale(column, new_min, new_max)  %}

    {#
        ((value - min of column) / (max of column - min of column)) * (new maximum - new minimum) + (new minimum)
    #}
    (
        (({{ column }}) - (min({{ column }}) over ()))
        /
        ((max({{ column }}) over ()) - (min({{ column }}) over ()))::FLOAT
    )
    *
    ({{ new_max }} - {{ new_min }})
    +
    {{ new_min }}

{% endmacro %}
