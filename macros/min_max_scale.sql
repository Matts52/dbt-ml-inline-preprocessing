{% macro min_max_scale(column, new_min=0, new_max=1) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("min_max_scale: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if new_min is none or new_max is none %}
        {{ exceptions.raise_compiler_error("min_max_scale: 'new_min' and 'new_max' parameters cannot be None.") }}
    {% endif %}

    {% if new_min >= new_max %}
        {{ exceptions.raise_compiler_error("min_max_scale: 'new_min' must be less than 'new_max'. Got new_min=" ~ new_min ~ ", new_max=" ~ new_max ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('min_max_scale', 'dbt_ml_inline_preprocessing')(column, new_min, new_max)) }}
{% endmacro %}

{% macro default__min_max_scale(column, new_min, new_max)  %}

    {#
        ((value - min of column) / (max of column - min of column)) * (new maximum - new minimum) + (new minimum)
    #}
    (
        (({{ column }}) - (min({{ column }}) over ()))
        /
        nullif(((max({{ column }}) over ()) - (min({{ column }}) over ())), 0)::FLOAT
    )
    *
    ({{ new_max }} - {{ new_min }})
    +
    {{ new_min }}

{% endmacro %}
