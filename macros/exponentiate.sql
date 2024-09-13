{% macro exponentiate(column, base=2.71) %}
    {{ return(adapter.dispatch('exponentiate', 'dbt_ml_inline_preprocessing')(column, base)) }}
{% endmacro %}

{% macro default__exponentiate(column, base)  %}

    {% if base == 2.71 %}
        EXP({{ column }})
    {% else %}
        POW({{ base }}, {{ column }})
    {% endif %}

{% endmacro %}
