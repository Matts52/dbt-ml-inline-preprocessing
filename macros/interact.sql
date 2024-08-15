{% macro interact(column_one, column_two, interaction='multiplicative') %}
    {{ return(adapter.dispatch('interact', 'dbt_ml_inline_preprocessing')(column_one, column_two, interaction)) }}
{% endmacro %}

{% macro default__interact(column_one, column_two, interaction)  %}

    {% if interaction == 'multiplicative' %}
        ({{ column_one }} * {{ column_two }})
    {% elif interaction == 'additive' %}
        ({{ column_one }} + {{ column_two }})
    {% elif interaction == 'exponential' %}
        POW({{ column_one }}, {{ column_two }})
    {% else %}
        {% do exceptions.warn('Unsupported interaction. Please use "multaplicative, "additive", or "exponential"') %}
    {% endif %}

{% endmacro %}