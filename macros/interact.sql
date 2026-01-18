{% macro interact(column_one, column_two, interaction='multiplicative') %}
    {# Validate inputs #}
    {% if column_one is none or column_one == '' %}
        {{ exceptions.raise_compiler_error("interact: 'column_one' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if column_two is none or column_two == '' %}
        {{ exceptions.raise_compiler_error("interact: 'column_two' parameter is required and cannot be empty.") }}
    {% endif %}

    {% set valid_interactions = ['multiplicative', 'additive', 'exponential'] %}
    {% if interaction not in valid_interactions %}
        {{ exceptions.raise_compiler_error("interact: Invalid interaction '" ~ interaction ~ "'. Valid options are: " ~ valid_interactions | join(", ") ~ ".") }}
    {% endif %}

    {{ return(adapter.dispatch('interact', 'dbt_ml_inline_preprocessing')(column_one, column_two, interaction)) }}
{% endmacro %}

{% macro default__interact(column_one, column_two, interaction)  %}

    {% if interaction == 'multiplicative' %}
        ({{ column_one }} * {{ column_two }})
    {% elif interaction == 'additive' %}
        ({{ column_one }} + {{ column_two }})
    {% elif interaction == 'exponential' %}
        POW({{ column_one }}, {{ column_two }})
    {% endif %}

{% endmacro %}