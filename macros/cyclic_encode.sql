{% macro cyclic_encode(column, period, func='sin') %}
    {{ return(adapter.dispatch('cyclic_encode', 'dbt_ml_inline_preprocessing')(column, period, func)) }}
{% endmacro %}

{% macro default__cyclic_encode(column, period, func='sin') %}
    {# Map period -> extract expression + cycle length #}
    {% if period == "hour_of_day" %}
        {% set expr = "extract(hour from " ~ column ~ ")" %}
        {% set max_val = 24 %}
    {% elif period == "day_of_week" %}
        {# Adjusting for function returning 0-6 range #}
        {% set expr = "(extract(dow from " ~ column ~ ") + 1)" %}
        {% set max_val = 7 %}
    {% elif period == "day_of_month" %}
        {% set expr = "extract(day from " ~ column ~ ")" %}
        {# Note, this does not adjust for different months having a different number of days #}
        {% set max_val = 31 %}
    {% elif period == "month_of_year" %}
        {% set expr = "extract(month from " ~ column ~ ")" %}
        {% set max_val = 12 %}
    {% elif period == "week_of_year" %}
        {% set expr = "extract(week from " ~ column ~ ")" %}
        {% set max_val = 53 %}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "Unsupported period type: " ~ period ~ ". Supported period types are: hour_of_day, day_of_week, day_of_month, month_of_year, week_of_year."
        ) }}
    {% endif %}

    {{ func }}(2 * pi() * ({{ expr }}::float / {{ max_val }}))

{% endmacro %}
