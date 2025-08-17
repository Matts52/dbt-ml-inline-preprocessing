{% macro cyclic_encode(column, period, func='sin') %}
    {{ return(adapter.dispatch('cyclic_encode', 'dbt_ml_inline_preprocessing')(column, period, func)) }}
{% endmacro %}

{% macro default__cyclic_encode(column, period, func='sin') %}
    {# Map period -> extract expression + cycle length #}
    {% if period == "hour_of_day" %}
        {% set expr = "extract(hour from " ~ column ~ ")" %}
        {% set max_val = 24 %}
    {% elif period == "day_of_week" %}
        {% set expr = "extract(dow from " ~ column ~ ")" %}
        {% set max_val = 7 %}
    {% elif period == "day_of_month" %}
        {% set expr = "extract(day from " ~ column ~ ")" %}
        {% set max_val = 31 %}
    {% elif period == "month_of_year" %}
        {% set expr = "extract(month from " ~ column ~ ")" %}
        {% set max_val = 12 %}
    {% elif period == "week_of_year" %}
        {% set expr = "extract(week from " ~ column ~ ")" %}
        {% set max_val = 52 %}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "Unsupported period type: " ~ period ~ ". Supported period types are: hour_of_day, day_of_week, day_of_month, month_of_year, week_of_year."
        ) }}
    {% endif %}

    {{ func }}(2 * pi() * ({{ expr }}::float / {{ max_val }}))

{% endmacro %}
