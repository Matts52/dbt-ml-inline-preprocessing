{% macro power_transform(column, lambda=0, offset=0) %}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("power_transform: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if lambda is none %}
        {{ exceptions.raise_compiler_error("power_transform: 'lambda' parameter cannot be None.") }}
    {% endif %}

    {% if offset is none %}
        {{ exceptions.raise_compiler_error("power_transform: 'offset' parameter cannot be None.") }}
    {% endif %}

    {{ return(adapter.dispatch('power_transform', 'dbt_ml_inline_preprocessing')(column, lambda, offset)) }}
{% endmacro %}

{% macro default__power_transform(column, lambda, offset) %}

    {#
        Yeo-Johnson transform applied to (column + offset):
          y >= 0, lambda != 0 : ((y + 1)^lambda - 1) / lambda
          y >= 0, lambda == 0 : ln(y + 1)
          y <  0, lambda != 2 : -((-y + 1)^(2 - lambda) - 1) / (2 - lambda)
          y <  0, lambda == 2 : -ln(-y + 1)
    #}
    case
        when {{ column }} is null then null
        when ({{ column }} + {{ offset }}) >= 0 and {{ lambda }} != 0
            then (power(({{ column }} + {{ offset }}) + 1, {{ lambda }}) - 1) / {{ lambda }}
        when ({{ column }} + {{ offset }}) >= 0 and {{ lambda }} = 0
            then ln(({{ column }} + {{ offset }}) + 1)
        when ({{ column }} + {{ offset }}) < 0 and {{ lambda }} != 2
            then -(power(-({{ column }} + {{ offset }}) + 1, 2 - {{ lambda }}) - 1) / (2 - {{ lambda }})
        when ({{ column }} + {{ offset }}) < 0 and {{ lambda }} = 2
            then -ln(-({{ column }} + {{ offset }}) + 1)
    end

{% endmacro %}
