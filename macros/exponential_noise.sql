{%- macro exponential_noise(column, rate=1.0, scale=1.0) -%}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("exponential_noise: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if rate is none %}
        {{ exceptions.raise_compiler_error("exponential_noise: 'rate' parameter cannot be None.") }}
    {% endif %}

    {% if rate <= 0 %}
        {{ exceptions.raise_compiler_error("exponential_noise: 'rate' parameter must be positive. Got: " ~ rate ~ ".") }}
    {% endif %}

    {% if scale is none %}
        {{ exceptions.raise_compiler_error("exponential_noise: 'scale' parameter cannot be None.") }}
    {% endif %}

    {% if scale < 0 %}
        {{ exceptions.raise_compiler_error("exponential_noise: 'scale' parameter must be non-negative. Got: " ~ scale ~ ".") }}
    {% endif %}

    {{ adapter.dispatch('exponential_noise', 'dbt_ml_inline_preprocessing')(column, rate, scale) }}
{%- endmacro -%}

{%- macro default__exponential_noise(column, rate, scale) -%}
    ({{ column }} + (-ln(1 - random()) / {{ rate }}) * {{ scale }})
{%- endmacro -%}
