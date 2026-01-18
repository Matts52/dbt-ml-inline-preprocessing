{%- macro uniform_noise(column, scale=1.0) -%}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("uniform_noise: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if scale is none %}
        {{ exceptions.raise_compiler_error("uniform_noise: 'scale' parameter cannot be None.") }}
    {% endif %}

    {% if scale < 0 %}
        {{ exceptions.raise_compiler_error("uniform_noise: 'scale' parameter must be non-negative. Got: " ~ scale ~ ".") }}
    {% endif %}

    {{ adapter.dispatch('uniform_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__uniform_noise(column, scale) -%}
    {{ column }} + (random() * {{ scale }})
{%- endmacro -%}
