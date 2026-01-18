{%- macro laplacian_noise(column, scale=1.0) -%}
    {# Validate inputs #}
    {% if column is none or column == '' %}
        {{ exceptions.raise_compiler_error("laplacian_noise: 'column' parameter is required and cannot be empty.") }}
    {% endif %}

    {% if scale is none %}
        {{ exceptions.raise_compiler_error("laplacian_noise: 'scale' parameter cannot be None.") }}
    {% endif %}

    {% if scale <= 0 %}
        {{ exceptions.raise_compiler_error("laplacian_noise: 'scale' parameter must be positive. Got: " ~ scale ~ ".") }}
    {% endif %}

    {{ adapter.dispatch('laplacian_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__laplacian_noise(column, scale) -%}
    {# Invert the exponential distribution on either side of the number and generate jitter #}
    ({{ column }} + sign(random() - 0.5) * ln(1 - 2 * abs(random() - 0.5)) * {{ scale }})
{%- endmacro -%}
