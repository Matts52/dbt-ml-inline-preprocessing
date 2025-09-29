{%- macro laplacian_noise(column, scale=1.0) -%}
	{{ adapter.dispatch('laplacian_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__laplacian_noise(column, scale) -%}
    {# Invert the exponential distribution on either side of the number and generate jitter #}
	({{ column }} + sign(random() - 0.5) * ln(1 - 2 * abs(random() - 0.5)) * {{ scale }})
{%- endmacro -%}
