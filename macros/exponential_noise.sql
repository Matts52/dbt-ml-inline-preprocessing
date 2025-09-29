{%- macro exponential_noise(column, rate=1.0, scale=1.0) -%}
	{{ adapter.dispatch('exponential_noise', 'dbt_ml_inline_preprocessing')(column, rate, scale) }}
{%- endmacro -%}

{%- macro default__exponential_noise(column, rate, scale) -%}
	({{ column }} + (-ln(1 - random()) / {{ rate }}) * {{ scale }})
{%- endmacro -%}
