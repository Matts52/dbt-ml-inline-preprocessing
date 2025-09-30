{%- macro cauchy_noise(column, scale=1.0) -%}
	{{ adapter.dispatch('cauchy_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__cauchy_noise(column, scale) -%}
	({{ column }} + {{ scale }} * tan(pi() * (random() - 0.5)))
{%- endmacro -%}
