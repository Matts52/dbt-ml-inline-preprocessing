
{%- macro uniform_noise(column, scale=1.0) -%}
	{{ adapter.dispatch('uniform_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__uniform_noise(column, scale) -%}
	{{ column }} + (random() * {{ scale }})
{%- endmacro -%}
