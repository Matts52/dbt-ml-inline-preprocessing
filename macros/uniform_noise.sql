
{%- macro uniform_noise(column, scale=1.0) -%}
	{{ adapter.dispatch('uniform_noise', 'dbt_ml_inline_preprocessing')(column, scale, prefix) }}
{%- endmacro -%}

{%- macro default__uniform_noise(column, scale, prefix) -%}
	{{ column }} + (random() * {{ scale }})
{%- endmacro -%}
