{%- macro gaussian_noise(column, scale=1.0) -%}
	{{ adapter.dispatch('gaussian_noise', 'dbt_ml_inline_preprocessing')(column, scale) }}
{%- endmacro -%}

{%- macro default__gaussian_noise(column, scale) -%}
    {# Using the Box-Muller method to generate standard normal noise #}
    {# See https://en.wikipedia.org/wiki/Box%E2%80%93Muller_transform #}
	({{ column }} + (
		sqrt(-2 * ln(random())) * cos(2 * pi() * random()) * {{ scale }}
	))
{%- endmacro -%}
