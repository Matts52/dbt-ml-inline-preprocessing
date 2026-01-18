{%- macro zscore_outlier(column, threshold=3) -%}
	{{ adapter.dispatch('zscore_outlier', 'dbt_ml_inline_preprocessing')(column, threshold) }}
{%- endmacro -%}

{%- macro default__zscore_outlier(column, threshold) -%}
	case
		when abs(({{ column }} - avg({{ column }}) over ()) / nullif(stddev({{ column }}) over (), 0)) > {{ threshold }} then 1
		else 0
	end
{%- endmacro -%}
