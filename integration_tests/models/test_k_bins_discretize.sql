with data_k_bins_discretize as (
    select * from {{ ref('data_k_bins_discretize') }}
)

select
    {{ dbt_ml_inline_preprocessing.k_bins_discretize('input', 5, strategy='quantile') }} as actual_quantile,
    {{ dbt_ml_inline_preprocessing.k_bins_discretize('input', 5, strategy='uniform') }} as actual_uniform,
    output_quantile as expected_quantile,
    output_uniform as expected_uniform
from data_k_bins_discretize