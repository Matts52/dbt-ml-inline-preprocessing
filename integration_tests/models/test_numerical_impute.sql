with data_numerical_impute as (
    select * from {{ ref('data_numerical_impute') }}
)

select
    {{ dbt_ml_inline_preprocessing.numerical_impute('input', measure='mean', source_relation=ref('data_numerical_impute')) }} as actual_mean,
    {{ dbt_ml_inline_preprocessing.numerical_impute('input', measure='median', source_relation=ref('data_numerical_impute')) }} as actual_median,
    output_mean as expected_mean,
    output_median as expected_median
from data_numerical_impute
