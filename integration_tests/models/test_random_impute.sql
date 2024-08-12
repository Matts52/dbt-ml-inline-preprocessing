with data_random_impute as (
    select * from {{ ref('data_random_impute') }}
)

select
    {{ dbt_ml_inline_preprocessing.random_impute('input', ref('data_random_impute'), consider_distribution=false) }} as actual
from data_random_impute
