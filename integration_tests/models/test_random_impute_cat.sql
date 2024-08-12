with data_random_impute_cat as (
    select * from {{ ref('data_random_impute_cat') }}
)

select
    {{ dbt_ml_inline_preprocessing.random_impute('input', ref('data_random_impute_cat'), 'categorical', consider_distribution=false) }} as actual
from data_random_impute_cat
