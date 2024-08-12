with data_random_impute_num as (
    select * from {{ ref('data_random_impute_num') }}
)

select
    {{ dbt_ml_inline_preprocessing.random_impute('input', ref('data_random_impute_num'), 'numerical', consider_distribution=true) }} as actual
from data_random_impute_num
