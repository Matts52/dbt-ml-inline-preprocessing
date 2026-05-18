with data_power_transform as (
    select * from {{ ref('data_power_transform') }}
)

select
    {{ dbt_ml_inline_preprocessing.power_transform('input', lambda=0) }} as actual_lam0,
    expected_lam0,
    {{ dbt_ml_inline_preprocessing.power_transform('input', lambda=2) }} as actual_lam2,
    expected_lam2
from data_power_transform
