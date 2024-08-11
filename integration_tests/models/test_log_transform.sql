with data_log_transform as (
    select * from {{ ref('data_log_transform') }}
)

select
    {{ dbt_ml_inline_preprocessing.log_transform('input') }} as actual,
    output as expected
from data_log_transform
