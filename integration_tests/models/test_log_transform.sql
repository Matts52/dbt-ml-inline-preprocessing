with data_log_transform as (
    select * from {{ ref('data_log_transform') }}
)

select
    {{ dbt_ml_inline_preprocessing.log_transform('input') }} as actual_base10,
    output_base10 as expected_base10,
    {{ dbt_ml_inline_preprocessing.log_transform('input', base=2) }} as actual_base2,
    output_base2 as expected_base2,
    {{ dbt_ml_inline_preprocessing.log_transform('input', offset=1) }} as actual_offset1,
    output_offset1 as expected_offset1
from data_log_transform
