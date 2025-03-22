with data_poly_transform as (
    select * from {{ ref('data_poly_transform') }}
)

select
    {{ dbt_ml_inline_preprocessing.poly_transform('input', 2) }} as actual_2,
    {{ dbt_ml_inline_preprocessing.poly_transform('input', 0.5) }} as actual_1_2,
    output_2 as expected_2,
    output_1_2 as expected_1_2
from data_poly_transform
