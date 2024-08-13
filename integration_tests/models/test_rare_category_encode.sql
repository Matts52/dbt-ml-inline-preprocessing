with data_rare_category_encode as (
    select * from {{ ref('data_rare_category_encode') }}
)

select
    {{ dbt_ml_inline_preprocessing.rare_category_encode('input', cutoff=0.20) }} as actual_20,
    output_20 as expected_20
from data_rare_category_encode
