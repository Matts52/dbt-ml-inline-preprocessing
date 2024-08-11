with data_min_max_scale as (
    select * from {{ ref('data_min_max_scale') }}
)

select
    {{ dbt_ml_inline_preprocessing.min_max_scale('input') }} as actual,
    output as expected
from data_min_max_scale
