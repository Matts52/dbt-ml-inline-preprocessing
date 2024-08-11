with data_max_absolute_scale as (
    select * from {{ ref('data_max_absolute_scale') }}
)

select
    {{ dbt_ml_inline_preprocessing.max_absolute_scale('input') }} as actual,
    output as expected
from data_max_absolute_scale
