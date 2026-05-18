with data_missing_encode as (
    select * from {{ ref('data_missing_encode') }}
)

select
    {{ dbt_ml_inline_preprocessing.missing_encode('input') }} as actual,
    expected
from data_missing_encode
