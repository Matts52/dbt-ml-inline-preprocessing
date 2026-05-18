with data_frequency_encode as (
    select * from {{ ref('data_frequency_encode') }}
)

select
    {{ dbt_ml_inline_preprocessing.frequency_encode('input') }} as actual,
    expected
from data_frequency_encode
