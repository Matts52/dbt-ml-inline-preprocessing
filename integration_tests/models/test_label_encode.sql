with data_label_encode as (
    select * from {{ ref('data_label_encode') }}
)

select
    input,
    {{ dbt_ml_inline_preprocessing.label_encode('input') }} as actual,
    output as expected
from data_label_encode
