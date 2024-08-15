with data_one_hot_encode as (
    select * from {{ ref('data_one_hot_encode') }}
)

select
    {{ dbt_ml_inline_preprocessing.one_hot_encode('input', ref('data_one_hot_encode')) }},
    output_is_input__cat as expected_is_input__cat,
    output_is_input__dog as expected_is_input__dog,
    output_is_input__ as expected_is_input__
from data_one_hot_encode
