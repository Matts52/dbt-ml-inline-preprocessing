with data_bool_to_one_hot as (
    select * from {{ ref('data_bool_to_one_hot') }}
)

select
    {{
        dbt_ml_inline_preprocessing.bool_to_one_hot(
            columns=['active', 'deleted', 'verified']
        )
    }},
    output_is_active as expected_is_active,
    output_is_deleted as expected_is_deleted,
    output_is_verified as expected_is_verified
from data_bool_to_one_hot
