with data_interact as (
    select * from {{ ref('data_interact') }}
)

select
    {{ dbt_ml_inline_preprocessing.interact('input_one', 'input_two', interaction='multiplicative') }} as actual_mult,
    {{ dbt_ml_inline_preprocessing.interact('input_one', 'input_two', interaction='additive') }} as actual_add,
    {{ dbt_ml_inline_preprocessing.interact('input_one', 'input_two', interaction='exponential') }} as actual_exp,
    output_mult as expected_mult,
    output_add as expected_add,
    output_exp as expected_exp
from data_interact
