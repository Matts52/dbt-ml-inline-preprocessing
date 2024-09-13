with data_exponentiate as (
    select * from {{ ref('data_exponentiate') }}
)

select
    {{ dbt_ml_inline_preprocessing.exponentiate('input') }} as actual,
    output as expected
from data_exponentiate
