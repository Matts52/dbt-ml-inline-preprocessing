with data_standardize as (
    select * from {{ ref('data_standardize') }}
)

select
    {{ dbt_ml_inline_preprocessing.standardize('input') }} as actual,
    output::FLOAT as expected
from data_standardize
