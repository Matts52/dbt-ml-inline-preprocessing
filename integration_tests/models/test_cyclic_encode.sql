with data_cyclic_encode as (
    select *
    from {{ ref('data_cyclic_encode') }}
)

select
    {{ dbt_ml_inline_preprocessing.cyclic_encode('input', 'week_of_year') }} as actual,
    output as expected
from data_cyclic_encode
