with data_robust_scale as (
    select * from {{ ref('data_robust_scale') }}
)

select
    {{ dbt_ml_inline_preprocessing.robust_scale('input', source_relation=ref('data_robust_scale')) }} as actual,
    output::FLOAT as expected
from data_robust_scale