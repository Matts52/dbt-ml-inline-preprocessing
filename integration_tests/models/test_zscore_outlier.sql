with data_zscore_outlier as (
    select * from {{ ref('data_zscore_outlier') }}
)

select
    {{ dbt_ml_inline_preprocessing.zscore_outlier('input') }} as actual,
    output as expected
from data_zscore_outlier
