with data_categorical_impute as (
    select * from {{ ref('data_categorical_impute') }}
)

select
    {{ dbt_ml_inline_preprocessing.categorical_impute('input', source_relation=ref('data_categorical_impute')) }} as actual,
    output as expected
from data_categorical_impute
