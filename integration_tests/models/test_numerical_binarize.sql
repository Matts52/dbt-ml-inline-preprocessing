with data_numerical_binarize as (
    select * from {{ ref('data_numerical_binarize') }}
)

select
    {{ dbt_ml_inline_preprocessing.numerical_binarize('input', 5, strategy='value') }} as actual_value,
    {{
        dbt_ml_inline_preprocessing.numerical_binarize(
            'input',
            0.2,
            strategy='percentile',
            direction='>',
            source_relation=ref('data_numerical_binarize')
        )
    }} as actual_percentile,
    output_value as expected_value,
    output_percentile as expected_percentile
from data_numerical_binarize
