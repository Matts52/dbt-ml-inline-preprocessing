This [dbt](https://github.com/dbt-labs/dbt) package contains macros that can be (re)used across dbt projects.

Note: All methods in this package are meant to be used inline within a select statement.

**Current supported tested databases include:**
- Postgres
- Snowflake
- DuckDB

| **Method**                       | **Postgres** | **Snowflake** | **DuckDB** |
|----------------------------------|--------------|---------------|------------|
| [bool_to_one_hot](#bool_to_one_hot) | ✅           | ✅             | ✅          |
| [categorical_impute](#categorical_impute) | ✅           | ✅             | ✅          |
| [numerical_impute](#numerical_impute)     | ✅           | ✅             | ✅          |
| [random_impute](#random_impute)           | ✅           | ✅             | ✅          |
| [label_encode](#label_encode)             | ✅           | ✅             | ✅          |
| [one_hot_encode](#one_hot_encode)         | ✅           | ✅             | ✅          |
| [rare_category_encode](#rare_category_encode) | ✅      | ✅             | ✅          |
| [exponentiate](#exponentiate)             | ✅           | ✅             | ✅          |
| [interact](#interact)                     | ✅           | ✅             | ✅          |
| [k_bins_discretize](#k_bins_discretize)   | ✅           | ✅             | ✅          |
| [log_transform](#log_transform)          | ✅           | ✅             | ✅          |
| [max_absolute_scale](#max_absolute_scale) | ✅           | ✅             | ✅          |
| [min_max_scale](#min_max_scale)           | ✅           | ✅             | ✅          |
| [numerical_binarize](#numerical_binarize) | ✅           | ✅             | ✅          |
| [poly_transform](#poly_transform)         | ✅           | ✅             | ✅           
| [robust_scale](#robust_scale)             | ✅           | ✅             | ✅          |
| [standardize](#standardize)               | ✅           | ✅             | ✅          |


## Installation Instructions

To import this package into your dbt project, add the following to either the `packages.yml` or `dependencies.yml` file:

```
packages:
  - package: "Matts52/dbt_ml_inline_preprocessing"
    version: [">=0.2.0"]
```

and run a `dbt deps` command to install the package to your project.

Check [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## dbt Versioning

This package currently support dbt versions 1.1.0 through 2.0.0

## Adapter Support

Currently this package supports:
- `dbt-duckdb`
- `dbt-postgres`
- `dbt-snowflake`

----

* [Installation Instructions](#installation-instructions)
* [Imputation](#imputation)
    * [categorical_impute](#categorical_impute)
    * [numerical_impute](#numerical_impute)
    * [random_impute](#random_impute)
* [Encoding](#encoding)
    * [bool_to_one_hot](#bool_to_one_hot)
    * [label_encode](#label_encode)
    * [one_hot_encode](#one_hot_encode)
    * [rare_category_encode](#rare_category_encode)
* [Numerical Transformation](#numerical-transformation)
    * [exponentiate](#exponentiate)
    * [interact](#interact)
    * [k_bins_discretize](#k_bins_discretize)
    * [log_transform](#log_transform)
    * [max_absolute_scale](#max_absolute_scale)
    * [min_max_scale](#min_max_scale)
    * [numerical_binarize](#numerical_binarize)
    * [poly_transform](#poly_transform)
    * [robust_scale](#robust_scale)
    * [standardize](#standardize)


----

## Imputation

### categorical_impute
([source](macros/categorical_impute.sql))

This macro returns impute categorical data for a column in a model, source, or CTE

**Args:**

- `column` (required): Name of the field that is to be imputed
- `measure` (optional): The measure by which to impute the data. It is set to use the 'mode' by default
- `source_relation` (required for some databases): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.categorical_impute(
    column='user_type',
    measure='mode',
    relation=ref('my_model'),
   )
}}
```

### numerical_impute
([source](macros/numerical_impute.sql))

This macro returns imputed numerical data for a column in a model, source, or CTE

**Args:**

- `column` (required): Name of the field that is to be imputed
- `measure` (optional): The measure by which to impute the data. It is set to use the 'mean' by default, but also support 'median' and 'percentile'
- `percentile` (optional): If percentile is selected for the measure, this indicates the percentile value to impute into null values
- `source_relation` (required for some databases): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.numerical_impute(
    column='purchase_value',
    measure='mean',
    percentile=0.25,
    source_relation=ref('my_model'),
   )
}}
```

### random_impute
([source](macros/random_impute.sql))

This macro returns randomly imputed data for a column in a model or source based on values that already exist within the column

NOTE: This method assumes that at least one value has been observed in the input column

**Args:**

- `column` (required): Name of the field that is to be imputed
- `source_relation` (required): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from
- `data_type` (required): The data type of this column. Either `numerical` or `categorical`
- `consider_distribution` (optional): Boolean of whether or not the distribution of existing values should be taken into account. Macro will run faster when this is set to false. Default is false

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.random_impute(
    column='user_type',
    source_relation=ref('my_model'),
    data_type=`categorical`,
    consider_distribution=true
   )
}}
```


## Encoding

### bool_to_one_hot
([source](macros/bool_to_one_hot.sql))

This macro converts boolean columns to one-hot encoded format (0 or 1). It can handle multiple columns at once and provides options for handling null values.

**Args:**

- `columns` (required): List of boolean column names to convert to one-hot format
- `null_as_one` (optional): Whether to treat null values as 1 (true) or 0 (false). Default is false
- `prefix` (optional): Prefix to use for the output column names. Default is 'is_'

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.bool_to_one_hot(
    columns=['active', 'deleted', 'verified'],
    null_as_one=false,
    prefix='is_'
   )
}}
```

### label_encode
([source](macros/categorical_encode.sql))

This macro returns a the labels encoded with individual integers from 0 to n-1. Note that this has the side effect of re-ordering the dataset.

**Args:**

- `column` (required): Name of the field that is to be encoded

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.label_encode(
    column='user_type',
   )
}}
```

### one_hot_encode
([source](macros/one_hot_encode.sql))

This macro returns one hot encoded fields from a categorical column

NOTE: One hot encoded fields will have the naming convention `is_{column_name}_{value}`

**Args:**

- `column` (required): Name of the field that is to be one hot encoded
- `source_relation` (optional): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from
- `source_condition` (optional): A where clause condition to filter the field to be one-hot encoded by
- `categories` (optional): An explicit list of categories to one-hot encode into

Note: Either `source_relation` or `categories` must be set and do not impact each other. If `source_relation` is set, `source_condition` may be optionall used to filter categories found in the source relation.

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.one_hot_encode(
    column='purchase_value',
    source_relation=ref('my_model'),
    source_condition='purchase_value > 15',
   )
}}
```

```sql
{{ dbt_ml_inline_preprocessing.one_hot_encode(
    column='purchase_value',
    categories=['low', 'medium', 'high', 'other']
   )
}}
```

### rare_category_encode
([source](macros/rare_category_encode.sql))

This macro encodes rarely occuring categorical values with 'Other', leaving the rest of the categorical column values as is

**Args:**

- `column` (required): Name of the field that is to be rare category encoded
- `cutoff` (optional): The percentage value (in decimal) that is to serve as the point where any values occuring with a lesser frequency are rare category encoded. Default is 0.05 (ie 5%)

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.rare_category_encode(
    column='user_type',
    cutoff=0.10,
   )
}}
```

## Numerical Transformation

### exponentiate
([source](macros/exponentiate.sql))

This macro returns the given column after applying a exponential transformation to the numerical data. Often this is useful for when values are in logged form. By default the base will be `e` (the exponential constant)

**Args:**

- `column` (required): Name of the field that is to be exponentiated
- `base` (optional): The base of the exponentiation to apply. By default this is 2.71, indicating that the exponential constant `e` should be used.

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.exponentiate(
    column='log_purchase_value',
    base=10,
   )
}}
```

### interact
([source](macros/interact.sql))

This macro creates an interaction term between two numerical columns

**Args:**

- `column_one` (required): Name of the first field in the interaction term
- `column_two` (required): Name of the second field in the interaction term
- `interaction` (optional): The interaction to apply. Options are "multiplicative", "additive", "exponential". Default is "multiplicative"

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.interact(
    colum_one_='purchase_value',
    column_two='discount_value',
    interaction='multiplicative',
   )
}}
```

### k_bins_discretize
([source](macros/k_bins_discretize.sql))

This macro returns the given column after discretizing it into a specified number of bins

**Args:**

- `column` (required): Name of the field that is to be k bins discretized
- `k` (required): The number of bins to discretize into
- `strategy` (optional): The method by which to discretize the column into bins. Supported options are "quantile" to discretize into equal sized bins and "uniform" to bin into equal width bins. Default is "quantile"

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.k_bins_discretize(
    column='purchase_value',
    k=5,
    strategy='quantile',
   )
}}
```


### log_transform
([source](macros/log_transform.sql))

This macro returns the given column after applying a log transformation to the numerical data

**Args:**

- `column` (required): Name of the field that is to be log transformed
- `base` (optional): The base of the log function that is transforming the column. Default is 10
- `offset` (optional): Value to be added to all values in the column before log transforming. Common use case is when zero values are included in the column. Default is 0

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.log_transform(
    column='purchase_value',
    base=10,
    offset=1,
   )
}}
```

### max_absolute_scale
([source](macros/max_absolute_scale.sql))

This macro transforms the given column by dividing each value by the maximum absolute value within the column. This transforms the range of values within the column to be [-1, 1]

**Args:**

- `column` (required): Name of the field that is to be transformed

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.max_absolute_scale(
    column='user_rating',
   )
}}
```

### min_max_scale
([source](macros/min_max_scale.sql))

This macro transforms the given column to have a specified minimum and specified maximum, and scaling all values to fit that range. This transforms the range of values within the column to be [new minimum, new maximum]

**Args:**

- `column` (required): Name of the field that is to be transformed
- `new_min` (optional): The new minimum value to scale towards. Default is 0.0
- `new_max` (optional): The new maximum value to scale towards. Default is 1.0

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.min_max_scale(
    column='user_rating',
    new_min=0,
    new_max=10,
   )
}}
```

### numerical_binarize
([source](macros/numerical_binarize.sql))

This macro transforms the given numerical column into binary value based on either a specified cutoff value or percentile

**Args:**

- `column` (required): Name of the field that is to be transformed
- `cutoff` (required): The value that serves as the boundary point for the binary variable. This should be a value between 0 and 1 for percentile cutoff's. Default is 0.5
- `strategy` (optional): The method with which to set the boundary point for the binary variable, options are "percentile" and "value". Default is 'percentile'
- `direction` (optional): The direction that the 1 value should signify for the binary variable. Options are ">", ">=", "<", and "<=". Default is ">="
- `source_relation` (required for some databases): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from


**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.numerical_binarize(
        'input',
        strategy='percentile',
        cutoff=0.2,
        direction='>',
        source_relation=ref('data_numerical_binarize')
    )
}}
```

### poly_transform
([source](macros/poly_transform.sql))

This macro transforms the given column into IQR scaled values to more effectively deal with concerning outlier datapoints

**Args:**

- `column` (required): Name of the field that is to be transformed
- `degree` (optional): The degree of the polynomial transformation. Default is 2

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.poly_transform(
    column='user_rating',
    degree=0.5
   )
}}
```

### robust_scale
([source](macros/robust_scale.sql))

This macro transforms the given column into IQR scaled values to more effectively deal with concerning outlier datapoints

**Args:**

- `column` (required): Name of the field that is to be transformed
- `iqr` (optional): The interquantile range to consider and scale by. Expects a number between 0 and 1 excluse. Default is 0.5, leading to a interquantile range stretching from the 25th percentile to the 75th percentile
- `source_relation` (required for some databases): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.robust_scale(
    column='user_rating',
    iqr=0.5
    source_relation=ref('my_model')
   )
}}
```

### standardize
([source](macros/standardize.sql))

This macro transforms the given column into a normal distribution. Transforms to a standard normal distribution by default

**Args:**

- `column` (required): Name of the field that is to be transformed
- `target_mean` (optional): The new mean that the column assumes. Default is 0
- `target_stddev` (optional): The new standard deviation that the column assumes. Default is 1

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.standardize(
    column='user_rating',
    target_mean=0,
    target_stddev=0,
   )
}}
```
