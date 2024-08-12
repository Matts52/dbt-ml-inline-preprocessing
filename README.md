This [dbt](https://github.com/dbt-labs/dbt) package contains macros that can be (re)used across dbt projects.

## Installation Instructions

To import this package into your dbt project, add the following to either the `packages.yml` or `dbt_project.yml` file:

```
packages:
  - package: "Matts52/dbt_ml_inline_preprocessing"
    version: [">=0.0.1"]
```

and run a `dbt deps` command to install the package to your project.

Check [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

## dbt Versioning

This package currently support dbt versions 1.1.0 through 2.0.0

## Adapter Support

Currently this package supports the Snowflake and Postgres adapters

----

* [Installation Instructions](#installation-instructions)
* [Imputation](#imputation)
    * [categorical_impute](#categorical_impute)
    * [numerical_impute](#numerical_impute)
* [Encoding](#encoding)
    * [one_hot_encode](#one_hot_encode)
* [Numerical Transformation](#numerical-transformation)
    * [log_transform](#log_transform)
    * [max_absolute_scale](#max_absolute_scale)
    * [min_max_scale](#min_max_scale)
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

## Encoding

### one_hot_encode
([source](macros/one_hot_encode.sql))

This macro returns one hot encoded fields from a categorical column

**Args:**

- `source_relation` (required for some databases): a Relation (a `ref` or `source`) that contains the list of columns you wish to select from
- `source_column` (required): Name of the field that is to be one hot encoded

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.one_hot_encode(
    source_relation=ref('my_model'),
    source_column='purchase_value',
   )
}}
```

## Numerical Transformation

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
- `new_min` (optional): The new minimum value to scale towards
- `new_max` (optional): The new maximum value to scale towards

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.min_max_scale(
    column='user_rating',
    new_min=0,
    new_max=10,
   )
}}
```

### min_max_scale
([source](macros/min_max_scale.sql))

This macro transforms the given column to have a specified minimum and specified maximum, and scaling all values to fit that range. This transforms the range of values within the column to be [new minimum, new maximum]

**Args:**

- `column` (required): Name of the field that is to be transformed
- `new_min` (optional): The new minimum value to scale towards
- `new_max` (optional): The new maximum value to scale towards

**Usage:**

```sql
{{ dbt_ml_inline_preprocessing.min_max_scale(
    column='user_rating',
    new_min=0,
    new_max=10,
   )
}}
```
