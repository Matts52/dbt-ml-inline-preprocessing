# Running Integration Tests

### Prerequisites
- python3

### Configure credentials
Edit the `profiles.yml` file to hold your database credentials and information

An example `profiles.yml` is as follows:

```yaml
integration_tests:
  target: postgres
  outputs:
    postgres:
      type: postgres
      host: localhost
      user: newuser
      pass: password
      port: 5432
      dbname: test_database
      schema: ml_inline_preprocessing_integration_tests_postgres
      threads: 1
```


### Setup virtual environment
It is reccomended to use a virtual environment when developing this package. Run the following commands in the root (one folder up) of this project

```shell
python3 -m venv env
source env/bin/activate
```

This will create and activate a new Python virtual environment.

#### Adding an integration test
This directory contains an example dbt project which tests the macros in the `dbt-ml-inline-preprocessing` package. An integration test typically involves making 1) a new seed file 2) a new model file 3) a generic test to assert anticipated behaviour.

For an example integration tests, check out the tests for the `log_transform` macro:

1. [Macro definition](https://github.com/Matts52/dbt-ml-inline-preprocessing/blob/main/macros/log_transform.sql)
2. [Seed file with fake data](https://github.com/Matts52/dbt-ml-inline-preprocessing/blob/main/integration_tests/data/data_log_transform.csv)
3. [Model to test the macro](https://github.com/Matts52/dbt-ml-inline-preprocessing/blob/main/integration_tests/models/test_log_transform.sql)
4. [A generic test to assert the macro works as expected](https://github.com/Matts52/dbt-ml-inline-preprocessing/blob/main/integration_tests/models/schema.yml)


### Running integration tests

Assuming you are in the `integration_tests` folder:

```shell
dbt deps
dbt seed
dbt build --select +{your_model_name}
```
To fully reload a seed, if changes have been make run:

```shell
db seed --full-refresh
```