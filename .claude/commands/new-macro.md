# New Macro

Plan and implement a new dbt-ml-inline-preprocessing macro end-to-end.

## Steps

1. **Understand context** — read 1-2 similar existing macros to match style and patterns.

2. **Plan** — before writing any code, present a plan covering:
   - What the macro does and its SQL logic
   - Signature: macro name, required/optional args with defaults
   - Null handling behavior
   - Whether a Snowflake-specific dispatch is needed (required when using `::FLOAT` cast)
   - Files to create/modify:
     - `macros/<name>.sql`
     - `integration_tests/data/data_<name>.csv` (seed with known expected values, include a null row)
     - `integration_tests/models/test_<name>.sql`
     - `integration_tests/models/schema.yml` (new test entry)
     - `README.md` (summary table row, TOC entry, `###` docs section)
   - Ask the user to confirm before implementing.

3. **Implement** — create/edit all files identified in the plan:
   - Macro: input validation with `raise_compiler_error`, `default__` dispatch, `snowflake__` dispatch if needed
   - Seed CSV: small dataset with exact, hand-verifiable expected values; always include at least one null input row
   - Test model: selects `{{ dbt_ml_inline_preprocessing.<name>(...) }} as actual` alongside `expected`
   - Schema: add `assert_equal` (numeric) or `assert_equal_string` (categorical) tests; add `assert_not_null` only for columns that must never be null
   - README: match formatting of adjacent macros exactly

## Argument to this skill

Optionally pass the macro name and a one-line description, e.g.:

```
/new-macro frequency_encode — replace each category with its relative frequency
```

If no argument is provided, ask the user what macro they want to add before proceeding.
