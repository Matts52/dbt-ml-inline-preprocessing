# Update Docs

Regenerate the dbt docs assets and sync them to the `/docs` folder.

## Steps

1. Run `dbt docs generate` from the `integration_tests/` directory using the project's dbt binary:
   ```
   cd integration_tests && /Users/mattsenick/repos/dbt-env/bin/dbt docs generate
   ```

2. Copy the three generated files from `integration_tests/target/` to `docs/`:
   - `catalog.json`
   - `manifest.json`
   - `run_results.json`

   The `index.html` in `docs/` is the static dbt docs UI and does not need to be regenerated — it loads the above JSON files at runtime.

3. Confirm the copy succeeded and report which files were updated.

## Notes

- Run this after adding or modifying any macros, models, seeds, or tests so the hosted docs stay in sync.
- The `docs/` folder is what gets served as the project's documentation site — always update it before opening a PR that adds new macros.
- `run_results.json` reflects the last test run, so run `dbt seed && dbt run && dbt test` before generating docs if you want the results to reflect a clean passing state.
