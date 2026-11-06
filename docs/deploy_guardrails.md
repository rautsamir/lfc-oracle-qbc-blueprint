# DABs deploy guardrails for QBC pipelines

A pipeline owns the tables it creates. Anything that makes the bundle delete and recreate a pipeline also deletes its tables, and the new pipeline has no saved cursor state, so you're back to a full load.

## Changes that are safe vs. risky

| Change | Effect | Treat as |
|---|---|---|
| Add a table to `objects` | New table, full load for that table only | Safe |
| Remove a table from `objects` | Table is no longer managed; check whether it's dropped | Review |
| Change cursor column, PKs, SCD type, delete settings | In place; may trigger a refresh of that table | Test in QA |
| Change `source_catalog` (Oracle service name) | Same pipeline, cursor state kept, incremental continues (verified in a customer QA, same host). Not documented as a guarantee | Safe on the same host; still check the next run in QA |
| **Change `connection_name`** | **Pipeline recreated; its tables dropped** | **Migration** |
| Rename the pipeline resource key | Bundle sees delete + create | Migration |
| Missing explicit `catalog`/`target` | Spurious diff, possible recreate | Always set both |

## Before every qa/prod deploy

1. `databricks bundle plan -t <target>` and read it. Any `delete` or `recreate` on a pipeline is a stop.
2. Never use `--auto-approve` for changes to connection, gateway, or pipeline identity.
3. Run qa and prod deploys as a service principal (`run_as`), not a person. Ownership changes can block UNDROP and restores.
4. Keep environment differences in target variables only. A QA bundle pointing at the wrong source connection is a silent data problem; check `connection_name` per target in review.

## Adopting pipelines that already exist

If pipelines were created in the UI or by an earlier bundle, don't redeploy them as new resources. Bind each one so the bundle takes ownership of the existing pipeline, tables, and cursor state:

```bash
databricks bundle deployment bind qbc_s_00 <existing_pipeline_id> -t qa
databricks bundle plan -t qa      # should show update, never delete/recreate
```

Unbind with `databricks bundle deployment unbind <key>` if you need to hand a pipeline back.

## If tables were dropped

- `UNDROP` may fail if the owning pipeline no longer exists. Open an ES ticket immediately with pipeline IDs, and don't redeploy again until engineering answers.
- Plan for a full reload from source. Restore is not guaranteed.

## Full refresh

"Full refresh" means two different things. Keep them apart:

- **Full load as a load mode** (`load_mode=full`): the table has no cursor, so every run reads all of it. That's normal operation for that table.
- **Full refresh as an action** (UI button, `full_refresh: true`, `full_refresh_selection`): resets a table and reloads it from scratch. Use it after a cursor or schema change, for a corrupted table, or on a schedule for incremental+periodic tables. Never from the nightly scheduler.

- For incremental tables, confirm in the event log that a full refresh actually re-read the source. A 28-second "full refresh" that produced 608 rows from a 345M-row table is the warning sign.
