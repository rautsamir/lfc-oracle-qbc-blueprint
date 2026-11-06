# Oracle to Databricks with Lakeflow Connect (Query-Based Connector): Field Blueprint

A reusable blueprint for landing a large Oracle ERP (CGI Advantage, PeopleSoft, Oracle Financials, and similar) into Unity Catalog with the Lakeflow Connect query-based connector (QBC), deployed with Declarative Automation Bundles (DABs).

It comes from a production migration of a large public-sector ERP: about 1,700 tables and 4 TB, full load in about 15 hours and nightly incrementals in about 3.5 hours. Every rule below is something that broke at least once.

**What's in the box**

| Path | What it is |
|---|---|
| `README.md` | The playbook: decide, design, network, deploy, run, troubleshoot |
| `databricks.yml` | Bundle with dev / qa / prod targets and per-target variables |
| `sql/oracle_inventory_extract.sql` | One query that writes the inventory straight from Oracle (sizes, keys, cursor guess, index check) |
| `inventory/tables.csv` | Sample table inventory (one row per source table) |
| `scripts/check_load_mode.py` | Profiles each table's cursor in Oracle and recommends incremental, full, or incremental+periodic |
| `scripts/generate_pipelines.py` | Turns the inventory into tiered, size-balanced pipeline YAML, the job, and a plan CSV |
| `scripts/plan_to_sheet.py` | Publishes the plan and inventory to a Google Sheet tracker (status, failure class, count vs. volume) |
| `resources/` | Generated pipelines plus a tiered orchestration job |
| `sql/oracle_preflight.sql` | DBA queries to run before you pick keys, cursors, and partition columns |
| `sql/validate_bronze.sql` | Post-load reconciliation checks |
| `docs/failure_taxonomy.md` | The three failure classes and how to tell them apart |
| `docs/deploy_guardrails.md` | DABs changes that silently drop tables, and how to avoid them |
| `docs/network_patterns.md` | Private connectivity patterns for serverless and the idle-timeout trap |

---

## 1. Decide: QBC, CDC, or Federation

| Need | Use | Why |
|---|---|---|
| Daily or hourly batch, read-only account, no DBA changes to redo logs | **QBC** | No gateway VM, no LogMiner, no supplemental logging. Incremental by cursor column |
| Sub-hour latency or full delete capture without PK scans | **Oracle CDC** | LogMiner-based; needs archive log mode, supplemental logging, and a classic-compute ingestion gateway |
| Ad hoc or low-volume reads, no copy | **Lakehouse Federation** | Query in place through Unity Catalog |

Start with QBC unless latency or delete fidelity rules it out. You can move hot tables to CDC later.

## 2. Design each table (the inventory is the project)

Pull the inventory from Oracle with `sql/oracle_inventory_extract.sql`, export it as `inventory/tables.csv`, then review it with the DBA and app team. For every table you need:

1. **Primary key**, written in the source's case (Oracle: UPPERCASE). Lowercase keys pass most of the time and then fail on a handful of tables with "column does not exist in source."
2. **Cursor column**: a single, monotonically increasing column such as `LAST_UPDATE_DT`. Rows with NULL cursors are never ingested. If the table is append-only, a sequence PK can be the cursor.
3. **Is the cursor indexed?** The connector runs `MIN`/`MAX` on it. Unindexed on a large table means a full table scan, which can run past network idle timeouts.
4. **Size tier**: S, M, L, XL by GB and rows. The tier decides how many tables share a pipeline.
5. **Partition column override (XL only, if needed)**: must be the first column of an index, should have at most about 50,000 rows per distinct value, and should not be the cursor column.
6. **Deletes**: none, soft (`deletion_condition`), or hard (`hard_deletion_sync_min_interval_in_seconds`, Beta, SCD1 only, requires primary keys).
7. **Load mode**: incremental, full, or incremental+periodic (below).

### Choose a load mode

| Use | When | Inventory |
|---|---|---|
| **Incremental** | A single cursor column the app updates on every insert and update, never NULL; a primary key; big enough that a nightly reload costs real time | `load_mode=incremental` (default) |
| **Full load every run** | The app truncates and reloads the table; no trustworthy cursor (updates don't touch it, back-dated rows, bulk loads); a small lookup table where reloading is cheap | `load_mode=full`, cursor left blank |
| **Incremental + periodic full refresh** | Incremental works, but some changes slip past the cursor now and then (back-dated corrections, a batch that skips the timestamp) | `load_mode=incremental+periodic` |

- Every table needs primary keys for SCD Type 1. They don't have to be an Oracle constraint: a unique column set from the app team works.
- Full load on an L or XL table is expensive every night. Fixing the cursor with the app team is usually better.
- incremental+periodic tables get their own pipelines and a monthly job (paused by default) that full-refreshes only them. That job is the only place `full_refresh: true` appears.
- The docs say a table without a monotonically increasing cursor is fully loaded on each run, but the reference also lists the cursor as required for connection-based ingestion. Prove a `full` table in QA, and check that rows deleted in Oracle disappear; if they don't, keep hard-delete sync on.

`scripts/check_load_mode.py` makes the first pass. Per table it checks NULL cursors, whether the cursor moved since Oracle last recorded updates (`ALL_TAB_MODIFICATIONS`), truncates, deletes, and primary keys, then writes a recommendation and an updated inventory. Anything marked `review` needs a person, usually the app team.

```bash
# No Python access to Oracle: generate SQL, run it in SQL Developer or DBeaver, export the grid as CSV
python scripts/check_load_mode.py emit inventory/tables.csv --owner APP_OWNER --only DOC_HDR,PO_DOC_HDR > load_mode_check.sql
python scripts/check_load_mode.py recommend inventory/tables.csv load_mode_results.csv

# Or directly (pip install oracledb; password is prompted, never passed as an argument)
ORACLE_USER=readonly ORACLE_DSN=host:1521/SERVICE python scripts/check_load_mode.py run inventory/tables.csv --owner APP_OWNER
```

Start with a few tables (`--only`); one statement across a whole ERP is slow. L and XL tables are sampled at 1 percent.

Packaged ERPs use composite keys whose leading column is a low-cardinality code (fiscal year, document type). That design is right for the application and wrong for parallel reads. Expect to handle a small set of tables specially; don't ask the customer to redesign their schema.

Run `sql/oracle_preflight.sql` with the DBA to fill the inventory.

### From inventory to pipelines

1. **Profile the distribution first.** Sort by size and look at cumulative bytes. ERP estates are heavily skewed: in the reference migration, 5 of about 1,700 tables held about half of the 4 TB, and 99.4% of tables were under 100 GB. That's two problems: pack the bulk efficiently, and engineer the tail one table at a time.
2. **Tier by size, then adjust for shape.** Leave `tier` blank to auto-tier from `size_gb` (S under 1 GB, M under 50, L under 200, XL above). Move a table up a tier, or set `isolate=Y`, for composite keys with a low-cardinality leading column, an unindexed cursor, or NULLs in candidate columns. A 7.6 GB composite-key table failed while 100 GB tables passed.
3. **Pack by bytes and by count.** The generator fills pipelines largest-first up to a table cap and a GB cap per tier, so pipelines in the same tier finish at similar times.
4. **Schedule around the critical path.** Full-load time is roughly the time of the single biggest table. S runs first as a smoke test, then M, L, and XL start together so the biggest table isn't waiting. In the pilot, do the opposite: test XL early, because that's where the unknowns are.
5. **Track by pipeline, report by count and by volume.** `scripts/plan_to_sheet.py` creates the tracker. Record a failure class (A, B, or C) on each failed pipeline, so "4 failed" becomes "2 retry alone, 2 need config."

```bash
python scripts/check_load_mode.py recommend inventory/tables.csv load_mode_results.csv   # optional first pass
python scripts/generate_pipelines.py inventory/tables.csv resources/ --plan inventory/plan.csv
GOOGLE_TOKEN=$(gcloud auth print-access-token) python scripts/plan_to_sheet.py inventory/tables.csv inventory/plan.csv "ERP QBC Tracker"
```

The runtime estimate uses a flat GB-per-hour rate. Treat it as a planning number and recalibrate `GB_PER_HOUR` from the pilot.

## 3. Network (serverless)

QBC runs on serverless by default. For a private, on-prem Oracle the path is:

serverless pipeline → NCC private endpoint → Azure Private Link → Private Link Service on a Standard Load Balancer → proxy/NVA → ExpressRoute → Oracle

- **The ~5 minute idle timeout.** Private Link Service has a fixed idle timeout of about 5 minutes. Long metadata queries or slow fetches drop with `ORA-17008 Closed connection` or `ORA-17002 Connection reset`. The fix is TCP keepalive on the connector side (`dlt-pipelines.qbcOracleKeepaliveEnabled`, default on for QBC since mid-2026), not a bigger load balancer.
- **Two paths.** The pipeline wizard discovers tables over serverless even when a classic gateway exists. Private sources need the NCC private endpoint for setup as well as runtime.
- **The proxy is not the bottleneck.** In production the proxy VM sat at about 8% CPU while moving tens of GB. Connection resets came from concurrency (tables x partitions = 100+ simultaneous Oracle sessions), not capacity.

See `docs/network_patterns.md`.

## 4. Deploy with DABs

- One bundle, three targets (dev, qa, prod). Connection name, source service name, and destination catalog are **target variables**, never hard-coded.
- Generate pipelines from the inventory (`scripts/generate_pipelines.py`) instead of hand-editing 1,700 table entries.
- Set `catalog` and `target` explicitly on every pipeline so the deploy diff stays clean.
- **Changing `connection_name` on a deployed pipeline recreates it and drops every table it manages.** Treat connection changes as a migration, not a config edit. See `docs/deploy_guardrails.md`.
- Never run `bundle deploy --auto-approve` in qa or prod. Review the plan for `delete` or `recreate` first.
- Pipelines created in the UI first? **Bind them** to the bundle (`databricks bundle deployment bind <key> <pipeline_id>`) instead of letting the bundle create new ones, so existing tables and cursor state are kept.
- The scheduler never full-refreshes: every job task sets `full_refresh: false`. The one exception is the paused monthly job for incremental+periodic tables.

```bash
python scripts/generate_pipelines.py inventory/tables.csv resources/          # add --classic if only classic compute can reach Oracle (QBC Beta)
databricks bundle validate -t dev
databricks bundle plan -t qa        # read it: any delete/recreate is a stop
databricks bundle deploy -t qa
databricks bundle run qbc_orchestration -t qa
```

## 5. Run in tiers, not all at once

| Tier | Tables per pipeline | Notes |
|---|---|---|
| S | up to ~50 | Finishes in minutes |
| M | ~10 to 20 | |
| L | 3 to 5 | Isolate repeat offenders |
| XL | 1 | Own pipeline; partition override only if the default fails |

Run tiers as separate job tasks. In production, running every tier in parallel took 36 hours and failed 33 tables. Running tiers separately completed 99% in 29 hours, and small tables went from up to 2.5 hours to 5 to 15 minutes. Keep each pipeline under 100 tables.

## 6. Troubleshoot by class, not by table

| Class | Signature | Fix |
|---|---|---|
| A. Partition split | Fails every retry, even alone; composite-key tables | Config: partition override or composite-key handling |
| B. Connection drop | `ORA-17008` / `ORA-17002`; passes on retry or alone | Keepalive, fewer concurrent tables, isolate |
| C. Shuffle | `FetchFailed`, missing shuffle files, no Oracle error | Compute side: avoid full refresh of the group, reduce skew |

A retry matrix (fails every time vs. passes on retry) separates A from B and C in one run. Details in `docs/failure_taxonomy.md`.

## 7. Validate before you call it done

- Reconcile counts and value checks (`sql/validate_bronze.sql`), not just "pipeline green."
- Report by **count and by volume**. "99.8% of tables" was about 70% of bytes when the last four tables were the biggest.
- Separate "succeeded once" from "repeatable." Promote only after a clean full run and at least one clean incremental.
- If downstream gold moves from classic to serverless, reconcile financial outputs to the cent. Serverless always runs Photon, which can change decimal results.

## 8. Lessons that cost us time

- The UI **Full Refresh** on an incremental table can rebuild from staged changes, not from source. Confirm in the event log that a source read happened.
- Custom DBR images can't run on serverless. If a spec pins a custom image and sets serverless, it silently runs on classic.
- Connector fixes ship with the pipeline engine release, independent of the DBR version number. Ask engineering which release carries a fix; don't infer it from the runtime.
- The Oracle service name is `source_catalog` in the pipeline spec, not a connection option. In a customer QA, changing it on the same host kept cursor state and did not trigger a full reload. That isn't a documented guarantee, so confirm the first run after the change is incremental.
- Default settings beat hand-tuning more often than you'd think. Remove overrides that don't earn their keep.

## Credits

The bundle-generator pattern (inventory in, `databricks.yml` plus pipeline and job YAML out, explicit `catalog` and `target`, `bundle bind` for existing pipelines) started with Austin Rosenberg's QBC DAB generator, built for the same migration. This blueprint extends it with multi-column keys, tiering and bin-packing, an Oracle-side inventory extract, delete settings, a tiered job, and the field guardrails above.
