# QBC failure taxonomy

Most large-migration failures fall into three classes. They look alike in the UI (a red table) and have different fixes, so classify first.

## How to classify: the retry matrix

Rerun the failed tables in a small, isolated pipeline.

| Result | Class |
|---|---|
| Fails every retry, even alone | **A** (configuration / partitioning) |
| Passes alone or on retry | **B** (connection drop under concurrency) or **C** (shuffle) |
| Error mentions Oracle (`ORA-`) | **B** |
| No Oracle error, shuffle or fetch errors | **C** |

## A. Partition split failure

- **Signature:** `Task 0 in stage N failed 4 times`; repeatable; tends to hit composite-key ERP tables.
- **Cause:** the connector splits on a poor column, usually a low-cardinality leading key column.
- **Fix (configuration, not runtime):**
  1. Check composite-key handling defaults with engineering.
  2. Set `dlt-pipelines.qbcPartitionColumn` on a dedicated pipeline: first column of an index, at most ~50,000 rows per distinct value, no NULLs, not the cursor column.
  3. Write primary keys UPPERCASE. A lowercase key can surface as "column does not exist in source" on only some tables.

## B. Connection drop

- **Signature:** `ORA-17008 Closed connection` or `ORA-17002 Connection reset`; passes on retry.
- **Cause:** idle flows dropped by the network path (Private Link Service ~5 min fixed idle timeout, Azure Firewall, NAT), made worse by 100+ concurrent sessions.
- **Fix:**
  1. Confirm connector keepalive is on (`dlt-pipelines.qbcOracleKeepaliveEnabled`).
  2. Fewer tables per pipeline; run tiers separately.
  3. Index the cursor so `MIN`/`MAX` returns in seconds instead of scanning.
  4. Check the proxy's telemetry before resizing anything. It is rarely the bottleneck.

## C. Shuffle failure

- **Signature:** `FetchFailedException`, missing shuffle index files, `CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND`; no Oracle or OOM errors.
- **Cause:** after the read, during shuffle; triggered by full refresh of a group and skewed partitions. One lost executor can fail the whole pipeline.
- **Fix:** don't full-refresh a whole group; isolate the skewed table; reduce skew. This is compute-side; escalate with the pipeline ID and update ID.

## Escalation packet

When you open an ES ticket, include: pipeline ID, update ID, table, class (A/B/C) with the retry-matrix result, the exact error, the inventory row, and the preflight results (index list, rows per value, EXPLAIN PLAN).
