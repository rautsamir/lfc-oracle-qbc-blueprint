-- Oracle preflight for Lakeflow Connect QBC. Run with the DBA before filling inventory/tables.csv.
-- Replace :OWNER and :TABLE. Read-only.

-- 0. Which service names exist on this host? (QBC source_catalog = service name)
SELECT name, network_name FROM v$services ORDER BY name;

-- 1. Size inventory: pick tiers (S < 1 GB, M < 50 GB, L < 200 GB, XL above)
SELECT segment_name AS table_name, ROUND(SUM(bytes) / 1024 / 1024 / 1024, 1) AS size_gb
FROM dba_segments
WHERE owner = :OWNER AND segment_type LIKE 'TABLE%'
GROUP BY segment_name
ORDER BY size_gb DESC;

-- 2. Primary keys in order (write these UPPERCASE in the inventory)
SELECT c.table_name, LISTAGG(cc.column_name, '|') WITHIN GROUP (ORDER BY cc.position) AS pk_columns
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
WHERE c.owner = :OWNER AND c.constraint_type = 'P'
GROUP BY c.table_name;

-- 3. First-of-index columns (the only valid partition-column candidates)
SELECT i.table_name, i.index_name, c.column_name, i.status
FROM all_indexes i
JOIN all_ind_columns c ON c.index_owner = i.owner AND c.index_name = i.index_name
WHERE i.table_owner = :OWNER AND i.table_name = :TABLE AND c.column_position = 1;

-- 4. Is the cursor column indexed? (needed for fast MIN/MAX on L/XL tables)
SELECT i.index_name, c.column_position
FROM all_ind_columns c
JOIN all_indexes i ON i.owner = c.index_owner AND i.index_name = c.index_name
WHERE i.table_owner = :OWNER AND i.table_name = :TABLE AND c.column_name = 'LAST_UPDATE_DT';

-- 5. Partition candidate check: rows per distinct value should be <= ~50,000, and no NULLs
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT REC_NO) AS distinct_values,
       ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT REC_NO), 0)) AS rows_per_value,
       COUNT(*) - COUNT(REC_NO) AS null_values
FROM :OWNER.:TABLE;

-- 6. Cursor NULLs (rows with NULL cursor are never ingested)
SELECT COUNT(*) - COUNT(LAST_UPDATE_DT) AS null_cursor_rows FROM :OWNER.:TABLE;

-- 7. MIN/MAX should use INDEX FULL SCAN (MIN/MAX), not TABLE ACCESS FULL
EXPLAIN PLAN FOR
SELECT (SELECT MIN(LAST_UPDATE_DT) FROM :OWNER.:TABLE) AS min_v,
       (SELECT MAX(LAST_UPDATE_DT) FROM :OWNER.:TABLE) AS max_v
FROM dual;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- 8. Session headroom: QBC opens many concurrent sessions during full loads
SELECT resource_name, current_utilization, max_utilization, limit_value
FROM v$resource_limit WHERE resource_name IN ('sessions', 'processes');
