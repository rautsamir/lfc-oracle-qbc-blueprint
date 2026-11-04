-- One-pass Oracle inventory for Lakeflow Connect QBC planning (Oracle 12c+; tested shape on 19c).
-- Output columns match inventory/tables.csv. Export the result as CSV and review the guesses:
--   cursor_column is a best guess (latest-looking date column); confirm with the app team.
--   tier is left blank so scripts/generate_pipelines.py assigns it from size_gb.
-- Needs SELECT on DBA_SEGMENTS (or swap to USER_SEGMENTS) plus ALL_* views. Replace :OWNER.
-- Gather stats first if num_rows looks stale: EXEC DBMS_STATS.GATHER_SCHEMA_STATS(:OWNER);

WITH seg AS (
  SELECT segment_name AS table_name, SUM(bytes) / POWER(1024, 3) AS size_gb
  FROM dba_segments
  WHERE owner = :OWNER AND segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
  GROUP BY segment_name
),
pk AS (
  SELECT c.table_name,
         LISTAGG(cc.column_name, '|') WITHIN GROUP (ORDER BY cc.position) AS primary_keys,
         COUNT(*) AS pk_cols
  FROM all_constraints c
  JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
  WHERE c.owner = :OWNER AND c.constraint_type = 'P'
  GROUP BY c.table_name
),
cur AS (
  -- Prefer columns that look like "last updated" timestamps
  SELECT table_name, column_name AS cursor_column
  FROM (
    SELECT table_name, column_name,
           ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY
             CASE
               WHEN column_name LIKE '%LAST_UPD%' THEN 1
               WHEN column_name LIKE '%TBL_LAST_DT%' THEN 2
               WHEN column_name LIKE '%LAST%DT%' THEN 3
               WHEN column_name LIKE '%MOD%DT%' OR column_name LIKE '%UPDT%' THEN 4
               ELSE 9 END) AS rn
    FROM all_tab_columns
    WHERE owner = :OWNER AND (data_type LIKE 'TIMESTAMP%' OR data_type = 'DATE')
  )
  WHERE rn = 1
),
idx1 AS (
  -- Columns that lead some index (valid cursor / partition candidates)
  SELECT DISTINCT i.table_name, c.column_name
  FROM all_indexes i
  JOIN all_ind_columns c ON c.index_owner = i.owner AND c.index_name = i.index_name
  WHERE i.table_owner = :OWNER AND c.column_position = 1 AND i.status IN ('VALID', 'N/A')
)
SELECT t.table_name                                         AS source_table,
       pk.primary_keys                                      AS primary_keys,
       cur.cursor_column                                    AS cursor_column,
       CASE WHEN idx1.column_name IS NOT NULL THEN 'Y' ELSE 'N' END AS cursor_indexed,
       ROUND(NVL(seg.size_gb, 0), 2)                        AS size_gb,
       ''                                                   AS tier,
       ''                                                   AS partition_column,
       CASE WHEN pk.primary_keys IS NOT NULL THEN 'hard' ELSE 'none' END AS deletes,
       t.num_rows                                           AS num_rows,
       NVL(pk.pk_cols, 0)                                   AS pk_cols,
       ''                                                   AS isolate,
       CASE
         WHEN pk.primary_keys IS NULL THEN 'no PK: append-only or needs a key'
         WHEN cur.cursor_column IS NULL THEN 'no date column: choose a numeric cursor or full load'
         WHEN pk.pk_cols >= 4 THEN 'composite key: check partition split'
       END                                                  AS notes
FROM all_tables t
LEFT JOIN seg  ON seg.table_name = t.table_name
LEFT JOIN pk   ON pk.table_name = t.table_name
LEFT JOIN cur  ON cur.table_name = t.table_name
LEFT JOIN idx1 ON idx1.table_name = t.table_name AND idx1.column_name = cur.cursor_column
WHERE t.owner = :OWNER
  AND t.temporary = 'N'
  AND t.table_name NOT LIKE 'BIN$%'
ORDER BY size_gb DESC;
