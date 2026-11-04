-- Post-load validation in Databricks SQL. Uses a foreign catalog over the same Oracle
-- connection (Lakehouse Federation) to compare source and bronze without exporting data.
-- Replace fc_oracle_erp, APP_OWNER, and the bronze catalog/schema for your target.

-- 1. Row counts, source vs bronze, for one table
SELECT 'source' AS side, COUNT(*) AS row_count FROM fc_oracle_erp.APP_OWNER.VENDOR
UNION ALL
SELECT 'bronze', COUNT(*) FROM qa_bronze.erp_bronze.vendor;

-- 2. Value check: control totals on an amount column, compared to the cent
SELECT 'source' AS side, BFY, SUM(LN_AM) AS total
FROM fc_oracle_erp.APP_OWNER.JRNL_ACTG GROUP BY BFY
UNION ALL
SELECT 'bronze', bfy, SUM(ln_am) FROM qa_bronze.erp_bronze.jrnl_actg GROUP BY bfy
ORDER BY 2, 1;

-- 3. Freshness: bronze high-water mark vs source (gap = rows not yet ingested)
SELECT (SELECT MAX(LAST_UPDATE_DT) FROM fc_oracle_erp.APP_OWNER.VENDOR) AS source_max,
       (SELECT MAX(last_update_dt) FROM qa_bronze.erp_bronze.vendor) AS bronze_max;

-- 4. Duplicate keys in bronze (should be zero for SCD1)
SELECT vend_cust_cd, COUNT(*) FROM qa_bronze.erp_bronze.vendor
GROUP BY vend_cust_cd HAVING COUNT(*) > 1;

-- 5. Pipeline health across all QBC pipelines in the last day (event log)
SELECT origin.pipeline_name, event_type, level, timestamp, message
FROM event_log(TABLE(qa_bronze.erp_bronze.vendor))
WHERE level IN ('ERROR', 'WARN') AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;
