-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Inspect Silver (read-only)
-- MAGIC Spot checks that Silver is clean, typed, and readable.

-- COMMAND ----------

-- Codes decoded into readable labels. This feeds the Q2 "human-readable label".
SELECT series_id, series_title, sector_name, measure_text, duration_text, class_text, seasonal_text, base_year
FROM workspace.data_quest.silver_bls_series
WHERE series_id IN ('PRS30006011', 'PRS30006032', 'PRS85006092')
ORDER BY series_id;

-- COMMAND ----------

-- Types and ranges: year INT, value DECIMAL(18,3), population BIGINT. 92,328 rows = AllData only (no double count).
SELECT 'bls_data' AS tbl, typeof(year) AS year_type, typeof(value) AS value_type,
       min(year) || '-' || max(year) AS years, count(*) AS rows_
FROM workspace.data_quest.silver_bls_data
UNION ALL
SELECT 'population', typeof(year), typeof(population), min(year) || '-' || max(year), count(*)
FROM workspace.data_quest.silver_population GROUP BY 1, 2, 3;

-- COMMAND ----------

-- No duplicate keys after dedupe (expect 0).
SELECT count(*) - count(DISTINCT series_id, year, period) AS duplicate_keys
FROM workspace.data_quest.silver_bls_data;
