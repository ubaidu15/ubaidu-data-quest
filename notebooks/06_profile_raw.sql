-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Profile the raw BLS files (read-only)
-- MAGIC Facts that drive the Bronze/Silver/Gold design. `read_files` queries files in the Volume directly, with no table needed.
-- MAGIC **Gotcha:** BLS headers are padded, so the columns are literally `` `series_id        ` `` and `` `       value` `` (backticks required).

-- COMMAND ----------

-- Per data file: rows, series, year range, Q05 rows, null keys, unparseable values, duplicate keys.
WITH d AS (
  SELECT trim(`series_id        `) sid, trim(year) yr, trim(period) per, trim(`       value`) val,
         trim(footnote_codes) fn, _metadata.file_name f
  FROM read_files('/Volumes/workspace/data_quest/raw/bls/pr/current/pr.data.*', format => 'csv', sep => '\t', header => true)
)
SELECT f, count(*) rows_, count(DISTINCT sid) series, min(yr) min_yr, max(yr) max_yr,
       count_if(per = 'Q05') q05_rows,
       count_if(sid IS NULL OR yr IS NULL OR per IS NULL) null_keys,
       count_if(try_cast(val AS DOUBLE) IS NULL) bad_values,
       count(*) - count(DISTINCT sid, yr, per) dup_keys,
       array_sort(collect_set(per)) periods, collect_set(fn) footnotes
FROM d GROUP BY f;

-- COMMAND ----------

-- Is Current a pure subset of AllData? (0 / 0 means yes, so Silver can use AllData alone.)
WITH d AS (
  SELECT trim(`series_id        `) sid, trim(year) yr, trim(period) per, trim(`       value`) val, _metadata.file_name f
  FROM read_files('/Volumes/workspace/data_quest/raw/bls/pr/current/pr.data.*', format => 'csv', sep => '\t', header => true)
), c AS (SELECT * FROM d WHERE f = 'pr.data.0.Current'), a AS (SELECT * FROM d WHERE f = 'pr.data.1.AllData')
SELECT (SELECT count(*) FROM c LEFT ANTI JOIN a USING (sid, yr, per))            AS current_not_in_all,
       (SELECT count(*) FROM c JOIN a USING (sid, yr, per) WHERE c.val <> a.val) AS same_key_diff_value;

-- COMMAND ----------

-- Q05 (annual average) coverage per series. Key finding: some series have ONLY Q05 (no quarterly data).
WITH a AS (
  SELECT trim(`series_id        `) sid, trim(period) per, cast(trim(year) AS INT) yr
  FROM read_files('/Volumes/workspace/data_quest/raw/bls/pr/current/pr.data.1.AllData', format => 'csv', sep => '\t', header => true)
), per_series AS (SELECT sid, count_if(per <> 'Q05') q_rows, count_if(per = 'Q05') q05_rows FROM a GROUP BY sid)
SELECT count(*) total_series, count_if(q_rows = 0) only_q05_series,
       count_if(q05_rows = 0) no_q05_series, count_if(q_rows > 0 AND q05_rows > 0) both
FROM per_series;
