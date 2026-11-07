-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 · Validate Gold against the raw files (read-only)
-- MAGIC Recomputes each answer **directly from the raw files in the Volume** with `read_files`, bypassing Bronze and Silver
-- MAGIC (no shared code with the pipeline), then compares with Gold. Every check should return true or a full match count.
-- MAGIC A second, fully external check (plain Python, live sources, no Spark) is in `validation/independent_check.py`.

-- COMMAND ----------

-- Q1: raw population JSON → mean/std dev 2013–2018 vs. Gold. Expect q1_match = true.
WITH raw_pop AS (
  SELECT CAST(r.Year AS INT) yr, CAST(r.Population AS BIGINT) pop
  FROM (SELECT explode(data) r
        FROM read_files('/Volumes/workspace/data_quest/raw/datausa/current/population.json', format => 'json', multiLine => true))
), raw_q1 AS (
  SELECT count(*) n, round(avg(pop), 2) mean_p, round(stddev_samp(pop), 2) sd_s, round(stddev_pop(pop), 2) sd_p
  FROM raw_pop WHERE yr BETWEEN 2013 AND 2018
)
SELECT r.*, g.mean_population, g.stddev_population_sample, g.stddev_population_pop,
       (r.n = g.years_count AND r.mean_p = g.mean_population
        AND r.sd_s = g.stddev_population_sample AND r.sd_p = g.stddev_population_pop) AS q1_match
FROM raw_q1 r CROSS JOIN workspace.data_quest.gold_q1_population_stats g;

-- COMMAND ----------

-- Q2: raw AllData → hybrid best year per series vs. Gold. Expect raw_series = gold_series = q2_exact_matches = 337.
WITH raw AS (
  SELECT trim(`series_id        `) sid, CAST(trim(year) AS INT) yr, trim(period) per,
         CAST(trim(`       value`) AS DECIMAL(18,3)) val
  FROM read_files('/Volumes/workspace/data_quest/raw/bls/pr/current/pr.data.1.AllData', format => 'csv', sep => '\t', header => true)
), qseries AS (SELECT DISTINCT sid FROM raw WHERE per <> 'Q05'),
yearly AS (
  SELECT sid, yr, sum(val) tot, count(*) n FROM raw
  WHERE (per <> 'Q05' AND sid IN (SELECT sid FROM qseries))
     OR (per = 'Q05' AND sid NOT IN (SELECT sid FROM qseries))
  GROUP BY sid, yr
), best AS (SELECT * FROM yearly QUALIFY row_number() OVER (PARTITION BY sid ORDER BY tot DESC, yr) = 1)
SELECT
  (SELECT count(*) FROM best) AS raw_series,
  (SELECT count(*) FROM workspace.data_quest.gold_q2_series_best_year) AS gold_series,
  (SELECT count(*) FROM best b JOIN workspace.data_quest.gold_q2_series_best_year g ON g.series_id = b.sid
     WHERE g.best_year = b.yr AND g.best_year_value = b.tot AND g.periods_in_best_year = b.n) AS q2_exact_matches;

-- COMMAND ----------

-- Q3: raw rows for PRS30006032 Q01 + raw population vs. Gold. Expect raw_rows = gold_rows = q3_exact_matches = 39.
-- <=> is NULL-safe equality (NULL <=> NULL is true), needed for years without population.
WITH raw AS (
  SELECT CAST(trim(year) AS INT) yr, CAST(trim(`       value`) AS DECIMAL(18,3)) val
  FROM read_files('/Volumes/workspace/data_quest/raw/bls/pr/current/pr.data.1.AllData', format => 'csv', sep => '\t', header => true)
  WHERE trim(`series_id        `) = 'PRS30006032' AND trim(period) = 'Q01'
), raw_pop AS (
  SELECT CAST(r.Year AS INT) yr, CAST(r.Population AS BIGINT) pop
  FROM (SELECT explode(data) r
        FROM read_files('/Volumes/workspace/data_quest/raw/datausa/current/population.json', format => 'json', multiLine => true))
), raw_q3 AS (SELECT raw.yr, raw.val, p.pop FROM raw LEFT JOIN raw_pop p ON p.yr = raw.yr)
SELECT
  (SELECT count(*) FROM raw_q3) AS raw_rows,
  (SELECT count(*) FROM workspace.data_quest.gold_q3_prs30006032_q01_population) AS gold_rows,
  (SELECT count(*) FROM raw_q3 r JOIN workspace.data_quest.gold_q3_prs30006032_q01_population g
     ON g.year = r.yr AND g.value = r.val AND g.population <=> r.pop) AS q3_exact_matches;
