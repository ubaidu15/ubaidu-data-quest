-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 09 · Gold answers (read-only)
-- MAGIC The three quest answers, plus edge-case checks. These are the queries used for the answer screenshots.

-- COMMAND ----------

-- Q1: mean and standard deviation of US population, 2013–2018 (years_count must be 6).
SELECT * FROM workspace.data_quest.gold_q1_population_stats;

-- COMMAND ----------

-- Q2: best year per series (337 rows) with a readable label. value_basis shows quarters vs. annual-only.
SELECT series_id, series_label, best_year, best_year_value, periods_in_best_year, value_basis
FROM workspace.data_quest.gold_q2_series_best_year
ORDER BY series_id;

-- COMMAND ----------

-- Q3: PRS30006032 Q01 value by year, with population where available (NULL = no population for that year).
SELECT series_id, series_label, year, period, value, population
FROM workspace.data_quest.gold_q3_prs30006032_q01_population
ORDER BY year;

-- COMMAND ----------

-- Edge case 1: ties in Q2. Series where 2+ years share the top total (exact DECIMAL sums make ties real).
-- Rule: the earliest year wins.
WITH y AS (
  SELECT series_id, year, sum(value) AS t FROM workspace.data_quest.silver_bls_data
  WHERE NOT is_annual_average
     OR series_id NOT IN (SELECT series_id FROM workspace.data_quest.silver_bls_data WHERE NOT is_annual_average)
  GROUP BY 1, 2)
SELECT series_id, collect_list(year) AS tied_years
FROM (SELECT * FROM y QUALIFY rank() OVER (PARTITION BY series_id ORDER BY t DESC) = 1)
GROUP BY series_id HAVING count(*) > 1;

-- COMMAND ----------

-- Edge case 2: a best year built from fewer than 4 quarters (series starting mid-year).
SELECT series_id, series_label, best_year, best_year_value, periods_in_best_year
FROM workspace.data_quest.gold_q2_series_best_year
WHERE periods_in_best_year < 4 AND value_basis LIKE 'Sum%';
