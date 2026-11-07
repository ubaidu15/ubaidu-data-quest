-- GOLD Q1 (primary: Spark SQL). Alternative in PySpark: notebooks/08_gold_alternatives.py
-- Question: mean and standard deviation of the annual US population across 2013–2018 inclusive.
-- Both standard deviations are given and labelled:
--   * stddev_samp (sample, divides by n-1): Spark's default `stddev`. The usual choice for "standard deviation".
--   * stddev_pop  (population, divides by n): if the 6 years are treated as the complete set.
-- years_count makes the answer self-checking: it must be 6 (all years present).

CREATE OR REFRESH MATERIALIZED VIEW gold_q1_population_stats (
  CONSTRAINT all_six_years_present EXPECT (years_count = 6)
)
COMMENT 'Q1: mean and standard deviation of annual US population, 2013–2018 inclusive.'
AS
SELECT
  2013                                     AS start_year,
  2018                                     AS end_year,
  count(*)                                 AS years_count,
  round(avg(population), 2)                AS mean_population,
  round(stddev_samp(population), 2)        AS stddev_population_sample,
  round(stddev_pop(population), 2)         AS stddev_population_pop
FROM silver_population
WHERE year BETWEEN 2013 AND 2018;
