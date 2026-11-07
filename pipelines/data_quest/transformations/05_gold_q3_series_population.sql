-- GOLD Q3 (primary: Spark SQL). Alternative in PySpark: notebooks/08_gold_alternatives.py
-- Question: for series_id = PRS30006032 and period = Q01, the value each year, joined with that year's
-- population where available.
-- LEFT JOIN = "where available": every BLS year is kept; population is NULL for years the API doesn't cover
-- (before 2013, 2020 which has no ACS 1-year release, and years after the latest release).

CREATE OR REFRESH MATERIALIZED VIEW gold_q3_prs30006032_q01_population (
  CONSTRAINT value_present EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Q3: PRS30006032 (Manufacturing hours worked, % change from previous quarter) Q01 value by year, with US population where available.'
AS
SELECT
  d.series_id,
  s.series_title  AS series_label,
  d.year,
  d.period,
  d.value,
  p.population
FROM silver_bls_data d
LEFT JOIN silver_population p ON p.year = d.year
LEFT JOIN silver_bls_series s ON s.series_id = d.series_id
WHERE d.series_id = 'PRS30006032'
  AND d.period = 'Q01';
