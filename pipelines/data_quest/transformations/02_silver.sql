-- SILVER layer: clean, typed, deduplicated, human-readable. Written in Spark SQL.
-- Bronze kept values as raw strings; here we trim padding, cast types, validate keys, dedupe, and decode BLS codes.
-- Expectations DROP invalid rows (counted in data quality metrics) or WARN where we only want visibility.

-- --------------------------------------------------------------------------------------------
-- BLS observations: one row per (series_id, year, period).
-- Source = pr.data.1.AllData only: profiling proved pr.data.0.Current is a pure subset (0 extra rows, 0 differing values),
-- so using both would double count. value → DECIMAL(18,3): BLS stores 3 decimals; decimal sums are exact (no float drift).
-- --------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_data (
  CONSTRAINT series_id_valid EXPECT (series_id RLIKE '^PR[SU][0-9]{8}$')                 ON VIOLATION DROP ROW,
  CONSTRAINT year_valid      EXPECT (year BETWEEN 1900 AND year(current_date()) + 1)     ON VIOLATION DROP ROW,
  CONSTRAINT period_valid    EXPECT (period IN ('Q01','Q02','Q03','Q04','Q05'))          ON VIOLATION DROP ROW,
  CONSTRAINT value_numeric   EXPECT (value IS NOT NULL)                                  ON VIOLATION DROP ROW
)
COMMENT 'Clean BLS PR observations (from pr.data.1.AllData): trimmed, typed, deduplicated. Q05 = annual average.'
AS
SELECT
  trim(series_id)                              AS series_id,
  try_cast(trim(year) AS INT)                  AS year,
  trim(period)                                 AS period,
  try_cast(trim(value) AS DECIMAL(18,3))       AS value,
  nullif(trim(footnote_codes), '')             AS footnote_codes,
  trim(period) = 'Q05'                         AS is_annual_average,
  _source_file,
  _source_modified_at
FROM bronze_bls_data
WHERE _source_file = 'pr.data.1.AllData'
-- Dedupe defensively: if a key ever appears twice, keep the row from the most recently modified file.
QUALIFY row_number() OVER (PARTITION BY trim(series_id), trim(year), trim(period) ORDER BY _source_modified_at DESC) = 1;

-- --------------------------------------------------------------------------------------------
-- Series dimension: each series_id decoded into readable text using the 7 BLS mapping files.
-- This is what makes Gold Q2 understandable to someone who doesn't know BLS codes.
-- LEFT JOINs: a missing lookup code must not drop the series. The WARN expectation flags it instead.
-- --------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_series (
  CONSTRAINT series_id_valid EXPECT (series_id RLIKE '^PR[SU][0-9]{8}$') ON VIOLATION DROP ROW,
  CONSTRAINT labels_decoded  EXPECT (sector_name IS NOT NULL AND measure_text IS NOT NULL
                                     AND duration_text IS NOT NULL AND class_text IS NOT NULL
                                     AND seasonal_text IS NOT NULL)
)
COMMENT 'BLS PR series with all codes decoded to human-readable labels (sector, measure, duration, worker class, seasonality).'
AS
SELECT
  trim(s.series_id)                            AS series_id,
  trim(s.series_title)                         AS series_title,
  trim(s.sector_code)                          AS sector_code,   trim(sec.sector_name)   AS sector_name,
  trim(s.measure_code)                         AS measure_code,  trim(m.measure_text)    AS measure_text,
  trim(s.duration_code)                        AS duration_code, trim(d.duration_text)   AS duration_text,
  trim(s.class_code)                           AS class_code,    trim(c.class_text)      AS class_text,
  trim(s.seasonal)                             AS seasonal_code, trim(sea.seasonal_text) AS seasonal_text,
  try_cast(nullif(trim(s.base_year), '-') AS INT) AS base_year,  -- '-' means "not an index series"
  try_cast(trim(s.begin_year) AS INT)          AS begin_year,
  trim(s.begin_period)                         AS begin_period,
  try_cast(trim(s.end_year) AS INT)            AS end_year,
  trim(s.end_period)                           AS end_period,
  nullif(trim(s.footnote_codes), '')           AS footnote_codes
FROM bronze_bls_series s
LEFT JOIN bronze_bls_sector   sec ON trim(s.sector_code)   = trim(sec.sector_code)
LEFT JOIN bronze_bls_measure  m   ON trim(s.measure_code)  = trim(m.measure_code)
LEFT JOIN bronze_bls_duration d   ON trim(s.duration_code) = trim(d.duration_code)
LEFT JOIN bronze_bls_class    c   ON trim(s.class_code)    = trim(c.class_code)
LEFT JOIN bronze_bls_seasonal sea ON trim(s.seasonal)      = trim(sea.seasonal_code);

-- --------------------------------------------------------------------------------------------
-- US population by year. Population arrives as a float (316128839.0), so cast to BIGINT. One row per year.
-- --------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_population (
  CONSTRAINT year_valid         EXPECT (year BETWEEN 1900 AND year(current_date()) + 1) ON VIOLATION DROP ROW,
  CONSTRAINT population_positive EXPECT (population > 0)                               ON VIOLATION DROP ROW,
  CONSTRAINT nation_is_us        EXPECT (nation_id = '01000US')
)
COMMENT 'US total population by year (Census ACS 1-year via DataUSA). 2020 is absent at source (no ACS 1-year release).'
AS
SELECT
  nation_id,
  nation,
  CAST(year AS INT)                AS year,
  CAST(population AS BIGINT)       AS population,
  _source_file,
  _source_modified_at
FROM bronze_population
QUALIFY row_number() OVER (PARTITION BY year ORDER BY _source_modified_at DESC) = 1;
