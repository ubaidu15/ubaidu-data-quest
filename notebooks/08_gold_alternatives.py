# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Gold alternatives: each question in the *other* language, proven identical
# MAGIC The quest asks for every analytical question in **both** Spark SQL and PySpark. One version feeds the Gold table
# MAGIC (the pipeline); the other lives here as a documented alternative.
# MAGIC
# MAGIC | Question | Primary (pipeline → Gold table) | Alternative (this notebook) |
# MAGIC |---|---|---|
# MAGIC | Q1 population mean/std dev | Spark SQL: `03_gold_q1_population_stats.sql` | **PySpark** |
# MAGIC | Q2 best year per series | PySpark: `04_gold_q2_series_best_year.py` | **Spark SQL** |
# MAGIC | Q3 PRS30006032 Q01 + population | Spark SQL: `05_gold_q3_series_population.sql` | **PySpark** |
# MAGIC
# MAGIC Every alternative reads the same Silver tables and is compared **row by row** with its Gold table.
# MAGIC Any difference raises an error, so a green run = both implementations agree.

# COMMAND ----------

# Comparison helper: rows in A but not in B, and vice versa. exceptAll keeps duplicates, so counts must match exactly.
from pyspark.sql import DataFrame, Window, functions as F

S = "workspace.data_quest"

def assert_identical(alt: DataFrame, gold_table: str) -> None:
    gold = spark.table(f"{S}.{gold_table}")
    alt = alt.select(*gold.columns)                     # same column order as the Gold table
    only_alt, only_gold = alt.exceptAll(gold).count(), gold.exceptAll(alt).count()
    if only_alt or only_gold:
        raise AssertionError(f"{gold_table}: {only_alt} rows only in alternative, {only_gold} only in Gold")
    print(f"✔ {gold_table}: alternative == Gold ({gold.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 · PySpark alternative
# MAGIC Filter 2013–2018 → aggregate. `F.stddev_samp` (n-1) is what SQL's `stddev_samp` / `stddev` compute; `F.stddev_pop` divides by n.
# MAGIC *Why SQL is primary:* one filter + one aggregate reads most clearly as SQL; PySpark adds no value here.

# COMMAND ----------

q1_alt = (spark.table(f"{S}.silver_population")
          .where(F.col("year").between(2013, 2018))
          .agg(F.count("*").alias("years_count"),
               F.round(F.avg("population"), 2).alias("mean_population"),
               F.round(F.stddev_samp("population"), 2).alias("stddev_population_sample"),
               F.round(F.stddev_pop("population"), 2).alias("stddev_population_pop"))
          .select(F.lit(2013).alias("start_year"), F.lit(2018).alias("end_year"), "*"))

display(q1_alt)
assert_identical(q1_alt, "gold_q1_population_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 · Spark SQL alternative
# MAGIC Same hybrid rule and tie-break as the PySpark primary, written as a chain of CTEs:
# MAGIC `flagged` (has_quarterly via window) → `basis` (quarters, or Q05 for annual-only series) → `yearly` (sum per year)
# MAGIC → `best` (`QUALIFY row_number()` by total DESC, year ASC) → join labels.
# MAGIC *Why PySpark is primary:* five dependent steps are easier to read, name, and unit-test as DataFrame variables;
# MAGIC this SQL shows the same logic is just as expressible with CTEs + `QUALIFY`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW q2_alt AS
# MAGIC WITH flagged AS (
# MAGIC   SELECT *,
# MAGIC          max(CASE WHEN NOT is_annual_average THEN 1 ELSE 0 END) OVER (PARTITION BY series_id) = 1 AS has_quarterly
# MAGIC   FROM workspace.data_quest.silver_bls_data
# MAGIC ),
# MAGIC basis AS (
# MAGIC   SELECT * FROM flagged
# MAGIC   WHERE (has_quarterly AND NOT is_annual_average)        -- series with quarters: sum Q01–Q04 only
# MAGIC      OR (NOT has_quarterly AND is_annual_average)        -- annual-only series: use Q05
# MAGIC ),
# MAGIC yearly AS (
# MAGIC   SELECT series_id, year, has_quarterly, sum(value) AS total_value, count(*) AS periods_summed
# MAGIC   FROM basis GROUP BY series_id, year, has_quarterly
# MAGIC ),
# MAGIC best AS (
# MAGIC   SELECT * FROM yearly
# MAGIC   QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY total_value DESC, year ASC) = 1   -- tie → earliest year
# MAGIC )
# MAGIC SELECT b.series_id,
# MAGIC        s.series_title    AS series_label,
# MAGIC        b.year            AS best_year,
# MAGIC        b.total_value     AS best_year_value,
# MAGIC        b.periods_summed  AS periods_in_best_year,
# MAGIC        CASE WHEN b.has_quarterly THEN 'Sum of quarters Q01-Q04'
# MAGIC             ELSE 'Annual average Q05 (series has no quarterly data)' END AS value_basis,
# MAGIC        s.sector_name, s.measure_text, s.duration_text, s.class_text, s.seasonal_text
# MAGIC FROM best b
# MAGIC LEFT JOIN workspace.data_quest.silver_bls_series s ON s.series_id = b.series_id;
# MAGIC
# MAGIC SELECT * FROM q2_alt ORDER BY series_id;

# COMMAND ----------

assert_identical(spark.table("q2_alt"), "gold_q2_series_best_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 · PySpark alternative
# MAGIC Filter the series and period → left join population (keeps years without population) → left join the label.
# MAGIC *Why SQL is primary:* a filter plus two joins is the textbook case for SQL; the DataFrame version is equivalent.

# COMMAND ----------

data   = spark.table(f"{S}.silver_bls_data")
pop    = spark.table(f"{S}.silver_population").select("year", "population")
series = spark.table(f"{S}.silver_bls_series").select("series_id", F.col("series_title").alias("series_label"))

q3_alt = (data.where((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
              .join(pop, "year", "left")          # "where available": no population → NULL, the row is kept
              .join(series, "series_id", "left")
              .select("series_id", "series_label", "year", "period", "value", "population"))

display(q3_alt.orderBy("year"))
assert_identical(q3_alt, "gold_q3_prs30006032_q01_population")
