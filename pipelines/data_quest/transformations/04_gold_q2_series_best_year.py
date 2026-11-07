"""
GOLD Q2 (primary: PySpark). Alternative in Spark SQL: notebooks/08_gold_alternatives.py

Question: for every series_id, the best year = the year with the largest sum of `value` across its quarters,
plus a human-readable label.

Hybrid rule for Q05 (annual average), decided after profiling:
  * 283 series have quarters Q01–Q04 AND a Q05 annual average → sum Q01–Q04 only (adding Q05 would double count).
  * 54 series publish ONLY Q05 (annual-only Manufacturing series, per pr.txt) → use Q05, so every series is answered.
  `value_basis` records which rule applied. `periods_in_best_year` shows how many rows were summed
  (a partial year, e.g. the current year with 3 quarters, can be seen at a glance).
Ties (two years with the same total): the earliest year wins (deterministic).
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F


@dp.materialized_view(
    name="gold_q2_series_best_year",
    comment="Q2: best year (largest summed value) per BLS PR series, with human-readable labels.")
@dp.expect_or_fail("best_year_present", "best_year IS NOT NULL AND best_year_value IS NOT NULL")
@dp.expect("label_present", "series_label IS NOT NULL")
def gold_q2_series_best_year():
    data = spark.read.table("silver_bls_data")

    # 1) Does this series have any quarterly rows? (window over the series, so no extra join needed)
    by_series = Window.partitionBy("series_id")
    data = data.withColumn(
        "has_quarterly", F.max(F.when(~F.col("is_annual_average"), 1).otherwise(0)).over(by_series) == 1)

    # 2) Keep the rows the hybrid rule says to sum: quarters if the series has them, else the annual average.
    basis_rows = data.where(
        (F.col("has_quarterly") & ~F.col("is_annual_average")) |
        (~F.col("has_quarterly") & F.col("is_annual_average")))

    # 3) Sum per series per year.
    yearly = (basis_rows.groupBy("series_id", "year", "has_quarterly")
              .agg(F.sum("value").alias("total_value"),
                   F.count("*").alias("periods_summed")))

    # 4) Rank years within each series: highest total first; tie → earliest year. Keep rank 1.
    rank_years = Window.partitionBy("series_id").orderBy(F.desc("total_value"), F.asc("year"))
    best = (yearly.withColumn("rn", F.row_number().over(rank_years))
                  .where("rn = 1")
                  .select("series_id",
                          F.col("year").alias("best_year"),
                          F.col("total_value").alias("best_year_value"),
                          F.col("periods_summed").alias("periods_in_best_year"),
                          F.when(F.col("has_quarterly"), F.lit("Sum of quarters Q01-Q04"))
                           .otherwise(F.lit("Annual average Q05 (series has no quarterly data)"))
                           .alias("value_basis")))

    # 5) Attach readable labels from the decoded series dimension.
    series = spark.read.table("silver_bls_series").select(
        "series_id",
        F.col("series_title").alias("series_label"),
        "sector_name", "measure_text", "duration_text", "class_text", "seasonal_text")

    return (best.join(series, "series_id", "left")
                .select("series_id", "series_label", "best_year", "best_year_value", "periods_in_best_year",
                        "value_basis", "sector_name", "measure_text", "duration_text", "class_text",
                        "seasonal_text"))
