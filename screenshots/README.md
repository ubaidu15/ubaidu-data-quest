# Screenshots

Evidence for reviewers who can't access the live Databricks workspace. Save each PNG in this folder with the exact name below.

**When:** after the full pipeline (Bronze, Silver, Gold) has run successfully.

## Checklist

| # | File name | Where in Databricks | What must be visible |
|---|-----------|---------------------|----------------------|
| 1 | `01_pipeline_graph.png` | Jobs & Pipelines → `data_quest_pipeline` → latest update | Full graph Bronze → Silver → Gold, status **Completed** |
| 2 | `02_pipeline_data_quality.png` | Same page → click a table (e.g. `silver_bls_data`) → **Data quality** tab | Expectations with passed/failed counts |
| 3 | `03_tables_catalog.png` | Catalog → workspace → `data_quest` | List of bronze_/silver_/gold_ tables plus the `raw` Volume |
| 4 | `04_volume_raw_files.png` | Catalog → workspace → data_quest → Volumes → `raw` → `bls/pr/current/` | The 12 landed BLS files |
| 5 | `05_ingestion_job_run.png` | Jobs & Pipelines → Job runs → `data-quest-ingestion-*` | Two successful tasks (ingest_bls → ingest_population) |
| 6 | `06_ingest_log_rerun.png` | SQL Editor: query from `notebooks/04_verify_ingestion.sql` (first cell) | new → unchanged → changed/removed across runs |
| 7 | `07_q1_population_stats.png` | SQL Editor: `SELECT * FROM workspace.data_quest.gold_q1_population_stats` | Mean and std dev |
| 8 | `08_q2_best_year.png` | SQL Editor: `SELECT series_id, series_label, best_year, best_year_value, periods_in_best_year, value_basis FROM workspace.data_quest.gold_q2_series_best_year ORDER BY series_id` | Label, best year, summed value, basis; the row count (337) visible |
| 9 | `09_q3_value_population.png` | SQL Editor: `SELECT year, value, population FROM workspace.data_quest.gold_q3_prs30006032_q01_population ORDER BY year` | 2013–2024 visible (population join plus 2020 NULL) |

The queries for 7–9 are also in `notebooks/09_gold_answers.sql`. SQL Editor: pick the **Serverless Starter Warehouse** (top right) before running.

## Tips
- Windows: **Win + Shift + S** → select area → paste into Paint → save as PNG here.
- Hide anything sensitive outside the Databricks content (other tabs, bookmarks).
