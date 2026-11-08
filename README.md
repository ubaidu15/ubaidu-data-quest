# Rearc Data Quest: Databricks Edition

End-to-end Databricks solution: **idempotent ingestion** of the BLS productivity time series and the DataUSA population API into a Unity Catalog **Volume**, a **Spark Declarative Pipeline** (Bronze → Silver → Gold) with data-quality expectations, and the three analytical questions answered in **both Spark SQL and PySpark**. The whole flow runs as one orchestrated job.

```
BLS pr/ folder ─┐   idempotent ingestion       UC Volume                 Spark Declarative Pipeline
DataUSA API  ───┴─► (manifest + hashing) ───►  raw/…/current/  ───►  Bronze ──► Silver ──► Gold (Q1, Q2, Q3)
                     notebooks/02, 03            + _archive/            PySpark     SQL      SQL / PySpark / SQL
                    └──────────── job: data_quest_daily (pipeline runs only if a source changed) ────────────┘
```

## Answers at a glance
| Question | Answer | Gold table |
|---|---|---|
| **Q1** Mean and std dev of US population, 2013–2018 | Mean **322,069,808** · Std dev **4,158,441.04** (sample; population std dev 3,796,119.94) | `gold_q1_population_stats` |
| **Q2** Best year per `series_id` (largest summed value) | **337 series**, each with a human-readable label | `gold_q2_series_best_year` |
| **Q3** `PRS30006032` · `Q01` value per year + population | **39 years** (1988–2026); population where available | `gold_q3_prs30006032_q01_population` |

All objects live in Unity Catalog under **`workspace.data_quest`**.

## Write-ups
- **[PROCESS.md](PROCESS.md):** architecture, SQL vs PySpark choices, re-run safety, assumptions, trade-offs, retrospective.
- **[BONUS_PROCESS.md](BONUS_PROCESS.md):** dashboard, freshness/failure alert, schema-drift drill.

---

## Repository layout
```
├── notebooks/                      # Databricks notebooks (source format; open directly in a Git folder)
│   ├── 00_setup_schema_volume.sql      schema, Volume, ingest_manifest, ingest_log
│   ├── 01_explore_sources.py           read-only look at BLS + population API
│   ├── 02_ingest_bls.py                Task 1: idempotent BLS sync → Volume
│   ├── 03_ingest_population.py         Task 1: population JSON → Volume
│   ├── 04_verify_ingestion.sql         manifest vs Volume, actions per run
│   ├── 05_test_rerun_scenarios.py      proof: changed / metadata_only / removed / unchanged
│   ├── 06_profile_raw.sql              profiling that drove the layer design
│   ├── 07_inspect_silver.sql           Silver spot checks
│   ├── 08_gold_alternatives.py         Task 2: alternative implementations, asserted == Gold
│   ├── 09_gold_answers.sql             the three answers + edge cases
│   ├── 10_validate_from_raw.sql        recompute answers from raw files (bypassing the pipeline)
│   ├── 11_schema_drift_drill.py        Bonus: schema-drift drill
│   └── 12_ops_health.sql               Bonus: health view for the alert
├── pipelines/data_quest/transformations/   # Task 2: Spark Declarative Pipeline source
│   ├── 01_bronze.py                    Bronze (PySpark)
│   ├── 02_silver.sql                   Silver (Spark SQL)
│   ├── 03_gold_q1_population_stats.sql Gold Q1 (Spark SQL, primary)
│   ├── 04_gold_q2_series_best_year.py  Gold Q2 (PySpark, primary)
│   └── 05_gold_q3_series_population.sql Gold Q3 (Spark SQL, primary)
├── jobs/data_quest_daily.json      # orchestrated job definition
├── validation/independent_check.py # plain-Python recompute of all answers (no Spark)
├── dashboards/                     # Bonus: AI/BI dashboard (build script + generated definition)
├── alerts/pipeline_health_alert.json   # Bonus: SQL alert definition
├── screenshots/                    # evidence for Tasks 1–2 (see screenshots/README.md)
├── bonus_screenshots/              # evidence for bonus work (see bonus_screenshots/README.md)
├── PROCESS.md
└── BONUS_PROCESS.md
```

---

## Task 1: Source it (ingestion into a Volume)

### Where to look
| Requirement | Where it's implemented | Evidence |
|---|---|---|
| Pull **all** files in the BLS `pr/` folder (not just data) | [`notebooks/02_ingest_bls.py`](notebooks/02_ingest_bls.py): parses the directory listing every run (no hardcoded filenames) | 12 files in `/Volumes/workspace/data_quest/raw/bls/pr/current/` |
| Resolve the **403** the way BLS wants | `User-Agent` with contact info, per the [BLS access policy](https://www.bls.gov/bls/pss.htm); polite retries/backoff | `notebooks/02_ingest_bls.py` (config cell) |
| **Don't reprocess** what's already ingested | `ingest_manifest` Delta table; HEAD (ETag + Last-Modified) → SHA-256 → write only if bytes changed | [`screenshots/06_ingest_log_rerun.png`](screenshots/06_ingest_log_rerun.png): re-run = all `unchanged` |
| Handle source **added / changed / removed** data | `current/` mirror + `_archive/<run_id>/`; removed files archived and marked `removed` | [`notebooks/05_test_rerun_scenarios.py`](notebooks/05_test_rerun_scenarios.py) |
| Population API saved as **JSON in the same Volume** | [`notebooks/03_ingest_population.py`](notebooks/03_ingest_population.py): validated, canonical-hash change detection | `/Volumes/workspace/data_quest/raw/datausa/current/population.json` |
| Orchestration, and skip work when nothing changed | [`jobs/data_quest_daily.json`](jobs/data_quest_daily.json): task values + if/else condition | [`screenshots/05_job_graph_changed.png`](screenshots/05_job_graph_changed.png), [`05b_job_graph_skipped.png`](screenshots/05b_job_graph_skipped.png) |

### How to run Task 1
1. **One-time setup:** run [`notebooks/00_setup_schema_volume.sql`](notebooks/00_setup_schema_volume.sql) (creates `workspace.data_quest`, the `raw` Volume and the state tables; safe to re-run).
2. **Ingest:** run [`02_ingest_bls`](notebooks/02_ingest_bls.py), then [`03_ingest_population`](notebooks/03_ingest_population.py) on **Serverless** compute, or run the job `data_quest_daily`.
3. **Verify:** run [`04_verify_ingestion.sql`](notebooks/04_verify_ingestion.sql). The manifest size equals the Volume size for every file; a second run shows every file `unchanged`.
4. *(Optional proof)* [`05_test_rerun_scenarios.py`](notebooks/05_test_rerun_scenarios.py) simulates changed / metadata-only / removed files and re-runs ingestion.

---

## Task 2: Spark Declarative Pipeline (Bronze / Silver / Gold)

### Where to look
| Layer | File | Language | Tables |
|---|---|---|---|
| **Bronze** | [`01_bronze.py`](pipelines/data_quest/transformations/01_bronze.py) | PySpark | `bronze_bls_data`, `bronze_bls_series`, 7 × `bronze_bls_<lookup>`, `bronze_population` |
| **Silver** | [`02_silver.sql`](pipelines/data_quest/transformations/02_silver.sql) | Spark SQL | `silver_bls_data`, `silver_bls_series` (decoded labels), `silver_population` |
| **Gold** | `03_…q1…sql`, `04_…q2…py`, `05_…q3…sql` | SQL / PySpark / SQL | the three answer tables |

**Expectations (data quality):** schema mismatch → **FAIL** the update; null or invalid keys → **DROP** the row; unexpected-but-valid → **WARN**. Shown in [`screenshots/02_pipeline_data_quality.png`](screenshots/02_pipeline_data_quality.png).

### Each question in both languages
| Question | Primary (feeds the Gold table) | Documented alternative |
|---|---|---|
| Q1 | Spark SQL: [`03_gold_q1_population_stats.sql`](pipelines/data_quest/transformations/03_gold_q1_population_stats.sql) | PySpark: [`08_gold_alternatives.py`](notebooks/08_gold_alternatives.py) |
| Q2 | PySpark: [`04_gold_q2_series_best_year.py`](pipelines/data_quest/transformations/04_gold_q2_series_best_year.py) | Spark SQL: [`08_gold_alternatives.py`](notebooks/08_gold_alternatives.py) |
| Q3 | Spark SQL: [`05_gold_q3_series_population.sql`](pipelines/data_quest/transformations/05_gold_q3_series_population.sql) | PySpark: [`08_gold_alternatives.py`](notebooks/08_gold_alternatives.py) |

Each alternative is asserted **row-for-row identical** to its Gold table (two-way `exceptAll`). The check runs as a task in the job after every refresh.

### Key interpretation choices (details in PROCESS.md)
- **Q1:** both sample (n-1) and population (n) standard deviations are reported, labelled.
- **Q2:** `Q05` is an annual *average*, not a quarter. I sum Q01–Q04, and use Q05 only for the 54 series that publish nothing else (`value_basis` says which). Ties go to the earliest year.
- **Q3:** a LEFT JOIN keeps all years; population is NULL where Census has none (pre-2013, **2020**, 2025+).

### How to run Task 2
1. **Pipeline:** Jobs & Pipelines → **`data_quest_pipeline`** → **Run pipeline**. To recreate it: ETL pipeline, Serverless, catalog `workspace`, schema `data_quest`, source folder `pipelines/data_quest/transformations/`, configuration `data_quest.raw_root = /Volumes/workspace/data_quest/raw`.
2. **Answers:** run [`notebooks/09_gold_answers.sql`](notebooks/09_gold_answers.sql).
3. **Cross-language check:** run [`notebooks/08_gold_alternatives.py`](notebooks/08_gold_alternatives.py) (passes = all three identical).
4. **Independent validation:**
   - [`notebooks/10_validate_from_raw.sql`](notebooks/10_validate_from_raw.sql) recomputes from the raw files.
   - `python validation/independent_check.py` recomputes in plain Python from the live sources, without Spark.

### Evidence (screenshots/)
| What | File |
|---|---|
| Pipeline graph | [`01_pipeline_graph.png`](screenshots/01_pipeline_graph.png) |
| Data quality (expectations) | [`02_pipeline_data_quality.png`](screenshots/02_pipeline_data_quality.png) |
| Tables in Unity Catalog | [`03_tables_catalog.png`](screenshots/03_tables_catalog.png) |
| Q1 output | [`07_q1_population_stats.png`](screenshots/07_q1_population_stats.png) |
| Q2 output | [`08_q2_best_year.png`](screenshots/08_q2_best_year.png) |
| Q3 output | [`09_q3_value_population.png`](screenshots/09_q3_value_population.png) |

---

## Bonus work

### 1. AI/BI dashboard on Gold
| | |
|---|---|
| **What** | 4-page published dashboard for non-technical stakeholders: freshness tiles, Q1 headline numbers, Q2 with Sector/Measure filters and search, Q3 trend, "About the data" |
| **Where** | [`dashboards/build_dashboard.py`](dashboards/build_dashboard.py) generates [`dashboards/data_quest_dashboard.lvdash.json`](dashboards/data_quest_dashboard.lvdash.json) |
| **How to open / recreate** | Databricks → **Dashboards** → *Data Quest - US Productivity & Population*. To recreate: Dashboards → ⋮ → **Import dashboard from file** → the `.lvdash.json` → Publish |
| **Evidence** | [`B7_dashboard_overview.png`](bonus_screenshots/B7_dashboard_overview.png), [`B8_dashboard_best_year.png`](bonus_screenshots/B8_dashboard_best_year.png), [`B9_dashboard_hours_population.png`](bonus_screenshots/B9_dashboard_hours_population.png) |

### 2. Freshness and failure alert
| | |
|---|---|
| **What** | `ops_pipeline_health` view (ingestion > 48 h stale, BLS > 100 days unchanged, last pipeline update failed, Gold incomplete), plus a daily **SQL Alert** when `issue_count > 0` |
| **Where** | [`notebooks/12_ops_health.sql`](notebooks/12_ops_health.sql) (view and a fire/restore simulation), [`alerts/pipeline_health_alert.json`](alerts/pipeline_health_alert.json) |
| **How to run** | SQL Editor: `SELECT * FROM workspace.data_quest.ops_pipeline_health;`. Alert: Databricks → **Alerts** → *data_quest: pipeline health* |
| **Evidence** | [`B4_health_view_ok.png`](bonus_screenshots/B4_health_view_ok.png) |

### 3. Schema-drift drill
| | |
|---|---|
| **What** | Renames a column in the Volume copy of `pr.series`, asserts the pipeline **fails on `expected_schema`**, restores the file (`finally`), and asserts recovery. Production mode auto-retries and heals itself |
| **Where** | [`notebooks/11_schema_drift_drill.py`](notebooks/11_schema_drift_drill.py) |
| **How to run** | Open the notebook on Serverless → **Run all** (about 5 minutes; BLS is never touched) |
| **Evidence** | [`B1_drift_failed_update.png`](bonus_screenshots/B1_drift_failed_update.png), [`B2_drift_auto_recovered.png`](bonus_screenshots/B2_drift_auto_recovered.png) |

---

## Running it in another workspace
1. **Databricks:** Workspace → Create → **Git folder** → this repository's URL.
2. Run `notebooks/00_setup_schema_volume.sql`.
3. Create the pipeline (settings in *How to run Task 2*), then the job from [`jobs/data_quest_daily.json`](jobs/data_quest_daily.json). Update the notebook paths and the `pipeline_id` for your workspace.
4. Run the job **`data_quest_daily`**: ingest → (if changed) pipeline → cross-language check.
5. *(Bonus)* Run `notebooks/12_ops_health.sql`, create the alert from `alerts/`, and import the dashboard from `dashboards/`.

**Requirements:** a Databricks workspace with Unity Catalog and serverless compute (Free Edition works). Before ingesting, change the BLS `User-Agent` contact to your own.
