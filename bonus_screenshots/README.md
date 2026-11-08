# Bonus screenshots

Evidence for the bonus work. Save each PNG in this folder with the exact name below. Full steps are in each section of `BONUS_PROCESS.md`.

| # | File name | Where in Databricks | What must be visible |
|---|-----------|---------------------|----------------------|
| B1 | `B1_drift_failed_update.png` | Jobs & Pipelines → `data_quest_pipeline` → Run dropdown → the **FAILED** update (≈18:38) → click `bronze_bls_series` | Red `bronze_bls_series` plus the "failed an expectation check" error |
| B2 | `B2_drift_auto_recovered.png` | Same Run dropdown → next update (≈18:40, Completed), dropdown open if possible | FAILED followed by Completed (auto-retry) |
| B3 | `B3_drift_drill_notebook.png` (optional) | Job runs → `data-quest-schema-drift-drill` → task output | "✔ Safeguard works" and "✔ Recovered automatically" lines |

| B4 | `B4_health_view_ok.png` | SQL Editor: `SELECT status, issue_count, issues, hours_since_ingestion_check, days_since_bls_published, last_pipeline_state, q1_rows, q2_rows, q3_rows FROM workspace.data_quest.ops_pipeline_health;` | Row with `OK`, `0`, `[]` |
| B5 | `B5_alert_definition.png` | Alerts → `data_quest: pipeline health` (click Run/Evaluate first if available) | Query, condition `issue_count > 0`, schedule, status |
| B6 | `B6_health_view_alert_simulated.png` (optional) | SQL Editor: run the "3 DAYS" simulation from `notebooks/12_ops_health.sql`, **then restore** | `ALERT` + `INGESTION_STALE` reason |

_More rows are added as the bonus items are built (dashboard)._
