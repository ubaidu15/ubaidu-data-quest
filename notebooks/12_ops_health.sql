-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 12 · Ops health view + alert (freshness and failure monitoring)
-- MAGIC One-row health check that a Databricks SQL Alert evaluates daily (definition: `alerts/pipeline_health_alert.json`).
-- MAGIC Signals: ingestion freshness (manifest), BLS source freshness (server Last-Modified), last pipeline outcome
-- MAGIC (pipeline `event_log`), Gold completeness. `status` = OK / ALERT; `issues` says why.
-- MAGIC `system.lakeflow` job tables aren't readable for this account, so pipeline health comes from the pipeline's own event log.

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.data_quest.ops_pipeline_health
COMMENT 'One-row health check for the Data Quest product: ingestion freshness, source freshness, last pipeline outcome, Gold completeness. status = OK or ALERT; issues lists why.'
AS
WITH ingest AS (                                   -- freshness of OUR ingestion and of the BLS source itself
  SELECT max(last_checked_at)                                    AS last_ingestion_check_at,
         max(CASE WHEN source = 'bls_pr' THEN last_modified END) AS bls_source_last_modified
  FROM workspace.data_quest.ingest_manifest
  WHERE status = 'active'
),
pipe AS (                                          -- final state of each pipeline update, from the event log
  SELECT max_by(final_state, ts)                              AS last_pipeline_state,
         max(ts)                                              AS last_pipeline_update_at,
         max(CASE WHEN final_state = 'COMPLETED' THEN ts END) AS last_successful_refresh_at
  FROM (
    SELECT origin.update_id, max(timestamp) AS ts,
           max_by(details:update_progress.state::string, timestamp) AS final_state
    FROM event_log('e1054e19-a357-4a09-b971-4875982328b1')
    WHERE event_type = 'update_progress'
    GROUP BY origin.update_id
  )
  WHERE final_state IN ('COMPLETED', 'FAILED', 'CANCELED')
),
gold AS (                                          -- Gold must be populated (Q1 exactly 1 row)
  SELECT (SELECT count(*) FROM workspace.data_quest.gold_q1_population_stats)           AS q1_rows,
         (SELECT count(*) FROM workspace.data_quest.gold_q2_series_best_year)           AS q2_rows,
         (SELECT count(*) FROM workspace.data_quest.gold_q3_prs30006032_q01_population) AS q3_rows
),
signals AS (
  SELECT current_timestamp()                                                             AS checked_at,
         i.last_ingestion_check_at,
         round((unix_timestamp() - unix_timestamp(i.last_ingestion_check_at)) / 3600, 1) AS hours_since_ingestion_check,
         i.bls_source_last_modified,
         datediff(current_date(), to_date(i.bls_source_last_modified))                  AS days_since_bls_published,
         p.last_pipeline_state, p.last_pipeline_update_at, p.last_successful_refresh_at,
         g.q1_rows, g.q2_rows, g.q3_rows
  FROM ingest i CROSS JOIN pipe p CROSS JOIN gold g
)
SELECT *, size(issues) AS issue_count, CASE WHEN size(issues) = 0 THEN 'OK' ELSE 'ALERT' END AS status
FROM (
  SELECT *,
         filter(array(                             -- each rule adds a readable reason when it fires
           CASE WHEN hours_since_ingestion_check > 48   THEN 'INGESTION_STALE: no ingestion check in >48h (daily job not running?)' END,
           CASE WHEN days_since_bls_published   > 100  THEN 'BLS_SOURCE_STALE: BLS data unchanged >100 days (normal cadence ~6 weeks)' END,
           CASE WHEN last_pipeline_state <> 'COMPLETED' THEN concat('PIPELINE_LAST_UPDATE_', last_pipeline_state) END,
           CASE WHEN q1_rows <> 1 OR q2_rows = 0 OR q3_rows = 0 THEN 'GOLD_INCOMPLETE: a Gold table is empty or malformed' END
         ), x -> x IS NOT NULL) AS issues
  FROM signals
);

-- COMMAND ----------

-- Current health (this is what the alert evaluates: it triggers when issue_count > 0).
SELECT status, issue_count, issues, hours_since_ingestion_check, days_since_bls_published,
       last_pipeline_state, last_successful_refresh_at, q1_rows, q2_rows, q3_rows
FROM workspace.data_quest.ops_pipeline_health;

-- COMMAND ----------

-- Proof the alert logic fires: pretend ingestion last ran 3 days ago → expect ALERT / INGESTION_STALE.
UPDATE workspace.data_quest.ingest_manifest SET last_checked_at = last_checked_at - INTERVAL 3 DAYS;
SELECT status, issue_count, issues, hours_since_ingestion_check FROM workspace.data_quest.ops_pipeline_health;

-- COMMAND ----------

-- Restore exactly (inverse of the update above) → back to OK.
UPDATE workspace.data_quest.ingest_manifest SET last_checked_at = last_checked_at + INTERVAL 3 DAYS;
SELECT status, issue_count, hours_since_ingestion_check FROM workspace.data_quest.ops_pipeline_health;
