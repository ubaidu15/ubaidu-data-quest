-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Verify ingestion
-- MAGIC Checks that what's in the Volume matches the manifest, and shows what each run did. Read-only.

-- COMMAND ----------

-- What did each run do? First run: everything "new". A re-run with no upstream changes: everything "unchanged".
SELECT run_id, action, count(*) AS files
FROM workspace.data_quest.ingest_log
GROUP BY run_id, action
ORDER BY run_id, action;

-- COMMAND ----------

-- Manifest vs. actual files in the Volume: size_bytes should equal bytes_in_volume for every active file.
-- read_files with binaryFile lists the files (with metadata) without parsing them.
SELECT m.source, m.file_name, m.status, m.size_bytes, f.bytes_in_volume, m.etag, m.last_modified, m.last_ingested_at
FROM workspace.data_quest.ingest_manifest m
LEFT JOIN (
  SELECT _metadata.file_name AS file_name, max(_metadata.file_size) AS bytes_in_volume
  FROM read_files('/Volumes/workspace/data_quest/raw/', format => 'binaryFile', recursiveFileLookup => true)
  GROUP BY 1
) f USING (file_name)
ORDER BY m.source, m.file_name;

-- COMMAND ----------

-- Browse the Volume folders directly (also visible in Catalog Explorer → data_quest → raw).
LIST '/Volumes/workspace/data_quest/raw/bls/pr/current/';

-- COMMAND ----------

LIST '/Volumes/workspace/data_quest/raw/datausa/current/';
