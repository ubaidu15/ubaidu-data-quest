# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Test re-run scenarios (idempotency proof)
# MAGIC Simulates upstream changes **in our own copy** (BLS is never touched), then re-runs ingestion and checks the result.
# MAGIC Expected after re-running `02_ingest_bls` + `03_ingest_population`:
# MAGIC | file | simulated as | expected action |
# MAGIC |---|---|---|
# MAGIC | `pr.series` | we hold an older version (old ETag + old hash) | `changed` (re-downloaded, old copy archived) |
# MAGIC | `pr.class` | server version ID moved, bytes identical | `metadata_only` (downloaded, not rewritten) |
# MAGIC | `population.json` | we hold an older version (old hash) | `changed` |
# MAGIC | `pr.simulated_removed` | a file we have that BLS no longer lists | `removed` (moved to `_archive`) |
# MAGIC | all other files | nothing changed | `unchanged` (not even downloaded) |

# COMMAND ----------

# Scenario setup 1: pretend our manifest holds older versions.
spark.sql("""
UPDATE workspace.data_quest.ingest_manifest
SET etag   = CASE WHEN file_name IN ('pr.series','pr.class') THEN '"simulated-old"' ELSE etag END,
    sha256 = CASE WHEN file_name IN ('pr.series','population.json') THEN 'simulated-old' ELSE sha256 END
WHERE file_name IN ('pr.series','pr.class','population.json')
""")

# COMMAND ----------

# Scenario setup 2: a file that "used to exist" at BLS. Put it in the Volume and the manifest as active.
path = "/Volumes/workspace/data_quest/raw/bls/pr/current/pr.simulated_removed"
dbutils.fs.put(path, "simulated removed file\n", overwrite=True)
spark.sql(f"""
INSERT INTO workspace.data_quest.ingest_manifest VALUES
('bls_pr','pr.simulated_removed','https://download.bls.gov/pub/time.series/pr/pr.simulated_removed','"x"',
 TIMESTAMP'2020-01-01 00:00:00', 25, 'simulated', '{path}', 'active',
 current_timestamp(), current_timestamp(), current_timestamp())
""")

# COMMAND ----------

# Re-run ingestion (same notebooks as production). dbutils.notebook.run executes them as child runs.
dbutils.notebook.run("./02_ingest_bls", 600)
dbutils.notebook.run("./03_ingest_population", 300)

# COMMAND ----------

# Check: only the simulated files should show a non-"unchanged" action in the latest runs.
display(spark.sql("""
SELECT source, file_name, action FROM workspace.data_quest.ingest_log
WHERE run_id >= (SELECT max(run_id) FROM workspace.data_quest.ingest_log WHERE source = 'bls_pr')
  AND action <> 'unchanged'
ORDER BY file_name
"""))

# COMMAND ----------

# Check the archive. Note: read_files/Spark skip folders starting with "_" (treated as hidden), so list with dbutils instead.
for d in ["/Volumes/workspace/data_quest/raw/bls/pr/_archive/", "/Volumes/workspace/data_quest/raw/datausa/_archive/"]:
    for run in dbutils.fs.ls(d):
        display(dbutils.fs.ls(run.path))
