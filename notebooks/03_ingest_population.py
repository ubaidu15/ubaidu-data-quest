# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ingest DataUSA population JSON into the Volume (idempotent)
# MAGIC Saves the API response to `/Volumes/workspace/data_quest/raw/datausa/current/population.json`.
# MAGIC - The API has no ETag/Last-Modified, so we always fetch it (about 1.4 KB) and **write only if the content hash changed**.
# MAGIC - The response is validated before landing, so a bad or empty response never overwrites good data.
# MAGIC - The previous version is archived on change. State goes in the same `ingest_manifest` / `ingest_log` as BLS.

# COMMAND ----------

# Config. Same tables and Volume as the BLS ingestion; a different source key.
import hashlib, json, os, shutil
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCE      = "datausa_population"
FILE_NAME   = "population.json"
API_URL     = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
               "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")
VOLUME_ROOT = "/Volumes/workspace/data_quest/raw"
CURRENT_DIR = f"{VOLUME_ROOT}/datausa/current"
ARCHIVE_DIR = f"{VOLUME_ROOT}/datausa/_archive"
PATH        = f"{CURRENT_DIR}/{FILE_NAME}"
MANIFEST    = "workspace.data_quest.ingest_manifest"
LOG         = "workspace.data_quest.ingest_log"
HEADERS     = {"User-Agent": "ubaidu-data-quest (ubaidurahman15@gmail.com)"}  # identify ourselves here too (good practice)

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)
RUN_ID = RUN_TS.strftime("%Y%m%dT%H%M%SZ")
os.makedirs(CURRENT_DIR, exist_ok=True)

# COMMAND ----------

# Fetch with retries on 429/5xx (same policy as BLS).
session = requests.Session()
session.headers.update(HEADERS)
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"], respect_retry_after_header=True)))

resp = session.get(API_URL, timeout=60)
resp.raise_for_status()
raw = resp.content

# COMMAND ----------

# Validate before landing: must be JSON with a non-empty "data" list holding the expected fields.
payload = json.loads(raw)
records = payload.get("data")
if not records:
    raise RuntimeError("Population API returned no records. Aborting, keeping the previous file.")
missing = {"Year", "Population"} - set(records[0])
if missing:
    raise RuntimeError(f"Population API schema changed; missing fields: {missing}")
print(f"{len(records)} records, years {min(r['Year'] for r in records)}–{max(r['Year'] for r in records)}")

# COMMAND ----------

# Hash a canonical form (sorted keys, no whitespace), so a cosmetic change in formatting doesn't count as "changed".
# The raw bytes are still what gets landed.
digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

old = next(iter(r.asDict() for r in spark.table(MANIFEST)
                .where(f"source = '{SOURCE}' AND file_name = '{FILE_NAME}'").collect()), None)

if old and old["sha256"] == digest and os.path.exists(PATH):
    action, ingested_at = "unchanged", old["last_ingested_at"]
else:
    action = "changed" if old else "new"
    if os.path.exists(PATH):                       # keep the previous version for audit and rollback
        os.makedirs(f"{ARCHIVE_DIR}/{RUN_ID}", exist_ok=True)
        shutil.move(PATH, f"{ARCHIVE_DIR}/{RUN_ID}/{FILE_NAME}")
    with open(PATH, "wb") as f:
        f.write(raw)
    ingested_at = RUN_TS

# COMMAND ----------

# Persist state: MERGE into the manifest, append to the log.
row = {"source": SOURCE, "file_name": FILE_NAME, "source_url": API_URL, "etag": None,
       "last_modified": None, "size_bytes": len(raw), "sha256": digest, "volume_path": PATH,
       "status": "active",
       "first_ingested_at": old["first_ingested_at"] if old else RUN_TS,
       "last_ingested_at": ingested_at, "last_checked_at": RUN_TS}

spark.createDataFrame([row], schema=spark.table(MANIFEST).schema).createOrReplaceTempView("updates")
spark.sql(f"""
  MERGE INTO {MANIFEST} t USING updates s
  ON t.source = s.source AND t.file_name = s.file_name
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
spark.createDataFrame([(RUN_ID, SOURCE, FILE_NAME, action, digest, RUN_TS)],
                      schema=spark.table(LOG).schema).write.mode("append").saveAsTable(LOG)

print(f"Population: {action}")

# COMMAND ----------

# Tell the job whether anything changed across BOTH sources. The job's condition task reads total_changed
# and skips the pipeline refresh when it's 0 (nothing new means nothing to reprocess).
# debugValue is used when this notebook is run by hand outside a job.
pop_changed = 1 if action in ("new", "changed") else 0
bls_changed = dbutils.jobs.taskValues.get(taskKey="ingest_bls", key="changed_files", default=0, debugValue=0)
dbutils.jobs.taskValues.set(key="changed_files", value=pop_changed)
dbutils.jobs.taskValues.set(key="total_changed", value=bls_changed + pop_changed)
print(f"Changed this run: BLS={bls_changed}, population={pop_changed}")
