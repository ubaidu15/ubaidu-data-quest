# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingest BLS `pr` files into the Volume (idempotent sync)
# MAGIC Mirrors https://download.bls.gov/pub/time.series/pr/ into `/Volumes/workspace/data_quest/raw/bls/pr/current/`.
# MAGIC - Files are discovered from the listing page every run (nothing hardcoded).
# MAGIC - Unchanged files are skipped (HEAD → ETag/Last-Modified, then SHA-256), so nothing is re-downloaded or rewritten needlessly.
# MAGIC - Changed files: the old copy is archived and the new one written. Removed files: moved to `_archive`, marked `removed`.
# MAGIC - State lives in `workspace.data_quest.ingest_manifest`; every action is appended to `ingest_log`.

# COMMAND ----------

# Config. Paths and the BLS-compliant User-Agent (BLS returns 403 without contact info: bls.gov/bls/pss.htm).
import hashlib, os, re, shutil, time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCE      = "bls_pr"
BASE_URL    = "https://download.bls.gov/pub/time.series/pr/"
VOLUME_ROOT = "/Volumes/workspace/data_quest/raw"
CURRENT_DIR = f"{VOLUME_ROOT}/bls/pr/current"
ARCHIVE_DIR = f"{VOLUME_ROOT}/bls/pr/_archive"
MANIFEST    = "workspace.data_quest.ingest_manifest"
LOG         = "workspace.data_quest.ingest_log"
HEADERS     = {"User-Agent": "ubaidu-data-quest (ubaidurahman15@gmail.com)"}
PAUSE_SEC   = 0.5   # be polite to BLS between requests

spark.conf.set("spark.sql.session.timeZone", "UTC")   # so timestamps read back from the manifest compare correctly with UTC server dates
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)   # stored as UTC
RUN_ID = RUN_TS.strftime("%Y%m%dT%H%M%SZ")
os.makedirs(CURRENT_DIR, exist_ok=True)

# COMMAND ----------

# State tables (no-op if they already exist), so this notebook is self-contained.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {MANIFEST} (
  source STRING NOT NULL, file_name STRING NOT NULL, source_url STRING, etag STRING,
  last_modified TIMESTAMP, size_bytes BIGINT, sha256 STRING, volume_path STRING, status STRING,
  first_ingested_at TIMESTAMP, last_ingested_at TIMESTAMP, last_checked_at TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG} (
  run_id STRING, source STRING, file_name STRING, action STRING, sha256 STRING, logged_at TIMESTAMP)""")

# COMMAND ----------

# HTTP session with retries: backs off on 429/5xx and honours Retry-After. 403/404 are NOT retried (they won't fix themselves).
session = requests.Session()
session.headers.update(HEADERS)
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET"], respect_retry_after_header=True)))

def list_source_files() -> list[str]:
    """Parse the directory listing for file links inside /pr/ (skips the parent-directory link)."""
    resp = session.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    names = re.findall(r'HREF="/pub/time\.series/pr/([^"/]+)"', resp.text, flags=re.IGNORECASE)
    return sorted(set(names))

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def archive(path: str, name: str) -> None:
    """Move the current copy into _archive/<run_id>/ before it's overwritten or removed (audit + rollback)."""
    dest_dir = f"{ARCHIVE_DIR}/{RUN_ID}"
    os.makedirs(dest_dir, exist_ok=True)
    shutil.move(path, f"{dest_dir}/{name}")

# COMMAND ----------

# Discover what's on the site today. Safety guard: an empty listing means something broke, so don't wipe everything as "removed".
source_files = list_source_files()
if not source_files:
    raise RuntimeError("BLS listing returned 0 files. Aborting so we don't mark everything removed.")
print(f"{len(source_files)} files on BLS:", source_files)

# What we already have (our memory from previous runs).
prev = {r.file_name: r.asDict() for r in spark.table(MANIFEST).where(f"source = '{SOURCE}'").collect()}

# COMMAND ----------

# Sync loop: decide new / changed / unchanged / metadata_only for each file.
rows, log = [], []

for name in source_files:
    url, path, old = BASE_URL + name, f"{CURRENT_DIR}/{name}", prev.get(name)

    # 1) Cheap check: HEAD gives the server's version info without downloading.
    h = session.head(url, timeout=30); h.raise_for_status()
    etag = h.headers.get("ETag")
    lm   = h.headers.get("Last-Modified")
    last_modified = parsedate_to_datetime(lm).replace(tzinfo=None) if lm else None

    if (old and old["status"] == "active" and os.path.exists(path)
            and old["etag"] == etag and old["last_modified"] == last_modified):
        action, row = "unchanged", {**old, "last_checked_at": RUN_TS}
    else:
        # 2) Server says something differs: download and fingerprint the bytes.
        content = session.get(url, timeout=120); content.raise_for_status()
        data, digest = content.content, sha256(content.content)

        if old and old["sha256"] == digest and os.path.exists(path):
            action = "metadata_only"          # date/ETag moved but the bytes are identical, so don't rewrite
            ingested_at = old["last_ingested_at"]
        else:
            action = "changed" if old else "new"
            if os.path.exists(path):
                archive(path, name)
            with open(path, "wb") as f:       # land the raw bytes exactly as served
                f.write(data)
            ingested_at = RUN_TS

        row = {"source": SOURCE, "file_name": name, "source_url": url, "etag": etag,
               "last_modified": last_modified, "size_bytes": len(data), "sha256": digest,
               "volume_path": path, "status": "active",
               "first_ingested_at": old["first_ingested_at"] if old and old["first_ingested_at"] else RUN_TS,
               "last_ingested_at": ingested_at, "last_checked_at": RUN_TS}

    rows.append(row)
    log.append((RUN_ID, SOURCE, name, action, row["sha256"], RUN_TS))
    time.sleep(PAUSE_SEC)

# 3) Removed upstream: in our manifest as active, but no longer on the site.
for name, old in prev.items():
    if name not in source_files and old["status"] == "active":
        if os.path.exists(old["volume_path"]):
            archive(old["volume_path"], name)
        rows.append({**old, "status": "removed", "last_checked_at": RUN_TS})
        log.append((RUN_ID, SOURCE, name, "removed", old["sha256"], RUN_TS))

# COMMAND ----------

# Persist state atomically: MERGE = update existing rows, insert new ones (one transaction).
manifest_schema = spark.table(MANIFEST).schema
spark.createDataFrame(rows, schema=manifest_schema).createOrReplaceTempView("updates")
spark.sql(f"""
  MERGE INTO {MANIFEST} t
  USING updates s
  ON t.source = s.source AND t.file_name = s.file_name
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
spark.createDataFrame(log, schema=spark.table(LOG).schema).write.mode("append").saveAsTable(LOG)

# COMMAND ----------

# Run summary: what this run did. On a re-run with no upstream changes, every file should say "unchanged".
display(spark.table(LOG).where(f"run_id = '{RUN_ID}'").groupBy("action").count())
display(spark.table(LOG).where(f"run_id = '{RUN_ID}'").orderBy("file_name"))

# COMMAND ----------

# Tell the job how many files actually changed (new/changed/removed). The job only refreshes the pipeline if > 0.
changed_files = sum(1 for entry in log if entry[3] in ("new", "changed", "removed"))
dbutils.jobs.taskValues.set(key="changed_files", value=changed_files)
print(f"BLS files changed this run: {changed_files}")
