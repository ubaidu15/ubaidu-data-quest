# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Schema-drift drill (proves the fail-fast safeguard)
# MAGIC Simulates BLS changing a file's columns **in our own Volume copy only** (BLS itself is never touched), then:
# MAGIC 1. Backs up `pr.series`, renames the column `series_title` → `series_name` (realistic upstream drift).
# MAGIC 2. Runs the pipeline and **asserts it fails** on the `expected_schema` expectation (not some random error).
# MAGIC 3. **Always restores** the original file (`finally`) the moment the failure is confirmed.
# MAGIC 4. Asserts the pipeline is green again. In production mode Databricks **auto-retries** a failed update
# MAGIC    (`RETRY_ON_FAILURE`); once the file is restored, that retry recovers by itself. If it didn't, we start one.
# MAGIC
# MAGIC Why: a schema change must stop the pipeline loudly instead of silently loading misaligned or missing data.

# COMMAND ----------

# Config and helpers. The Databricks SDK is pre-installed and authenticates automatically inside a notebook.
import os, shutil, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceConflict

w = WorkspaceClient()
PIPELINE_ID = "e1054e19-a357-4a09-b971-4875982328b1"
FILE        = "/Volumes/workspace/data_quest/raw/bls/pr/current/pr.series"
BACKUP      = "/Volumes/workspace/data_quest/raw/bls/pr/_drill_backup/pr.series"   # "_" prefix keeps Spark from reading it
DONE        = ("COMPLETED", "FAILED", "CANCELED")

def updates():
    return w.pipelines.list_updates(pipeline_id=PIPELINE_ID, max_results=10).updates or []

def wait_idle():
    """Block until no pipeline update is running (e.g. a production-mode auto-retry)."""
    while any(u.state.value not in DONE for u in updates()):
        time.sleep(10)

def wait_for(update_id: str) -> str:
    while (state := w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update_id).update.state.value) not in DONE:
        time.sleep(5)
    return state

def start_update() -> str:
    """Start an update once the pipeline is idle. If the SDK's own retry collides with an update it already
    created, fall back to the newest update instead of failing."""
    wait_idle()
    before = time.time() * 1000
    try:
        return w.pipelines.start_update(pipeline_id=PIPELINE_ID).update_id
    except ResourceConflict:
        return next(u.update_id for u in updates() if u.creation_time >= before)

def error_messages(update_id: str) -> list[str]:
    return [e.message for e in w.pipelines.list_pipeline_events(pipeline_id=PIPELINE_ID, max_results=250)
            if e.origin and e.origin.update_id == update_id and e.level and e.level.value == "ERROR"]

# COMMAND ----------

# 1) Back up the real file, then inject drift: rename one header column (data rows untouched).
os.makedirs(os.path.dirname(BACKUP), exist_ok=True)
shutil.copy(FILE, BACKUP)
with open(FILE, encoding="utf-8") as f:
    header, rest = f.read().split("\n", 1)
assert "series_title" in header, "unexpected: column already missing"
with open(FILE, "w", encoding="utf-8") as f:
    f.write(header.replace("series_title", "series_name") + "\n" + rest)
print("Drift injected → columns now:", [c.strip() for c in header.replace("series_title", "series_name").split("\t")])

# COMMAND ----------

# 2) Run the pipeline on drifted input: expect FAILED on expected_schema. 3) finally: restore immediately.
try:
    drift_update = start_update()
    drift_state = wait_for(drift_update)
    errors = error_messages(drift_update)
    print(f"Drifted run {drift_update[:8]}: {drift_state}")
    for m in errors[:3]:
        print("  ERROR:", m[:250])
    assert drift_state == "FAILED", f"Expected FAILED on drift, got {drift_state}"
    assert any("expectation" in m and "bronze_bls_series" in m for m in errors), "Failed, but not on the schema expectation"
    print("✔ Safeguard works: drift stopped the pipeline at bronze_bls_series (expected_schema = FAIL).")
finally:
    shutil.copy(BACKUP, FILE)
    print("Original pr.series restored.")

# COMMAND ----------

# 4) Recovery. Let any production-mode auto-retry finish; the newest update must be COMPLETED.
time.sleep(20)                     # give the auto-retry a moment to be scheduled
wait_idle()
latest = updates()[0]
if latest.state.value != "COMPLETED":
    latest_id = start_update()
    assert wait_for(latest_id) == "COMPLETED", "Recovery run did not complete"
    print("✔ Recovered with a fresh run.")
else:
    print(f"✔ Recovered automatically: update {latest.update_id[:8]} (cause={latest.cause.value}) COMPLETED.")
print("Gold was never touched by the bad input: materialized views kept the last good data.")
