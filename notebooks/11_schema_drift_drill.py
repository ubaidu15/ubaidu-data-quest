# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Schema-drift drill (proves the fail-fast safeguard)
# MAGIC Simulates BLS changing a file's columns **in our own Volume copy only** (BLS itself is never touched), then:
# MAGIC 1. Backs up `pr.series`, renames the column `series_title` → `series_name` (realistic upstream drift).
# MAGIC 2. Runs the pipeline and **asserts it fails** on the `expected_schema` expectation (not some random error).
# MAGIC 3. **Always restores** the original file (`finally`), re-runs the pipeline, and asserts it's green again.
# MAGIC
# MAGIC Why: a schema change must stop the pipeline loudly instead of silently loading misaligned or missing data.

# COMMAND ----------

# Config. The Databricks SDK is pre-installed and authenticates automatically inside a notebook.
import shutil, time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
PIPELINE_ID = "e1054e19-a357-4a09-b971-4875982328b1"
FILE        = "/Volumes/workspace/data_quest/raw/bls/pr/current/pr.series"
BACKUP      = "/Volumes/workspace/data_quest/raw/bls/pr/_drill_backup/pr.series"   # "_" prefix keeps Spark from reading it

def run_pipeline(full_refresh: bool = False) -> tuple[str, str]:
    """Start a pipeline update, wait for it to finish, return (update_id, final_state)."""
    update_id = w.pipelines.start_update(pipeline_id=PIPELINE_ID, full_refresh=full_refresh).update_id
    while True:
        state = w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update_id).update.state.value
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return update_id, state
        time.sleep(15)

def error_messages(update_id: str) -> list[str]:
    """ERROR-level messages from the pipeline event log for one update."""
    return [e.message for e in w.pipelines.list_pipeline_events(pipeline_id=PIPELINE_ID, max_results=250)
            if e.origin and e.origin.update_id == update_id and e.level and e.level.value == "ERROR"]

# COMMAND ----------

# 1) Back up the real file, then inject drift: rename one header column (data rows untouched).
import os
os.makedirs(os.path.dirname(BACKUP), exist_ok=True)
shutil.copy(FILE, BACKUP)

with open(FILE, encoding="utf-8") as f:
    header, rest = f.read().split("\n", 1)
assert "series_title" in header, "unexpected: column already missing"
with open(FILE, "w", encoding="utf-8") as f:
    f.write(header.replace("series_title", "series_name") + "\n" + rest)
print("Drift injected:", [c.strip() for c in header.replace("series_title", "series_name").split("\t")])

# COMMAND ----------

# 2) Run the pipeline with drifted input. Expect FAILED, caused by the expected_schema expectation.
#    3) finally: restore the original file no matter what happens.
try:
    drift_update, drift_state = run_pipeline()
    errors = error_messages(drift_update)
    print(f"Drifted run: {drift_state}")
    for m in errors[:5]:
        print("  ERROR:", m[:300])
    assert drift_state == "FAILED", f"Expected the pipeline to FAIL on drift, got {drift_state}"
    assert any("expected_schema" in m for m in errors), "Pipeline failed, but not on the expected_schema expectation"
    print("✔ Safeguard works: drift stopped the pipeline at Bronze via expected_schema (FAIL).")
finally:
    shutil.copy(BACKUP, FILE)
    print("Original pr.series restored.")

# COMMAND ----------

# 4) Recovery: with the real file back, the pipeline must be green again.
recover_update, recover_state = run_pipeline()
assert recover_state == "COMPLETED", f"Recovery run ended {recover_state}"
print(f"✔ Recovery run: {recover_state}. Gold untouched by the bad input (materialized views kept the last good data).")
