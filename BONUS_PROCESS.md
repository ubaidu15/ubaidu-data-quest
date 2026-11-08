# BONUS_PROCESS.md

## Why I picked these three
Once the core quest was done, I asked myself what would actually stop this product from being trusted in production. Three gaps stood out:

1. **Nobody but an engineer could use the answers.** The Gold tables were correct, but a stakeholder would need SQL and BLS series codes to read them.
2. **Nobody would know if it broke or went stale.** The pipeline had strong data quality checks, but if the scheduled job stopped, or BLS stopped publishing, the Gold tables would silently age.
3. **My strongest safeguard was untested.** I'd claimed that schema drift fails the pipeline, but I had never seen it happen.

So I built one bonus for each gap:

| # | Bonus | Gap it closes | Evidence |
|---|-------|---------------|----------|
| 1 | **AI/BI dashboard on Gold** | Usability for non-technical stakeholders | `bonus_screenshots/B7–B9` |
| 2 | **Freshness and failure alert** | Observability: knowing before the stakeholder does | `bonus_screenshots/B4` |
| 3 | **Schema-drift drill** | Proving resilience instead of claiming it | `bonus_screenshots/B1–B2` |

I deliberately chose items I could build *and verify* end to end with my permissions in this workspace, rather than items I could only half-configure.

---

## 1. AI/BI dashboard on the Gold layer

### How I thought about it
I started from the audience, not the data. A non-technical stakeholder looking at this has three questions:
- *Can I trust these numbers right now?*
- *What are the answers?*
- *How do I find the one thing I care about?*

That shaped the structure: a trust signal first, then the answers, then self-service tools (filters and search), with caveats written where people will actually read them.

I also decided early that the dashboard would read **only from Gold** (plus my health view). If it touched Silver or raw data, it would bypass the validation I'd put in place, and two views of "the truth" could drift apart.

### What I built
A 4-page Databricks AI/BI dashboard, **Data Quest - US Productivity & Population**, published with **embedded credentials**, so viewers can use it without being granted access to the underlying tables:

| Page | What's on it | Why |
|---|---|---|
| **Overview** | Data health, last refresh and "BLS last published" tiles; average and standard deviation of US population 2013–2018 (Q1); population-by-year line | Trust first, then the headline answer |
| **Best year by series** (Q2) | **Sector** and **Measure** filters; a searchable table (*What is measured · Best year · Total in best year · How it was calculated · BLS series ID*); bar chart of how many series peaked in each year | 337 rows is too many to scan, so filters and search make it self-service |
| **Manufacturing hours vs population** (Q3) | An explanation of what PRS30006032 means in plain English; a line of the Q1 % change by year; a year-by-year table with population | Turns a cryptic series ID into a readable story, and explains the blanks |
| **About the data** | Sources, refresh logic, quality checks, how to read %-change and index totals | Answers "where does this come from?" without a meeting |

### How I built it
- **Dashboard as code.** Rather than clicking widgets together in the UI, I wrote `dashboards/build_dashboard.py`. It defines small helper functions (counter, chart, table, filter, text) and generates the full dashboard definition, `dashboards/data_quest_dashboard.lvdash.json`. I then created and published it through the **Lakeview REST API**.
  - **Reviewable:** a change to the dashboard is a diff in git.
  - **Rebuildable:** if the dashboard is deleted, one script recreates it.
  - **Portable:** the same JSON imports into another workspace, a step toward Asset Bundles.
- **Datasets** are five small SQL queries over Gold and the health view. I cast `DECIMAL` values to `DOUBLE` only at the dashboard layer, for charting; the Gold tables keep exact decimals.
- **Verification:** I ran all five dataset queries independently (1, 11, 337, 39 and 1 rows), then reviewed the rendered pages. The line chart, bar chart and table all display correctly.

### What I had to work through
The first API call failed with "Expected Scalar value for String field `serialized_dashboard`". PowerShell 5.1's `Get-Content` attaches hidden metadata properties to strings, so the definition was sent as a JSON *object* instead of a JSON *string*. I moved the API call to a small Python script (`urllib`), which serialises the body exactly as intended.

### Design choices I'd defend
- **Plain-English labels everywhere.** No column names or BLS codes are needed to understand any widget.
- **Caveats in context:** "2020 not published by Census" sits in the chart title, right where someone would wonder about the gap.
- **The bar chart is also a sanity check.** It shows **2025 as by far the most common best year (~77 series)**, which is what I'd expect: index and level series trend upward over time, and 2025 is the latest complete year. If the Q2 logic were broken, this chart would look wrong immediately.

### Assumptions
- Stakeholders mainly want the three answers plus context. Filters and search cover self-service, and a **Genie space** is the natural next step for free-text questions.
- The workspace owner's credentials are acceptable for a published dashboard. For a client, I'd publish with a service principal.

---

## 2. Freshness and failure alert

### How I thought about it
I asked: *what are all the ways this product can silently go wrong after deployment?* I came up with four, each with a different cause and owner:

1. **Our job stops running** (paused, failing, credentials expired). This is an engineering problem.
2. **BLS stops publishing or changes location.** This is an upstream problem, and our job would keep running "successfully" while the data ages.
3. **The pipeline fails** (drift, bad data, a platform issue).
4. **Gold ends up empty or malformed** (a logic regression).

A single "did the job succeed?" check would only catch #1 and #3. I wanted one place that catches all four and says *which one* happened.

### What I built
**A health view, `workspace.data_quest.ops_pipeline_health`**, returning one row:

| Signal | Where I read it from | Rule | Why this threshold |
|---|---|---|---|
| Ingestion freshness | `ingest_manifest.last_checked_at` | Stale if **> 48 h** | The job runs daily; 48 h allows one missed run before alerting |
| Source freshness | BLS server `Last-Modified`, stored in the manifest | Stale if **> 100 days** | `pr.txt` says BLS updates near the start of the 2nd and 3rd month of each quarter (about every 6 weeks), so 100 days is clearly abnormal |
| Pipeline outcome | Final state of the latest update in the pipeline's `event_log()` | Alert if not `COMPLETED` | A failed or cancelled refresh means Gold may be behind |
| Gold completeness | Row counts of the 3 Gold tables | Alert if Q1 ≠ 1 row, or Q2/Q3 empty | Catches logic regressions that technically "succeed" |

It outputs `status` (OK/ALERT), `issue_count`, and an **`issues` array of human-readable reasons**, for example *"INGESTION_STALE: no ingestion check in >48h (daily job not running?)"*. I built the array with `filter(array(CASE…), x -> x IS NOT NULL)`, so each rule contributes a message only when it fires, and adding a fifth rule is one line.

**A Databricks SQL Alert** (`alerts/pipeline_health_alert.json`), created via the Alerts API:
- It evaluates **daily at 15:00 UTC**, one hour after the job's 14:00 slot, so it checks that day's run rather than racing it.
- It **triggers when `issue_count > 0`** and **notifies again on recovery** (`notify_on_ok`), so people know when it's fixed, not just when it broke.
- **An empty result counts as an error.** If the view itself breaks, the alert must not read that as "no issues".

### How I worked within my permissions
My first idea was to read job run history from `system.lakeflow.job_run_timeline`, but my account has no `USE SCHEMA` on `system.lakeflow`. Rather than ask for admin rights, I looked for a signal I already owned: the **pipeline's own `event_log()`** table-valued function. It records every update's final state and is available to the pipeline owner. Pipeline failures were the signal I actually needed, so this was a better fit anyway.

### How I proved it
I didn't want to wait 48 hours to see the alert fire, so I simulated the condition and then reversed it exactly:
1. `UPDATE ingest_manifest SET last_checked_at = last_checked_at - INTERVAL 3 DAYS` → the view returned **ALERT · "INGESTION_STALE…" · 73.2 h**.
2. `UPDATE … + INTERVAL 3 DAYS` (the exact inverse) → **OK · 1.2 h**.

The live state is OK (`bonus_screenshots/B4_health_view_ok.png`: OK, 0 issues, last pipeline state COMPLETED). During the schema-drift drill (below), this same view would have reported `PIPELINE_LAST_UPDATE_FAILED` until the auto-retry succeeded, which is exactly the signal I want.

### Assumptions and trade-offs
- **The daily job is deployed paused** on this shared workspace to avoid spending compute. Once ingestion is more than 48 h old, the alert will correctly report `INGESTION_STALE`, because the job really isn't running. That's correct behaviour, not a false alarm.
- **No notification recipients are configured.** The alert changes state in the UI. For a client, I'd route it to an on-call email or Slack destination and reference a runbook in the alert description.
- Thresholds are constants in the view. At scale, I'd move them into a small config table per source.

---

## 3. Schema-drift drill

### How I thought about it
In Bronze, every file has an `expected_schema` expectation with action **FAIL**: if BLS renames, adds or drops a column, the pipeline should stop rather than load misaligned data. That was a claim in my design, but I'd never seen it fire.

I'd already proven ingestion idempotency by *simulating* changes rather than waiting for BLS. I wanted the same standard here: **drill it, don't assume it.** I also wanted the drill to be safe (it must never leave the system broken) and repeatable (anyone can rerun it).

### What I built
`notebooks/11_schema_drift_drill.py`, a self-contained drill using the Databricks SDK:

1. **Back up** `pr.series` to `raw/bls/pr/_drill_backup/`. I chose a `_`-prefixed folder deliberately, because Spark treats such paths as hidden (something I'd discovered earlier), so the pipeline can never read the backup by accident.
2. **Inject realistic drift** into my Volume copy only: rename the header column `series_title` → `series_name`. Data rows are untouched, and BLS itself is never touched.
3. **Start a pipeline update and assert two things:** it ended **FAILED**, *and* the failure came from the **`bronze_bls_series` expectation**. A drill that passes on *any* failure would prove nothing.
4. **Restore the original file in a `finally` block**, so the restore is guaranteed even if an assertion throws or the API errors.
5. **Assert recovery:** wait for the pipeline to go idle and confirm the newest update is `COMPLETED`.

### Results
| Time | Update | State | Cause | Meaning |
|---|---|---|---|---|
| 18:38 | `0e3d3491` | **FAILED** | My drill (API call) | Stopped at Bronze: `expected_schema · FAIL · 1 failed record` (`B1`) |
| 18:40 | `2b5d96ed` | **COMPLETED** | `RETRY_ON_FAILURE` | Production mode auto-retried; with the file restored, the pipeline recovered with no human action (`B2`: 337 rows, 0 dropped) |

### What went wrong first, and what I learned from it
My first run of the drill ended with a `ResourceConflict: an active update already exists`. My first instinct was that the drill had failed, but I checked the pipeline's update history before changing anything. It showed:
- The drill **had worked**: its update failed on `bronze_bls_series` exactly as intended.
- About **2 seconds later, production mode automatically started a retry** (`RETRY_ON_FAILURE`). I hadn't accounted for that.
- Meanwhile, the **Databricks SDK's own HTTP retry** re-sent my "start update" request, which collided with the auto-retry and raised the conflict.
- My **`finally` block still restored the file**, so the auto-retry ran on clean input and completed. The safety design held under a real, unplanned error.

I then hardened the notebook:
- It **waits until the pipeline is idle** before starting an update.
- If the SDK collision happens, it **picks up the update it actually created** instead of crashing.
- It treats **"recovered automatically by the production retry"** as a pass, because that *is* the desired production behaviour.

The clean rerun succeeded end to end.

### Why this matters
- **Fail loudly, never silently.** Without the safeguard, the renamed column would have loaded as NULL `series_title`, and the Q2 labels on the dashboard would quietly go blank.
- **Consumers are protected.** The failed update never replaced the Gold materialized views, so the dashboard kept showing the last good data throughout.
- **The system self-heals.** Production mode's automatic retries mean that once the root cause is fixed, recovery needs no manual intervention.

### Assumptions
- Renaming one column is representative of real upstream drift. A dropped or added column hits the same `_schema_ok` check, because I compare the full column set.
- Running the drill against the production pipeline is acceptable here, since it restores automatically and Gold is never overwritten by bad input. For a client, I'd run it in a dev or staging environment as part of release testing.

---

## How the bonus work strengthens the core design
| Concern | Core quest | What the bonus adds |
|---|---|---|
| Data quality | Expectations with FAIL / DROP / WARN by severity | **Drill** proves FAIL actually fires and that recovery is automatic |
| Re-run safety | Manifest + hashing + condition task | **Alert** detects when the scheduled runs stop happening at all |
| Correctness | SQL ↔ PySpark assertion + independent validation | **Dashboard** exposes only validated Gold answers, with caveats |
| Operations | Job + production-mode pipeline | **Alert** + front-page freshness make the product observable |

## What I'd do next
- A **Genie space** over Gold for free-text questions such as "which manufacturing measure peaked most recently?".
- **Unity Catalog grants** for a read-only analyst group (`USE CATALOG`, `USE SCHEMA`, `SELECT` on Gold only). This needs a dedicated catalog: in this shared catalog all account users inherit full privileges, and privileges are additive, so a schema-level grant can't restrict anyone.
- **Databricks Asset Bundles** to deploy the pipeline, job, alert and dashboard together across dev, staging and prod, using the JSON definitions I already keep in the repo.
