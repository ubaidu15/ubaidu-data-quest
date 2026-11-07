# PROCESS.md

## Summary
I built an end-to-end, re-runnable Databricks solution: idempotent ingestion into a Unity Catalog Volume, a Spark Declarative Pipeline (Bronze → Silver → Gold) with layered data quality, and a single orchestrated job that only reprocesses when a source actually changes.

```
BLS pr/ folder ─┐   idempotent ingestion       Unity Catalog Volume        Spark Declarative Pipeline
DataUSA API  ───┴─► (manifest + hashing) ───►  raw/…/current/  ───►  Bronze ──► Silver ──► Gold (Q1, Q2, Q3)
                     notebooks 02, 03            + _archive/            PySpark     SQL      SQL / PySpark / SQL
                                   └─────────── one job: data_quest_daily (pipeline runs only if a source changed) ──┘
```

**Stack I used:**
- Unity Catalog (`workspace.data_quest`) with a managed Volume
- Delta Lake
- Serverless compute
- Spark Declarative Pipelines (`pyspark.pipelines` + SQL materialized views with expectations)
- Databricks Jobs (task values + if/else condition tasks)
- A Databricks Git folder synced to GitHub

**Answers:**
- **Q1:** mean **322,069,808**, standard deviation **4,158,441.04** (sample).
- **Q2:** the best year for **all 337 series**, with readable labels.
- **Q3:** **39 years** of PRS30006032 Q01, with population where available.

Every answer is implemented in both Spark SQL and PySpark, and I proved them identical. I also validated them independently from the raw files and outside Spark.

---

## 1. Architecture

### 1.1 Ingestion: how I made re-running safe

**My approach**
- **Discover, never hardcode.** On every run I parse the BLS directory listing with a regex, so any file BLS adds is ingested automatically and no filename is baked into the code.
- **Persist state in Delta.** I designed an `ingest_manifest` Delta table (one row per file: ETag, Last-Modified, size, SHA-256, Volume path, status, first/last ingested, last checked) and an append-only `ingest_log`.
  - I chose Delta over a JSON state file because `MERGE` gives an atomic upsert, so a crashed run can't leave half-written state.
  - The state is also queryable and auditable in SQL, and time travel gives me history for free.
- **Detect change in two stages, cheapest first:**
  1. A `HEAD` request compares the server's ETag and Last-Modified against the manifest. If they match, I skip the file **without downloading it**.
  2. Only if they differ do I download it and compute SHA-256. If the bytes are unchanged, I record `metadata_only` and **don't rewrite the file**.
- **Mirror plus archive in the Volume.** I designed `current/` as an exact mirror of the source.
  - Before overwriting a changed file, I move the old copy to `_archive/<run_id>/`.
  - Files removed upstream are archived and marked `removed`.
  - The pipeline always sees the true current state, nothing is ever lost, and a bad upstream release can be rolled back.
- **Treat the population API differently on purpose.** It returns no ETag or Last-Modified, so I always fetch it (about 1.4 KB).
  - I **validate it before landing**: valid JSON, a non-empty `data` array, and the `Year`/`Population` fields present. A 200-OK error body can never overwrite good data.
  - I write only if a **canonical-JSON SHA-256** (sorted keys, no whitespace) changed, so cosmetic reformatting by the API doesn't trigger reprocessing.
- **Respect the provider.** I resolved the BLS 403 the way BLS's access policy asks: a `User-Agent` with contact info.
  - I built an HTTP session with exponential-backoff retries on 429/5xx that honours `Retry-After`, deliberately **not** retrying 403/404 because they won't fix themselves.
  - I added a pause between requests.
- **Fail safe.** If the listing ever returns zero files, I abort the run instead of marking every file as removed.
- **Extend idempotency to the pipeline.** I had the ingestion notebooks publish change counts as **job task values** and added an **if/else condition task**. When nothing changed, the pipeline refresh is skipped entirely, with zero reprocessing and zero wasted compute.

**My reasoning**
- BLS files are **full snapshots**: `pr.data.1.AllData` is the entire dataset on every release. Appending each version would duplicate rows, so I mirror the source rather than accumulate it.
- I tested my assumptions against real data instead of trusting conventions. I found **8 different BLS files sharing the same ETag**, because the server derives it from the modification timestamp, not the content.
  - That confirmed my design choice: the ETag is only a cheap pre-filter, and **SHA-256 is the real proof of change**.

**How I proved it works**
- **A plain re-run:** all 13 files came back `unchanged`, with no downloads and no writes.
- **Simulated upstream changes:**
  - `pr.series` came back `changed`, with the old copy archived.
  - `pr.class` came back `metadata_only`, so it was not rewritten.
  - The population file came back `changed`.
  - A fake upstream-removed file came back `removed`.
- **Both job branches:** a change ran the pipeline and the Gold check; no change skipped both.

**Assumptions I made**
- BLS publishes full snapshots, not increments. I verified this: Current is a strict subset of AllData.
- A file removed at the source should leave the current view but be retained for audit.
- The BLS listing keeps its HTML format. The zero-file guard protects against a silent break.

---

### 1.2 Bronze / Silver / Gold: why I modelled it this way

**My approach: profile first, design second.** Before writing any table, I profiled the landed files directly with `read_files` (`notebooks/06_profile_raw.sql`). The findings drove every design decision:
- **Data files:** AllData has 92,328 rows and 337 series covering 1947–2026. `pr.data.0.Current` is a **pure subset**, with 0 extra rows and 0 differing values.
- **Data cleanliness:** no null keys, no unparseable values, no duplicate keys.
- **54 series publish only annual averages (Q05).**
- **Messy headers:** BLS pads them (`"       value"`, `"series_id        "`) and the casing is inconsistent (`Seasonal_code`).
- **Outdated documentation:** BLS's own `pr.txt` disagrees with the data. It says the base year is 2005 (the data says 2017), says there are ~282 series (there are 337), and doesn't document the `series_title` column. So I chose to trust the files and enforce the schema in code.

**Bronze (PySpark), 10 tables: a faithful raw copy**
- **Values as raw strings.** I load every file with values exactly as published and normalise only the column *names* (trim + lowercase), so the padded headers become usable without altering any data.
- **Lineage on every row.** Each row gets `_source_file`, `_source_modified_at` and `_loaded_at`, so any value is traceable to its file and load.
- **A factory function for the lookups.** I generate the 7 BLS lookup tables from one dictionary using a factory function, which avoids Python's late-binding closure bug, where every table in a loop would point at the last iteration.
- **Materialized views, not streaming tables.** This was a deliberate choice. Auto Loader streaming only processes *new* files: it would ignore an overwritten snapshot, and appending snapshots would duplicate rows. Materialized views recompute from the current snapshot, and Databricks refreshes them incrementally where possible.

**Silver (Spark SQL), 3 tables: clean, typed, conformed**
- **`silver_bls_data` uses AllData only.** I proved Current is a subset, so using both would double count.
  - I trim every field and use `try_cast` so a bad value becomes NULL (and is then caught by an expectation) rather than crashing the update.
  - I type `value` as **`DECIMAL(18,3)`**, not DOUBLE. BLS stores three decimals, and decimal arithmetic makes the Q2 sums exact (a double can turn 0.1 + 0.2 into 0.30000000000000004).
  - I add an `is_annual_average` flag and a defensive dedupe with `QUALIFY row_number()`.
- **`silver_bls_series` is a series dimension** that decodes every code through five BLS mapping files: sector, measure, duration, worker class and seasonality.
  - I deliberately used **LEFT JOINs**: a missing lookup code must never make a series disappear. A WARN expectation surfaces it instead.
- **`silver_population`** casts the float population to `BIGINT` and dedupes by year.

**Gold, 3 tables:** one table per question, so each answer is a stable, documented, directly queryable product (details in 1.4).

**Data quality: I matched each expectation's action to the severity of the problem**
| Rule | Action | My reasoning |
|---|---|---|
| File schema ≠ documented schema | **FAIL** the update | Schema drift must stop the pipeline, never silently load misaligned data |
| Null/invalid key, invalid year or period, non-numeric value | **DROP** the row | The row is unusable; drops are counted in the pipeline's data quality metrics |
| Undecoded label, nation ≠ US, Q1 years ≠ 6 | **WARN** | Valid but unexpected; I want visibility without losing data |

To make the schema rule enforceable, I compute a `_schema_ok` flag per file. If a documented column is missing, I add it as NULL so the **expectation** reports the drift clearly instead of the code crashing with a cryptic error.

---

### 1.3 SQL vs. PySpark: what I chose as primary and why

I chose the primary language **per task, by fit**, rather than forcing one language everywhere:

| Component | Primary | Why I chose it |
|---|---|---|
| Bronze | **PySpark** | It's programmatic: one loop generates 7 lookup tables, and schema validation is easier in Python |
| Silver | **Spark SQL** | Column-by-column cleaning and joins read best in SQL, with constraints declared next to the table |
| **Q1** population stats | **Spark SQL** | One filter and one aggregate; SQL is the most direct expression |
| **Q2** best year per series | **PySpark** | Five dependent steps (window flag → conditional filter → aggregate → rank → join) are clearest and most testable as named DataFrame stages |
| **Q3** series + population | **Spark SQL** | A filter plus two joins is the textbook SQL case |

**The alternatives** are Q1 and Q3 in PySpark, and Q2 in SQL using chained CTEs and `QUALIFY row_number()`. They live in `notebooks/08_gold_alternatives.py`.
- **They're proven, not just written.** Each one is compared with its Gold table using a **two-way `exceptAll`**, which catches missing rows, extra rows and duplicates. Any difference raises an error.
- **The proof runs continuously.** It's a task in the job, after every pipeline refresh, so a future divergence between the two implementations fails the job loudly.
- **I kept the alternatives out of the pipeline on purpose.** Including them would compute every answer twice on every run and publish duplicate Gold tables for consumers to trip over.

---

### 1.4 The analytical questions: my approach and assumptions

**Q1: mean and standard deviation of the annual US population, 2013–2018**
- **Answer:** mean **322,069,808**; standard deviation **4,158,441.04** (sample).
- **Assumption:** "standard deviation" is ambiguous. I report both the **sample** value (n-1; Spark's default `stddev`) and the **population** value (n; **3,796,119.94**), clearly labelled.
- I added a `years_count = 6` expectation so the answer is self-checking: a missing year would be flagged.

**Q2: best year per series (largest sum of `value` across its quarters)**
- **What I found:** the question says "across all its quarters", but **Q05 is an annual average, not a quarter**, and profiling showed **54 series have only Q05**.
- **Why neither obvious reading works:**
  - Excluding Q05 everywhere would make those 54 series disappear and break "for every series_id".
  - Including Q05 everywhere would double count the other 283 series (four quarters plus their average).
- **My rule (hybrid):** sum Q01–Q04 when a series has quarterly data, and use Q05 only for annual-only series.
  - All **337** series are answered, with no double counting.
  - I made the decision transparent with a `value_basis` column rather than burying it.
- **Ties:** because I used exact decimal sums, I found **7 genuine ties**, for example two years with identical totals. I break them deterministically: **the earliest year wins**. A float-based sum would have resolved them arbitrarily.
- **Partial years:** I kept them as the question defines them, for example a series that starts in Q02 1987. I exposed `periods_in_best_year` so they're visible at a glance.
- **Human-readable label:** BLS's `series_title`, plus the decoded sector, measure, duration, worker class and seasonality, so someone unfamiliar with BLS codes can understand each row.
- **Caveat I'm aware of:** summing %-change series isn't economically meaningful, but it's what the question defines. Because the sum is computed within each series, units never mix.

**Q3: PRS30006032, period Q01, value per year joined with population**
- **I used a LEFT JOIN to implement "where available":** all **39** BLS years are kept.
- **Assumption, which I verified:** population is NULL before 2013, for **2020** (Census published no ACS 1-year estimates that year because of COVID data-quality problems), and for 2025–26, after the latest release.

---

### 1.5 How I validated correctness

I didn't treat "the pipeline ran" as proof. I layered the checks so that no single bug could hide:

| Layer | What it proves |
|---|---|
| Pipeline expectations | Schema and key rules hold on every file and row |
| Ingestion re-run simulations | Added, changed and removed data are handled, and nothing already ingested is reprocessed |
| SQL ↔ PySpark `exceptAll` assertion (in the job) | Both implementations agree, after every refresh |
| Recompute from the raw Volume files, bypassing Bronze/Silver (`notebooks/10_validate_from_raw.sql`) | My transformations didn't distort the data: Q1 identical, Q2 **337/337**, Q3 **39/39** exact |
| Plain-Python recompute from live sources (`validation/independent_check.py`) | The answers are right independently of Spark and Databricks |

The last two matter most to me. If Silver had a bug, Gold *and* the alternatives (which both read Silver) would agree on the wrong answer. Recomputing from raw files, and again outside Spark entirely, rules that out.

---

## 2. Trade-offs: what I'd handle differently for a real client

For this quest I made deliberate, proportionate choices. For a production client engagement, I'd evolve them as follows:

| Area | What I did here, and why it's sufficient | What I'd do for a real client |
|---|---|---|
| **Schema drift** | Fail fast on any schema mismatch; Bronze keeps raw strings, so nothing is lost in parsing | Versioned **data contracts** per source; a **quarantine table** for rejected rows instead of dropping them; alerts to the owning team; controlled schema evolution (`_rescued_data`, `mergeSchema`) for append-style sources |
| **Data volume** | About 4 MB of snapshots; recomputing materialized views is cheap and simple | **Streaming tables with Auto Loader** for append sources; **AUTO CDC from snapshot** so snapshot sources process only the diff; **liquid clustering** on `series_id`/`year`; parallelised downloads |
| **Cost** | Serverless and triggered, production mode; the pipeline is **skipped when nothing changed**; the schedule was created paused | Serverless **budget policies** and tags per project; cost dashboards from `system.billing`; a schedule aligned to the BLS release calendar instead of daily |
| **Access control** | A single owner in a shared workspace | **Separate dev and prod catalogs**; jobs run as a **service principal**; analysts get `USE CATALOG/SCHEMA` and `SELECT` on Gold only; the raw Volume and manifest restricted to engineering; contact details and secrets in a **secret scope** |
| **Monitoring** | Audit log table, expectation metrics, job status | Failure notifications (email/Slack); **freshness SLA** alerts tied to BLS release dates; dashboards on the pipeline event log (drop rates per expectation); **Lakehouse Monitoring** on Gold for drift |
| **Deployment** | Git folder plus a job definition stored as JSON in the repo | **Databricks Asset Bundles** with dev/staging/prod targets; parameterised paths (no user-specific paths); CI running unit tests for parsing and change detection on every pull request |
| **Retention** | Archive kept indefinitely | A retention policy for `_archive/` (e.g. 12 months) and scheduled `VACUUM` on Delta tables |
| **Environment** | A shared workspace, which I confirmed was acceptable | A dedicated workspace or catalog per engagement and per environment |

---

## 3. Retrospective: what was hardest to get right

1. **Interpreting Q2 correctly.** The wording ("sum across all its quarters") collides with how BLS actually publishes data: Q05 is an annual average, and 54 series have nothing else. I only caught this because I profiled coverage per series before writing Gold. Choosing the hybrid rule, and making it visible with `value_basis` instead of silently picking an interpretation, was the most important judgement call in the project.
2. **True idempotency with snapshot files.** The easy paths were to trust ETags or reach for Auto Loader. Testing showed both were wrong for this source: BLS ETags are timestamp-based and shared across files, and streaming would mishandle overwritten snapshots. Getting it right meant HEAD-then-hash detection, a mirror-plus-archive layout, materialized views, and finally skipping the pipeline when nothing changed.
3. **Messy data and outdated documentation.** The padded headers broke my first profiling queries, and the provider's documentation disagreed with the data on the base year, the series count and an undocumented column. I learned to trust the actual files, enforce the schema with expectations, and record every discrepancy I found.
4. **Proving correctness, not just producing numbers.** Self-consistency isn't proof. Building the raw-file and outside-Spark recomputes took extra effort, but it's what gives me real confidence in the answers. Using exact decimals also surfaced 7 genuine ties that I then resolved deterministically.
5. **Smaller gotchas I'll remember:**
   - Spark skips paths that start with `_`, so `read_files` couldn't see `_archive`. That turned out to be a useful safeguard, because the pipeline can never ingest archived files by accident.
   - I had to compare manifest timestamps in UTC to avoid false "changed" results.
   - PowerShell 5.1 sends request bodies in a legacy encoding, which garbled UTF-8 when I created the job through the REST API. Sending UTF-8 bytes fixed it.

---

## 4. AI usage
> _To be written by me._
