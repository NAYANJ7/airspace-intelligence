# Performance update — apply and test on your machine

This is a cumulative patch for **Airspace Intelligence(1).zip**. It includes the seven-page Phase 12 dashboard and the new performance work. You do not need to apply the older dashboard ZIP first.

**No tests or benchmarks were run for this revision in the sandbox, as requested. All results below are expectations for your checks, not reported passes.** No new dependency versions or base Dockerfiles were introduced.

## What changed and what to expect

| Change | Expected behavior |
| --- | --- |
| Persistent Parquet receipts | First run registers existing files. Later runs read only new/changed files. |
| Atomic file commit | Observations and the receipt commit together; a failed file can be retried. Observation inserts still use ON CONFLICT DO NOTHING. |
| Shared loader lock | Hourly/manual loader waits up to 60 seconds; live worker skips a cycle if another loader owns the lock. No two upgraded loaders merge simultaneously. |
| Smaller Python COPY batches | At most 4,096 rows of Python objects at once; Arrow still holds one complete file. |
| Optional live loader | Checks for new files 30 seconds after each completed cycle; no hourly wait. |
| Optional live ML worker | Checks every 60 seconds after each cycle. Scores on committed-file/model changes, continues a bounded backlog, and rescans every 10 minutes for otherwise untracked late arrivals. Never trains. |
| Lighter Overview | Snapshot KPIs first. Exact archive count and trend run only after selecting their checkbox. |
| Faster initial Data Quality page | Reads the last saved hourly audit, labelled with its time. Full-history recalculation and gap/history scans require explicit choices. |
| Focused refresh | Only selected-page caches are cleared. Snapshot cache is 30 seconds; ML is 60 seconds; archive analytics is 10 minutes. Page auto-refresh defaults to one minute. |

A practical goal after backlog catch-up is new observations appearing within a few minutes while ingestion is active. This is **a target to measure**, not a guarantee or benchmark. API backoff, machine load, query time, worker duration and browser refresh add delay.

## 1. Back up and apply

Download **Airspace_Performance_Update.zip** to Downloads. Run from PowerShell:

```powershell
Set-Location "C:\Users\nayan\OneDrive\Desktop\programming\projects\Airspace Intelligence"
$env:COMPOSE_PROJECT_NAME = "airspaceintelligence"
$backup = Join-Path .. ("Airspace_Performance_Backup_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $backup -ErrorAction Stop | Out-Null
Copy-Item -Path .\src, .\streamlit_app, .\.streamlit, .\tests -Destination $backup -Recurse -ErrorAction Stop
Expand-Archive -LiteralPath "$HOME\Downloads\Airspace_Performance_Update.zip" -DestinationPath . -Force -ErrorAction Stop
$composeLive = @("-f", ".\docker-compose.yaml", "-f", ".\compose.live.yaml", "--profile", "live")
```

The patch contains no `.env`, runtime data, checkpoints or model artifacts. Keep all your existing files in those locations. The complete source ZIP is for reference; use the small update ZIP on your live project.

## 2. Validate configuration and build

```powershell
docker compose @composeLive config --quiet
"Config exit code: $LASTEXITCODE"
docker compose build dashboard
docker compose --profile tools build loader
docker compose @composeLive build loader-live ml-live
docker compose up -d --no-deps --wait postgres
```

Expected: config is silent with exit code **0**; all builds succeed; PostgreSQL is healthy. Stop at a failure and share it. These commands do not recreate Kafka or Spark.

## 3. Run lightweight tests

```powershell
docker compose --profile tools run --rm --no-deps --entrypoint python --volume "${PWD}/tests:/app/tests:ro" loader -m unittest discover -s /app/tests -p test_performance.py -v
docker compose run --rm --no-deps --entrypoint python --volume "${PWD}/tests:/app/tests:ro" dashboard -m unittest discover -s /app/tests -p test_dashboard.py -v
```

Expected: **4 tests, OK** then **20 tests, OK**. The dashboard tests use synthetic fixtures and Streamlit AppTest; they are not live DB or visual-browser tests. No skipped test counts as a pass.

Optional compilation without writing bytecode into your mounted source:

```powershell
docker compose --profile tools run --rm --no-deps -e PYTHONPYCACHEPREFIX=/tmp/phase12-pycache --entrypoint python loader -m compileall -q /app/src
docker compose run --rm --no-deps -e PYTHONPYCACHEPREFIX=/tmp/phase12-pycache --entrypoint python dashboard -m compileall -q /app/streamlit_app
```

Expected: no syntax errors and exit code 0 for each command.

## 4. Test actual SQL, rollback and idempotency

These opt-in tests create and remove **only uniquely named test schemas**, with tiny synthetic fixtures. They require CREATE SCHEMA permission. Public observations, models and predictions are not modified.

```powershell
docker compose --profile tools run --rm --no-deps -e PERFORMANCE_TEST_POSTGRES=1 --entrypoint python --volume "${PWD}/tests:/app/tests:ro" loader -m unittest discover -s /app/tests -p test_performance_postgres.py -v
docker compose run --rm --no-deps -e DASHBOARD_TEST_POSTGRES=1 --entrypoint python --volume "${PWD}/tests:/app/tests:ro" --volume "${PWD}/src:/app/src:ro" dashboard -m unittest discover -s /app/tests -p test_dashboard_postgres.py -v
```

Expected: **1 test, OK** for each command. The loader test checks COPY, restart skip, budget continuation, rollback before a receipt, force replay without duplicate observations, and concurrent-loader exclusion. The dashboard test exercises real views, saved audits and load metadata in read-only query mode.

## 5. Register existing files, then check the fast repeat

Run twice, letting each command finish:

```powershell
docker compose --profile tools run --rm --no-deps loader
docker compose --profile tools run --rm --no-deps loader
```

Expected:

- First run can take as long as the old loader: it safely rereads existing files once to create receipts. Existing observations are not rewritten.
- Second run with no new files: `files_loaded: 0`, `rows_read: 0`, `rows_inserted: 0`, and `files_skipped` equals the number already registered.
- With Spark still writing, the second run may load a few **new** files. Zero inserts is not required while new data arrives; it should not reread the entire archive.
- Both runs: `files_failed: 0`, `files_pending: 0`. Compare their logged `elapsed_seconds`; no speed-up multiplier is promised.
- Exit 75 means another loader still owns the lock after waiting; retry after it finishes. Do not delete files or receipts.

New files are identified by source key + relative name + size + nanosecond mtime. This assumes Spark's completed part files are immutable. If an external restore/editor changes bytes while preserving those metadata values, replay safely with:

```powershell
docker compose --profile tools run --rm --no-deps loader python -m src.postgres_loader --force
```

Do not use force for routine work. It deliberately rereads the archive. PostgreSQL ON CONFLICT still preserves existing observations.

## 6. Start frequent refresh without changing the hourly pipeline

First check the ML worker once with the existing active model:

```powershell
docker compose @composeLive run --rm --no-deps ml-live ml --once
```

Expected: a normal scoring summary and exit 0. No training messages. If the batch budget is reached, that is pending work: the running worker continues on later cycles. If another scorer is active, retry after it finishes. If a model/library mismatch is reported, stop and share the output; do not retrain just to bypass it.

Use the live worker as the routine scorer while it is enabled, to avoid competing hourly scoring jobs:

```powershell
docker compose exec -T airflow airflow dags pause opensky_ml_scoring
docker compose @composeLive up -d --no-deps loader-live ml-live
docker compose up -d --no-deps --wait dashboard
docker compose @composeLive ps loader-live ml-live dashboard
docker compose @composeLive logs --tail 60 loader-live ml-live
Invoke-RestMethod "http://localhost:8501/_stcore/health"
Start-Process "http://localhost:8501"
```

Expected: workers stay Up, dashboard health is `ok`; loader reports only new files or idle skips. ML logs scoring summaries or `ML idle`. With no changes, a periodic rescan still occurs after ten minutes.

Only the **ML scoring DAG** is paused here. `opensky_pipeline` remains hourly for health checks, fallback loading and DQ auditing; its schedule and task graph are unchanged. ML failure cannot stop the independent loader. If live ML fails to start, restore hourly scoring immediately with the rollback command below.

Live ML preserves the existing scoring lookback (default 72 hours), eligibility rules and model. Old observations outside that window still require an explicit backfill with your existing Phase 11 CLI.

## 7. Verify timestamps and the dashboard

After a few active ingestion cycles, run:

```powershell
Get-Content -Raw .\src\sql\performance_verify.sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
Get-Content -Raw .\src\sql\ml_verify.sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
$since = (Get-Date).ToUniversalTime().AddDays(-1).ToString("yyyy-MM-dd")
docker compose exec -T airflow airflow dags list-runs -d opensky_pipeline -s $since
```

Expected: file receipts/revision advance when new files arrive; observation time advances during active ingestion; ML predictions grow for newly eligible rows; duplicate prediction keys remain **0**; the next hourly pipeline run still succeeds. Matching maximum timestamps alone does not prove every eligible observation has been scored.

Dashboard checks:

1. Overview opens with snapshot KPIs; archive count says **On demand** until its checkbox is selected.
2. Enable archive details: the exact count and trend appear. This explicit request may take longer.
3. Live Airspace filters and map remain usable. Missing positions are excluded and counted.
4. Data Quality opens a labelled **saved audit**. Its time can be older than the latest observation. Choose recalculation only when needed.
5. ML Anomalies shows the existing model and statistical anomaly results; Open aircraft history still works.
6. Leave Overview open with **Every 1 min**. During active ingestion, its observation timestamp should advance before the next hourly DAG run.
7. Record the first page load and a second visit. Share elapsed times and any error/timeout. Do not treat a target of a few seconds as a measured result until you observe it.

Share test summaries, the two loader summaries, worker logs, the SQL output and dashboard observations. Do not share `.env` or rendered Compose configuration containing secrets.

## Stop the optional workers / restore hourly scoring

```powershell
docker compose @composeLive stop loader-live ml-live
docker compose exec -T airflow airflow dags unpause opensky_ml_scoring
```

The incremental loader and dashboard can remain installed. The new tracking tables can remain too; they are additive bookkeeping. For a source rollback, restore the backed-up `src`, `streamlit_app`, `.streamlit` and `tests`, then rebuild/restart only dashboard. Do not delete database tables, volumes, model artifacts or checkpoints.

## Optional tuning in the existing .env

No changes are necessary to start. These are new optional non-secret settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| LOADER_SOURCE_KEY | spark_aircraft_processed | Logical source identity shared across all loader containers; keep it the same for the same dataset. |
| LIVE_LOAD_MAX_FILES | 100 | Maximum files attempted per live cycle. |
| LIVE_LOAD_BUDGET_SECONDS | 45 | Soft cycle budget, checked between file transactions. |
| LIVE_ML_MAX_BATCHES | 4 | Maximum existing 5,000-row scoring batches per cycle, unless ML_BATCH_SIZE is configured differently. |
| LIVE_ML_RESCAN_SECONDS | 600 | Forced rescan when no tracked change is detected. |

Intervals are explicit in `compose.live.yaml` (loader 30 seconds, scoring check 60 seconds); they run after work completes, not on a hard real-time schedule. Existing ML settings still apply. API polling, Kafka offsets/log paths, Spark trigger/checkpoint and hourly DQ schedule are unchanged.
