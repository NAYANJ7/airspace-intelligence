# Phase 11 — ML aircraft anomaly detection

Implemented against the attached Phase 1–10 project. The new component reads
`aircraft_observations`, trains a CPU Isolation Forest separately, and appends
versioned predictions to dedicated ML tables. Its output is **statistical
anomalies, not confirmed flight-safety incidents**.

No model trained on your real observations is included: the archive contains
source, not a database export, and your live services were not accessible here.
See `PHASE11_VALIDATION.md` for the actual checks and their limits.

## 1. What was actually in the project

The original ZIP has an `archive/` root, 60 files, and no `.env`, `.env.example`,
database dump, Kafka volume, Spark output or checkpoint data. It includes 11
cached Python bytecode files and one obsolete Compose backup. The release
omits those generated/obsolete files; it retains all other original files.

Existing runtime source:

```text
archive/
  docker-compose.yaml
  Dockerfile.{ingestion,spark,loader,airflow,dashboard,monitor}
  requirements.txt, requirements-loader.txt, requirements-spark.txt
  src/
    config.py, main.py, opensky_client.py
    normalizer.py, validator.py, deduplicator.py, storage.py
    kafka_producer.py, kafka_consumer.py, spark_streaming.py
    postgres_schema.sql, postgres_loader.py, run_sql.py, pipeline_monitor.py
    dags/opensky_pipeline.py, dags/opensky_maintenance.py
    sql/analytics.sql, data_quality.sql, dq_history.sql, explain.sql
    sql/pipeline_monitor.sql, grafana_role.sql
  streamlit_app/app.py, db.py, queries.py, render.py, requirements.txt, README.md
  .streamlit/config.toml
  grafana/dashboards/airspace_pipeline.json
  grafana/provisioning/dashboards/airspace.yaml
  grafana/provisioning/datasources/airspace.yaml
  grafana/README.md
  architecture.png, OpenSky_Airspace_Intelligence_Full_Code_Walkthrough.pdf
  phase6_check.txt
```

The service chain is OpenSky OAuth2 ingestion/normalization → Kafka
`aircraft_states` → continuous Spark → Parquet at
`data/spark/aircraft_processed` → hourly idempotent PostgreSQL loader.
Streamlit reads PostgreSQL, and the monitor collects pipeline state for Grafana.

Compose services are `kafka`, `ingestion`, `spark`, `postgres`, `loader`,
`airflow`, `dashboard`, `monitor`, `grafana-init`, `grafana`. `loader` has the
`tools` profile; the last three have `monitoring`. Phase 11 adds only `ml`
under an explicit `ml` profile. The named volumes remain `kafka_data`,
`postgres_data`, `airflow_home`, `grafana_data`.

PostgreSQL table `aircraft_observations` has **23 columns** and primary key
`(icao24, observation_time)`. Existing code uses unqualified table names and
the connection's normal search path. Phase 11 follows that convention.

| Type | Actual columns |
|---|---|
| `TEXT` | `icao24`, `callsign`, `origin_country`, `squawk`, `flight_status` |
| `TIMESTAMPTZ` | `observation_time`, `ingested_at` |
| `DOUBLE PRECISION` | `longitude`, `latitude`, `baro_altitude`, `geo_altitude`, `velocity`, `true_track`, `vertical_rate`, `altitude_km`, `velocity_kmh`, `ingestion_latency_seconds` |
| `BOOLEAN` | `on_ground`, `spi` |
| `SMALLINT` | `position_source`, `category`, `observation_hour` |
| `DATE` | `observation_date` |

`icao24`, `observation_time`, `flight_status` are NOT NULL. The flight-status
check allows only `AIRBORNE` and `ON_GROUND`. Existing indexes cover observation
time and `(origin_country, observation_time)` in addition to the primary key.

Existing analytics views: `v_plausibility_limits`, `v_latest_aircraft`,
`v_current_status`, `v_daily_activity`, `v_hourly_activity`,
`v_top_origin_countries`, `v_flight_metrics_by_status`,
`v_flight_metrics_by_country`, `v_most_observed_aircraft`.
Also present: `aircraft_history(text)`, `v_snapshot_gaps`,
`v_data_quality_checks`, and the `data_quality_history` persistence table.
These definitions were inspected in source; their installation in the live
database was not verified.

Airflow 2.10.4 uses `SequentialExecutor`. The existing `opensky_pipeline`
DAG runs hourly; its `postgres_loader` task loads data, then
`data_quality_checks` records DQ results. `opensky_maintenance` runs daily.
Neither existing DAG is modified. Streamlit uses a `PAGES` mapping in
`streamlit_app/app.py`, query caching, and read-only, parameterized DB helpers.

## 2. Model and data contract

- Population: `flight_status='AIRBORNE'` AND `on_ground=false`, a six-character
  hexadecimal `icao24`, finite usable speed, and at least one usable altitude.
  Unknown ground status is excluded rather than silently treated as airborne.
- Core bounds: velocity 0–1500 m/s and altitude −1500–60000 m. These deliberately
  broad corruption filters retain speeds above the existing DQ 400 m/s limit.
  Rows failing the core requirements are not scored or written to DQ tables.
- Six input features: altitude in metres (geometric, with barometric fallback),
  velocity in m/s, vertical rate in m/s, geometric-minus-barometric altitude,
  `sin(true_track)` and `cos(true_track)`. Heading 360° is equivalent to 0°.
- Vertical rates outside −300…300 m/s, headings outside 0…360°, non-finite
  optional values, and absent optional values become missing. Missing altitude
  difference and heading components remain separate missing features.
- `SimpleImputer` learns medians **only at training**. A feature absent throughout
  training is filled with zero. Fixed missing flags are appended for all six
  features, giving 12 model inputs. No scaling is necessary for this tree model.
- Aircraft IDs, callsigns, country, coordinates, ingestion latency, and time
  are not predictors. No target labels or flight-safety assertions are used.
- Defaults: 200 trees, max 256 samples per tree, contamination 0.01, random
  seed 42, one CPU worker. Fixed data/order/settings yield reproducible scores;
  each training invocation deliberately receives a fresh model-version ID.
- `anomaly_score = -forest.score_samples(...)`: larger means more unusual.
  `decision_function = score_samples - learned_offset`; a negative value sets
  `is_anomaly=true`. Neither output is a probability. Contamination controls
  the training threshold, not a guaranteed percentage in future batches.

Historical extraction defaults to 30 days / at most 100,000 rows, using a
deterministic daily quota and newest eligible rows within each day. Sparse
days are not topped up. At least 1,000 usable rows and variation in both speed
and altitude are required. A short history may return far fewer rows than the
cap: shorten the training lookback or raise the row limit if appropriate.
This is a bounded stratified sample, not a uniform sample of all traffic.

Scoring defaults to a rolling 72-hour window, in 5,000-row batches, at most
20 batches per invocation. Time bounds are frozen for each invocation and
are `[as_of - lookback, as_of)`. Without `--as-of`, the upper bound is current
UTC, not the newest stored observation. Stale archives need an explicit
historical `--as-of` or a sufficiently large lookback.

`NOT EXISTS` checks the observation key **and model version**. It rescans the
window rather than trusting a maximum timestamp, so late arrivals inside the
window are picked up next time. A keyset cursor advances through batches;
older arrivals behind that cursor are found on the next invocation. Work
outside the window needs a deliberate wider backfill. A batch-budget warning
means backlog may remain; raise the limit or run scoring again before old rows
age out of the window. There is no perpetual global watermark guarantee.

## 3. Persistence and failure behavior

New tables are `aircraft_ml_models` (version, training time, metadata) and
`aircraft_anomaly_predictions` (key, version, score, decision, flag, imputed
feature count, scoring timestamp). The prediction primary key is:

```sql
PRIMARY KEY (icao24, observation_time, model_version)
```

Foreign keys reference the existing observation key and model registry. Batch
writes use `ON CONFLICT (icao24, observation_time, model_version) DO NOTHING`.
Each batch commits atomically. A failed later batch can be retried without
duplicating earlier commits. A PostgreSQL advisory lock prevents concurrent
scorers for the same version; the database key is still the final safeguard.

`init-db` explicitly creates only ML tables/indexes. `train` and `score` require
them but do not silently run upstream schema scripts. No code truncates,
updates, deletes, or rewrites existing observations, DQ results, Kafka state,
or Spark state. The schema checks also reject non-finite scores and inconsistent
flags.

Model bundles live under `artifacts/ml/<model-version>/`:

```text
model.joblib       # fitted imputer, missing flags, forest
metadata.json     # features, settings, sampling, runtime versions, hashes, diagnostics
```

`artifacts/ml/active.json` selects the default version. Bundles are immutable;
the active pointer is replaced atomically only after saving and registration
succeed. Failed/empty training leaves the active model intact. An interrupted
save may leave an unreferenced temporary directory; it never becomes active.
No automatic artifact or prediction deletion is performed.

The full bundle checksum, feature schema, Python major/minor, scikit-learn,
NumPy, SciPy, joblib and threadpoolctl versions are checked before loading.
Joblib is for **trusted local bundles only**; a checksum is not authentication.
Use the same runtime for training/scoring. A Windows Python 3.14 model is
deliberately rejected by the Python 3.12 Airflow environment. For automation,
train with the `ml` Compose service, which shares dependency pins and Python
3.12 with Airflow's ML virtualenv. No extra host Python version is needed.

## 4. Merge into the running project safely

Extract this release into a separate folder for review. Merge only files
marked `added` or `modified` in `PHASE11_CHANGES.json` into **your existing
project directory**. Back up the four modified source/config files first.
If they differ from the provided original hashes, review your local changes
before merging. Keep the existing project directory and Compose project name
so Compose continues to address the same live named volumes.

Do not replace your `.env`, `data/`, checkpoint, or model-artifact directory.
This ZIP supplies none of that runtime state. Do not start Compose from the
fresh extracted `archive/` folder. Do not reset the topic/offsets, change
`KAFKA_LOG_DIRS`, run a volume-removal command, or recreate PostgreSQL/Kafka.
No such action is needed for Phase 11.

The only changed original files are `.gitignore`, `Dockerfile.airflow`,
`docker-compose.yaml`, and `streamlit_app/app.py`. Existing services other than
Airflow have identical Compose definitions. The existing default Airflow
bootstrap password and password echo were removed: existing accounts in
`airflow_home` keep working; only creation of a new admin needs an explicit
private `AIRFLOW_ADMIN_PASSWORD` in `.env`. No credentials are bundled.

## 5. PowerShell: recommended Docker execution

Run from the existing project root after merging the source. Your `postgres`
service must already be running. `--no-deps` deliberately prevents these
one-shot commands from starting or recreating upstream services.

```powershell
# 1. Install ML dependencies into the separate tool image.
New-Item -ItemType Directory -Force .\artifacts\ml | Out-Null
docker compose --profile ml build ml

# 2. Initialize only the new ML tables. Safe to repeat.
docker compose --profile ml run --rm --no-deps ml init-db

# 3. Train and activate a model using historical PostgreSQL observations.
docker compose --profile ml run --rm --no-deps ml train --lookback-days 30 --max-rows 100000

# 4. Score new/unscored eligible observations; this does not retrain.
docker compose --profile ml run --rm --no-deps ml score

# 5. Unit tests (no live DB access by default).
docker compose --profile ml run --rm --no-deps --entrypoint python ml -m unittest discover -s tests -v

# 6. Read-only PostgreSQL verification.
Get-Content -Raw .\src\sql\ml_verify.sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

These tests use the standard library's `unittest`; there is no pytest
dependency. Dashboard contract tests additionally use the project's existing
pandas dependency and skip if it is absent (as in the minimal ML image).
The real Airflow and PostgreSQL tests skip unless their prerequisites exist.

To exercise real PostgreSQL idempotency and constraints, an optional test
creates a unique `phase11_test_*` schema and drops only that test schema when
finished. It needs CREATE SCHEMA permission; it never targets the public
observation tables:

```powershell
docker compose --profile ml run --rm --no-deps -e ML_TEST_POSTGRES=1 --entrypoint python ml -m unittest discover -s tests -p test_ml_postgres.py -v
```

## 6. PowerShell: host Python alternative

For manual host use, use an isolated venv made from your installed Python
(including Python 3.14). This does not replace your project environment or
require another Python installation. Keep the existing `.env` POSTGRES values;
host access normally uses `localhost:5432`, not Docker's `postgres` hostname.

```powershell
# 1. Dependencies.
python -m venv .venv-ml
.\.venv-ml\Scripts\python.exe -m pip install -r .\requirements-ml.txt

# Override only host/port in this PowerShell session if necessary.
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5432"

# 2. Schema.
.\.venv-ml\Scripts\python.exe -m src.ml init-db

# 3. Training.
.\.venv-ml\Scripts\python.exe -m src.ml train --lookback-days 30 --max-rows 100000

# 4. Scoring.
.\.venv-ml\Scripts\python.exe -m src.ml score

# 5. Tests.
.\.venv-ml\Scripts\python.exe -m unittest discover -s tests -v

# 6. Verification through the existing PostgreSQL container.
Get-Content -Raw .\src\sql\ml_verify.sql | docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Choose one runtime for routine training/scoring. If you use this host workflow
and later enable Airflow, train again with the Compose `ml` service before
enabling the scoring DAG. That creates/activates a compatible new version;
historical models and predictions remain available.

## 7. Airflow and dashboard enablement

Two new DAGs are initially paused. `opensky_ml_scoring` has the same `@hourly`
schedule/start date as `opensky_pipeline`. Its rescheduling ExternalTaskSensor
waits for that logical hour's `postgres_loader` task, not the whole pipeline
or its DQ outcome. A failed ML DAG cannot mark the existing DAG as failed.
For ad-hoc scoring, use the CLI: a manually triggered scoring DAG may have no
loader run with the same logical date.

`opensky_ml_training` has `schedule=None`. Trigger it intentionally when you
want a new model. There is no hourly retraining. The BashOperators invoke the
isolated `/opt/airspace-ml` venv, not Airflow's Python environment. Imports and
database/model I/O do not occur during DAG parsing.

Scoring tasks have a 10-minute execution timeout; training tasks have 15
minutes. Airflow still has SequentialExecutor: executing ML work occupies
its one task slot and can delay other Airflow tasks, although ingestion,
Kafka, Spark and Grafana remain independent services. For lengthy initial
training/backfills, use the separate `ml` CLI container outside Airflow.

After initializing, training in Docker and scoring successfully, validate and
rebuild only Airflow and the dashboard. Let active Airflow tasks finish before
recreating that container:

```powershell
# Validates Compose without printing expanded secrets.
docker compose --profile ml config --quiet
docker compose build airflow dashboard
docker compose up -d --no-deps airflow dashboard

# Confirm DAG parsing and model compatibility before enabling the schedule.
docker compose exec -T airflow airflow dags list-import-errors
docker compose exec -T airflow /opt/airspace-ml/bin/python -m src.ml inspect-model
docker compose exec -T airflow airflow dags unpause opensky_ml_scoring

# Optional: enable the manual-only training DAG, then explicitly trigger it.
docker compose exec -T airflow airflow dags unpause opensky_ml_training
# Run the next line only when a new model is wanted:
# docker compose exec -T airflow airflow dags trigger opensky_ml_training
```

Both Airflow and `ml` share a writable bind mount of `./artifacts/ml`; container
recreation does not erase models. The ML image uses UID 50000 / GID 0, matching
the Airflow image, so private model directories are accessible to both.
Existing read-only project mounts and all
named volumes are retained. On Docker Desktop this bind mount is normally
writable; on a Linux host ensure its ownership permits the Airflow container
user to write it. Do not solve permissions by deleting the directory.

The dashboard adds one **ML anomalies** page. It queries only PostgreSQL,
never loads a model, and needs no new dashboard dependencies. Missing ML tables
or models produce setup messages without changing existing pages. It shows
model/window selection, scored/flagged counts, imputation counts, hourly
counts, recent observations with raw measurements and scores, and metadata.
Counts are observations, not unique aircraft. The selected version does not
change the active model. The display window follows the newest stored source
observation and warns when source data or predictions are stale.

## 8. Useful operations and limitations

All optional settings and defaults are listed in `.env.ml.example`. Append
only desired overrides to your current `.env`; do not copy it over `.env`.
Environment variables win over dotenv. Secrets are never included in ML
metadata or arbitrary exception logs. Unexpected errors log their class and
SQLSTATE; e.g. a missing table or insufficient privilege needs database setup,
while a cancelled query may need a smaller batch or longer ML query timeout.

```powershell
# Wider recent backfill; run repeatedly if a batch-budget warning appears.
docker compose --profile ml run --rm --no-deps ml score --lookback-hours 720 --max-batches 40

# Historical example: set this timestamp to the desired exclusive upper bound.
docker compose --profile ml run --rm --no-deps ml score --as-of "2026-10-17T00:00:00Z" --lookback-hours 72

# Inspect the active bundle (safe metadata, no database credentials).
docker compose --profile ml run --rm --no-deps ml inspect-model

# Train a candidate without changing the active pointer.
docker compose --profile ml run --rm --no-deps ml train --no-activate

# Use an actual version from inspect-model / aircraft_ml_models; not a made-up ID.
$ModelVersion = Read-Host "Existing compatible model version"
docker compose --profile ml run --rm --no-deps ml activate --model-version $ModelVersion
```

This first version detects unusual measurement combinations, not trajectory
changes, flight intent, or incidents. Normal rare aircraft/operations and
sensor errors can be flagged. The mixed airborne population, change-only
ingestion, daily sampling, class imbalance and drift affect the baseline.
There are no labels or measured precision/recall, and training flag rate is not
accuracy. Review model output, monitor backlog and flag rates, and retrain
intentionally as data changes. Ground observations and invalid core readings
are outside the modeled population. No automatic cleanup/retention job is added.

Implementation references:
- https://scikit-learn.org/1.8/modules/generated/sklearn.ensemble.IsolationForest.html
- https://scikit-learn.org/stable/model_persistence.html
- https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/sensors/external_task/index.html
- https://airflow.apache.org/docs/apache-airflow/2.10.4/_api/airflow/operators/bash/index.html

The existing PDF and Grafana/Streamlit READMEs remain the Phase 1–10 reference.
Use this README for the new ML workflows and the changed Airflow bootstrap.

## 9. Complete tree of changes

`[A]` added; `[M]` modified. All paths are relative to the existing project root.

```text
archive/
  .dockerignore  [A]
  .env.ml.example  [A]
  .gitignore  [M]
  Dockerfile.airflow  [M]
  Dockerfile.ml  [A]
  PHASE11_CHANGES.json  [A]
  PHASE11_VALIDATION.md  [A]
  README_PHASE11.md  [A]
  docker-compose.yaml  [M]
  requirements-ml.txt  [A]
  src/
    dags/
      opensky_ml.py  [A]
    ml/
      __init__.py  [A]
      __main__.py  [A]
      config.py  [A]
      db.py  [A]
      features.py  [A]
      model.py  [A]
      service.py  [A]
    sql/
      ml_schema.sql  [A]
      ml_verify.sql  [A]
  streamlit_app/
    app.py  [M]
    ml_anomalies.py  [A]
  tests/
    test_ml.py  [A]
    test_ml_integrations.py  [A]
    test_ml_postgres.py  [A]
  validation/
    unit_tests.txt  [A]
```
