# Phase 11 validation report

Validation date: 2026-10-16 UTC. Tests ran against the delivered source in a
Linux sandbox using Python 3.12.14. No connection to the user's live services
was made. No Docker commands affecting runtime services or volumes were run.

| Check | Result | Evidence and boundary |
|---|---|---|
| Python compilation | **PASS** | `python -m compileall -q src streamlit_app tests` exited 0 for all existing/new Python source. |
| Unit/contract suite | **PASS** | `python -m unittest discover -s tests -v`: 34 collected, 32 passed, 2 skipped, 0 failures/errors; final run 1.321 s. Full output: `validation/unit_tests.txt`. |
| Real Isolation Forest | **PASS** | Synthetic data was fitted/scored using scikit-learn 1.8.0, including reproducibility, score direction and persisted bundle round-trip checks. No real observation training was performed. |
| Preprocessing | **PASS** | Empty input, core nulls, invalid identifiers/status, infinities/NaNs, optional missing fields, all-missing training columns, altitude fallback, circular heading and non-mutating input handling. |
| Incremental/idempotent workflow | **PASS** | In-memory repository tests: repeat scoring inserts zero, late arrival inside the window is found, second model gets distinct keys, partial failure resumes after committed batch, bounded batches and rejected-row cursor progression. These are not PostgreSQL execution tests. |
| Model failure handling | **PASS** | Missing model, path traversal, corrupt checksum, changed feature schema/runtime versions, failed activation, failed/empty training, and scoring without retraining. |
| Credential-safe error output | **PASS** | Synthetic exception containing a fake password is not printed. Runtime metadata contains no database connection settings. PDF reference credential assignments were identified as placeholders. |
| SQL syntax | **PASS** | PostgreSQL parser `pglast` 7.14 accepted both new SQL files plus 14 generated backend/dashboard statements (placeholders replaced with NULL for parsing). Syntax parsing does not establish server-side execution or privileges. |
| Source schema consistency | **PASS** | All selected observation columns exist in the 23-column original DDL. Observation FK matches `(icao24, observation_time)`. No destructive/upstream DML appears in the new DDL. |
| Live PostgreSQL DDL, transactions, FK/PK and concurrent retries | **NOT TESTED** | No PostgreSQL server or psql installed; no live database credentials supplied. Opt-in isolated-schema integration test is included and was skipped. |
| Airflow source contract | **PASS** | Python/AST checks verify hourly scoring, manual training, initially paused DAGs, rescheduling sensor, actual loader DAG/task names, and no parse-time ML imports. APIs were checked against Airflow 2.10.4 official documentation. |
| Real Airflow DagBag parse/scheduler execution | **NOT TESTED** | Airflow is not installed. The real DagBag test is included and skipped; no stub import was presented as an Airflow parse. |
| Streamlit page logic | **PASS** | Six tests with mocked Streamlit/DB interfaces cover missing tables, missing models, no predictions, populated rendering branches, DB errors and bound read-only query parameters. Existing five page implementations are unchanged. |
| Real Streamlit AppTest/browser rendering/live queries | **NOT TESTED** | Streamlit is not installed; no visual or live application claim is made. |
| Compose structural configuration | **PASS** | PyYAML 6.0.3 parsing, duplicate-key rejection, service dependency references, build-file existence and Airflow startup shell `bash -n`. |
| Docker Compose CLI config, builds and containers | **NOT TESTED** | Docker/Compose are unavailable. `docker compose --profile ml config --quiet` and targeted build/run commands are provided in the README. |
| Phase 1–10 preservation | **PASS** | Byte comparison against input ZIP shows only four original files changed. Nine original Compose service definitions and all four named-volume definitions are identical. Original pipeline/maintenance DAGs, Kafka/Spark source, loader, schema, analytics and DQ SQL are byte-identical. |
| Windows PowerShell / Python 3.14 execution | **NOT TESTED** | Commands are written for PowerShell, but no Windows runtime is available here. Model fingerprint checks deliberately prevent host Python 3.14 bundles being used by the Python 3.12 Airflow worker. |

Installed numerical versions used for the passing tests: scikit-learn 1.8.0,
NumPy 2.3.5, SciPy 1.17.0, joblib 1.5.3, threadpoolctl 3.6.0. They match
`requirements-ml.txt`. The lightweight SQL/connection helpers used
psycopg 3.2.13 and python-dotenv 1.2.2. Pandas 2.2.3 supported the mocked
dashboard tests. Pglast/PyYAML were validation tools, not new application
requirements. No Airflow, Streamlit, Docker or PostgreSQL installation was
attempted to manufacture an integration result.

## What must be checked on the real machine

1. Merge only changed/new files into the existing project and preserve its
   directory/Compose project name, `.env`, data, checkpoint and volumes.
2. Validate Compose quietly, build the ML tool image, initialize the new
   tables, train a compatible model and score a batch. Inspect raw rows with
   `src/sql/ml_verify.sql`.
3. Repeat scoring and confirm existing observation/model keys are unchanged.
   Zero new inserts is expected only when no new eligible rows arrived and
   the prior run already exhausted the chosen window; active ingestion or
   remaining backlog can legitimately produce new inserts.
4. Optionally run the isolated PostgreSQL integration test. It exercises
   real schema creation, reads, duplicate writes, keys and constraints using
   synthetic observations in its own uniquely named schema.
5. Rebuild only Airflow/dashboard, inspect Airflow import errors and model
   compatibility, then unpause the scoring DAG. Open the dashboard's ML page
   and check its populated/empty behavior against the actual database.

The system cannot currently measure anomaly precision/recall without labeled
review data. Scores reflect this model's airborne population and feature
contract, not flight-safety risk. Bounded rolling-window scans, the training
quota, model/version growth and SequentialExecutor slot sharing are documented
operational limits, not claims of unlimited processing capacity.
