"""Orchestration and monitoring of the OpenSky pipeline (scheduled hourly).

Ingestion, Kafka and Spark are long-running Docker services. Airflow never starts,
runs or waits for them: it only checks that they are alive and producing recent
output, loads what Spark wrote into PostgreSQL, runs the SQL data-quality checks
and reports the outcome.

    check_ingestion ------------------------+
    check_kafka -> check_aircraft_states ---+--> postgres_loader --> data_quality_checks
    check_spark ----------------------------+     (runs even if a check failed)
    every task above ------------------------------------------------> report_status

- check_ingestion: looks at most recent file under data/processed/
  (written by phase 2's storage.py). Does not call the opensky api itself - that
  would mean a second api caller competing with whatever ingestion process is
  already running, and unplanned credit usage.
- check_kafka: confirms the broker is reachable (kafka:19092 from inside Docker).
- check_aircraft_states: confirms the aircraft_states topic exists via broker
  metadata. Does not consume any messages from it.
- check_spark: Spark is a continuous service, so the evidence of recent processing
  is its checkpoint: the newest commits/<batch> file. Read-only - the checkpoint is
  never modified. (Airflow cannot see Docker containers, so "is the container
  running" is shown by `docker compose ps`, not here.)
- postgres_loader: runs the existing src/postgres_loader.py. It is idempotent on
  (icao24, observation_time), so a retry or an overlapping run cannot duplicate rows.
- data_quality_checks: reads the existing v_data_quality_checks view and stores one
  row per check in data_quality_history. Only status 'error' fails the task;
  'warn' and 'info' findings are logged and kept, never fatal.
- report_status: always runs last; fails the run (red in the UI) if any task above
  failed, and lists which ones."""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/project/data"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
FRESHNESS_WINDOW = timedelta(minutes=10)
SPARK_CHECKPOINT_DIR = DATA_DIR / "checkpoints" / "aircraft_stream"  # the live checkpoint; read-only here
DQ_HISTORY_SQL = Path(__file__).resolve().parents[1] / "sql" / "dq_history.sql"


def check_ingestion(**context):
    processed_dir = DATA_DIR / "processed"
    files = list(processed_dir.glob("**/*.parquet")) if processed_dir.exists() else []
    if not files:
        raise AirflowException(
            f"No Parquet files under {processed_dir} - is `python -m src.main` running?"
        )

    latest = max(files, key=lambda f: f.stat().st_mtime)
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(latest.stat().st_mtime, tz=timezone.utc)
    print(f"Most recent ingestion output: {latest} (age: {age})")
    if age > FRESHNESS_WINDOW:
        raise AirflowException(f"Latest ingestion output is {age} old (older than {FRESHNESS_WINDOW}).")

def check_kafka(**context):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient
    admin=AdminClient({"bootstrap.servers":KAFKA_BOOTSTRAP_SERVERS})
    try:
        admin.list_topics(timeout=5)
    except KafkaException as e:
        raise AirflowException(f"Kafka is not reachable at {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka reachable at {KAFKA_BOOTSTRAP_SERVERS}")

def check_aircraft_states(**context):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    try:
        metadata = admin.list_topics(timeout=5)
    except KafkaException as e:
        raise AirflowException(f"Could not reach Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}")

    if "aircraft_states" not in metadata.topics:
        raise AirflowException(
            f"Topic 'aircraft_states' not found on {KAFKA_BOOTSTRAP_SERVERS} "
            "(has it been created - see Phase 3 setup?)."
        )
    print("Topic 'aircraft_states' exists.")


def _age(path: Path) -> timedelta:
    return datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def check_spark(**context):
    commits_dir = SPARK_CHECKPOINT_DIR / "commits"
    batches = sorted(int(p.name) for p in commits_dir.glob("*") if p.name.isdigit()) if commits_dir.exists() else []
    if not batches:
        raise AirflowException(
            f"No committed batches under {commits_dir} - the live checkpoint is missing, "
            "or Spark has never completed a batch."
        )
    age = _age(commits_dir / str(batches[-1]))
    print(f"Spark checkpoint: {len(batches)} committed batches; latest batch {batches[-1]} committed {age} ago")
    if age <= FRESHNESS_WINDOW:
        return

    # A stale commit only means a problem if there was something new to process: Spark
    # commits a batch only when new Kafka data arrives, and ingestion writes its Parquet in
    # the same poll cycle in which it publishes to Kafka.
    processed_dir = DATA_DIR / "processed"
    files = list(processed_dir.glob("**/*.parquet")) if processed_dir.exists() else []
    ingestion_age = min((_age(f) for f in files), default=None)
    if ingestion_age is None or ingestion_age > FRESHNESS_WINDOW:
        print("No recent ingestion output either, so Spark has nothing new to process "
              "(check_ingestion reports that problem).")
        return
    raise AirflowException(
        f"Spark is not keeping up: ingestion wrote output {ingestion_age} ago, but the latest Spark "
        f"batch was committed {age} ago. Check `docker compose ps` and `docker compose logs spark`."
    )


def run_postgres_loader(**context):
    # The existing loader, unchanged. It reads POSTGRES_* and SPARK_PROCESSED_PATH from the
    # environment and never prints the password.
    from src import postgres_loader

    if postgres_loader.main() != 0:
        raise AirflowException("PostgreSQL loader failed - see its output above.")


def run_data_quality_checks(**context):
    import psycopg
    from src.postgres_loader import connect

    with connect() as conn:
        conn.execute(DQ_HISTORY_SQL.read_text(encoding="utf-8"))  # CREATE TABLE IF NOT EXISTS
        try:
            rows = conn.execute(
                "INSERT INTO data_quality_history "
                "(check_time, check_name, severity, issue_count, rows_checked, status, detail) "
                "SELECT now(), check_name, severity, issue_count, rows_checked, status, detail "
                "FROM v_data_quality_checks "
                "RETURNING check_name, severity, issue_count, rows_checked, status, detail"
            ).fetchall()
        except psycopg.errors.UndefinedTable:
            raise AirflowFailException(
                "View v_data_quality_checks does not exist - run once: "
                "python -m src.run_sql analytics data_quality"
            )

    for name, severity, issue_count, rows_checked, status, detail in rows:
        if status != "ok":
            print(f"[{status.upper():5}] {name}: {issue_count} of {rows_checked} - {detail}")
    errors = [r[0] for r in rows if r[4] == "error"]
    warnings = [r[0] for r in rows if r[4] == "warn"]
    print(f"{len(rows)} checks stored in data_quality_history: {len(errors)} error, "
          f"{len(warnings)} warn, {len([r for r in rows if r[4] == 'info'])} info")
    if errors:
        raise AirflowFailException("Data-integrity check(s) with status 'error': " + ", ".join(errors))


def report_status(**context):
    results = {
        ti.task_id: str(ti.state)
        for ti in context["dag_run"].get_task_instances()
        if ti.task_id != "report_status"
    }
    print("Pipeline run summary:")
    for task_id, state in sorted(results.items()):
        print(f"  {task_id:<24} {state}")
    problems = {t: s for t, s in results.items() if s != "success"}
    if problems:
        raise AirflowException(
            "Pipeline run has problems - " + ", ".join(f"{t}: {s}" for t, s in sorted(problems.items()))
            + ". Open the task log in the Airflow UI for the cause."
        )
    print("All tasks succeeded.")


with DAG(
    dag_id="opensky_pipeline",
    description="Hourly health checks, PostgreSQL load and data-quality checks for the OpenSky pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never two runs at once
    dagrun_timeout=timedelta(minutes=55),
    is_paused_upon_creation=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=10)},
    tags=["opensky"],
) as dag:
    t_ingestion = PythonOperator(task_id="check_ingestion", python_callable=check_ingestion)
    t_kafka = PythonOperator(task_id="check_kafka", python_callable=check_kafka)
    t_topic = PythonOperator(task_id="check_aircraft_states", python_callable=check_aircraft_states)
    t_spark = PythonOperator(task_id="check_spark", python_callable=check_spark)
    # A failed health check must not stop data that Spark already wrote from being loaded.
    t_loader = PythonOperator(
        task_id="postgres_loader",
        python_callable=run_postgres_loader,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=30),
    )
    t_dq = PythonOperator(task_id="data_quality_checks", python_callable=run_data_quality_checks)
    t_report = PythonOperator(
        task_id="report_status", python_callable=report_status, trigger_rule=TriggerRule.ALL_DONE, retries=0
    )

    t_kafka >> t_topic
    [t_ingestion, t_topic, t_spark] >> t_loader >> t_dq
    [t_ingestion, t_kafka, t_topic, t_spark, t_loader, t_dq] >> t_report
