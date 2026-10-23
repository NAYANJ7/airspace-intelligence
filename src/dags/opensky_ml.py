"""Independent Phase 11 DAGs; no edits or dependency edges in opensky_pipeline.

Scheduled runs share the pipeline's hourly logical date. For ad-hoc scoring,
use the CLI: a manually triggered scoring DAG may have no matching loader run.
No ML imports, database connections or model loads happen during DAG parsing.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

ML_PYTHON = "/opt/airspace-ml/bin/python"
CPU_ENV = {"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"}

with DAG(
    dag_id="opensky_ml_scoring",
    description="Score statistical aircraft anomalies after the matching hourly PostgreSQL load",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=55),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["opensky", "ml"],
) as scoring_dag:
    wait_for_loader = ExternalTaskSensor(
        task_id="wait_for_postgres_loader",
        external_dag_id="opensky_pipeline",
        external_task_id="postgres_loader",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        skipped_states=["skipped"],
        check_existence=True,
        mode="reschedule",  # releases the SequentialExecutor slot while waiting
        deferrable=False,
        poke_interval=60,
        timeout=40 * 60,
        retries=0,
    )
    score_new_observations = BashOperator(
        task_id="score_new_observations",
        bash_command=f"exec {ML_PYTHON} -m src.ml score",
        cwd="/opt/project",
        env=CPU_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=10),
    )
    wait_for_loader >> score_new_observations

with DAG(
    dag_id="opensky_ml_training",
    description="Manually train and activate an independent Isolation Forest model version",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=20),
    default_args={"retries": 0},
    tags=["opensky", "ml"],
) as training_dag:
    BashOperator(
        task_id="train_model",
        bash_command=f"exec {ML_PYTHON} -m src.ml train",
        cwd="/opt/project",
        env=CPU_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=15),
    )
