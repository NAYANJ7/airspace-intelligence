"""Low-frequency PostgreSQL maintenance, kept separate from the hourly load.

VACUUM ANALYZE refreshes the planner statistics and the visibility map of
aircraft_observations. It runs once a day (03:00 UTC), NOT after every loader run;
it takes no lock that blocks the loader's inserts or the analytics queries."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def vacuum_analyze(**context):
    from src.postgres_loader import connect

    # connect() is autocommit, which VACUUM requires (it cannot run inside a transaction).
    with connect() as conn:
        conn.execute("VACUUM (ANALYZE, PARALLEL 0) aircraft_observations")
    print("VACUUM ANALYZE aircraft_observations: done")


with DAG(
    dag_id="opensky_maintenance",
    description="Daily VACUUM ANALYZE of aircraft_observations",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["opensky", "maintenance"],
) as dag:
    PythonOperator(
        task_id="vacuum_analyze_aircraft_observations",
        python_callable=vacuum_analyze,
        execution_timeout=timedelta(hours=1),
    )
