"""Phase 10 pipeline monitor: one small sample per minute, stored in PostgreSQL for Grafana.

It measures only what Grafana cannot see for itself, and it only READS the pipeline:
  Kafka      broker metadata (low/high watermark of each aircraft_states partition). No message is consumed,
             no consumer group is joined and no offset is committed, so Spark's progress is untouched.
  Spark      the live checkpoint, read-only: the newest commits/<batch> file gives the time of the last
             committed batch; offsets/<batch> gives the end offsets that batch consumed.
             Lag = Kafka high watermark - those offsets. It is stored only when both sides list the same
             partitions and the checkpoint is not ahead of the topic; otherwise it is NULL with a reason.
  Ingestion  newest Parquet file in the newest date partition of data/processed.
  Airflow    dag_run / task_instance rows of the two DAGs from Airflow's SQLite metadata DB, opened read-only.
  PostgreSQL newest observation_time.
Anything that cannot be measured is stored as NULL and explained in `notes`; nothing is estimated.

Run:  python -m src.pipeline_monitor          (loop, every MONITOR_INTERVAL_SECONDS, default 60)
      python -m src.pipeline_monitor --once   (one sample, for a quick check)
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

TOPIC = os.getenv("KAFKA_TOPIC", "aircraft_states")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "/app/data"))
CHECKPOINT = DATA_DIR / "checkpoints" / "aircraft_stream"  # the live checkpoint - only ever read
INGESTION_DIR = DATA_DIR / "processed"
AIRFLOW_DB = os.getenv("AIRFLOW_DB_PATH", "/airflow_home/airflow.db")
INTERVAL = max(15, int(os.getenv("MONITOR_INTERVAL_SECONDS", "60")))
RETENTION_DAYS = int(os.getenv("MONITOR_RETENTION_DAYS", "14"))
DAGS = ("opensky_pipeline", "opensky_maintenance")
RUNS_PER_DAG = 24
ACTIVITY_WINDOW = timedelta(minutes=15)
SQL_FILE = Path(__file__).resolve().parent / "sql" / "pipeline_monitor.sql"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _first_line(exc: Exception) -> str:
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    password = os.getenv("POSTGRES_PASSWORD")
    if password:
        text = text.replace(password, "***")
    return f"{type(exc).__name__}: {text[:150]}" if text else type(exc).__name__


def _mtime(path) -> datetime:
    return datetime.fromtimestamp(os.stat(path).st_mtime, tz=timezone.utc)


# --------------------------------------------------------------------------- Spark (checkpoint, read-only)
def read_spark_checkpoint(now: datetime, notes: list[str]) -> dict:
    """Read this BEFORE asking Kafka for its high watermarks, so the watermark can never be older than the offsets."""
    out = {"ok": False, "batch": None, "committed_at": None, "offsets": None, "batches_15m": None}
    commits = CHECKPOINT / "commits"
    try:
        batches = sorted(int(p.name) for p in commits.iterdir() if p.name.isdigit())
    except OSError as exc:
        notes.append(f"spark: checkpoint not readable ({type(exc).__name__})")
        return out
    if not batches:
        notes.append("spark: no committed batch in the checkpoint")
        return out
    last = batches[-1]
    try:
        out["committed_at"] = _mtime(commits / str(last))
        out["batches_15m"] = sum(1 for b in batches if now - _mtime(commits / str(b)) <= ACTIVITY_WINDOW)
    except OSError as exc:  # a batch file removed by Spark's own cleanup between listing and stat
        notes.append(f"spark: commit file vanished ({type(exc).__name__})")
        return out
    out["ok"], out["batch"] = True, last
    # offsets/<batch>: line 1 "v1", line 2 batch metadata, last line {"<topic>": {"<partition>": <end offset>, ...}}
    try:
        text = (CHECKPOINT / "offsets" / str(last)).read_text(encoding="utf-8").strip()
        parts = json.loads(text.splitlines()[-1]).get(TOPIC)
        offsets = {int(k): int(v) for k, v in parts.items()} if isinstance(parts, dict) and parts else None
        out["offsets"] = offsets if offsets and all(v >= 0 for v in offsets.values()) else None
        if out["offsets"] is None:
            notes.append(f"spark: no usable {TOPIC} offsets in offsets/{last}")
    except (OSError, ValueError, IndexError, AttributeError) as exc:
        notes.append(f"spark: offsets/{last} unreadable ({type(exc).__name__})")
    return out


# --------------------------------------------------------------------------- Kafka (broker metadata only)
def kafka_watermarks(notes: list[str]) -> dict | None:
    """{partition: (low, high)} or None. Uses a throw-away consumer purely to ask the broker for watermarks."""
    try:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "pipeline-monitor",       # required by the client; never subscribes, so no group is formed
            "enable.auto.commit": False,
            "socket.timeout.ms": 5000,
            "log_level": 0,                       # failures are reported through the exception below,
            "error_cb": lambda err: None,         # not as librdkafka log lines every minute
        })
        try:
            topic = consumer.list_topics(TOPIC, timeout=5).topics.get(TOPIC)
            if topic is None or topic.error is not None or not topic.partitions:
                notes.append(f"kafka: topic {TOPIC} not found")
                return None
            return {
                p: tuple(consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5, cached=False))
                for p in sorted(topic.partitions)
            }
        finally:
            consumer.close()
    except Exception as exc:  # broker down, timeout, library missing: all mean "not measured"
        notes.append(f"kafka: {_first_line(exc)}")
        return None


def spark_lag(marks: dict | None, offsets: dict | None, notes: list[str]) -> int | None:
    if not marks or not offsets:
        return None
    if set(marks) != set(offsets):
        notes.append("spark: lag not computed - checkpoint and topic list different partitions")
        return None
    if any(offsets[p] > marks[p][1] for p in offsets):
        notes.append("spark: lag not computed - checkpoint offsets are ahead of the topic's end offsets")
        return None
    return sum(marks[p][1] - offsets[p] for p in offsets)


# --------------------------------------------------------------------------- ingestion output
def ingestion_output(now: datetime, notes: list[str]) -> tuple[datetime | None, int | None]:
    """Newest Parquet file in the newest date partition (Observation_date=YYYY-MM-DD). Only that folder is
    listed, so the cost stays flat as history grows."""
    try:
        partitions = sorted(d for d in INGESTION_DIR.iterdir() if d.is_dir())
        for folder in (list(reversed(partitions[-2:])) or [INGESTION_DIR]):
            newest, recent = None, 0
            with os.scandir(folder) as entries:
                for e in entries:
                    if e.name.endswith(".parquet") and e.is_file():
                        m = datetime.fromtimestamp(e.stat().st_mtime, tz=timezone.utc)
                        newest = m if newest is None or m > newest else newest
                        recent += now - m <= ACTIVITY_WINDOW
            if newest is not None:
                return newest, recent
        notes.append("ingestion: no Parquet files under data/processed")
    except OSError as exc:
        notes.append(f"ingestion: data/processed not readable ({type(exc).__name__})")
    return None, None


# --------------------------------------------------------------------------- Airflow (SQLite, read-only)
def _ts(value) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)  # Airflow stores UTC


def airflow_runs(notes: list[str]) -> list[dict] | None:
    if not Path(AIRFLOW_DB).is_file():
        notes.append("airflow: metadata DB not found")
        return None
    try:
        con = sqlite3.connect(f"file:{AIRFLOW_DB}?mode=ro", uri=True, timeout=5)
        try:
            runs = []
            for dag in DAGS:
                rows = con.execute(
                    "SELECT run_id, run_type, state, queued_at, start_date, end_date FROM dag_run "
                    "WHERE dag_id = ? ORDER BY id DESC LIMIT ?", (dag, RUNS_PER_DAG),
                ).fetchall()
                if not rows:
                    continue
                ids = [r[0] for r in rows]
                tasks = con.execute(
                    f"SELECT run_id, task_id, state, end_date FROM task_instance "
                    f"WHERE dag_id = ? AND run_id IN ({','.join('?' * len(ids))})", (dag, *ids),
                ).fetchall()
                for run_id, run_type, state, queued, start, end in rows:
                    mine = [t for t in tasks if t[0] == run_id]
                    loader = next((t for t in mine if t[1] == "postgres_loader"), None)
                    runs.append({
                        "dag_id": dag, "run_id": run_id, "run_type": run_type, "state": state,
                        "queued_at": _ts(queued), "started_at": _ts(start), "ended_at": _ts(end),
                        "task_states": {t[1]: (t[2] or "none") for t in mine},
                        "loader_state": (loader[2] or "none") if loader else None,
                        "loader_ended_at": _ts(loader[3]) if loader else None,
                    })
            return runs
        finally:
            con.close()
    except (sqlite3.Error, ValueError) as exc:
        notes.append(f"airflow: {_first_line(exc)}")
        return None


# --------------------------------------------------------------------------- PostgreSQL
def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
        application_name="pipeline_monitor",
        options="-c statement_timeout=30000",
    )


UPSERT_RUN = """
INSERT INTO pipeline_airflow_runs
    (dag_id, run_id, run_type, state, queued_at, started_at, ended_at, task_states, loader_state, loader_ended_at, updated_at)
VALUES (%(dag_id)s, %(run_id)s, %(run_type)s, %(state)s, %(queued_at)s, %(started_at)s, %(ended_at)s,
        %(task_states)s::jsonb, %(loader_state)s, %(loader_ended_at)s, now())
ON CONFLICT (dag_id, run_id) DO UPDATE SET
    run_type = EXCLUDED.run_type, state = EXCLUDED.state, queued_at = EXCLUDED.queued_at,
    started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at, task_states = EXCLUDED.task_states,
    loader_state = EXCLUDED.loader_state, loader_ended_at = EXCLUDED.loader_ended_at, updated_at = now()
WHERE (pipeline_airflow_runs.state, pipeline_airflow_runs.started_at, pipeline_airflow_runs.ended_at,
       pipeline_airflow_runs.task_states, pipeline_airflow_runs.loader_state, pipeline_airflow_runs.loader_ended_at)
      IS DISTINCT FROM
      (EXCLUDED.state, EXCLUDED.started_at, EXCLUDED.ended_at, EXCLUDED.task_states, EXCLUDED.loader_state,
       EXCLUDED.loader_ended_at)
"""

INSERT_SAMPLE = """
INSERT INTO pipeline_monitor_samples
    (sampled_at, kafka_ok, kafka_start_offset, kafka_end_offset, spark_ok, spark_batch, spark_committed_at,
     spark_committed_offset, spark_lag, spark_batches_15m, ingestion_last_file_at, ingestion_files_15m,
     pg_latest_observation, airflow_ok, notes)
VALUES (%(sampled_at)s, %(kafka_ok)s, %(kafka_start)s, %(kafka_end)s, %(spark_ok)s, %(spark_batch)s,
        %(spark_committed_at)s, %(spark_committed_offset)s, %(spark_lag)s, %(spark_batches_15m)s,
        %(ingestion_last_file_at)s, %(ingestion_files_15m)s, %(pg_latest)s, %(airflow_ok)s, %(notes)s)
ON CONFLICT (sampled_at) DO NOTHING
"""


def sample_once(prune: bool) -> dict:
    now = utcnow()
    notes: list[str] = []
    spark = read_spark_checkpoint(now, notes)         # 1st: checkpoint ...
    marks = kafka_watermarks(notes)                   # 2nd: ... then the broker (see read_spark_checkpoint)
    lag = spark_lag(marks, spark["offsets"], notes)
    ing_at, ing_files = ingestion_output(now, notes)
    runs = airflow_runs(notes)

    with connect() as conn:
        try:
            pg_latest = conn.execute("SELECT max(observation_time) FROM aircraft_observations").fetchone()[0]
        except psycopg.errors.UndefinedTable:
            pg_latest = None
            notes.append("postgres: aircraft_observations does not exist yet")
        row = {
            "sampled_at": now,
            "kafka_ok": marks is not None,
            "kafka_start": sum(lo for lo, _ in marks.values()) if marks else None,
            "kafka_end": sum(hi for _, hi in marks.values()) if marks else None,
            "spark_ok": spark["ok"], "spark_batch": spark["batch"], "spark_committed_at": spark["committed_at"],
            "spark_committed_offset": sum(spark["offsets"].values()) if spark["offsets"] else None,
            "spark_lag": lag, "spark_batches_15m": spark["batches_15m"],
            "ingestion_last_file_at": ing_at, "ingestion_files_15m": ing_files,
            "pg_latest": pg_latest, "airflow_ok": runs is not None, "notes": "; ".join(notes) or None,
        }
        conn.execute(INSERT_SAMPLE, row)
        for run in runs or []:
            conn.execute(UPSERT_RUN, {**run, "task_states": json.dumps(run["task_states"])})
        if prune:
            conn.execute("DELETE FROM pipeline_monitor_samples WHERE sampled_at < now() - make_interval(days => %s)",
                         (RETENTION_DAYS,))
            conn.execute("DELETE FROM pipeline_airflow_runs WHERE updated_at < now() - interval '30 days'")
    return row


def ensure_tables() -> None:
    delay = 2
    while True:
        try:
            with connect() as conn:
                conn.execute(SQL_FILE.read_text(encoding="utf-8"))
            return
        except psycopg.Error as exc:
            print(f"waiting for PostgreSQL: {_first_line(exc)}", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main() -> int:
    missing = [v for v in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not os.getenv(v)]
    if missing:
        print("Missing environment variable(s): " + ", ".join(missing) + ". Set them in .env.", file=sys.stderr)
        return 1
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # PID 1 ignores SIGTERM without a handler
    once = "--once" in sys.argv
    ensure_tables()
    n = 0
    while True:
        started = time.monotonic()
        try:
            row = sample_once(prune=(n % 60 == 0))
            if once or row["notes"] or n % 10 == 0:
                print(f"{row['sampled_at']:%H:%M:%S} kafka_end={row['kafka_end']} spark_batch={row['spark_batch']} "
                      f"lag={row['spark_lag']} ingest_files_15m={row['ingestion_files_15m']} "
                      f"airflow_ok={row['airflow_ok']} notes={row['notes']}", flush=True)
        except Exception as exc:  # one failed sample (e.g. PostgreSQL restarting) must not stop the monitor
            print(f"sample failed: {_first_line(exc)}", flush=True)
        n += 1
        if once:
            return 0
        time.sleep(max(1.0, INTERVAL - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())