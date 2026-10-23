-- Phase 10: tables written by the pipeline-monitor service (src/pipeline_monitor.py) and read by Grafana.
--
-- They hold monitoring samples only (no flight data) and the monitor prunes old rows itself.
-- Safe to run repeatedly: the monitor applies this file on every start.
-- A NULL means "could not be measured at that moment" (the reason is in pipeline_monitor_samples.notes);
-- nothing is estimated or back-filled.

-- @ pipeline_monitor_samples: table
CREATE TABLE IF NOT EXISTS pipeline_monitor_samples (
    sampled_at             TIMESTAMPTZ PRIMARY KEY,
    kafka_ok               BOOLEAN     NOT NULL,   -- broker answered and the topic exists
    kafka_start_offset     BIGINT,                 -- sum of the topic partitions' low watermarks
    kafka_end_offset       BIGINT,                 -- sum of the partitions' high watermarks (grows by 1 per message)
    spark_ok               BOOLEAN     NOT NULL,   -- live checkpoint readable and has at least one committed batch
    spark_batch            BIGINT,                 -- newest committed batch id
    spark_committed_at     TIMESTAMPTZ,            -- mtime of commits/<batch>
    spark_committed_offset BIGINT,                 -- sum of the end offsets that batch consumed
    spark_lag              BIGINT,                 -- kafka_end_offset - spark_committed_offset; NULL unless reliable
    spark_batches_15m      INTEGER,                -- committed batches (retained in the checkpoint) in the last 15 min
    ingestion_last_file_at TIMESTAMPTZ,            -- newest Parquet file under data/processed
    ingestion_files_15m    INTEGER,                -- Parquet files written there in the last 15 min
    pg_latest_observation  TIMESTAMPTZ,            -- max(observation_time) in aircraft_observations
    airflow_ok             BOOLEAN     NOT NULL,   -- Airflow metadata DB readable
    notes                  TEXT
);

-- @ pipeline_airflow_runs: table
-- Recent DAG runs copied (read-only) from Airflow's metadata DB, refreshed every sample.
CREATE TABLE IF NOT EXISTS pipeline_airflow_runs (
    dag_id           TEXT        NOT NULL,
    run_id           TEXT        NOT NULL,
    run_type         TEXT,                          -- scheduled / manual / backfill_job
    state            TEXT,                          -- queued / running / success / failed
    queued_at        TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    ended_at         TIMESTAMPTZ,
    task_states      JSONB,                         -- {"check_kafka": "success", ...}
    loader_state     TEXT,                          -- state of the postgres_loader task (opensky_pipeline only)
    loader_ended_at  TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, run_id)
);