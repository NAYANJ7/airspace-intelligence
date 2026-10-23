-- Phase 8: history of the data-quality checks, one row per check per Airflow run.
--
-- The checks themselves stay in data_quality.sql (the view v_data_quality_checks is the
-- source of truth); the opensky_pipeline DAG copies that view's result here after each load:
--     INSERT INTO data_quality_history SELECT now(), check_name, ... FROM v_data_quality_checks
-- so dashboards can plot a check's issue_count over time. Safe to run repeatedly
-- (python -m src.run_sql dq_history, or automatically by the DAG).

-- @ data_quality_history: table
CREATE TABLE IF NOT EXISTS data_quality_history (
    check_time   TIMESTAMPTZ NOT NULL,   -- one value per run (the same for all of its checks)
    check_name   TEXT        NOT NULL,
    severity     TEXT        NOT NULL,   -- error / warn / info (what the check is allowed to be)
    issue_count  BIGINT      NOT NULL,
    rows_checked BIGINT,                 -- NULL for checks that are not row counts
    status       TEXT        NOT NULL,   -- ok, or the severity when issue_count > 0
    detail       TEXT,
    PRIMARY KEY (check_time, check_name)
);
