"""Bounded PostgreSQL reads and append-only ML writes (psycopg 3)."""
from __future__ import annotations

import math
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from .config import MLError, Settings
from .features import ELIGIBLE_SQL, SOURCE_COLUMNS

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "sql" / "ml_schema.sql"
COLUMNS = ", ".join("o." + name for name in SOURCE_COLUMNS)
INSERT_PREDICTION_SQL = """
    INSERT INTO aircraft_anomaly_predictions
        (icao24, observation_time, model_version, anomaly_score,
         decision_function, is_anomaly, imputed_feature_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (icao24, observation_time, model_version) DO NOTHING
"""


def connect(settings: Settings):
    import psycopg
    from psycopg.rows import dict_row

    missing = [name for name in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not os.getenv(name)]
    if missing:
        raise MLError("Set existing database settings in .env: " + ", ".join(missing))
    try:
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise MLError("POSTGRES_PORT must be an integer in [1, 65535].") from None
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=port,
        dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], connect_timeout=10,
        autocommit=True, row_factory=dict_row,
        options=f"-c timezone=UTC -c statement_timeout={settings.query_timeout_s * 1000} "
                "-c lock_timeout=5000 -c application_name=airspace_ml",
    )


class Repository:
    def __init__(self, connection):
        self.conn = connection

    def initialize(self) -> None:
        # Atomic DDL for NEW ML tables only. Never invokes Phase 6/7 migrations.
        with self.conn.transaction():
            self.conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))

    def require_schema(self) -> None:
        row = self.conn.execute(
            "SELECT to_regclass('aircraft_ml_models') AS models, "
            "to_regclass('aircraft_anomaly_predictions') AS predictions"
        ).fetchone()
        if not row["models"] or not row["predictions"]:
            raise MLError("ML tables are missing. Run: python -m src.ml init-db")

    def training_rows(self, since: datetime, until: datetime, limit: int) -> list[dict]:
        # Deterministic daily stratification, so a large recent snapshot cannot
        # consume the entire training limit. Sparse days are not topped up.
        days = max(1, math.ceil((until - since).total_seconds() / 86400))
        query = f"""
            WITH windows AS (
                SELECT start_at, LEAST(start_at + interval '1 day', %(until)s) AS end_at
                FROM generate_series(%(since)s::timestamptz,
                     %(until)s::timestamptz - interval '1 microsecond', interval '1 day') AS start_at
            )
            SELECT sample.* FROM windows w
            CROSS JOIN LATERAL (
                SELECT {COLUMNS} FROM aircraft_observations o
                WHERE o.observation_time >= w.start_at AND o.observation_time < w.end_at
                  AND {ELIGIBLE_SQL}
                ORDER BY o.observation_time DESC, o.icao24
                LIMIT %(per_day)s
            ) sample
            ORDER BY sample.observation_time, sample.icao24 LIMIT %(limit)s
        """
        return self.conn.execute(query, {
            "since": since, "until": until, "limit": limit, "per_day": math.ceil(limit / days),
        }).fetchall()

    def pending_rows(self, since: datetime, until: datetime, version: str, limit: int,
                     after: tuple[datetime, str] | None = None) -> list[dict]:
        # NOT EXISTS (rather than a saved max timestamp) admits late arrivals.
        # A per-invocation keyset cursor advances even past rejected records.
        keyset = "AND (o.observation_time, o.icao24) > (%(after_time)s, %(after_icao)s)" if after else ""
        query = f"""
            SELECT {COLUMNS} FROM aircraft_observations o
            WHERE o.observation_time >= %(since)s AND o.observation_time < %(until)s
              AND {ELIGIBLE_SQL} {keyset}
              AND NOT EXISTS (
                  SELECT 1 FROM aircraft_anomaly_predictions p
                  WHERE p.icao24 = o.icao24 AND p.observation_time = o.observation_time
                    AND p.model_version = %(version)s
              )
            ORDER BY o.observation_time, o.icao24 LIMIT %(limit)s
        """
        params = {"since": since, "until": until, "version": version, "limit": limit}
        if after:
            params.update(after_time=after[0], after_icao=after[1])
        return self.conn.execute(query, params).fetchall()

    def register_model(self, metadata: dict) -> None:
        from psycopg.types.json import Jsonb

        with self.conn.transaction():
            self.conn.execute(
                "INSERT INTO aircraft_ml_models (model_version, trained_at, metadata) "
                "VALUES (%s, %s, %s) ON CONFLICT (model_version) DO NOTHING",
                (metadata["model_version"], metadata["trained_at"], Jsonb(metadata)),
            )
            existing = self.conn.execute(
                "SELECT metadata FROM aircraft_ml_models WHERE model_version = %s",
                (metadata["model_version"],),
            ).fetchone()["metadata"]
            if existing != metadata:
                raise MLError("Model version already exists with different metadata; refusing to reuse it.")

    def persist(self, records: list[tuple]) -> int:
        if not records:
            return 0
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.executemany(INSERT_PREDICTION_SQL, records)
                return cur.rowcount

    @contextmanager
    def scoring_lock(self, version: str):
        params = ("airspace_ml_score", version)
        row = self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s)) AS acquired", params
        ).fetchone()
        if not row["acquired"]:
            raise MLError("Another scoring run for this model is active; retry after it completes.")
        try:
            yield
        finally:
            self.conn.execute("SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))", params)
