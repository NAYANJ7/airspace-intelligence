"""Opt-in real PostgreSQL tests in a disposable, uniquely named test schema.

Uses the existing POSTGRES_* settings ONLY when ML_TEST_POSTGRES=1. Does not
touch public tables, observations, roles, databases or Docker volumes.
"""
from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.ml.config import Settings


@unittest.skipUnless(os.getenv("ML_TEST_POSTGRES") == "1", "Set ML_TEST_POSTGRES=1 for isolated PostgreSQL integration tests")
class PostgreSQLTests(unittest.TestCase):
    def test_schema_real_queries_retries_and_constraints(self):
        import psycopg
        from psycopg import sql
        from src.ml.db import Repository, connect
        from src.ml import service

        with tempfile.TemporaryDirectory() as directory:
            settings = replace(Settings.from_env(), model_dir=Path(directory), min_train_rows=20,
                               train_lookback_days=1, train_max_rows=200, n_estimators=20,
                               batch_size=17, max_batches=10)
            name = "phase11_test_" + uuid.uuid4().hex
            with connect(settings) as conn:
                conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
                try:
                    conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(name)))
                    original = Path(__file__).resolve().parents[1] / "src" / "postgres_schema.sql"
                    conn.execute(original.read_text(encoding="utf-8"))
                    repo = Repository(conn)
                    repo.initialize()
                    repo.initialize()
                    now = datetime.now(timezone.utc)
                    values = [(f"{i:06x}", now - timedelta(minutes=i+1), 9000+i*10, 8900+i*10,
                               200+i, i % 6-3, i*5) for i in range(60)]
                    with conn.cursor() as cur:
                        cur.executemany("""
                            INSERT INTO aircraft_observations
                            (icao24, observation_time, geo_altitude, baro_altitude, velocity,
                             vertical_rate, true_track, flight_status, on_ground)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,'AIRBORNE',false)
                        """, values)
                    service.train(repo, settings, now)
                    first = service.score(repo, settings, now)
                    self.assertEqual(first["inserted"], 60)
                    self.assertEqual(service.score(repo, settings, now)["inserted"], 0)
                    self.assertEqual(conn.execute("SELECT count(*) AS n FROM aircraft_anomaly_predictions").fetchone()["n"], 60)
                    self.assertEqual(conn.execute("SELECT count(*) AS n FROM aircraft_observations").fetchone()["n"], 60)
                    # Actual duplicate insert is rejected by PK and ON CONFLICT returns zero.
                    r = conn.execute("SELECT * FROM aircraft_anomaly_predictions LIMIT 1").fetchone()
                    record = tuple(r[k] for k in ("icao24", "observation_time", "model_version", "anomaly_score",
                                                 "decision_function", "is_anomaly", "imputed_feature_count"))
                    self.assertEqual(repo.persist([record, record]), 0)
                    # Actual FK prohibits a prediction without a matching observation.
                    with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                        repo.persist([("ffffff",) + record[1:]])
                finally:
                    conn.execute("SET search_path TO public")
                    conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


if __name__ == "__main__":
    unittest.main()
