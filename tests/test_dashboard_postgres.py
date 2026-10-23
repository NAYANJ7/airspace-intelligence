"""Opt-in real PostgreSQL checks in a unique disposable schema, never public tables.

Enable only with DASHBOARD_TEST_POSTGRES=1. Requires a database user permitted to
create a schema. Query calls are read-only after fixture setup; cleanup drops only
the generated phase12_test_<uuid> schema.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
READY = (os.getenv("DASHBOARD_TEST_POSTGRES") == "1" and
         all(importlib.util.find_spec(name) for name in ("streamlit", "pandas", "psycopg", "altair", "pydeck")))


@unittest.skipUnless(READY, "Set DASHBOARD_TEST_POSTGRES=1 with dashboard dependencies for isolated PostgreSQL checks")
class DashboardPostgreSQLTests(unittest.TestCase):
    def test_real_views_filters_ml_and_read_only_queries(self):
        sys.path.insert(0, str(ROOT / "streamlit_app"))
        import psycopg
        from psycopg import sql
        from psycopg.types.json import Jsonb
        import streamlit as st
        import db
        import queries as q
        import ml_anomalies as ml
        schema = "phase12_test_" + uuid.uuid4().hex
        now = datetime.now(timezone.utc).replace(microsecond=0)
        conn = psycopg.connect(**db._settings(), autocommit=True, connect_timeout=5)
        created = False
        try:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            created = True
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            conn.execute("SET timezone TO 'UTC'")
            for name in ["src/postgres_schema.sql", "src/sql/analytics.sql", "src/sql/data_quality.sql",
                         "src/sql/ml_schema.sql", "src/sql/dq_history.sql", "src/sql/loader_tracking.sql"]:
                conn.execute((ROOT / name).read_text(encoding="utf-8"), prepare=False)
            rows = [
                ("abc123", now - timedelta(minutes=2), "India", 28., 77., False),
                ("abc123", now, "India", 200., 77., False),
                ("def456", now, "Germany", 50., 8., False),
                ("fed321", now, "India", 29., 78., False),
                ("000000", now, "India", None, None, True),
            ]
            for icao, stamp, country, lat, lon, ground in rows:
                conn.execute("""
                    INSERT INTO aircraft_observations
                    (icao24, observation_time, origin_country, latitude, longitude, on_ground, flight_status,
                     callsign, velocity, velocity_kmh, geo_altitude, baro_altitude, altitude_km, vertical_rate,
                     true_track, observation_date, observation_hour, ingested_at, ingestion_latency_seconds)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'TEST',200,720,10000,9000,10,1,45,%s,%s,%s,1)
                """, (icao, stamp, country, lat, lon, ground, "ON_GROUND" if ground else "AIRBORNE",
                       stamp.date(), stamp.hour, stamp + timedelta(seconds=1)))
            conn.execute("INSERT INTO aircraft_ml_models(model_version,trained_at,metadata) VALUES (%s,%s,%s)",
                         ("test-v1", now, Jsonb({"training_rows": 1000})))
            conn.execute("""INSERT INTO aircraft_anomaly_predictions
                (icao24,observation_time,model_version,anomaly_score,decision_function,is_anomaly,imputed_feature_count)
                VALUES ('fed321',%s,'test-v1',0.7,-0.1,true,0)""", (now,))
            conn.execute("""INSERT INTO data_quality_history
                (check_time,check_name,severity,issue_count,rows_checked,status,detail)
                SELECT %s,check_name,severity,issue_count,rows_checked,status,detail FROM v_data_quality_checks""", (now,))
            conn.execute("INSERT INTO aircraft_load_state VALUES ('test',1,%s,'test.parquet')", (now,))
            conn.execute("SET default_transaction_read_only TO on")

            def execute(statement, params):
                cursor = conn.execute(statement, params)
                return [col.name for col in cursor.description], cursor.fetchall()

            st.cache_data.clear()
            with patch.object(db, "_execute", side_effect=execute):
                self.assertEqual(q.storage_summary()["total_rows"], 5)
                self.assertEqual(q.snapshot_summary()["aircraft"], 4)
                self.assertEqual(q.freshness()["newest"], now)
                self.assertEqual(q.pipeline_load()["last_load"], now)
                saved = q.quality_snapshot()
                self.assertEqual(saved.check_time.max(), now)
                self.assertEqual(int(saved.loc[saved.check_name == "invalid_latitude", "issue_count"].iloc[0]), 1)
                points, counts = q.map_snapshot(15, (), ("AIRBORNE",), 5000)
                self.assertEqual(set(points.icao24), {"def456", "fed321"})
                self.assertEqual(counts["excluded_positions"], 1)
                # Latest invalid abc123 must not resurrect its older valid position.
                points, counts = q.map_snapshot(15, ("India",), ("AIRBORNE",), 5000, (5., 38., 60., 100.))
                self.assertEqual(points.icao24.tolist(), ["fed321"])
                self.assertEqual(counts["matching_count"], 1)
                points, counts = q.map_snapshot(15, (), ("AIRBORNE",), 5000, (-90., 90., -180., 180.), (11., 12.))
                self.assertTrue(points.empty)
                self.assertEqual(counts["matching_count"], 0)
                self.assertEqual(len(q.aircraft_track("abc123", 5000, now - timedelta(minutes=1), now)), 1)
                self.assertEqual(int(q.hourly_activity(now.date(), now.date()).observations.sum()), 5)
                self.assertFalse(q.daily_activity().empty)
                self.assertFalse(q.metrics_by_status().empty)
                self.assertFalse(q.metrics_by_country().empty)
                self.assertFalse(q.top_countries().empty)
                self.assertFalse(q.most_observed().empty)
                checks = q.quality_checks()
                self.assertEqual(int(checks.loc[checks.check_name == "invalid_latitude", "issue_count"].iloc[0]), 1)
                self.assertTrue(ml.available())
                self.assertEqual(ml.summary("test-v1", now - timedelta(hours=1), now)["flagged"], 1)
                self.assertEqual(ml.observations("test-v1", now - timedelta(hours=1), now, True, 100).icao24.tolist(), ["fed321"])
                self.assertEqual(conn.execute("SHOW default_transaction_read_only").fetchone()[0], "on")
        finally:
            if created:
                conn.execute("SET default_transaction_read_only TO off")
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            conn.close()


if __name__ == "__main__":
    unittest.main()
