"""Opt-in real COPY/receipt/rollback tests. Writes only a unique disposable schema.

PERFORMANCE_TEST_POSTGRES=1 is required. No public tables are changed. This file
has deliberately not been run in the delivery sandbox; run it in the loader image.
"""
import importlib.util
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

HAS_DEPS = all(importlib.util.find_spec(name) for name in ("pyarrow", "psycopg", "dotenv"))


def write_fixture(path, icao):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from src.postgres_loader import COLUMNS

    now = datetime(2026, 10, 23, 10, tzinfo=timezone.utc)
    schema = pa.schema([
        ("icao24", pa.string()), ("callsign", pa.string()), ("origin_country", pa.string()),
        ("observation_time", pa.timestamp("us", tz="UTC")), ("ingested_at", pa.timestamp("us", tz="UTC")),
        *[(name, pa.float64()) for name in ("longitude", "latitude", "baro_altitude", "geo_altitude", "velocity", "true_track", "vertical_rate")],
        ("on_ground", pa.bool_()), ("squawk", pa.string()), ("spi", pa.bool_()),
        ("position_source", pa.int16()), ("category", pa.int16()), ("observation_date", pa.date32()),
        ("altitude_km", pa.float64()), ("velocity_kmh", pa.float64()), ("flight_status", pa.string()),
        ("observation_hour", pa.int16()), ("ingestion_latency_seconds", pa.float64()),
    ])
    values = [icao, "TEST", "Test", now, now, 77., 28., 9000., 9100., 200., 90., 0.,
              False, None, False, 0, 0, now.date(), 9.1, 720., "AIRBORNE", 10, 0.]
    pq.write_table(pa.Table.from_pylist([dict(zip(COLUMNS, values))], schema=schema), path)


@unittest.skipUnless(HAS_DEPS and os.getenv("PERFORMANCE_TEST_POSTGRES") == "1",
                     "Set PERFORMANCE_TEST_POSTGRES=1 in the loader image")
class LoaderPostgresTests(unittest.TestCase):
    def test_receipts_retries_budgets_atomic_rollback_and_lock(self):
        from psycopg import sql
        from src import incremental_loader as loader
        from src.postgres_loader import connect

        schema = "perf_test_" + uuid.uuid4().hex
        with connect() as conn, tempfile.TemporaryDirectory() as directory:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            try:
                conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
                root = Path(directory)
                write_fixture(root / "a.parquet", "abc123")
                write_fixture(root / "b.parquet", "abc124")
                first = loader.run_load(root, connection=conn, max_files=1)
                self.assertEqual(first["rows_inserted"], 1)
                self.assertEqual(first["files_pending"], 1)
                second = loader.run_load(root, connection=conn)
                self.assertEqual(second["files_skipped"], 1)
                self.assertEqual(second["rows_inserted"], 1)
                with patch.object(loader.base, "read_parquet_file", side_effect=AssertionError("Must not reread committed files")):
                    retry = loader.run_load(root, connection=conn)
                self.assertEqual(retry["rows_read"], 0)
                self.assertEqual(retry["files_skipped"], 2)

                third = root / "c.parquet"
                write_fixture(third, "abc125")
                stamp = loader.fingerprint(third)
                with patch.object(loader, "fingerprint", side_effect=[stamp, (stamp[0]+1, stamp[1])]):
                    with self.assertRaises(RuntimeError):
                        loader.load_tracked_file(conn, third, "spark_aircraft_processed", third.name, stamp)
                self.assertEqual(conn.execute("SELECT count(*) FROM aircraft_observations").fetchone()[0], 2)
                self.assertEqual(conn.execute("SELECT count(*) FROM aircraft_loaded_files").fetchone()[0], 2)
                self.assertEqual(loader.run_load(root, connection=conn)["rows_inserted"], 1)
                replay = loader.run_load(root, connection=conn, force=True)
                self.assertEqual(replay["rows_read"], 3)
                self.assertEqual(replay["rows_inserted"], 0)
                self.assertEqual(conn.execute("SELECT count(*) FROM aircraft_observations").fetchone()[0], 3)

                with connect() as other:
                    other.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
                    with loader.loader_lock(conn):
                        with self.assertRaises(loader.LoaderBusy):
                            with loader.loader_lock(other):
                                self.fail("Concurrent loader acquired an already-held lock")
            finally:
                conn.execute("SET search_path TO public")
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    unittest.main()
