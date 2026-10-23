"""Fast, deterministic tests with synthetic observations; no live services."""
from __future__ import annotations

import copy
import json
import math
import tempfile
import unittest
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

from src.ml import features, model, service
from src.ml.__main__ import main, timestamp
from src.ml.config import MLError, Settings
from src.ml.db import INSERT_PREDICTION_SQL, Repository

NOW = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)


def sample_rows(count=160):
    rng = np.random.default_rng(123)
    rows = []
    for i in range(count):
        altitude = float(rng.normal(10000, 800))
        rows.append({
            "icao24": f"{i:06x}", "observation_time": NOW - timedelta(minutes=count - i),
            "flight_status": "AIRBORNE", "on_ground": False,
            "geo_altitude": altitude, "baro_altitude": altitude - float(rng.normal(100, 30)),
            "velocity": float(rng.normal(220, 15)), "vertical_rate": float(rng.normal(0, 2)),
            "true_track": float(rng.uniform(0, 360)),
        })
    return rows


class MemoryRepository:
    """Only tests workflow/retry behavior. This is NOT a PostgreSQL emulator."""
    def __init__(self, rows):
        self.rows = list(rows)
        self.records = {}
        self.models = {}
        self.reads = []
        self.fail_batch = None
        self.writes = 0

    def require_schema(self):
        pass

    def training_rows(self, since, until, limit):
        return [r for r in self.rows if since <= r["observation_time"] < until][:limit]

    def scoring_lock(self, version):
        return nullcontext()

    def register_model(self, metadata):
        self.models.setdefault(metadata["model_version"], metadata)

    def pending_rows(self, since, until, version, limit, after=None):
        self.reads.append((since, until, version, limit, after))
        rows = sorted(self.rows, key=lambda r: (r["observation_time"], r["icao24"]))
        return [r for r in rows if since <= r["observation_time"] < until
                and (after is None or (r["observation_time"], r["icao24"]) > after)
                and (r["icao24"], r["observation_time"], version) not in self.records][:limit]

    def persist(self, records):
        self.writes += 1
        if self.writes == self.fail_batch:
            raise RuntimeError("simulated batch failure")
        before = len(self.records)
        for record in records:
            self.records.setdefault(record[:3], record)
        return len(self.records) - before


class FeatureTests(unittest.TestCase):
    def test_nulls_fallback_and_infinities(self):
        rows = sample_rows(5)
        rows[0].update(geo_altitude=None, vertical_rate=float("inf"), true_track=None)
        rows[1]["velocity"] = float("nan")
        rows[2]["on_ground"] = True
        rows[3]["geo_altitude"] = rows[3]["baro_altitude"] = None
        rows[4]["observation_time"] = NOW.replace(tzinfo=None)
        batch = features.build(rows)
        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(batch.skipped, 4)
        self.assertEqual(batch.matrix[0, 0], rows[0]["baro_altitude"])
        self.assertEqual(int(batch.missing_counts[0]), 4)

    def test_circular_heading_and_input_immutability(self):
        rows = sample_rows(2)
        rows[0]["true_track"], rows[1]["true_track"] = 0, 360
        before = copy.deepcopy(rows)
        batch = features.build(rows)
        np.testing.assert_allclose(batch.matrix[0, -2:], batch.matrix[1, -2:])
        self.assertEqual(rows, before)

    def test_bad_identifiers_and_optional_values(self):
        rows = sample_rows(4)
        rows[0]["icao24"] = "invalid"
        rows[1]["flight_status"] = "ON_GROUND"
        rows[2]["on_ground"] = None
        rows[3].update(vertical_rate="bad", true_track=-1)
        batch = features.build(rows)
        self.assertEqual(batch.skipped, 3)
        self.assertEqual(batch.missing_counts.tolist(), [3])

    def test_no_clipping_of_dq_flagged_speed(self):
        rows = sample_rows(1)
        rows[0]["velocity"] = 500.0
        self.assertEqual(features.build(rows).matrix[0, 1], 500.0)

    def test_empty_batch_shape(self):
        batch = features.build([])
        self.assertEqual(batch.matrix.shape, (0, len(features.FEATURE_NAMES)))
        self.assertEqual(batch.missing_counts.shape, (0,))


class ModelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.settings = Settings(model_dir=Path(self.temp.name), min_train_rows=20,
                                 n_estimators=30, max_samples=64, batch_size=41, max_batches=10)
        self.batch = features.build(sample_rows())

    def fitted(self, batch=None):
        return model.fit(batch or self.batch, self.settings, NOW - timedelta(days=1), NOW)

    def save_model(self):
        pipeline, metadata = self.fitted()
        saved = model.save(self.settings.model_dir, pipeline, metadata)
        model.activate(self.settings.model_dir, saved["model_version"])
        return pipeline, saved

    def test_reproducibility_score_sign_and_artifact_round_trip(self):
        first, metadata = self.save_model()
        second, second_meta = self.fitted()
        np.testing.assert_array_equal(model.predict(first, self.batch.matrix)[0],
                                      model.predict(second, self.batch.matrix)[0])
        self.assertEqual(metadata["training_data_sha256"], second_meta["training_data_sha256"])
        loaded, loaded_meta = model.load(self.settings.model_dir)
        scores, decisions = model.predict(loaded, self.batch.matrix)
        np.testing.assert_array_equal(scores, model.predict(first, self.batch.matrix)[0])
        np.testing.assert_allclose(decisions, -scores - metadata["offset"])
        self.assertEqual(metadata, loaded_meta)
        self.assertTrue(np.isfinite(scores).all())

    def test_all_missing_optional_training_columns_are_kept(self):
        rows = sample_rows()
        for row in rows:
            row.update(baro_altitude=None, vertical_rate=None, true_track=None)
        batch = features.build(rows)
        fitted, _ = self.fitted(batch)
        self.assertEqual(fitted.named_steps["preprocess"].transform(batch.matrix).shape[1], 12)
        self.assertTrue(np.isfinite(model.predict(fitted, batch.matrix)[0]).all())

    def test_empty_and_constant_training_fail(self):
        with self.assertRaises(MLError):
            self.fitted(features.build([]))
        rows = sample_rows()
        for row in rows:
            row["velocity"] = 100
        with self.assertRaises(MLError):
            self.fitted(features.build(rows))

    def test_missing_model_and_path_traversal_fail(self):
        with self.assertRaises(MLError):
            model.load(self.settings.model_dir)
        with self.assertRaises(MLError):
            model.load(self.settings.model_dir, "../../outside")

    def test_checksum_corruption_is_rejected_before_unpickling(self):
        _, metadata = self.save_model()
        path = self.settings.model_dir / metadata["model_version"] / "model.joblib"
        path.write_bytes(path.read_bytes() + b"corrupt")
        with patch("src.ml.model.joblib.load") as loader:
            with self.assertRaisesRegex(MLError, "checksum"):
                model.load(self.settings.model_dir)
            loader.assert_not_called()

    def test_version_mismatch_is_rejected(self):
        _, metadata = self.save_model()
        path = self.settings.model_dir / metadata["model_version"] / "metadata.json"
        metadata["runtime"]["scikit-learn"] = "0.0"
        path.write_text(json.dumps(metadata), encoding="utf-8")
        with self.assertRaisesRegex(MLError, "runtime"):
            model.load(self.settings.model_dir)

    def test_feature_schema_mismatch_is_rejected(self):
        _, metadata = self.save_model()
        path = self.settings.model_dir / metadata["model_version"] / "metadata.json"
        metadata["feature_schema"] = "different"
        path.write_text(json.dumps(metadata), encoding="utf-8")
        with self.assertRaisesRegex(MLError, "feature"):
            model.load(self.settings.model_dir)

    def test_failed_activation_and_training_preserve_active(self):
        _, metadata = self.save_model()
        pointer = (self.settings.model_dir / "active.json").read_bytes()
        with self.assertRaises(MLError):
            model.activate(self.settings.model_dir, "does-not-exist")
        with self.assertRaises(MLError):
            service.train(MemoryRepository([]), self.settings, NOW)
        self.assertEqual(pointer, (self.settings.model_dir / "active.json").read_bytes())

    def test_retry_does_not_duplicate_and_late_arrivals_are_found(self):
        self.save_model()
        repo = MemoryRepository(sample_rows())
        first = service.score(repo, self.settings, NOW)
        self.assertEqual(first["inserted"], 160)
        self.assertEqual(service.score(repo, self.settings, NOW)["inserted"], 0)
        late = sample_rows(1)[0]
        late.update(icao24="abc123", observation_time=NOW - timedelta(hours=2))
        repo.rows.append(late)
        self.assertEqual(service.score(repo, self.settings, NOW)["inserted"], 1)
        self.assertEqual(len(repo.records), 161)

    def test_new_model_gets_separate_prediction_keys(self):
        self.save_model()
        repo = MemoryRepository(sample_rows())
        service.score(repo, self.settings, NOW)
        self.save_model()
        self.assertEqual(service.score(repo, self.settings, NOW)["inserted"], 160)
        self.assertEqual(len(repo.records), 320)

    def test_partial_failure_resumes_after_committed_batch(self):
        self.save_model()
        repo = MemoryRepository(sample_rows())
        repo.fail_batch = 2
        with self.assertRaises(RuntimeError):
            service.score(repo, self.settings, NOW)
        self.assertEqual(len(repo.records), 41)
        repo.fail_batch = None
        self.assertEqual(service.score(repo, self.settings, NOW)["inserted"], 119)
        self.assertEqual(len(repo.records), 160)

    def test_budget_is_bounded_and_reports_remaining_work(self):
        self.save_model()
        repo = MemoryRepository(sample_rows())
        result = service.score(repo, replace(self.settings, max_batches=1), NOW)
        self.assertEqual(result["inserted"], 41)
        self.assertTrue(result["batch_budget_reached"])

    def test_rejected_rows_do_not_starve_later_batches(self):
        self.save_model()
        rows = sample_rows()
        for row in rows[:41]:
            row["velocity"] = None
        repo = MemoryRepository(rows)
        result = service.score(repo, self.settings, NOW)
        self.assertEqual(result["rejected"], 41)
        self.assertEqual(result["inserted"], 119)

    def test_scoring_never_fits_and_empty_scoring_succeeds(self):
        self.save_model()
        with patch("src.ml.model.fit", side_effect=AssertionError("scoring must not train")):
            result = service.score(MemoryRepository([]), self.settings, NOW)
        self.assertEqual(result["scored"], 0)


class ConfigAndSQLTests(unittest.TestCase):
    def test_bad_settings_rejected(self):
        for changes in ({"batch_size": 0}, {"contamination": 0}, {"contamination": math.nan},
                        {"train_max_rows": 10}, {"random_state": -1}, {"n_jobs": 0}):
            with self.subTest(changes=changes), self.assertRaises(MLError):
                replace(Settings(), **changes).validate()

    def test_explicit_timezone_required(self):
        import argparse
        with self.assertRaises(argparse.ArgumentTypeError):
            timestamp("2026-10-16T12:00:00")
        self.assertEqual(timestamp("2026-10-16T12:00:00Z"), NOW)

    def test_cli_does_not_log_raw_exception_secrets(self):
        with patch("src.ml.config.Settings.from_env", side_effect=RuntimeError("password=private")):
            with self.assertLogs("airspace.ml", "ERROR") as captured:
                self.assertEqual(main(["score"]), 1)
        self.assertNotIn("private", " ".join(captured.output))

    def test_repository_emits_conflict_safe_insert_in_transaction(self):
        from unittest.mock import MagicMock
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        repo = Repository(connection)
        self.assertEqual(repo.persist([("key",)]), 0)
        self.assertEqual(cursor.executemany.call_args.args[0], INSERT_PREDICTION_SQL)
        self.assertIn("ON CONFLICT (icao24, observation_time, model_version) DO NOTHING", INSERT_PREDICTION_SQL)
        connection.transaction.assert_called_once()


if __name__ == "__main__":
    unittest.main()
