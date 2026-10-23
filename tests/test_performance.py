"""Lightweight behavior checks for the performance changes. Run by the operator."""
import os
import unittest
from unittest.mock import patch

from src.refresh_worker import ScoreWatch, env_int


class ScoreWatchTests(unittest.TestCase):
    def test_new_files_and_model_changes_trigger_scoring(self):
        watch = ScoreWatch()
        token = ("model-a", "time-a", (("spark", 1),))
        self.assertTrue(watch.due(token, 100, 600))
        watch.record(token, 100, False)
        self.assertFalse(watch.due(token, 130, 600))
        self.assertTrue(watch.due(("model-a", "time-a", (("spark", 2),)), 130, 600))
        self.assertTrue(watch.due(("model-b", "time-a", (("spark", 1),)), 130, 600))

    def test_backlog_budget_never_marks_work_as_finished(self):
        watch = ScoreWatch()
        token = ("model-a", "time-a", ())
        watch.record(token, 100, False)
        watch.record(token, 200, True)
        self.assertTrue(watch.due(token, 201, 600))

    def test_periodic_rescan_finds_untracked_late_inserts(self):
        watch = ScoreWatch()
        token = ("model-a", "unchanged-max-time", ())
        watch.record(token, 100, False)
        self.assertFalse(watch.due(token, 699, 600))
        self.assertTrue(watch.due(token, 700, 600))

    def test_invalid_worker_budgets_rejected(self):
        for value in ("bad", "0", "100001"):
            with self.subTest(value=value), patch.dict(os.environ, {"LIVE_LOAD_MAX_FILES": value}):
                with self.assertRaises(ValueError):
                    env_int("LIVE_LOAD_MAX_FILES", 100, 1, 100000)


if __name__ == "__main__":
    unittest.main()
