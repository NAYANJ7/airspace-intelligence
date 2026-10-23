"""Phase 12 tests. Real Streamlit AppTest + synthetic query fixtures, not a live database test."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "streamlit_app"))
HAS_UI = all(importlib.util.find_spec(name) for name in ("streamlit", "pandas", "altair", "pydeck", "psycopg"))
if HAS_UI:
    import pandas as pd
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    import data_helpers as h
    import queries as q
    import ui
    import ml_anomalies as ml
    from db import DashboardDBError
    from dashboard_fixtures import NOW, LATEST, dashboard_data, history, predictions


@unittest.skipUnless(HAS_UI, "Install streamlit_app/requirements.txt for dashboard tests")
class DisplayDataTests(unittest.TestCase):
    def test_invalid_coordinates_never_become_markers(self):
        frame = pd.DataFrame({"latitude": [0, 90, -90, 91, None, float("inf"), "bad", 20],
                              "longitude": [0, 180, -180, 0, 2, 3, 4, float("nan")]})
        original = frame.copy(deep=True)
        result, rejected = h.valid_positions(frame)
        self.assertEqual(len(result), 3)
        self.assertEqual(rejected, 5)
        pd.testing.assert_frame_equal(frame, original)
        empty, count = h.valid_positions(pd.DataFrame())
        self.assertTrue(empty.empty)
        self.assertEqual(count, 0)

    def test_missing_values_and_history_gaps_break_lines(self):
        frame = history().head(4).copy()
        frame.loc[1, "altitude_km"] = float("inf")
        frame.loc[3, "observation_time"] = frame.loc[2, "observation_time"] + timedelta(minutes=10)
        result = h.history_series(frame, "altitude_km")
        self.assertEqual(len(result), 3)
        self.assertEqual(result.segment.nunique(), 3)
        self.assertIsNone(result.time.dt.tz)
        self.assertTrue(h.history_series(frame.assign(altitude_km=None), "altitude_km").empty)

    def test_freshness_boundaries_and_future_timestamp(self):
        self.assertEqual(ui.freshness_state(None)["key"], "empty")
        self.assertEqual(ui.freshness_state(timedelta(minutes=ui.FRESH_MIN))["key"], "fresh")
        self.assertEqual(ui.freshness_state(timedelta(minutes=ui.FRESH_MIN, seconds=1))["key"], "recent")
        self.assertEqual(ui.freshness_state(timedelta(minutes=ui.STALE_MIN, seconds=1))["key"], "stale")
        self.assertEqual(ui.freshness_state(timedelta(minutes=-5))["key"], "clock")

    def test_activity_window_keeps_full_dates_across_years(self):
        frame = pd.DataFrame({"observation_date": ["2025-12-31", "2026-01-01", "2026-01-02"], "observations": [3, 5, 7]})
        result = h.activity_window(frame, 2)
        self.assertEqual(result.observations.tolist(), [5, 7])

    def test_ml_display_converts_units_without_changing_source(self):
        frame = predictions()
        original = frame.copy(deep=True)
        shown = ml.display_table(frame)
        self.assertEqual(shown["Ground speed (km/h)"].iloc[0], 792.)
        self.assertEqual(shown["Geometric altitude (km)"].iloc[0], 10.5)
        self.assertEqual(shown["Anomaly score"].iloc[0], .7)
        pd.testing.assert_frame_equal(frame, original)

    def test_csv_protects_formulas_but_keeps_numeric_values(self):
        result = h.csv_bytes(pd.DataFrame({"text": ["=1+1", " +SUM(1)", "safe"], "speed": [-1., 2., 3.]})).decode("utf-8-sig")
        self.assertIn("'=1+1", result)
        self.assertIn("' +SUM(1)", result)
        self.assertIn("-1.0", result)


@unittest.skipUnless(HAS_UI, "Install streamlit_app/requirements.txt for dashboard tests")
class DashboardQueryTests(unittest.TestCase):
    def setUp(self):
        st.cache_data.clear()

    def test_map_filters_latest_then_coordinates_and_binds_values(self):
        attack = "'; DROP TABLE aircraft_observations; --"
        row = pd.DataFrame({"icao24": [None], "population_count": [1], "excluded_positions": [1], "matching_count": [0]})
        with patch.object(q, "run_query", return_value=row) as run:
            points, counts = q.map_snapshot(15, (attack,), ("AIRBORNE",), 999999)
        sql, params = run.call_args.args
        self.assertNotIn(attack, sql)
        self.assertEqual(params["countries"], [attack])
        self.assertEqual(params["limit"], 10000)
        self.assertNotIn("BETWEEN", sql.split(", population AS")[0])
        self.assertIn("FROM population", sql)
        self.assertTrue(points.empty)
        self.assertEqual(counts["excluded_positions"], 1)

    def test_history_and_hourly_ranges_are_bound(self):
        with patch.object(q, "run_query", return_value=history()) as run:
            q.aircraft_track("abc123", 10000, LATEST - timedelta(hours=1), LATEST)
        sql, params = run.call_args.args
        self.assertEqual(params["limit"], 5000)
        self.assertEqual(params["until"], LATEST)
        self.assertIn("%(since)s::timestamptz", sql)
        with patch.object(q, "run_query", return_value=pd.DataFrame()) as run:
            q.hourly_activity(LATEST.date(), LATEST.date())
        self.assertIn("observation_date >= %(since)s", run.call_args.args[0])

    def test_missing_optional_monitoring_is_not_an_observation_timestamp(self):
        with patch.object(q, "run_row", side_effect=DashboardDBError("Missing", "missing")):
            self.assertEqual(q.pipeline_load(), {})

    def test_freshness_does_not_run_full_table_count(self):
        with patch.object(q, "run_row", return_value={"newest": LATEST, "oldest": LATEST, "db_now": NOW}) as run:
            q.freshness()
        self.assertNotIn("count(*)", run.call_args.args[0])


@unittest.skipUnless(HAS_UI, "Install streamlit_app/requirements.txt for dashboard tests")
class DashboardAppTests(unittest.TestCase):
    def app(self):
        return AppTest.from_file(str(ROOT / "streamlit_app/app.py"), default_timeout=20)

    def assert_clean(self, app):
        self.assertEqual(len(app.exception), 0, [e.message for e in app.exception])

    def test_overview_skips_archive_queries_until_requested(self):
        with dashboard_data() as mocks:
            app = self.app().run()
            self.assert_clean(app)
            mocks["storage_summary"].assert_not_called()
            mocks["daily_activity"].assert_not_called()
            app.checkbox(key="overview_archive").check().run()
            self.assert_clean(app)
            mocks["storage_summary"].assert_called()
            mocks["daily_activity"].assert_called()

    def test_quality_opens_saved_audit_without_full_archive_scans(self):
        with dashboard_data() as mocks:
            app = self.app()
            app.session_state["page"] = "Data Quality"
            app.run()
            self.assert_clean(app)
            mocks["quality_snapshot"].assert_called()
            mocks["quality_checks"].assert_not_called()
            mocks["snapshot_gaps"].assert_not_called()
            mocks["quality_history"].assert_not_called()
            app.radio(key="dq_source").set_value("Recalculate all history").run()
            self.assert_clean(app)
            mocks["quality_checks"].assert_called()

    def test_missing_saved_audit_has_an_explicit_empty_state(self):
        with dashboard_data({"quality_snapshot": pd.DataFrame()}):
            app = self.app()
            app.session_state["page"] = "Data Quality"
            app.run()
            self.assert_clean(app)
            self.assertTrue(any("No quality results" in item.value for item in app.markdown))

    def test_all_seven_pages_and_analytics_sections_render(self):
        with dashboard_data():
            app = self.app().run()
            self.assert_clean(app)
            for name in ["Live Airspace", "Flight Explorer", "Analytics", "Data Quality", "ML Anomalies", "About / Pipeline Guide"]:
                app.radio(key="page").set_value(name).run()
                self.assert_clean(app)
            app.radio(key="page").set_value("Analytics").run()
            for name in ["Speed & altitude", "Countries & coverage"]:
                app.radio(key="analytics_view").set_value(name).run()
                self.assert_clean(app)

    def test_guide_survives_database_outage(self):
        with dashboard_data({"freshness": DashboardDBError("Database unavailable", "unavailable")}) as mocks:
            app = self.app().run()
            self.assert_clean(app)
            self.assertGreater(len(app.error), 0)
            calls = mocks["freshness"].call_count
            app.radio(key="page").set_value("About / Pipeline Guide").run()
            self.assert_clean(app)
            self.assertEqual(mocks["freshness"].call_count, calls)
            self.assertGreater(len(app.dataframe), 0)

    def test_empty_database_pages_do_not_raise(self):
        empty = {"freshness": {"newest": None, "oldest": None, "db_now": NOW, "fetched_at": NOW},
                 "map_countries": pd.DataFrame(), "map_snapshot": (pd.DataFrame(), {"population_count": 0, "excluded_positions": 0, "matching_count": 0}),
                 "recent_aircraft": pd.DataFrame(), "daily_activity": pd.DataFrame(), "quality_checks": pd.DataFrame()}
        with dashboard_data(empty, {"models": pd.DataFrame()}):
            app = self.app().run()
            for name in ["Overview", "Live Airspace", "Flight Explorer", "Analytics", "Data Quality", "ML Anomalies"]:
                app.radio(key="page").set_value(name).run()
                self.assert_clean(app)

    def test_ml_missing_tables_and_zero_predictions(self):
        for overrides in [{"available": False}, {"summary": {"scored": 0, "flagged": 0, "imputed_rows": 0, "last_scored": None, "latest_observation": None}}]:
            with self.subTest(overrides=overrides), dashboard_data(ml_overrides=overrides):
                app = self.app()
                app.session_state["page"] = "ML Anomalies"
                app.run()
                self.assert_clean(app)
                self.assertGreater(len(app.info), 0)

    def test_stale_snapshot_has_warning(self):
        info = {"newest": LATEST - timedelta(days=10), "oldest": LATEST - timedelta(days=20), "db_now": NOW, "fetched_at": NOW}
        with dashboard_data({"freshness": info}):
            app = self.app().run()
            self.assert_clean(app)
            self.assertTrue(any("historical" in m.value.lower() for m in app.warning))

    def test_map_filters_forward_values_and_no_status_shows_empty_state(self):
        with dashboard_data() as mocks:
            app = self.app()
            app.session_state["page"] = "Live Airspace"
            app.run()
            app.selectbox(key="map_region").set_value("South Asia").run()
            self.assert_clean(app)
            self.assertEqual(mocks["map_snapshot"].call_args.args[4], (5., 38., 60., 100.))
            app.multiselect(key="map_status").set_value([]).run()
            self.assert_clean(app)
            self.assertTrue(any("Choose a reported state" in m.value for m in app.markdown))

    def test_ml_observation_opens_aircraft_history(self):
        with dashboard_data():
            app = self.app()
            app.session_state["page"] = "ML Anomalies"
            app.run()
            self.assert_clean(app)
            app.button(key="ml_open_history").click().run()
            self.assert_clean(app)
            self.assertEqual(app.radio(key="page").value, "Flight Explorer")
            self.assertEqual(app.text_input(key="explorer_term").value, "abc123")


if __name__ == "__main__":
    unittest.main()
