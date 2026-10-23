"""Service-free integration contracts. Real Airflow/Streamlit are not simulated as live tests."""
from __future__ import annotations

import ast
import importlib.util
import re
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

try:
    import pandas as pd
except ImportError:
    pd = None

from src.ml.features import SOURCE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]


class SchemaAndDAGContracts(unittest.TestCase):
    def test_ml_reads_only_existing_observation_columns(self):
        ddl = (ROOT / "src/postgres_schema.sql").read_text()
        columns = set(re.findall(r"^\s+(\w+)\s+(?:TEXT|TIMESTAMPTZ|DOUBLE PRECISION|BOOLEAN|SMALLINT|DATE)\b", ddl, re.M))
        self.assertEqual(len(columns), 23)
        self.assertTrue(set(SOURCE_COLUMNS) <= columns)
        self.assertIn("PRIMARY KEY (icao24, observation_time)", ddl)

    def test_new_ddl_does_not_mutate_upstream_tables(self):
        ddl = (ROOT / "src/sql/ml_schema.sql").read_text()
        self.assertNotRegex(ddl, r"(?i)\b(TRUNCATE|DELETE|DROP|UPDATE|ALTER)\b")
        self.assertIn("PRIMARY KEY (icao24, observation_time, model_version)", ddl)
        self.assertIn("REFERENCES aircraft_observations(icao24, observation_time)", ddl)

    def test_airflow_configuration_contract_via_ast(self):
        # Syntax/API contract inspection only, NOT an Airflow DagBag parse.
        tree = ast.parse((ROOT / "src/dags/opensky_ml.py").read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        dags = [c for c in calls if isinstance(c.func, ast.Name) and c.func.id == "DAG"]
        values = [{kw.arg: ast.literal_eval(kw.value) for kw in call.keywords
                   if kw.arg in {"dag_id", "schedule", "catchup", "max_active_runs", "is_paused_upon_creation"}}
                  for call in dags]
        by_id = {v["dag_id"]: v for v in values}
        self.assertEqual(by_id["opensky_ml_scoring"]["schedule"], "@hourly")
        self.assertIsNone(by_id["opensky_ml_training"]["schedule"])
        self.assertTrue(all(v["is_paused_upon_creation"] and not v["catchup"] for v in values))
        sensor = next(c for c in calls if isinstance(c.func, ast.Name) and c.func.id == "ExternalTaskSensor")
        sensor_values = {k.arg: ast.literal_eval(k.value) for k in sensor.keywords if isinstance(k.value, ast.Constant)}
        self.assertEqual(sensor_values["mode"], "reschedule")
        self.assertEqual(sensor_values["external_dag_id"], "opensky_pipeline")
        self.assertEqual(sensor_values["external_task_id"], "postgres_loader")
        # Parse-time imports stay cheap and cannot load sklearn or connect to a database.
        self.assertFalse(any(isinstance(n, ast.ImportFrom) and n.module and
                             (n.module.startswith("src.ml") or n.module.startswith("sklearn"))
                             for n in tree.body))

    @unittest.skipUnless(importlib.util.find_spec("airflow"), "Airflow not installed; real DagBag parse is not tested")
    def test_real_airflow_dagbag_when_available(self):
        from airflow.models import DagBag
        bag = DagBag(dag_folder=str(ROOT / "src/dags/opensky_ml.py"), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        self.assertIn("opensky_ml_scoring", bag.dags)
        self.assertIn("opensky_ml_training", bag.dags)


@unittest.skipIf(pd is None, "Optional dashboard contract checks require the project's existing pandas dependency")
class DashboardContractTests(unittest.TestCase):
    def setUp(self):
        # Use narrow interface doubles to exercise branch/error logic without
        # installing Streamlit. This is not a browser or AppTest test.
        self.st = MagicMock()
        self.st.cache_data.side_effect = lambda **kw: lambda f: f
        self.st.selectbox.side_effect = lambda label, options, index=0, **kw: options[index]
        self.st.checkbox.return_value = True
        self.st.columns.return_value = [MagicMock() for _ in range(4)]
        db = types.ModuleType("db")
        class Error(Exception):
            def __init__(self, message, kind="query"):
                super().__init__(message)
                self.kind = kind
        self.error = Error
        db.DashboardDBError = Error
        db.run_query = MagicMock()
        db.run_row = MagicMock()
        self.q = types.ModuleType("queries")
        now = datetime.now(timezone.utc)
        self.q.freshness = lambda: {"newest": now, "db_now": now}
        renderer = types.ModuleType("render")
        renderer.fmt_ts = str
        renderer.show_error = MagicMock()
        self.renderer = renderer
        # Phase 12 moved presentation helpers out of render. Keep these narrow
        # contracts service-free; test_dashboard.py exercises the real UI with AppTest.
        ui = types.ModuleType("ui")
        ui.page_header = MagicMock()
        ui.freshness_banner = MagicMock()
        ui.fmt_num = lambda value, *args: str(value)
        ui.fmt_utc_column = lambda values: values.astype(str)
        ui.navigate = MagicMock()
        helpers = types.ModuleType("data_helpers")
        helpers.csv_bytes = lambda frame: b"test export"
        self.patcher = patch.dict(sys.modules, {"streamlit": self.st, "db": db, "queries": self.q, "render": renderer,
                                               "ui": ui, "data_helpers": helpers})
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        spec = importlib.util.spec_from_file_location("test_ml_page", ROOT / "streamlit_app/ml_anomalies.py")
        self.page_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.page_module)
        self.db = db

    def test_missing_ml_tables_do_not_raise(self):
        self.db.run_row.return_value = {"ready": False}
        self.page_module.page()
        self.st.info.assert_called_once()
        self.db.run_query.assert_not_called()

    def test_empty_model_list_is_handled(self):
        self.db.run_row.return_value = {"ready": True}
        self.db.run_query.return_value = pd.DataFrame()
        self.page_module.page()
        self.st.info.assert_called_once()

    def test_scored_data_renders_through_helpers(self):
        now = datetime.now(timezone.utc)
        self.db.run_row.side_effect = [
            {"ready": True},
            {"scored": 100, "flagged": 2, "imputed_rows": 3, "latest_observation": now, "last_scored": now},
        ]
        self.db.run_query.side_effect = [
            pd.DataFrame([{"model_version": "test-v1", "trained_at": now, "metadata": {"training_rows": 1000}}]),
            pd.DataFrame([{"hour_utc": now, "scored": 100, "flagged": 2}]),
            pd.DataFrame([{"icao24": "abc123", "anomaly_score": 0.7}]),
        ]
        self.page_module.page()
        self.st.line_chart.assert_called_once()
        self.st.dataframe.assert_called_once()
        self.st.json.assert_called_once()

    def test_zero_predictions_is_handled(self):
        now = datetime.now(timezone.utc)
        self.db.run_row.side_effect = [{"ready": True}, {"scored": 0, "flagged": 0, "imputed_rows": 0,
                                                        "last_scored": None, "latest_observation": None}]
        self.db.run_query.return_value = pd.DataFrame([{"model_version": "v1", "trained_at": now, "metadata": {}}])
        self.page_module.page()
        self.st.info.assert_called_once()
        self.st.line_chart.assert_not_called()

    def test_database_error_stays_on_ml_page(self):
        self.db.run_row.side_effect = self.error("Database unavailable", "unavailable")
        self.page_module.page()
        self.renderer.show_error.assert_called_once()

    def test_queries_are_read_only_and_bound(self):
        self.db.run_row.return_value = {"scored": 0}
        now = datetime.now(timezone.utc)
        attack = "'; DROP TABLE aircraft_observations; --"
        self.page_module.summary(attack, now - timedelta(hours=1), now)
        sql, params = self.db.run_row.call_args.args
        self.assertNotIn(attack, sql)
        self.assertEqual(params["version"], attack)
        self.assertTrue(sql.lstrip().startswith("SELECT"))


if __name__ == "__main__":
    unittest.main()
