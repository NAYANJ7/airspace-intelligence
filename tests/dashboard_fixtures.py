"""Synthetic TEST fixtures only. The production app never imports this module."""
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

NOW = datetime(2026, 10, 23, 9, 30, tzinfo=timezone.utc)
LATEST = NOW - timedelta(minutes=10)
VERSION = "test-only-iforest-v1"


def aircraft():
    return pd.DataFrame([
        {"icao24": "abc123", "callsign": "TEST101", "origin_country": "India", "observation_time": LATEST,
         "latitude": 28.6, "longitude": 77.2, "altitude_km": 10.5, "velocity_kmh": 820.0, "flight_status": "AIRBORNE"},
        {"icao24": "def456", "callsign": None, "origin_country": "Germany", "observation_time": LATEST - timedelta(minutes=2),
         "latitude": 50.1, "longitude": 8.6, "altitude_km": 0.1, "velocity_kmh": 10.0, "flight_status": "ON_GROUND"},
    ])


def recent():
    return aircraft()[["icao24", "callsign", "origin_country", "observation_time"]].rename(columns={"observation_time": "last_seen"})


def daily():
    return pd.DataFrame([{"observation_date": (LATEST - timedelta(days=i)).date(), "observations": 1000 + i * 100,
                          "aircraft": 90 + i, "snapshots": 10, "airborne_obs": 900 + i * 100, "ground_obs": 100}
                         for i in reversed(range(10))])


def history():
    return pd.DataFrame([{"observation_time": LATEST - timedelta(minutes=10 - i), "callsign": "TEST101", "latitude": 28.0 + i / 10,
                          "longitude": 77.0 + i / 10, "altitude_km": 8.0 + i / 4, "velocity_kmh": 780.0 + i * 4,
                          "vertical_rate": 1.0, "true_track": 45.0, "flight_status": "AIRBORNE", "total_rows": 11}
                         for i in range(11)])


def quality():
    return pd.DataFrame([
        {"check_name": "duplicate_observation_key", "severity": "error", "issue_count": 0, "rows_checked": 1000,
         "issue_pct": 0.0, "status": "ok", "detail": "Primary key prevents duplicates"},
        {"check_name": "implausible_velocity", "severity": "warn", "issue_count": 2, "rows_checked": 1000,
         "issue_pct": .2, "status": "warn", "detail": "Speed above 400 m/s"},
        {"check_name": "missing_position", "severity": "info", "issue_count": 10, "rows_checked": 1000,
         "issue_pct": 1., "status": "info", "detail": "No reported position"},
    ])


def predictions():
    return pd.DataFrame([{
        "observation_time": LATEST, "icao24": "abc123", "callsign": "TEST101", "origin_country": "India",
        "is_anomaly": True, "anomaly_score": .7, "decision_function": -.075, "imputed_feature_count": 1,
        "geo_altitude_m": 10500., "baro_altitude_m": 10200., "velocity_ms": 220., "vertical_rate_ms": 1.,
        "true_track": 45., "latitude": 28.6, "longitude": 77.2, "scored_at": NOW - timedelta(minutes=2),
    }])


def fixture_values():
    metrics = pd.DataFrame([
        {"flight_status": status, "observations": count, "avg_velocity_kmh": speed, "max_velocity_kmh": speed + 100,
         "avg_altitude_km": altitude, "max_altitude_km": altitude + 1, "velocity_excluded": 2, "altitude_excluded": 0}
        for status, count, speed, altitude in [("AIRBORNE", 900, 810, 10), ("ON_GROUND", 100, 12, .1)]])
    return {
        "freshness": {"newest": LATEST, "oldest": LATEST - timedelta(days=9), "db_now": NOW, "fetched_at": NOW},
        "snapshot_summary": {"aircraft": 100, "airborne": 90, "ground": 10, "countries": 12},
        "storage_summary": {"total_rows": 14500},
        "pipeline_load": {"last_load": NOW - timedelta(minutes=5), "monitor_updated": NOW - timedelta(minutes=1)},
        "daily_activity": daily(),
        "hourly_activity": pd.DataFrame({"observation_hour": range(24), "observations": [100 + i * 10 for i in range(24)], "days": [10] * 24}),
        "map_countries": pd.DataFrame({"origin_country": ["India", "Germany"], "aircraft": [1, 1]}),
        "map_snapshot": (aircraft(), {"population_count": 3, "excluded_positions": 1, "matching_count": 2}),
        "recent_aircraft": recent(), "search_aircraft": recent(), "aircraft_track": history(),
        "metrics_by_status": metrics,
        "metrics_by_country": pd.DataFrame({"origin_country": ["India"], "airborne_observations": [900], "aircraft": [80],
                                           "avg_velocity_kmh": [810.], "max_velocity_kmh": [900.], "avg_altitude_km": [10.], "max_altitude_km": [11.]}),
        "top_countries": pd.DataFrame({"origin_country": ["India", "Germany"], "observations": [900, 100], "aircraft": [90, 10],
                                        "pct_of_observations": [90., 10.]}),
        "most_observed": pd.DataFrame({"icao24": ["abc123"], "callsign": ["TEST101"], "origin_country": ["India"], "observations": [500],
                                       "first_seen": [LATEST - timedelta(days=9)], "last_seen": [LATEST], "span_minutes": [12960.], "pct_of_snapshots": [50.]}),
        "quality_checks": quality(),
        "quality_snapshot": quality().assign(check_time=NOW - timedelta(minutes=5)),
        "snapshot_gaps": pd.DataFrame({"gap_start": [LATEST - timedelta(days=1)], "gap_end": [LATEST], "gap_minutes": [1440.]}),
        "quality_history": pd.DataFrame({"check_time": [NOW - timedelta(minutes=5)], "errors": [0], "warnings": [1], "info": [1], "passing": [1]}),
    }


@contextmanager
def dashboard_data(overrides=None, ml_overrides=None):
    import queries
    import ml_anomalies
    values = fixture_values()
    values.update(overrides or {})
    ml_values = {
        "available": True,
        "models": pd.DataFrame({"model_version": [VERSION], "trained_at": [LATEST - timedelta(days=2)],
                                "metadata": [{"training_rows": 1000, "feature_schema": "airborne-kinematics-v1"}]}),
        "summary": {"scored": 1000, "flagged": 5, "imputed_rows": 10, "latest_observation": LATEST, "last_scored": NOW - timedelta(minutes=2)},
        "hourly": pd.DataFrame({"hour_utc": [LATEST - timedelta(hours=1), LATEST], "scored": [400, 600], "flagged": [2, 3]}),
        "observations": predictions(),
    }
    ml_values.update(ml_overrides or {})
    with ExitStack() as stack:
        mocks = {}
        for module, mapping in [(queries, values), (ml_anomalies, ml_values)]:
            for name, value in mapping.items():
                options = {"side_effect": value} if isinstance(value, Exception) else {"return_value": value}
                mocks[name] = stack.enter_context(patch.object(module, name, **options))
        yield mocks
