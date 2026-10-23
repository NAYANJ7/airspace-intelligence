"""Every query the dashboard runs, cached.

Analytics come from the Phase 7 views (v_current_status, v_daily_activity, v_hourly_activity,
v_top_origin_countries, v_flight_metrics_by_status/_by_country, v_most_observed_aircraft,
v_data_quality_checks, v_snapshot_gaps) and the aircraft_history() function. Only the map and the
aircraft search need their own SQL, because they filter by user input. All of it is read-only.

"Latest window" = every aircraft whose latest observation lies within N minutes of the NEWEST
observation in the table (the same definition as v_current_status). It is relative to the data,
not to now(), so the pages still work when ingestion has been stopped.

Cache lifetimes keep database polling gentle: snapshot data 30 s, full-table analytics 10 min.
The sidebar "Refresh data" button clears only the selected page's caches.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from db import DashboardDBError, run_query, run_row

LIVE_TTL = 30
ANALYTICS_TTL = 600
HEX6 = re.compile(r"^[0-9a-fA-F]{6}$")

# Shared by the map, its country list and the explorer's "recent aircraft" list.
_LATEST_WINDOW = """
    WITH b AS (SELECT max(observation_time) AS as_of FROM aircraft_observations),
    latest AS (
        SELECT DISTINCT ON (o.icao24)
               o.icao24, o.callsign,
               COALESCE(o.origin_country, 'Unknown')              AS origin_country,
               o.observation_time, o.latitude, o.longitude,
               COALESCE(o.altitude_km, o.baro_altitude / 1000.0)  AS altitude_km,
               o.velocity_kmh, o.flight_status
        FROM aircraft_observations o, b
        WHERE o.observation_time >= b.as_of - make_interval(mins => %(minutes)s)
        {extra_where}
        ORDER BY o.icao24, o.observation_time DESC
    )
"""


def _window(extra_where: str = "") -> str:
    return _LATEST_WINDOW.format(extra_where=extra_where)


# --------------------------------------------------------------------------- overview
@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def freshness() -> dict:
    row = run_row(
        "SELECT (SELECT max(observation_time) FROM aircraft_observations) AS newest, "
        "(SELECT min(observation_time) FROM aircraft_observations) AS oldest, now() AS db_now"
    )
    row["fetched_at"] = datetime.now(timezone.utc)
    return row


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def storage_summary() -> dict:
    """The full-table count has its own longer cache; it is not part of every freshness check."""
    return run_row("SELECT count(*) AS total_rows FROM aircraft_observations")


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def snapshot_summary(minutes: int = 15) -> dict:
    return run_row(_window() + """
        SELECT count(*) AS aircraft,
               count(*) FILTER (WHERE flight_status = 'AIRBORNE') AS airborne,
               count(*) FILTER (WHERE flight_status = 'ON_GROUND') AS ground,
               count(DISTINCT origin_country) FILTER (WHERE origin_country <> 'Unknown') AS countries
        FROM latest
    """, {"minutes": minutes})


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def pipeline_load() -> dict:
    """A committed-file timestamp, or optional hourly completion metadata as fallback."""
    try:
        row = run_row("SELECT max(last_committed_at) AS last_load FROM aircraft_load_state")
        if row.get("last_load"):
            row["load_source"] = "Last file committed by the loader · may be an hourly, manual or live-worker run"
            return row
    except DashboardDBError as exc:
        if exc.kind != "missing":
            raise
    try:
        return run_row("""
            SELECT max(loader_ended_at) FILTER (WHERE loader_state = 'success') AS last_load,
                   max(updated_at) AS monitor_updated
            FROM pipeline_airflow_runs WHERE dag_id = 'opensky_pipeline'
        """)
    except DashboardDBError as exc:
        if exc.kind == "missing":
            return {}
        raise


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def current_status() -> pd.DataFrame:
    return run_query("SELECT flight_status, aircraft, pct, as_of FROM v_current_status")


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def window_countries(minutes: int = 15) -> int:
    row = run_row(
        _window() + "SELECT count(DISTINCT origin_country) FILTER (WHERE origin_country <> 'Unknown') AS countries "
        "FROM latest",
        {"minutes": minutes},
    )
    return int(row.get("countries") or 0)


# --------------------------------------------------------------------------- map
@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def map_countries(minutes: int) -> pd.DataFrame:
    return run_query(
        _window() + "SELECT origin_country, count(*) AS aircraft FROM latest GROUP BY 1 ORDER BY 2 DESC, 1",
        {"minutes": minutes},
    )


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def map_aircraft(minutes: int, countries: tuple, statuses: tuple, limit: int) -> pd.DataFrame:
    """Latest position per aircraft in the window. Rows without a valid position are excluded in SQL
    (BETWEEN is false for NULL). Country/status filters apply to the LATEST observation of each aircraft."""
    return run_query(
        _window()
        + """
        SELECT *, count(*) OVER () AS matching
        FROM latest
        WHERE (%(countries)s::text[] IS NULL OR origin_country = ANY(%(countries)s))
          AND flight_status = ANY(%(statuses)s)
          AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180
        ORDER BY observation_time DESC
        LIMIT %(limit)s
        """,
        {"minutes": minutes, "countries": list(countries) or None, "statuses": list(statuses), "limit": limit},
    )


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def map_snapshot(minutes: int, countries: tuple, statuses: tuple, limit: int,
                 bounds: tuple = (-90.0, 90.0, -180.0, 180.0),
                 altitude: tuple | None = None, speed: tuple | None = None) -> tuple[pd.DataFrame, dict]:
    """One query returns capped markers and accurate counts, including when no markers match.

    Missing-position counts use country/status/window scope. Geography and optional
    kinematic ranges apply to markers only (missing coordinates cannot be placed in a region).
    """
    sql = _window() + """
        , population AS (
            SELECT * FROM latest
            WHERE (%(countries)s::text[] IS NULL OR origin_country = ANY(%(countries)s))
              AND flight_status = ANY(%(statuses)s)
        ), located AS (
            SELECT * FROM population
            WHERE latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180
        ), matching AS (
            SELECT * FROM located
            WHERE latitude BETWEEN %(south)s AND %(north)s
              AND longitude BETWEEN %(west)s AND %(east)s
              AND (%(alt_min)s::double precision IS NULL OR altitude_km BETWEEN %(alt_min)s AND %(alt_max)s)
              AND (%(speed_min)s::double precision IS NULL OR velocity_kmh BETWEEN %(speed_min)s AND %(speed_max)s)
        ), counts AS (
            SELECT (SELECT count(*) FROM population) AS population_count,
                   (SELECT count(*) FROM population) - (SELECT count(*) FROM located) AS excluded_positions,
                   (SELECT count(*) FROM matching) AS matching_count
        )
        SELECT p.*, c.population_count, c.excluded_positions, c.matching_count
        FROM counts c LEFT JOIN LATERAL (
            SELECT * FROM matching ORDER BY observation_time DESC, icao24 LIMIT %(limit)s
        ) p ON true
    """
    south, north, west, east = bounds
    result = run_query(sql, {
        "minutes": minutes, "countries": list(countries) or None, "statuses": list(statuses),
        "limit": min(max(int(limit), 1), 10000), "south": south, "north": north, "west": west, "east": east,
        "alt_min": altitude[0] if altitude else None, "alt_max": altitude[1] if altitude else None,
        "speed_min": speed[0] if speed else None, "speed_max": speed[1] if speed else None,
    })
    fields = ("population_count", "excluded_positions", "matching_count")
    counts = {name: int(result.iloc[0][name] or 0) if not result.empty else 0 for name in fields}
    points = result.loc[result["icao24"].notna()].drop(columns=list(fields)) if not result.empty else result
    return points.reset_index(drop=True), counts


# --------------------------------------------------------------------------- explorer
_AIRCRAFT_COLUMNS = """
    o.icao24,
    (array_agg(o.callsign ORDER BY o.observation_time DESC)
        FILTER (WHERE o.callsign IS NOT NULL AND btrim(o.callsign) <> ''))[1] AS callsign,
    max(o.origin_country)  AS origin_country,
    count(*)               AS observations,
    max(o.observation_time) AS last_seen
"""


def _like_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def search_aircraft(term: str) -> pd.DataFrame:
    """Exact ICAO24 (6 hex characters, served by the primary key) or callsign prefix. At most 50 rows."""
    term = (term or "").strip()
    if len(term) < 2:
        return pd.DataFrame(columns=["icao24", "callsign", "origin_country", "observations", "last_seen"])
    if HEX6.match(term):
        df = run_query(
            f"SELECT {_AIRCRAFT_COLUMNS} FROM aircraft_observations o WHERE o.icao24 = %(icao24)s GROUP BY o.icao24",
            {"icao24": term.lower()},
        )
        if not df.empty:
            return df
    return run_query(
        r"""
        WITH hits AS (
            SELECT DISTINCT icao24 FROM aircraft_observations
            WHERE callsign ILIKE %(pattern)s ESCAPE '\'
            LIMIT 500
        )
        SELECT """ + _AIRCRAFT_COLUMNS + r"""
        FROM aircraft_observations o JOIN hits h ON h.icao24 = o.icao24
        GROUP BY o.icao24
        ORDER BY last_seen DESC
        LIMIT 50
        """,
        {"pattern": _like_escape(term) + "%"},
    )


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def recent_aircraft(minutes: int = 15, limit: int = 25) -> pd.DataFrame:
    return run_query(
        _window() + "SELECT icao24, callsign, origin_country, observation_time AS last_seen FROM latest "
        "ORDER BY observation_time DESC LIMIT %(limit)s",
        {"minutes": minutes, "limit": limit},
    )


@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def aircraft_track(icao24: str, limit: int, since=None, until=None) -> pd.DataFrame:
    """Newest `limit` observations from the existing aircraft_history() function, returned oldest-first."""
    df = run_query(
        "SELECT h.*, count(*) OVER () AS total_rows FROM aircraft_history(%(icao24)s) h "
        "WHERE (%(since)s::timestamptz IS NULL OR h.observation_time >= %(since)s) "
        "AND (%(until)s::timestamptz IS NULL OR h.observation_time <= %(until)s) "
        "ORDER BY h.observation_time DESC LIMIT %(limit)s",
        {"icao24": icao24, "limit": min(max(int(limit), 1), 5000), "since": since, "until": until},
    )
    return df.sort_values("observation_time").reset_index(drop=True)


# --------------------------------------------------------------------------- analytics
@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def daily_activity() -> pd.DataFrame:
    return run_query(
        "SELECT observation_date, observations, aircraft, snapshots, airborne_obs, ground_obs "
        "FROM v_daily_activity ORDER BY observation_date"
    )


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def hourly_activity(since_date=None, until_date=None) -> pd.DataFrame:
    return run_query(
        "SELECT observation_hour, sum(observations)::bigint AS observations, "
        "count(DISTINCT observation_date) AS days FROM v_hourly_activity "
        "WHERE (%(since)s::date IS NULL OR observation_date >= %(since)s) "
        "AND (%(until)s::date IS NULL OR observation_date <= %(until)s) "
        "GROUP BY observation_hour ORDER BY 1",
        {"since": since_date, "until": until_date},
    )


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def top_countries(limit: int = 15) -> pd.DataFrame:
    return run_query(
        "SELECT origin_country, observations, aircraft, pct_of_observations "
        "FROM v_top_origin_countries LIMIT %(limit)s",
        {"limit": limit},
    )


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def metrics_by_status() -> pd.DataFrame:
    return run_query("SELECT * FROM v_flight_metrics_by_status")


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def metrics_by_country(limit: int = 15) -> pd.DataFrame:
    return run_query("SELECT * FROM v_flight_metrics_by_country LIMIT %(limit)s", {"limit": limit})


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def most_observed(limit: int = 15) -> pd.DataFrame:
    return run_query(
        "SELECT icao24, callsign, origin_country, observations, first_seen, last_seen, span_minutes, "
        "pct_of_snapshots FROM v_most_observed_aircraft LIMIT %(limit)s",
        {"limit": limit},
    )


# --------------------------------------------------------------------------- data quality
@st.cache_data(ttl=LIVE_TTL, show_spinner=False)
def quality_snapshot() -> pd.DataFrame:
    """Read the most recent completed audit; do not rescan the observation archive."""
    try:
        return run_query("""
            SELECT check_time, check_name, severity, issue_count, rows_checked,
                   round(100.0 * issue_count / NULLIF(rows_checked, 0), 3) AS issue_pct,
                   status, detail
            FROM data_quality_history
            WHERE check_time = (SELECT max(check_time) FROM data_quality_history)
            ORDER BY check_name
        """)
    except DashboardDBError as exc:
        if exc.kind == "missing":
            return pd.DataFrame()
        raise


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def quality_checks() -> pd.DataFrame:
    return run_query(
        "SELECT check_name, severity, issue_count, rows_checked, issue_pct, status, detail "
        "FROM v_data_quality_checks"
    )


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def snapshot_gaps(limit: int = 50) -> pd.DataFrame:
    return run_query(
        "SELECT gap_start, gap_end, gap_minutes FROM v_snapshot_gaps ORDER BY gap_start DESC LIMIT %(limit)s",
        {"limit": limit},
    )


@st.cache_data(ttl=ANALYTICS_TTL, show_spinner=False)
def quality_history(limit: int = 24) -> pd.DataFrame:
    """Per-run summary from data_quality_history (written by the Airflow DAG). Empty if the table is absent."""
    try:
        return run_query(
            "SELECT check_time, count(*) FILTER (WHERE status = 'error') AS errors, "
            "count(*) FILTER (WHERE status = 'warn') AS warnings, "
            "count(*) FILTER (WHERE status = 'info') AS info, "
            "count(*) FILTER (WHERE status = 'ok') AS passing "
            "FROM data_quality_history GROUP BY check_time ORDER BY check_time DESC LIMIT %(limit)s",
            {"limit": limit},
        )
    except DashboardDBError as exc:
        if exc.kind == "missing":
            return pd.DataFrame(columns=["check_time", "errors", "warnings", "info", "passing"])
        raise


def clear_page_cache(page: str) -> None:
    """Avoid invalidating expensive unrelated pages when a user refreshes a map."""
    groups = {
        "Overview": (freshness, snapshot_summary, storage_summary, pipeline_load, daily_activity),
        "Live Airspace": (freshness, map_countries, map_aircraft, map_snapshot),
        "Flight Explorer": (freshness, search_aircraft, recent_aircraft, aircraft_track),
        "Analytics": (freshness, daily_activity, hourly_activity, top_countries,
                      metrics_by_status, metrics_by_country, most_observed),
        "Data Quality": (freshness, quality_snapshot, quality_checks, snapshot_gaps, quality_history),
        "ML Anomalies": (freshness,),
    }
    for function in groups.get(page, ()):
        function.clear()
