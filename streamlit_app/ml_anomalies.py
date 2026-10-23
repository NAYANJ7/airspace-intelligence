"""Optional, read-only ML page; no model/scikit-learn dependency in the dashboard."""
from __future__ import annotations

from datetime import timedelta

import streamlit as st
import pandas as pd

import queries as q
from db import DashboardDBError, run_query, run_row
from render import fmt_ts, show_error
from ui import page_header, freshness_banner, fmt_num, fmt_utc_column, navigate
from data_helpers import csv_bytes


@st.cache_data(ttl=60, show_spinner=False)
def available() -> bool:
    row = run_row("SELECT to_regclass('aircraft_ml_models') IS NOT NULL "
                  "AND to_regclass('aircraft_anomaly_predictions') IS NOT NULL AS ready")
    return bool(row.get("ready"))


@st.cache_data(ttl=60, show_spinner=False)
def models():
    return run_query("SELECT model_version, trained_at, metadata FROM aircraft_ml_models ORDER BY trained_at DESC")


@st.cache_data(ttl=60, show_spinner=False)
def summary(version: str, since, until) -> dict:
    return run_row("""
        SELECT count(*) AS scored,
               count(*) FILTER (WHERE is_anomaly) AS flagged,
               count(*) FILTER (WHERE imputed_feature_count > 0) AS imputed_rows,
               max(observation_time) AS latest_observation, max(scored_at) AS last_scored
        FROM aircraft_anomaly_predictions
        WHERE model_version = %(version)s AND observation_time >= %(since)s AND observation_time <= %(until)s
    """, {"version": version, "since": since, "until": until})


@st.cache_data(ttl=60, show_spinner=False)
def hourly(version: str, since, until):
    return run_query("""
        SELECT date_trunc('hour', observation_time AT TIME ZONE 'UTC') AS hour_utc,
               count(*) AS scored, count(*) FILTER (WHERE is_anomaly) AS flagged
        FROM aircraft_anomaly_predictions
        WHERE model_version = %(version)s AND observation_time >= %(since)s AND observation_time <= %(until)s
        GROUP BY 1 ORDER BY 1
    """, {"version": version, "since": since, "until": until})


@st.cache_data(ttl=60, show_spinner=False)
def observations(version: str, since, until, only_flags: bool, limit: int):
    return run_query("""
        SELECT p.observation_time, p.icao24, o.callsign, o.origin_country,
               p.is_anomaly, p.anomaly_score, p.decision_function, p.imputed_feature_count,
               o.geo_altitude AS geo_altitude_m, o.baro_altitude AS baro_altitude_m,
               o.velocity AS velocity_ms, o.vertical_rate AS vertical_rate_ms,
               o.true_track, o.latitude, o.longitude, p.scored_at
        FROM aircraft_anomaly_predictions p
        JOIN aircraft_observations o USING (icao24, observation_time)
        WHERE p.model_version = %(version)s AND p.observation_time >= %(since)s
          AND p.observation_time <= %(until)s AND (NOT %(only_flags)s OR p.is_anomaly)
        ORDER BY p.observation_time DESC, p.anomaly_score DESC, p.icao24 LIMIT %(limit)s
    """, {"version": version, "since": since, "until": until, "only_flags": only_flags, "limit": limit})


def page() -> None:
    page_header("Investigate / ML Anomalies", "Unusual patterns. Measured interpretation.",
                "Inspect the existing Isolation Forest predictions alongside the observations that produced them.")
    st.caption("Isolation Forest flags unusual combinations of airborne measurements. "
               "These are statistical anomalies, not confirmed flight-safety incidents. "
               "Scores are not probabilities. Data-quality checks remain separate.")
    try:
        if not available():
            st.info("ML results are not available yet. The other dashboard pages remain usable. "
                    "The project operator can initialize, train and score through the existing ML command-line workflow.")
            return
        choices = models()
        if choices.empty:
            st.info("No trained model is registered yet. Predictions will appear after the project operator trains and scores a model.")
            return
        version = st.selectbox("Model version (newest trained first)", choices["model_version"].tolist(), key="ml_version")
        st.caption("This selector shows stored model versions; it does not change the active scoring model.")
        hours = st.selectbox("Observation window (hours)", [24, 72, 168, 720], index=1, key="ml_hours")
        only_flags = st.checkbox("Show only statistical anomalies", value=True, key="ml_only_flags")
        limit = st.selectbox("Maximum displayed rows", [100, 500, 1000], index=1, key="ml_limit")
        info = q.freshness()
        freshness_banner(info, compact=True)
        until = info["newest"]
        if until is None:
            st.info("There are no aircraft observations yet.")
            return
        since = until - timedelta(hours=hours)
        st.caption(f"Window ends at the newest stored observation: {fmt_ts(until)}. "
                   "The counts below describe scored observations, not distinct aircraft or all traffic.")
        totals = summary(version, since, until)
        scored, flagged = int(totals["scored"]), int(totals["flagged"])
        a, b, c, d = st.columns(4)
        a.metric("Scored observations", f"{scored:,}", border=True,
                 help="Prediction records for this model and time window, not distinct aircraft or all observations.")
        b.metric("Statistical anomalies", f"{flagged:,}", border=True,
                 help="Scored observations whose decision_function is below zero. A statistical flag is not a safety incident.")
        c.metric("Flagged share", f"{100 * flagged / scored:.2f}%" if scored else "Not available", border=True,
                 help="Flagged observations divided by scored observations in this window. Not accuracy or a probability.")
        d.metric("Rows using imputation", f"{int(totals['imputed_rows']):,}", border=True,
                 help="Scored rows with at least one missing optional feature filled by the stored training preprocessing.")
        st.caption("All four cards use the selected model and observation window. The anomalies-only checkbox and displayed-row limit affect the table, not these totals.")
        selected = choices.loc[choices["model_version"] == version].iloc[0]
        st.caption(f"Model trained: {fmt_ts(selected['trained_at'])}. "
                   f"Latest prediction write in this window: {fmt_ts(totals['last_scored'])}.")
        if not scored:
            st.info("No predictions for this model in this window. Widen the window or wait for the next scheduled scoring run.")
            return
        if until - totals["latest_observation"] > timedelta(hours=2):
            st.warning("Predictions lag source observations by over two hours. Check the ML scoring task and batch limits.")
        trend = hourly(version, since, until)
        if not trend.empty:
            st.subheader("When statistical anomalies were observed")
            st.caption("Flagged observation counts by source observation hour. Gaps are left as gaps; more flags can reflect more scored traffic or different coverage.")
            st.line_chart(trend.set_index("hour_utc").asfreq("h")[["flagged"]], x_label="Observation hour (UTC)",
                          y_label="Flagged observations", color="#f3bd72")
        table = observations(version, since, until, only_flags, limit)
        if table.empty:
            st.info("No statistical anomalies were flagged in this window.")
        else:
            st.caption("Newest observations first; higher anomaly_score means more unusual. "
                       "A negative decision_function sets the anomaly flag. Times are UTC.")
            displayed = display_table(table)
            st.dataframe(displayed, hide_index=True, width="stretch")
            st.download_button("Download displayed predictions", csv_bytes(displayed), "aircraft_anomaly_predictions.csv", "text/csv")
            inspect_observation(table)
        with st.expander("How to interpret this model", expanded=True):
            st.markdown(
                "- **Method:** Isolation Forest learns which combinations of airborne measurements are less common in its training sample.\n"
                "- **Inputs:** altitude (geometric, with barometric fallback), ground speed, vertical rate, geometric-minus-barometric altitude, "
                "and sine/cosine of true track. Aircraft identifiers and countries are not model inputs.\n"
                "- **Missing values:** stored training medians fill optional features, with missing-value indicators; entirely missing training features use zero.\n"
                "- **Score:** higher `anomaly_score` means more unusual. A negative `decision_function` sets the anomaly flag. "
                "Neither is a probability or an accuracy estimate.\n"
                "- **Scope:** eligible airborne observations only. Sensor noise, incomplete measurements and different operating patterns can all affect scores. "
                "The model does not provide a verified cause or flight-safety conclusion."
            )
        with st.expander("Model and training details"):
            st.json(selected["metadata"])
            st.caption("Training flag rate is a threshold diagnostic, not an accuracy estimate. "
                       "The model excludes on-ground observations and does not use aircraft identity or country as features.")
    except DashboardDBError as exc:
        if exc.kind == "missing":
            st.info("The ML tables are not ready. Other dashboard pages remain available; ask the project operator to initialize ML storage.")
        else:
            show_error(exc)


def display_table(table):
    """Convert display units only. Persisted predictions and raw observations stay unchanged."""
    result = table.copy()
    for column in ("observation_time", "scored_at"):
        if column in result:
            result[column] = fmt_utc_column(result[column])
    for column, factor in (("velocity_ms", 3.6), ("geo_altitude_m", 0.001), ("baro_altitude_m", 0.001)):
        if column in result:
            result[column] = pd.to_numeric(result[column], errors="coerce") * factor
    return result.rename(columns={
        "observation_time": "Observed (UTC)", "icao24": "ICAO24", "callsign": "Callsign", "origin_country": "Registration country",
        "is_anomaly": "Statistical flag", "anomaly_score": "Anomaly score", "decision_function": "Decision function",
        "imputed_feature_count": "Features imputed", "geo_altitude_m": "Geometric altitude (km)",
        "baro_altitude_m": "Barometric altitude (km)", "velocity_ms": "Ground speed (km/h)",
        "vertical_rate_ms": "Vertical rate (m/s)", "true_track": "True track (°)", "latitude": "Latitude (°)",
        "longitude": "Longitude (°)", "scored_at": "Scored (UTC)",
    })


def inspect_observation(table) -> None:
    required = {"icao24", "observation_time", "scored_at", "anomaly_score", "decision_function", "imputed_feature_count"}
    if not required <= set(table.columns):
        return
    with st.expander("Inspect an observation and open its aircraft history"):
        options = list(range(len(table)))
        index = st.selectbox("Observation", options, key="ml_inspect",
                             format_func=lambda i: f"{table.iloc[i]['icao24']} · {fmt_ts(table.iloc[i]['observation_time'])}")
        row = table.iloc[index]
        st.write(f"**Aircraft {row['icao24']}** · observed {fmt_ts(row['observation_time'])} · scored {fmt_ts(row['scored_at'])}")
        st.write(f"Anomaly score: **{fmt_num(row['anomaly_score'], 4)}** · "
                 f"Decision function: **{fmt_num(row['decision_function'], 4)}** · "
                 f"Imputed features: **{int(row['imputed_feature_count'])} of 6**")
        st.caption("Compare the measurements in the table with this aircraft's history. A flag does not identify its cause.")
        if st.button("Open aircraft history", key="ml_open_history"):
            navigate("Flight Explorer", str(row["icao24"]))
