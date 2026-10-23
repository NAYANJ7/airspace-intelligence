"""Onboarding grounded in this project's source, available even with PostgreSQL offline."""
from html import escape

import pandas as pd
import streamlit as st

from ui import FRESH_MIN, STALE_MIN, page_header, section_header

STAGES = [
    ("01 / Source", "OpenSky API", "Aircraft state snapshots: identifiers, positions, movement and observation time."),
    ("02 / Prepare", "Python ingestion", "OAuth2 access, validation, normalization and change detection; also saves local Parquet."),
    ("03 / Buffer", "Kafka", "The aircraft_states topic carries changed observations between ingestion and processing."),
    ("04 / Transform", "Spark streaming", "Reads Kafka, derives units and status, and commits progress to the existing checkpoint."),
    ("05 / Persist", "Parquet output", "Spark writes processed files. An idempotent loader inserts new observation keys."),
    ("06 / Serve", "PostgreSQL", "Stores observations, SQL analytics, quality results and separate ML predictions."),
]


def pipeline_diagram() -> None:
    items = "".join(f'<li><span class="step">{escape(step)}</span><strong>{escape(title)}</strong>'
                    f'<p>{escape(detail)}</p></li>' for step, title, detail in STAGES)
    st.markdown(f'<ol class="flow" aria-label="Data pipeline in processing order">{items}</ol>', unsafe_allow_html=True)
    st.markdown('<div class="pipeline-support"><strong>Orchestration · Airflow</strong> checks the services and runs '
                'the hourly PostgreSQL load and quality checks. The separate ML scoring DAG waits for that load.<br>'
                '<strong>Monitoring · Grafana</strong> reads health samples, Kafka offsets, Spark checkpoint progress '
                'and copied Airflow run states. These are supporting layers, not extra processing steps.</div>',
                unsafe_allow_html=True)
    st.caption("Read stages 01–06 in order. Streamlit reads PostgreSQL; Isolation Forest reads eligible "
               "observations and writes a separate prediction table. The dashboard does not control the pipeline.")


def quick_start() -> None:
    st.markdown('<div class="guide-grid">'
                '<div class="guide-card"><div class="number">01 / CHECK THE TIME</div><strong>Start with freshness</strong>'
                '<p>The latest observation tells you when the aircraft was recorded. A recent page refresh does not make old observations live.</p></div>'
                '<div class="guide-card"><div class="number">02 / EXPLORE A SNAPSHOT</div><strong>Find an aircraft</strong>'
                '<p>Use Live Airspace for recorded positions, or search Flight Explorer by an ICAO24 identifier or callsign prefix.</p></div>'
                '<div class="guide-card"><div class="number">03 / ADD CONTEXT</div><strong>Compare, then investigate</strong>'
                '<p>Analytics describes stored data. Quality checks flag data issues. ML highlights unusual measurements for closer inspection.</p></div>'
                '</div>', unsafe_allow_html=True)


GLOSSARY = {
    "Observation": "One aircraft state at one OpenSky snapshot time. Many observations can belong to the same aircraft.",
    "ICAO24": "A six-character hexadecimal aircraft/transponder address used to join observations. It is not a flight number.",
    "Callsign": "A reported flight or radio identifier. It can be absent, reused or change over time.",
    "Origin country": "The aircraft registration country associated with its ICAO24 address; not its departure airport or present location.",
    "Snapshot window": "Each aircraft's latest stored observation within N minutes of the newest database observation; not N minutes before the page was opened.",
    "Ground speed": "Reported horizontal speed relative to the ground. Displayed as km/h (source m/s × 3.6), not airspeed.",
    "Altitude": "Geometric height when available; the map falls back to barometric altitude. Charts label km; ML inspection labels raw metres.",
    "Vertical rate": "Reported climb or descent in m/s; positive means climbing and negative means descending.",
    "True track": "Direction of ground travel in degrees clockwise from north; not necessarily aircraft heading.",
    "Observation age": "Database clock minus the newest observation timestamp. Measures source freshness, not browser or query freshness.",
    "Ingestion latency": "Seconds between the OpenSky snapshot timestamp and this pipeline processing it; different from the hourly database-load delay.",
    "Last successful load": "Last committed Parquet file time from loader tracking, or hourly task completion from monitoring. The caption identifies which; neither is inferred from observation time.",
    "Statistical anomaly": "A measurement combination that the trained Isolation Forest treats as unusual. It is not a confirmed incident or wrongdoing.",
    "Anomaly score": "Negative sklearn score_samples in this implementation: larger means more unusual. It is not a probability or accuracy measure.",
    "Decision function": "The model's score relative to its learned threshold. A value below zero sets the stored anomaly flag.",
    "Imputation": "Filling missing optional features using training medians (zero for all-missing columns), with separate missing-value indicators.",
    "Model version": "The immutable model identifier attached to predictions. Viewing another version does not activate or retrain it.",
    "Checkpoint": "Spark's persisted streaming progress, used to resume processing safely after restarts.",
}


def page() -> None:
    page_header("Platform guide", "Understand the journey behind every point.",
                "A practical guide to the data, the pipeline and the boundaries of what this dashboard can tell you.")
    quick_start()
    section_header("From reported state to useful context", "The application monitors recorded aircraft states and pipeline health, not air traffic control decisions.")
    pipeline_diagram()

    with st.expander("Four kinds of information — four different meanings", expanded=True):
        st.dataframe(pd.DataFrame([
            ("Observed data", "Position, speed, altitude and timestamps reported by OpenSky.", "Live Airspace / Flight Explorer"),
            ("Calculated analytics", "SQL counts, distributions, derived units and summaries of stored observations.", "Overview / Analytics"),
            ("Data-quality findings", "Rule-based checks for missing values, invalid ranges, gaps and age.", "Data Quality"),
            ("ML predictions", "Isolation Forest scores and flags for eligible airborne observations.", "ML Anomalies"),
        ], columns=["Information", "Meaning", "Where to look"]), hide_index=True, width="stretch")

    with st.expander("Update frequency and freshness", expanded=True):
        st.markdown(
            "- **Ingestion:** polls OpenSky every 60 seconds by default; requests, retry backoff and API credits can increase the interval. "
            "`POLL_INTERVAL_SECONDS` is configurable. Only changed aircraft states are forwarded.\n"
            "- **Spark:** checks for new Kafka data on a 30-second trigger and writes processed Parquet.\n"
            "- **PostgreSQL:** the original Airflow load runs hourly. If enabled, the independent live loader checks for new files 30 seconds after each cycle.\n"
            "- **ML:** the hourly scoring DAG remains available. An optional independent worker checks for committed data or model changes every 60 seconds after each cycle. Training remains separate.\n"
            "- **Dashboard:** snapshot queries and saved audits are cached for 30 seconds, ML for 60 seconds and archive analytics for 10 minutes. "
            "Auto-refresh defaults to once per minute while the page is open. Refresh data clears only the selected page caches; it does not trigger ingestion. "
            "Archive totals, trends and gap scans are loaded on request; the quality page opens with the latest saved hourly audit."
        )
        st.caption(f"Dashboard labels: recent ≤ {FRESH_MIN} minutes; recorded snapshot ≤ {STALE_MIN} minutes; historical beyond that. "
                   "The existing SQL stale_data check uses a separate 15-minute threshold. A warning between hourly loads "
                   "can be expected, but a growing age still needs investigation.")

    with st.expander("Coverage and interpretation limits"):
        st.markdown(
            "- OpenSky coverage depends on receivers and available aircraft reports. This is not a complete global traffic inventory.\n"
            "- Changed-state ingestion favours moving aircraft. Parked aircraft can disappear from the latest window; record counts are not flight counts.\n"
            "- Gaps indicate missing stored observations; they do not establish why an aircraft disappeared or why collection paused.\n"
            "- Only finite latitude/longitude in valid ranges can be mapped. Missing coordinates are not replaced with a guessed location.\n"
            "- Times are UTC. Speeds are km/h and altitudes km unless a table explicitly labels raw m/s or metres.\n"
            "- Source anomalies, measurement noise and missing features can affect ML flags. No output establishes flight-safety incidents, wrongdoing or dangerous behaviour.\n"
            "- Maps require access to external basemap tiles. Tables and numeric summaries remain available if tiles cannot load."
        )

    section_header("A small glossary", "Search a term as you explore the dashboard.")
    term = st.text_input("Find a term", placeholder="Try: ICAO24, imputation, freshness", key="glossary_search").strip().lower()
    rows = [(name, detail) for name, detail in GLOSSARY.items() if term in name.lower() or term in detail.lower()]
    if rows:
        st.dataframe(pd.DataFrame(rows, columns=["Term", "Plain-English meaning"]), hide_index=True, width="stretch",
                     column_config={"Plain-English meaning": st.column_config.TextColumn(width="large")})
    else:
        st.info("No matching term. Try a shorter word or clear the search.")
    st.markdown("Data source: [The OpenSky Network](https://opensky-network.org/) · "
                "[OpenSky REST API reference](https://openskynetwork.github.io/opensky-api/rest.html)")
