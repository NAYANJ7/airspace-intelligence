"""Airspace Intelligence - Streamlit dashboard (Phase 12).

PostgreSQL is the only data source (read-only). Run from the project root:
    streamlit run streamlit_app/app.py
or with Docker Compose:  docker compose up -d --build dashboard   ->  http://localhost:8501
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # sibling modules, however the app is started

import streamlit as st  # noqa: E402

st.set_page_config(page_title="Airspace Intelligence", page_icon="✈", layout="wide", initial_sidebar_state="expanded")

import queries as q  # noqa: E402
import render  # noqa: E402
import ml_anomalies  # noqa: E402
import guide  # noqa: E402
import ui  # noqa: E402

PAGES = {
    "Overview": render.overview,
    "Live Airspace": render.aircraft_map,
    "Flight Explorer": render.explorer,
    "Analytics": render.analytics,
    "Data Quality": render.data_quality,
    "ML Anomalies": ml_anomalies.page,
    "About / Pipeline Guide": guide.page,
}
AUTO_REFRESH = {"Off": None, "Every 1 min": 60, "Every 5 min": 300, "Every 15 min": 900}

ui.install_style()
LEGACY_PAGES = {"Aircraft map": "Live Airspace", "Aircraft explorer": "Flight Explorer",
                "Flight analytics": "Analytics", "Data quality": "Data Quality", "ML anomalies": "ML Anomalies"}
if "_next_page" in st.session_state:
    st.session_state["page"] = st.session_state.pop("_next_page")
if "_next_aircraft" in st.session_state:
    st.session_state["explorer_term"] = st.session_state.pop("_next_aircraft")
    st.session_state.pop("explorer_aircraft", None)
if st.session_state.get("page") in LEGACY_PAGES:
    st.session_state["page"] = LEGACY_PAGES[st.session_state["page"]]

with st.sidebar:
    st.markdown('<div class="brand"><div class="brand-mark">AI</div><div><strong>Airspace Intelligence</strong>'
                '<small>Flight operations analytics</small></div></div>', unsafe_allow_html=True)
    st.caption("WORKSPACE")
    page = st.radio("Page", list(PAGES), label_visibility="collapsed", key="page")
    st.divider()
    st.caption("DATA CONTROLS")
    if st.button("Refresh data", width="stretch", help="Refreshes this page’s cached queries. Does not trigger ingestion, loading or training."):
        q.clear_page_cache(page)
        if page == "ML Anomalies":
            for function in (ml_anomalies.available, ml_anomalies.models, ml_anomalies.summary,
                             ml_anomalies.hourly, ml_anomalies.observations):
                function.clear()
    auto = st.selectbox("Auto-refresh", list(AUTO_REFRESH), index=1, key="auto_refresh",
                        help="Re-runs the current page on a timer. Query caches: 30 sec for snapshots, "
                             "1 min for ML, 10 min for archive analytics.")
    st.caption("Snapshot cache: 30 sec · ML: 1 min\n\nArchive analytics: 10 min")
    st.divider()
    st.caption("SOURCE · OPENSKY NETWORK\n\nRead-only PostgreSQL views\n\nAll timestamps are UTC")


@st.fragment(run_every=AUTO_REFRESH[auto])
def selected_page():
    # No global database gate: the guide and navigation survive database outages.
    PAGES[page]()
    st.markdown('<div class="footnote">AIRSPACE INTELLIGENCE · Recorded observations, with context. '
                'Statistical anomalies are not confirmed flight-safety incidents.</div>', unsafe_allow_html=True)


selected_page()
