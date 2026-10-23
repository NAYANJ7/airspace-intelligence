"""A bounded map of each aircraft's latest reported position."""
import pandas as pd
import pydeck as pdk
import streamlit as st

import queries as q
from data_helpers import csv_bytes, valid_positions
from ui import (STATUS_LABEL, STATUS_RGBA, fmt_num, fmt_utc_column, freshness_banner,
                guarded, empty_state, page_header)

WINDOWS = {5: "5 minutes", 15: "15 minutes", 30: "30 minutes", 60: "1 hour", 180: "3 hours"}
REGIONS = {
    "Global": (-90.0, 90.0, -180.0, 180.0),
    "Europe": (35.0, 72.0, -25.0, 45.0),
    "South Asia": (5.0, 38.0, 60.0, 100.0),
    "East Asia": (15.0, 55.0, 100.0, 150.0),
    "North America": (7.0, 72.0, -170.0, -50.0),
    "Oceania": (-50.0, 0.0, 110.0, 180.0),
}


def map_view(points: pd.DataFrame) -> pdk.ViewState:
    if len(points) > 1:
        return pdk.data_utils.compute_view(points[["longitude", "latitude"]].values.tolist(), view_proportion=0.85)
    return pdk.ViewState(latitude=float(points.latitude.iloc[0]), longitude=float(points.longitude.iloc[0]), zoom=5)


@guarded
def page() -> None:
    page_header("Explore / Live Airspace", "A position, with its timestamp.",
                "Each marker is one aircraft's latest recorded position in the selected snapshot window. "
                "This is a database view, not a direct live radar feed.")
    freshness_banner(q.freshness(), compact=True)
    with st.container(border=True):
        a, b, c = st.columns([1, 1, 2])
        minutes = a.selectbox("Snapshot window", list(WINDOWS), index=1, format_func=WINDOWS.get, key="map_window",
                              help="Counted back from the newest observation in PostgreSQL, not from now.")
        region = b.selectbox("Geographic area", list(REGIONS), key="map_region",
                             help="Approximate rectangular geographic bounds. This filter uses aircraft coordinates.")
        statuses = c.multiselect("Reported state", list(STATUS_LABEL), default=list(STATUS_LABEL),
                                 format_func=STATUS_LABEL.get, key="map_status")
        options = q.map_countries(minutes)
        names = options["origin_country"].tolist() if not options.empty else []
        st.session_state["map_country"] = [v for v in st.session_state.get("map_country", []) if v in names]
        a, b = st.columns([3, 1])
        countries = a.multiselect("Registration country", names, key="map_country", placeholder="All registration countries",
                                  help="OpenSky origin_country describes registration, not the country beneath an aircraft.")
        limit = b.selectbox("Marker limit", [2000, 5000, 10000], index=1, key="map_limit")
        altitude = speed = None
        if st.checkbox("Filter altitude and ground speed", key="map_ranges"):
            a, b = st.columns(2)
            altitude = a.slider("Altitude range (km)", -1.5, 60.0, (-0.5, 25.0), 0.5,
                                help="Geometric altitude, with barometric fallback. Missing values are excluded when this filter is on.")
            speed = b.slider("Ground speed range (km/h)", 0, 5400, (0, 1440), 10,
                             help="Reported speed; values are not clipped. Missing speeds are excluded when this filter is on.")
    if not statuses:
        empty_state("Choose a reported state", "Select Airborne, On ground or both to populate the map.")
        return
    frame, counts = q.map_snapshot(minutes, tuple(countries), tuple(statuses), limit, REGIONS[region], altitude, speed)
    points, invalid = valid_positions(frame)
    st.caption(f"{counts['matching_count']:,} aircraft match the map filters; showing at most {limit:,}. "
               f"{counts['excluded_positions']:,} latest records have no valid position in the selected window/country/state scope "
               "(before geographic and measurement filters). Older positions are not substituted.")
    if invalid:
        st.warning(f"{invalid:,} additional invalid coordinates were omitted during display validation.")
    if points.empty:
        empty_state("No positions match this view", "Try Global, widen the snapshot window, or clear the country and measurement filters. "
                    "An empty map does not mean the airspace has no aircraft.")
        return
    points["callsign"] = points.callsign.fillna("").astype(str).str.strip().replace("", "Not reported")
    points["observed_txt"] = fmt_utc_column(points.observation_time) + " UTC"
    points["altitude_txt"] = points.altitude_km.map(lambda v: fmt_num(v, 2, " km"))
    points["speed_txt"] = points.velocity_kmh.map(lambda v: fmt_num(v, 0, " km/h"))
    points["status_txt"] = points.flight_status.map(STATUS_LABEL).fillna("Unknown")
    points["color"] = points.flight_status.map(STATUS_RGBA)
    shown = points[["longitude", "latitude", "callsign", "icao24", "origin_country", "altitude_txt", "speed_txt",
                    "status_txt", "observed_txt", "color"]]
    layer = pdk.Layer("ScatterplotLayer", id="aircraft-positions", data=shown, get_position="[longitude, latitude]",
                      get_fill_color="color", get_radius=5000, radius_min_pixels=3, radius_max_pixels=9,
                      pickable=True, auto_highlight=True)
    tooltip = {"text": "{callsign} · {icao24}\nRegistration: {origin_country}\nState: {status_txt}\n"
                        "Altitude: {altitude_txt}\nGround speed: {speed_txt}\nObserved: {observed_txt}",
               "style": {"backgroundColor": "#10243b", "color": "#e4edf8", "fontSize": "12px"}}
    st.markdown('<div class="map-legend"><span><i style="background:#50d6df"></i>Airborne</span>'
                '<span><i style="background:#f3bd72"></i>On ground</span>'
                '<span>Hover to inspect · drag to pan · scroll to zoom</span></div>', unsafe_allow_html=True)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=map_view(shown), tooltip=tooltip, map_style=None),
                    height=560, width="stretch")
    st.caption(f"Showing {len(shown):,} markers, newest first. Map tiles require internet access; the table below uses the same observations.")
    table = shown.drop(columns=["color"]).rename(columns={
        "icao24": "ICAO24", "callsign": "Callsign", "origin_country": "Registration country",
        "longitude": "Longitude (°)", "latitude": "Latitude (°)", "altitude_txt": "Altitude",
        "speed_txt": "Ground speed", "status_txt": "Recorded state", "observed_txt": "Observed (UTC)",
    })
    with st.expander(f"Table alternative · {len(table):,} mapped aircraft"):
        st.dataframe(table, hide_index=True, width="stretch")
        st.download_button("Download mapped observations", csv_bytes(table), "airspace_positions.csv", "text/csv")
