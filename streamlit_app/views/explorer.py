"""Search and inspect a bounded aircraft history, without claiming flight segmentation."""
from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st

import queries as q
from data_helpers import csv_bytes, history_series, valid_positions
from ui import (CYAN, BLUE, STATUS_LABEL, draw_chart, empty_state, fmt_int, fmt_num, fmt_ts,
                fmt_utc_column, freshness_banner, guarded, metric, page_header, section_header)


def line_chart(history: pd.DataFrame, field: str, title: str, unit: str, color: str) -> None:
    series = history_series(history, field)
    if series.empty:
        empty_state(f"No reported {title.lower()}", "Missing measurements are left blank; no values are invented for the chart.")
        return
    chart = (alt.Chart(series).mark_line(color=color, strokeWidth=2, point=alt.OverlayMarkDef(size=22))
             .encode(x=alt.X("time:T", title="Observation time (UTC)", axis=alt.Axis(format="%d %b %H:%M")),
                     y=alt.Y("value:Q", title=unit, scale=alt.Scale(zero=False)),
                     detail="segment:N",
                     tooltip=[alt.Tooltip("time:T", title="Observed (UTC)", format="%Y-%m-%d %H:%M:%S"),
                              alt.Tooltip("value:Q", title=unit, format=",.2f")]).properties(height=260))
    draw_chart(chart)


@guarded
def page() -> None:
    page_header("Investigate / Flight Explorer", "Follow the observation history.",
                "Search an aircraft identifier or callsign, then inspect its recorded measurements. "
                "This history can span multiple flights; no flight boundaries are inferred.")
    freshness_banner(q.freshness(), compact=True)
    term = st.text_input("ICAO24 or callsign prefix", key="explorer_term", max_chars=12, placeholder="e.g. 4b1815 or SWR12",
                         help="Exact six-character ICAO24, or at least two characters from the beginning of a callsign.").strip()
    results = q.search_aircraft(term) if len(term) >= 2 else q.recent_aircraft()
    if results.empty:
        empty_state("No aircraft found", "Try a different identifier or callsign prefix. Recent aircraft appear when observations are available.")
        return
    if len(term) < 2:
        st.caption("Showing the most recently observed aircraft. Enter at least two characters to search all stored history.")
    labels = {r.icao24: f"{r.icao24} · {r.callsign if pd.notna(r.callsign) and str(r.callsign).strip() else 'No callsign'}"
              f" · {r.origin_country or 'Unknown registration'}" for r in results.itertuples()}
    a, b, c = st.columns([2, 1, 1])
    icao = a.selectbox("Aircraft", list(labels), format_func=labels.get, key="explorer_aircraft")
    hours = b.selectbox("History window", [24, 168, None], index=2,
                        format_func=lambda n: {24: "Last 24 hours", 168: "Last 7 days", None: "All stored history"}[n],
                        key="explorer_window", help="Ends at this aircraft's latest stored observation, not the current time.")
    limit = c.selectbox("Maximum observations", [500, 2000, 5000], index=2, key="explorer_limit")
    until = results.loc[results.icao24 == icao, "last_seen"].iloc[0]
    since = until - timedelta(hours=hours) if hours else None
    history = q.aircraft_track(icao, limit, since, until)
    if history.empty:
        empty_state("No history in this window", "Choose a longer history window or another aircraft.")
        return
    last = history.iloc[-1]
    st.caption(f"Aircraft: {icao} · Last reported {fmt_ts(last['observation_time'])}. "
               "The measurements below describe that observation, not its present state.")
    columns = st.columns(4)
    metric(columns[0], "Recorded state", STATUS_LABEL.get(last.flight_status, "Unknown"), "At the aircraft's last observation")
    metric(columns[1], "Geometric altitude", fmt_num(last.altitude_km, 2, " km"), "Source geometric altitude · latest row",
           "This history uses altitude_km from aircraft_history(). A missing geometric altitude stays missing here.")
    metric(columns[2], "Ground speed", fmt_num(last.velocity_kmh, 0, " km/h"), "Reported horizontal speed · latest row")
    metric(columns[3], "Matching observations", fmt_int(history.total_rows.iloc[0]), "Records for this aircraft and history window")
    valid_last, _ = valid_positions(history.tail(1))
    position = (f"{last.latitude:.4f}°, {last.longitude:.4f}°" if not valid_last.empty else "No valid position reported")
    st.caption(f"Latest position: {position}. Showing the newest {len(history):,} matching observations. "
               "Chart lines break at missing values and gaps over three minutes; gaps do not prove an interruption to a flight.")
    a, b = st.columns(2)
    with a, st.container(border=True):
        section_header("Altitude over time", "Recorded geometric height; the vertical scale adapts to the available measurements.")
        line_chart(history, "altitude_km", "Altitude", "Altitude (km)", CYAN)
    with b, st.container(border=True):
        section_header("Ground speed over time", "Horizontal speed relative to the ground, not airspeed; the vertical scale adapts to the data.")
        line_chart(history, "velocity_kmh", "Ground speed", "Ground speed (km/h)", BLUE)
    with st.expander(f"Observation records · {len(history):,} displayed", expanded=False):
        table = history.drop(columns=["total_rows"]).copy()
        table["observation_time"] = fmt_utc_column(table.observation_time)
        table = table.rename(columns={"observation_time": "Observed (UTC)", "callsign": "Callsign", "latitude": "Latitude (°)",
                                     "longitude": "Longitude (°)", "altitude_km": "Altitude (km)", "velocity_kmh": "Ground speed (km/h)",
                                     "vertical_rate": "Vertical rate (m/s)", "true_track": "True track (°)", "flight_status": "Recorded state"})
        st.dataframe(table, hide_index=True, width="stretch")
        st.download_button("Download displayed history", csv_bytes(table), f"aircraft_{icao}_history.csv", "text/csv")
    with st.expander(f"Search results · {len(results):,} aircraft"):
        table = results.copy()
        table["last_seen"] = fmt_utc_column(table.last_seen)
        st.dataframe(table.rename(columns={"icao24": "ICAO24", "callsign": "Callsign", "origin_country": "Registration country",
                                          "observations": "Historical observations", "last_seen": "Last seen (UTC)"}), hide_index=True)
