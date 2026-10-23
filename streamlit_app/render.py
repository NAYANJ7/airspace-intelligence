"""Overview and compatibility exports; specialist pages live in views/."""
import altair as alt
import pandas as pd
import streamlit as st

import queries as q
from db import DashboardDBError
from guide import quick_start
from ui import (CYAN, draw_chart, empty_state, fmt_age, fmt_int, fmt_num, fmt_ts,
                fmt_utc_column, freshness_banner, freshness_state, guarded, metric,
                page_header, section_header, show_error)
from views.airspace import page as aircraft_map
from views.explorer import page as explorer
from views.analytics import page as analytics
from views.quality import page as data_quality


@guarded
def overview() -> None:
    page_header("Airspace intelligence / Overview", "Airspace, in context.",
                "Explore recorded aircraft positions, understand flight patterns and investigate unusual "
                "measurements — with the source, timing and limitations always in view.")
    with st.expander("Start here · your 60-second tour", expanded=True):
        quick_start()
        st.caption("OpenSky → Python → Kafka → Spark → Parquet → PostgreSQL. "
                   "Airflow coordinates loads and scoring; Grafana monitors the pipeline. "
                   "The About / Pipeline Guide page explains each stage.")
    info = q.freshness()
    freshness_banner(info)
    if info.get("newest") is None:
        empty_state("Your first snapshot is on its way", "This page will populate when observations reach PostgreSQL. "
                    "Use the pipeline guide to understand the flow while you wait.")
        return
    snapshot = q.snapshot_summary()
    total = int(snapshot.get("aircraft") or 0)
    airborne, ground = int(snapshot.get("airborne") or 0), int(snapshot.get("ground") or 0)
    section_header("The latest recorded picture", "One latest observation per aircraft in the 15 minutes ending at the newest stored observation.")
    columns = st.columns(4)
    metric(columns[0], "Aircraft in snapshot", fmt_int(total), "Distinct ICAO24 identifiers · 15-min window",
           "A count of recently recorded aircraft, not a count of flights or every aircraft currently in the sky.")
    metric(columns[1], "Airborne", fmt_int(airborne), f"{100 * airborne / total:.1f}% of this snapshot" if total else "No aircraft in this window",
           "Aircraft whose latest recorded flight status is AIRBORNE. This status can be old; check observation age.")
    metric(columns[2], "On ground", fmt_int(ground), "Latest reported ground state",
           "Parked aircraft can drop out of the window because unchanged states are not repeatedly forwarded.")
    metric(columns[3], "Origin countries", fmt_int(snapshot.get("countries", 0)), "Registration countries · same snapshot",
           "Registration countries associated with ICAO24 addresses, not aircraft locations or departure countries.")

    section_header("Collection context", "The historical archive and the current snapshot answer different questions.")
    include_archive = st.checkbox("Show exact archive count and historical trend", value=False,
                                  key="overview_archive", help="Runs historical queries, cached for 10 minutes.")
    store = q.storage_summary() if include_archive else {}
    try:
        load = q.pipeline_load()
    except DashboardDBError:
        load = {}
    a, b, c = st.columns(3)
    metric(a, "Stored observations", fmt_int(store.get("total_rows")) if include_archive else "On demand",
           "Enable archive details above for an exact historical count",
           "Every row is one aircraft at one observation time. The same aircraft can contribute many records.")
    with b, st.container(border=True):
        st.caption("OBSERVATION HISTORY · UTC")
        st.write(f"**{fmt_ts(info['oldest'], False).replace(' UTC', '')}**")
        st.caption(f"through {fmt_ts(info['newest'], False)}")
    with c, st.container(border=True):
        st.caption("LAST SUCCESSFUL DATABASE LOAD")
        st.write(f"**{fmt_ts(load.get('last_load'), False)}**")
        if load.get("load_source"):
            st.caption(load["load_source"])
        elif load.get("monitor_updated"):
            st.caption(f"Hourly loader task completion · monitor sampled {fmt_ts(load['monitor_updated'], False)}")
        else:
            st.caption("Optional monitoring timestamp is unavailable; observation time is shown separately above.")

    daily = q.daily_activity().tail(14).copy() if include_archive else pd.DataFrame()
    if not daily.empty:
        daily["date"] = pd.to_datetime(daily["observation_date"])
        with st.container(border=True):
            section_header("How the archive grew", "Recorded observations per UTC date · latest 14 dates with data. Gaps can reflect collection pauses; counts are not flight totals.")
            chart = (alt.Chart(daily).mark_bar(color=CYAN, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                     .encode(x=alt.X("date:T", title="Observation date (UTC)", axis=alt.Axis(format="%d %b")),
                             y=alt.Y("observations:Q", title="Observation records"),
                             tooltip=[alt.Tooltip("date:T", title="UTC date", format="%Y-%m-%d"),
                                      alt.Tooltip("observations:Q", title="Observations", format=","),
                                      alt.Tooltip("aircraft:Q", title="Distinct aircraft that day", format=",")])
                     .properties(height=220))
            draw_chart(chart)
    st.caption(f"Snapshot queries fetched {fmt_ts(info['fetched_at'])}; cached for {q.LIVE_TTL} seconds. "
               "Refresh data reruns queries; it does not initiate a pipeline load.")
