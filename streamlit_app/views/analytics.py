"""SQL-view analytics, with explicit scope and lazy loading for each analysis."""
import altair as alt
import pandas as pd
import streamlit as st

import queries as q
from data_helpers import activity_window
from ui import (BLUE, CYAN, STATUS_LABEL, STATUS_COLOR, draw_chart, empty_state,
                fmt_utc_column, freshness_banner, guarded, page_header, section_header)


def activity() -> None:
    days = st.selectbox("Activity window", [7, 30, None], index=1,
                        format_func=lambda n: f"Latest {n} days" if n else "All recorded dates", key="analytics_days",
                        help="Calendar dates ending on the newest stored observation date. Applies to both activity charts below.")
    daily = activity_window(q.daily_activity(), days)
    if daily.empty:
        empty_state("No activity to compare", "The activity charts will populate after observations are loaded.")
        return
    since, until = daily.observation_date.min().date(), daily.observation_date.max().date()
    hourly = q.hourly_activity(since, until)
    st.caption(f"Both charts: {since:%Y-%m-%d} through {until:%Y-%m-%d} UTC · "
               f"{int(daily.observations.sum()):,} observation records. First and last dates may be partial.")
    with st.container(border=True):
        section_header("Recorded activity by day", "Compare collection volume over time. Repeated aircraft observations are counted; these are not departures or distinct flights.")
        chart = (alt.Chart(daily).mark_bar(color=CYAN, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                 .encode(x=alt.X("observation_date:T", title="Observation date (UTC)", axis=alt.Axis(format="%d %b %Y")),
                         y=alt.Y("observations:Q", title="Observation records"),
                         tooltip=[alt.Tooltip("observation_date:T", title="UTC date", format="%Y-%m-%d"),
                                  alt.Tooltip("observations:Q", title="Records", format=","),
                                  alt.Tooltip("aircraft:Q", title="Distinct aircraft that day", format=","),
                                  alt.Tooltip("snapshots:Q", title="Recorded snapshot times", format=",")])
                 .properties(height=280))
        draw_chart(chart)
    if hourly.empty:
        empty_state("No hourly activity in this range", "Choose a wider activity window.")
        return
    hourly = hourly.set_index("observation_hour").reindex(range(24), fill_value=0).reset_index()
    hourly["hour"] = hourly.observation_hour.map(lambda n: f"{int(n):02d}:00")
    with st.container(border=True):
        section_header("When observations were recorded", "Total records in each UTC hour of day across the selected dates. Zero means no stored records, not no flights.")
        chart = (alt.Chart(hourly).mark_bar(color=BLUE, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                 .encode(x=alt.X("hour:O", title="Hour of day (UTC)", sort=None, axis=alt.Axis(labelAngle=-45)),
                         y=alt.Y("observations:Q", title="Observation records"),
                         tooltip=[alt.Tooltip("hour:O", title="UTC hour"),
                                  alt.Tooltip("observations:Q", title="Records", format=",")]).properties(height=260))
        draw_chart(chart)


def flight_metrics() -> None:
    by_status, by_country = q.metrics_by_status(), q.metrics_by_country()
    st.caption("Scope: all stored observations. Plausibility filters affect speed/altitude statistics, not the observation counts.")
    if by_status.empty:
        empty_state("No measurements to summarize", "Load observations before comparing flight measurements.")
        return
    left, right = st.columns([1, 1])
    dist = by_status.assign(status=by_status.flight_status.map(STATUS_LABEL))
    with left, st.container(border=True):
        section_header("Recorded airborne and ground states", "Shares of historical observations; moving aircraft contribute more changed states.")
        chart = (alt.Chart(dist).mark_arc(innerRadius=65, stroke="#09111f", strokeWidth=3)
                 .encode(theta=alt.Theta("observations:Q"),
                         color=alt.Color("status:N", title="Recorded state",
                                         scale=alt.Scale(domain=list(STATUS_LABEL.values()), range=list(STATUS_COLOR.values()))),
                         tooltip=[alt.Tooltip("status:N", title="State"),
                                  alt.Tooltip("observations:Q", title="Observations", format=",")]).properties(height=260))
        draw_chart(chart)
    with right, st.container(border=True):
        section_header("Average reported ground speed", "Compare historical state groups. Speeds over 1,440 km/h are excluded by the existing analytics view.")
        chart = (alt.Chart(dist).mark_bar(color=BLUE, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                 .encode(x=alt.X("status:N", title="Recorded state"), y=alt.Y("avg_velocity_kmh:Q", title="Average ground speed (km/h)"),
                         tooltip=[alt.Tooltip("status:N", title="State"), alt.Tooltip("avg_velocity_kmh:Q", title="km/h", format=".1f")])
                 .properties(height=260))
        draw_chart(chart)
    section_header("Measurement summaries", "Altitudes outside −500 to 25,000 m and speeds over 400 m/s are excluded from the corresponding means and maxima. Stored source rows are unchanged.")
    st.dataframe(dist.drop(columns=["status"]).assign(flight_status=dist.status).rename(columns={
        "flight_status": "Recorded state", "observations": "Observations", "avg_velocity_kmh": "Mean speed (km/h)",
        "max_velocity_kmh": "Max speed (km/h)", "avg_altitude_km": "Mean altitude (km)", "max_altitude_km": "Max altitude (km)",
        "velocity_excluded": "Speed outliers excluded", "altitude_excluded": "Altitude outliers excluded",
    }), hide_index=True, width="stretch")
    with st.expander("Airborne measurements by registration country · top 15 by records"):
        if by_country.empty:
            st.info("No airborne country summaries are available.")
        else:
            st.dataframe(by_country.rename(columns={
                "origin_country": "Registration country", "airborne_observations": "Airborne observations", "aircraft": "Distinct aircraft",
                "avg_velocity_kmh": "Mean speed (km/h)", "max_velocity_kmh": "Max speed (km/h)",
                "avg_altitude_km": "Mean altitude (km)", "max_altitude_km": "Max altitude (km)",
            }), hide_index=True, width="stretch")


def countries_and_coverage() -> None:
    countries, frequent = q.top_countries(), q.most_observed()
    st.caption("Scope: all stored history. Registration country is not departure country or geographic location.")
    if countries.empty:
        empty_state("No country distribution available", "This analysis needs stored observations with registration metadata.")
        return
    with st.container(border=True):
        section_header("Where aircraft are registered", "Top 15 registration countries by observation records. Counts depend on receiver coverage and changed-state collection.")
        chart = (alt.Chart(countries).mark_bar(color=CYAN, cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
                 .encode(x=alt.X("observations:Q", title="Observation records"),
                         y=alt.Y("origin_country:N", title="Registration country", sort="-x"),
                         tooltip=[alt.Tooltip("origin_country:N", title="Registration"),
                                  alt.Tooltip("observations:Q", title="Records", format=","),
                                  alt.Tooltip("aircraft:Q", title="Distinct aircraft", format=","),
                                  alt.Tooltip("pct_of_observations:Q", title="Share of all records (%)", format=".2f")])
                 .properties(height=380))
        draw_chart(chart)
    section_header("Most frequently recorded aircraft", "All-history observation counts. More records do not establish more flights or more flight time.")
    if frequent.empty:
        st.info("No aircraft ranking is available.")
        return
    table = frequent.copy()
    table["first_seen"], table["last_seen"] = fmt_utc_column(table.first_seen), fmt_utc_column(table.last_seen)
    st.dataframe(table.rename(columns={"icao24": "ICAO24", "callsign": "Representative callsign", "origin_country": "Registration country",
                                      "observations": "Observations", "first_seen": "First seen (UTC)", "last_seen": "Last seen (UTC)",
                                      "span_minutes": "First-to-last span (min)", "pct_of_snapshots": "Share of snapshot times (%)"}),
                 hide_index=True, width="stretch")
    st.caption("The first-to-last span includes gaps and is not flight duration. Share of snapshot times is the fraction of recorded poll timestamps containing this aircraft.")


@guarded
def page() -> None:
    page_header("Understand / Analytics", "Patterns in the recorded airspace.",
                "Explore collection activity, reported measurements and registration coverage. These summaries describe the stored sample.")
    freshness_banner(q.freshness(), compact=True)
    choice = st.radio("Analysis", ["Activity", "Speed & altitude", "Countries & coverage"], horizontal=True, key="analytics_view")
    {"Activity": activity, "Speed & altitude": flight_metrics, "Countries & coverage": countries_and_coverage}[choice]()
    st.caption("Analytics use the existing PostgreSQL views and are cached for 10 minutes. Only the selected analysis is queried.")
