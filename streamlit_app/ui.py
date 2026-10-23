"""Shared visual language, formatting, freshness and safe page boundaries."""
from __future__ import annotations

import functools
import math
import os
from datetime import timedelta
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from db import DashboardDBError

BLUE, CYAN, AMBER = "#77aaff", "#50d6df", "#f3bd72"
STATUS_LABEL = {"AIRBORNE": "Airborne", "ON_GROUND": "On ground"}
STATUS_COLOR = {"AIRBORNE": CYAN, "ON_GROUND": AMBER}
STATUS_RGBA = {"AIRBORNE": [80, 214, 223, 220], "ON_GROUND": [243, 189, 114, 230]}


def _positive_setting(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        return value if value > 0 else default
    except ValueError:
        return default


FRESH_MIN = _positive_setting("DASHBOARD_FRESH_MINUTES", 15)
STALE_MIN = max(FRESH_MIN, _positive_setting("DASHBOARD_STALE_MINUTES", 90))


def install_style() -> None:
    st.markdown("<style>" + Path(__file__).with_name("styles.css").read_text(encoding="utf-8") + "</style>",
                unsafe_allow_html=True)


def fmt_int(value) -> str:
    return "Not available" if value is None or pd.isna(value) else f"{int(value):,}"


def fmt_num(value, digits=1, unit="") -> str:
    try:
        number = float(value)
        return f"{number:,.{digits}f}{unit}" if math.isfinite(number) else "Not available"
    except (TypeError, ValueError):
        return "Not available"


def fmt_ts(value, seconds: bool = True) -> str:
    if value is None or pd.isna(value):
        return "Not available"
    stamp = pd.Timestamp(value)
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return stamp.strftime("%Y-%m-%d %H:%M:%S UTC" if seconds else "%Y-%m-%d %H:%M UTC")


def fmt_utc_column(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")


def fmt_age(delta: timedelta) -> str:
    minutes = max(int(delta.total_seconds() // 60), 0)
    if minutes < 1:
        return "under 1 min"
    if minutes < 120:
        return f"{minutes} min"
    hours, rest = divmod(minutes, 60)
    return f"{hours} h {rest:02d} min" if hours < 48 else f"{hours // 24} d {hours % 24} h"


def freshness_state(age: timedelta | None) -> dict:
    if age is None:
        return {"key": "empty", "label": "No observations"}
    minutes = age.total_seconds() / 60
    if minutes < -1:
        return {"key": "clock", "label": "Future timestamp"}
    if minutes <= FRESH_MIN:
        return {"key": "fresh", "label": "Recent observations"}
    if minutes <= STALE_MIN:
        return {"key": "recent", "label": "Recorded snapshot"}
    return {"key": "stale", "label": "Historical snapshot"}


def freshness_banner(info: dict, compact: bool = False) -> None:
    newest = info.get("newest")
    if newest is None or pd.isna(newest):
        st.info("No observations have reached PostgreSQL yet. The guide is available while the first load completes.")
        return
    age = info["db_now"] - newest
    state = freshness_state(age)
    st.markdown(
        f'<div class="freshness-strip"><span class="status-pill {state["key"]}">{state["label"]}</span>'
        f'<span>Latest observation <strong>{escape(fmt_ts(newest))}</strong></span>'
        f'<span class="age">Age <strong>{escape(fmt_age(age))}</strong></span></div>',
        unsafe_allow_html=True,
    )
    if state["key"] == "stale":
        st.warning(f"These observations are over {STALE_MIN} minutes old. Treat this as historical airspace; "
                   "check ingestion and the hourly PostgreSQL load for current data.")
    elif state["key"] == "clock":
        st.warning("The latest observation is ahead of the database clock. Check timestamps before interpreting freshness.")
    if not compact:
        st.caption(f"PostgreSQL loads hourly. Recent means at most {FRESH_MIN} min old; historical means over "
                   f"{STALE_MIN} min. Observation time is different from pipeline completion time.")


def page_header(section: str, title: str, description: str) -> None:
    st.markdown(f'<div class="page-heading"><div class="eyebrow">{escape(section)}</div>'
                f'<h1>{escape(title)}</h1><p>{escape(description)}</p></div>', unsafe_allow_html=True)


def section_header(title: str, description: str = "") -> None:
    st.subheader(title)
    if description:
        st.caption(description)


def metric(column, label: str, value, description: str, help_text: str = "") -> None:
    with column:
        with st.container(border=True):
            st.metric(label, value, help=help_text or description)
            st.caption(description)


def empty_state(title: str, detail: str) -> None:
    st.markdown(f'<div class="empty-state"><strong>{escape(title)}</strong><p>{escape(detail)}</p></div>',
                unsafe_allow_html=True)


def show_error(exc: DashboardDBError) -> None:
    st.error("This data is temporarily unavailable. Other pages and the pipeline guide remain accessible.")
    with st.expander("Connection / query details"):
        st.write(str(exc))  # DashboardDBError has fixed, credential-free messages.
        st.caption("After the connection is restored, choose Refresh data in the sidebar.")


def guarded(page):
    @functools.wraps(page)
    def wrapper(*args, **kwargs):
        try:
            return page(*args, **kwargs)
        except DashboardDBError as exc:
            show_error(exc)
    return wrapper


def style_chart(chart):
    return (chart.configure_view(strokeWidth=0)
            .configure_axis(labelColor="#a8bbd2", titleColor="#cbd7e7", gridColor="#23334a",
                            domain=False, tickColor="#30425a", labelFontSize=11, titleFontSize=12)
            .configure_legend(labelColor="#cbd7e7", titleColor="#cbd7e7", orient="bottom"))


def draw_chart(chart) -> None:
    st.altair_chart(style_chart(chart), width="stretch", theme=None)


def navigate(page: str, icao24: str | None = None) -> None:
    st.session_state["_next_page"] = page
    if icao24:
        st.session_state["_next_aircraft"] = icao24
    st.rerun(scope="app")
