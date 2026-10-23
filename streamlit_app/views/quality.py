"""Explain quality findings without treating warnings as a blanket health guarantee."""
import pandas as pd
import streamlit as st

import queries as q
from data_helpers import quality_counts
from ui import (empty_state, fmt_int, fmt_ts, fmt_utc_column, freshness_banner,
                guarded, metric, page_header, section_header)

STATUS_LABELS = {"error": "Error", "warn": "Warning", "info": "Information", "ok": "Passing"}
RANK = {"error": 0, "warn": 1, "info": 2, "ok": 3}


@guarded
def page() -> None:
    page_header("Trust / Data Quality", "Understand the limits of the data.",
                "Rule-based checks explain missing fields, invalid values and collection gaps. These findings are separate from ML anomaly predictions.")
    info = q.freshness()
    freshness_banner(info, compact=True)
    if info.get("newest") is None:
        empty_state("No observations to assess", "Quality findings become meaningful after observations reach PostgreSQL.")
        return
    mode = st.radio("Quality source", ["Latest saved audit (fast)", "Recalculate all history"],
                    horizontal=True, key="dq_source")
    saved = mode == "Latest saved audit (fast)"
    checks = q.quality_snapshot() if saved else q.quality_checks()
    if checks.empty:
        empty_state("No quality results available", "Wait for the hourly quality task, or select Recalculate all history to query the current database.")
        return
    if saved:
        st.info(f"Saved audit from {fmt_ts(checks.check_time.max())}. Findings describe the database at that time. "
                "New observations can arrive before the next audit; the freshness banner above is separate.")
    else:
        st.caption("Full-history recalculation · can be slower on a large archive · result cached for 10 minutes.")
    counts = quality_counts(checks)
    if counts["error"]:
        st.error(f"{counts['error']} integrity check(s) need attention. Review the rule and affected-row counts below before relying on related measurements.")
    elif counts["warn"]:
        st.warning(f"No integrity errors were reported, but {counts['warn']} warning check(s) need context. "
                   "Review freshness and suspicious measurements; warnings are not automatically harmless.")
    else:
        st.success("No integrity errors or warning findings were reported by these checks.")
    columns = st.columns(4)
    metric(columns[0], "Error findings", counts["error"], "Checks reporting integrity problems · all history")
    metric(columns[1], "Warning findings", counts["warn"], "Checks reporting suspicious values or age")
    metric(columns[2], "Informational findings", counts["info"], "Checks reporting gaps or missing fields")
    metric(columns[3], "Checks passing", counts["ok"], "Checks with zero reported issues")
    st.caption("These cards count checks, not affected rows or aircraft. The table gives the count for each rule; a row can fail multiple rules.")
    choice = st.radio("Show checks", ["All", "Needs attention", "Errors", "Warnings", "Information", "Passing"],
                      horizontal=True, key="dq_filter")
    filter_values = {"Errors": ["error"], "Warnings": ["warn"], "Information": ["info"], "Passing": ["ok"],
                     "Needs attention": ["error", "warn", "info"]}
    subset = checks.loc[checks.status.isin(filter_values[choice])] if choice != "All" else checks
    subset = subset.assign(rank=subset.status.map(RANK)).sort_values("rank", kind="stable")
    if subset.empty:
        empty_state("No findings in this category", "Choose All to see the checks that passed as well.")
    else:
        shown = pd.DataFrame({
            "Result": subset.status.map(STATUS_LABELS),
            "Check": subset.check_name.str.replace("_", " ").str.capitalize(),
            "Issues": subset.issue_count.map(fmt_int), "Rows checked": subset.rows_checked.map(fmt_int),
            "Affected rows (%)": subset.issue_pct.map(lambda v: "Not applicable" if pd.isna(v) else f"{v:.3f}"),
            "Rule / observed detail": subset.detail,
        })
        st.dataframe(shown, hide_index=True, width="stretch",
                     column_config={"Rule / observed detail": st.column_config.TextColumn(width="large")})
    with st.expander("How to interpret warnings", expanded=True):
        st.markdown(
            "- **Stale data:** the SQL rule warns after 15 minutes, which can occur between hourly loads. "
            "Check whether the timestamp advances after the next load; persistent age needs investigation.\n"
            "- **Implausible speed or altitude:** values exceed the project's screening ranges. Inspect the source observation; "
            "a warning alone does not establish what an aircraft actually did.\n"
            "- **Missing positions:** aircraft without valid coordinates are omitted from maps, not moved to a guessed location.\n"
            "- **Snapshot gaps:** more than three minutes between stored snapshot times. These describe missing collection history, not a proven cause."
        )
        st.caption("Saved audits update after the hourly quality task. Recalculated checks read the current database. ML scoring does not erase either kind of finding.")
    if st.checkbox("Load snapshot gaps · latest 50", key="dq_load_gaps"):
        gaps = q.snapshot_gaps()
        if gaps.empty:
            st.write("No gaps over three minutes were found.")
        else:
            table = gaps.copy()
            table["gap_start"], table["gap_end"] = fmt_utc_column(table.gap_start), fmt_utc_column(table.gap_end)
            st.dataframe(table.rename(columns={"gap_start": "Gap start (UTC)", "gap_end": "Gap end (UTC)", "gap_minutes": "Gap (min)"}), hide_index=True)
    if st.checkbox("Load recorded quality-check runs", key="dq_load_history"):
        history = q.quality_history()
        if history.empty:
            st.write("No check-run history is available. This is separate from the current view results above.")
        else:
            st.caption(f"Last recorded quality run: {fmt_ts(history.check_time.max())}. This is a check timestamp, not an aircraft observation time.")
            table = history.copy()
            table["check_time"] = fmt_utc_column(table.check_time)
            st.dataframe(table.rename(columns={"check_time": "Check run (UTC)", "errors": "Error checks", "warnings": "Warning checks",
                                               "info": "Informational checks", "passing": "Passing checks"}), hide_index=True)
