# Phase 12 — dashboard and performance improvement

This combined update starts from **Airspace Intelligence(1).zip** and includes the dashboard redesign plus an incremental loader and optional frequent refresh workers.

Use **PERFORMANCE_GUIDE.md** for the current setup commands, operator tests, expected results and rollback. **PHASE12_CHANGES.json** lists every changed/added file relative to that upload.

The new changes have **not been executed or tested in the sandbox**, as requested. Earlier dashboard test results do not certify this revised code.

## Actual project inventory

- Entry point: `streamlit_app/app.py`. The original six pages used `render.py`, `queries.py`, `db.py` and a separate `ml_anomalies.py`.
- UI: Streamlit **1.65.0**, Altair charts, PyDeck maps; theme in `.streamlit/config.toml`. No dependency or Dockerfile changes.
- PostgreSQL: `aircraft_observations`, **23 columns**, primary key `(icao24, observation_time)`. Relevant fields include snapshot/ingestion timestamps, callsign, country, coordinates, geo/barometric altitude, velocity, track, vertical rate, on-ground flag and Spark-derived units/status/date/hour/latency.
- Analytics: `v_latest_aircraft`, `v_current_status`, `v_daily_activity`, `v_hourly_activity`, `v_top_origin_countries`, `v_flight_metrics_by_status`, `v_flight_metrics_by_country`, `v_most_observed_aircraft`, `v_plausibility_limits`, and `aircraft_history()`.
- Quality: `v_data_quality_checks`, `v_snapshot_gaps`, optional `data_quality_history`.
- ML exists: all `src/ml` modules, `aircraft_ml_models` / `aircraft_anomaly_predictions` DDL, two ML DAGs, active pointer, metadata and saved model. The attached version is `iforest-20261017T061015-d48969ccaf3d`. No model was retrained, activated or overwritten.
- Optional load-completion time comes from existing `pipeline_airflow_runs.loader_ended_at` for successful loaders. No completion timestamp is inferred from aircraft observations.
- Connections retain existing `POSTGRES_*`, `DASHBOARD_QUERY_TIMEOUT_S`, `DASHBOARD_FRESH_MINUTES` and `DASHBOARD_STALE_MINUTES` conventions. `db.py` is unchanged: bound parameters, read-only transactions, timeout and safe error messages remain.

## Improvements

1. **Consistent navy/cyan styling:** reusable CSS, clear headings, status strips, explained KPIs, readable tables and map legends. No new dependencies or external fonts.
2. **Seven focused pages:** Overview, Live Airspace, Flight Explorer, Analytics, Data Quality, ML Anomalies, About / Pipeline Guide. Specialist page code moved into `streamlit_app/views/`.
3. **Onboarding:** first-visit tour, six-stage pipeline explanation with orchestration/monitoring separate, searchable glossary and coverage/timing caveats. The guide works without PostgreSQL.
4. **Honest scope:** observation age, historical archive range and loader completion are distinct. Snapshot counts use one latest observation per aircraft in a window relative to the newest stored observation. Historical row counts are never labelled as current aircraft.
5. **Map correction:** select the latest observation before rejecting invalid coordinates. An older valid position no longer replaces an invalid latest one. Counts explain excluded positions and filtered results.
6. **Practical filters:** geographic region, registration country, state, optional altitude/speed ranges, marker caps, history windows and bounded CSV downloads. No inferred flight boundaries or invented positions.
7. **Clear analytics:** activity-date filters apply to both activity charts. Other analyses explicitly use all stored history. Only the selected analytics section is queried. Collection bias and partial dates are explained.
8. **Better quality interpretation:** warnings are not dismissed as harmless. Cards count checks; tables count affected rows. The SQL freshness threshold of 15 minutes is explained separately from the UI's default historical threshold of 90 minutes.
9. **ML inspection:** per-model/window KPIs, readable units, separate observation/scoring times, source-observation inspector and a button to open aircraft history. No training/activation controls. Flags are statistical anomalies, not confirmed incidents, wrongdoing or dangerous behaviour.
10. **Caching:** snapshots and saved audits use 30 seconds; ML uses 60 seconds; archive analytics use 600 seconds. Full archive counts/trends and gap scans load on request. Overview snapshot KPIs share one aggregate query.


## Performance additions

- Persistent Parquet receipts and observation inserts commit atomically; unchanged files are skipped.
- Hourly/manual loaders and the optional live loader share a PostgreSQL advisory lock.
- COPY row-object allocation is bounded to 4,096-row chunks; the full Arrow table for one file is still held in memory.
- Optional workers load every 30 seconds after each cycle and check scoring work every 60 seconds. Idle scoring is skipped until a source/model change or the ten-minute rescan.
- The original hourly pipeline, source ingestion, Spark code, Kafka configuration, observation schema and ML engine remain unchanged. The loader entry point is the one core file modified.
- Refresh data clears the selected page caches. The default auto-refresh is one minute.
- Quality opens the saved hourly audit with its timestamp; a full recalculation is an explicit choice.
