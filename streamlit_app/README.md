# Airspace Intelligence dashboard (Phase 12)

The dashboard now has seven pages, reusable styling and a built-in pipeline guide.
See [the performance guide](../PERFORMANCE_GUIDE.md) for setup, operator tests and expected results.

Read-only Streamlit dashboard on top of PostgreSQL (`aircraft_observations`, Phase 7 analytics/quality
views, Phase 11 predictions, and optional Phase 10 monitoring metadata).
Pages: Overview, Live Airspace, Flight Explorer, Analytics, Data Quality, ML Anomalies, About / Pipeline Guide.

## Run with Docker Compose (project root)
    docker compose build dashboard
    docker compose up -d --no-deps --wait dashboard
    Start-Process http://localhost:8501
    docker compose logs --tail 30 dashboard
Stop it (data is untouched):  `docker compose stop dashboard`   Start again:  `docker compose start dashboard`

## Run on Windows without Docker (project root, venv active)
    python -m pip install -r streamlit_app\requirements.txt
    python -m streamlit run streamlit_app\app.py
Open http://localhost:8501 (Ctrl+C stops it). The app reads `.env` from the project root and connects to
`localhost:5432`, the port the postgres container publishes.

## Settings (environment variables / .env, never printed by the app)
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` (required), `POSTGRES_HOST` (default `localhost`; the
  Compose service sets `postgres`), `POSTGRES_PORT` (default `5432`).
- Optional: `DASHBOARD_FRESH_MINUTES` (15), `DASHBOARD_STALE_MINUTES` (90), `DASHBOARD_QUERY_TIMEOUT_S` (60).

## Behaviour worth knowing
- Every connection is read-only (`default_transaction_read_only`) with a statement timeout; all SQL is parameterized.
- PostgreSQL is loaded hourly by Airflow; the optional live loader checks every 30 seconds after each cycle. The dashboard always shows recorded observations.
  A stale-data warning appears when the newest observation is older than `DASHBOARD_STALE_MINUTES`.
- Query caches: snapshots/saved audits 30 s, ML 60 s, archive analytics 10 min. Refresh clears only this page’s caches. Auto-refresh defaults to one minute.
- Overview archive count/trend and quality gap/history scans load on request. Quality opens the latest saved hourly audit with its timestamp.
- Needs the Phase 7 views once: `python -m src.run_sql analytics data_quality` (already done in your setup).
