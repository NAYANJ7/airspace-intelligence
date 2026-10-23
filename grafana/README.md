# Grafana monitoring (Phase 10)

One dashboard, **Airspace Pipeline Monitoring**, on top of PostgreSQL. No Prometheus, Loki or exporters.

```
Kafka / Spark checkpoint / Parquet / Airflow metadata DB --(read-only)--> monitor --> PostgreSQL <-- Grafana
                                                                       (1 sample/min)   (read-only role grafana_ro)
```

| Compose service (profile `monitoring`) | Purpose |
|---|---|
| `grafana-init` | one-shot: creates/updates the read-only PostgreSQL role `grafana_ro` (`src/sql/grafana_role.sql`) |
| `monitor` | `src/pipeline_monitor.py`: every 60 s writes Kafka offsets, Spark checkpoint time/lag, Parquet activity and Airflow run state to `pipeline_monitor_samples` / `pipeline_airflow_runs`. Its mounts are read-only. |
| `grafana` | http://localhost:3000 (this computer only). Data source + dashboard come from `grafana/provisioning` and `grafana/dashboards`; Grafana's own data lives in the `grafana_data` volume. |

`docker compose up -d` (without the profile) behaves exactly as before.

## PowerShell (run from the project folder)

```powershell
# 1. add three lines to .env (see .env.example), then validate
notepad .env        # GRAFANA_PG_PASSWORD=...  GRAFANA_ADMIN_USER=admin  GRAFANA_ADMIN_PASSWORD=...
docker compose --profile monitoring config -q      # prints nothing when valid

# 2. start only the new services (Kafka, Spark, ingestion, Airflow, Postgres are not recreated; no volume is touched)
docker compose --profile monitoring up -d --build monitor grafana

# 3. open Grafana, log in with GRAFANA_ADMIN_USER / GRAFANA_ADMIN_PASSWORD
Start-Process http://localhost:3000

# 4. logs and state
docker compose ps -a grafana-init monitor grafana          # grafana-init: Exited (0); monitor, grafana: Up
docker compose logs --tail 30 grafana-init monitor grafana

# 5. stop only the monitoring services (data, volumes and the rest of the pipeline stay as they are)
docker compose --profile monitoring stop monitor grafana
```

Never use `docker compose down -v`. The admin password is applied when Grafana first creates its volume; to change it later:
`docker compose exec grafana grafana cli admin reset-admin-password <new>`.

The dashboard is the Grafana home page (also Dashboards > Airspace). To change it, edit
`grafana/dashboards/airspace_pipeline.json`; Grafana reloads the file within about 30 s. UI edits are not saved on purpose.