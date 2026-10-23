-- Phase 10: read-only login role for Grafana. Idempotent; run by the grafana-init compose service as
--     psql -v gpw=<GRAFANA_PG_PASSWORD> -f grafana_role.sql
-- The password is passed on the command line from .env; it is never written in this file.

SELECT format('CREATE ROLE grafana_ro LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE', :'gpw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro')
\gexec

-- keeps the password in step with .env when it is changed later
SELECT format('ALTER ROLE grafana_ro PASSWORD %L', :'gpw')
\gexec

ALTER ROLE grafana_ro SET default_transaction_read_only = on;
ALTER ROLE grafana_ro SET statement_timeout = '30s';

SELECT format('GRANT CONNECT ON DATABASE %I TO grafana_ro', current_database())
\gexec

GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;          -- tables and views that exist now
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;  -- ones this user creates later