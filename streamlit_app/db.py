"""PostgreSQL access for the dashboard.

* Credentials come from the environment (.env on the host, env_file in Docker). They are never
  hardcoded and never shown: every error shown to the user is a fixed, safe sentence.
* Every connection is read-only (default_transaction_read_only) and has a statement timeout, so a
  dashboard bug or a slow view can neither change data nor hang a page.
* Queries are always parameterized (psycopg binds values on the server); user input is never
  concatenated into SQL text.
"""
from __future__ import annotations

import logging
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg

try:  # lets `streamlit run` on the host read the same .env as the rest of the project
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:  # python-dotenv missing: plain environment variables still work
    pass

log = logging.getLogger("dashboard.db")

QUERY_TIMEOUT_S = int(os.getenv("DASHBOARD_QUERY_TIMEOUT_S", "60"))


class DashboardDBError(Exception):
    """Its message is always safe to show. kind: config | unavailable | timeout | missing | query."""

    def __init__(self, message: str, kind: str = "query"):
        super().__init__(message)
        self.kind = kind


def _settings() -> dict:
    missing = [v for v in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not os.getenv(v)]
    if missing:
        raise DashboardDBError(
            "Database settings are missing: " + ", ".join(missing) + ". Set them in .env (see .env.example).",
            "config",
        )
    try:
        port = int(os.getenv("POSTGRES_PORT", "5432"))
    except ValueError:
        raise DashboardDBError("POSTGRES_PORT must be a number.", "config") from None
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": port,
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def _log(exc: Exception) -> None:
    """Server-side log only (container logs); the password is scrubbed even though libpq never prints it."""
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    password = os.getenv("POSTGRES_PASSWORD")
    if password:
        text = text.replace(password, "***")
    log.warning("database error: %s: %s", type(exc).__name__, text)


def _execute(sql: str, params: dict | None) -> tuple[list[str], list[tuple]]:
    try:
        with psycopg.connect(
            **_settings(),
            connect_timeout=5,
            autocommit=True,
            options=(
                "-c default_transaction_read_only=on "
                f"-c statement_timeout={QUERY_TIMEOUT_S * 1000} "
                "-c timezone=UTC -c application_name=airspace_dashboard"
            ),
        ) as conn:
            cur = conn.execute(sql, params)
            return [c.name for c in cur.description], cur.fetchall()
    except DashboardDBError:
        raise
    except psycopg.errors.QueryCanceled:
        raise DashboardDBError(
            "The query took too long and was cancelled. Try again, or narrow the filters.", "timeout"
        ) from None
    except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedFunction):
        raise DashboardDBError(
            "A table, view or function the dashboard needs does not exist yet. Load data with the loader and "
            "create the analytics views once: python -m src.run_sql analytics data_quality",
            "missing",
        ) from None
    except psycopg.OperationalError as exc:
        _log(exc)
        raise DashboardDBError(
            "PostgreSQL is not reachable, or it rejected the login. Check that the postgres container is "
            "running and that the POSTGRES_* settings in .env are correct.",
            "unavailable",
        ) from None
    except psycopg.Error as exc:
        _log(exc)
        raise DashboardDBError("The database could not answer this query.", "query") from None


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run one read-only, parameterized query and return a DataFrame (numeric -> float)."""
    columns, rows = _execute(sql, params)
    df = pd.DataFrame(rows, columns=columns)
    for col in df.columns:
        if df[col].dtype == object and df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = pd.to_numeric(df[col].map(lambda v: float(v) if isinstance(v, Decimal) else v))
    return df


def run_row(sql: str, params: dict | None = None) -> dict:
    """First row as a dict of plain Python values ({} when there is no row)."""
    columns, rows = _execute(sql, params)
    return dict(zip(columns, rows[0])) if rows else {}
