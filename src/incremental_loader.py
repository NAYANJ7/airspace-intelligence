"""Skip unchanged immutable Spark files, while retaining the Phase 6 COPY/merge.

Identity is a logical source key plus relative filename, size and nanosecond mtime.
Container mount paths are deliberately NOT part of identity. A force replay is
available for restored or externally edited files with preserved metadata.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

from . import postgres_loader as base

log = logging.getLogger("airspace.loader")
TRACKING_SQL = Path(__file__).with_name("sql") / "loader_tracking.sql"


class LoaderBusy(RuntimeError):
    """Another loader owns the database/schema lock; retry later."""


def fingerprint(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


@contextmanager
def loader_lock(conn, wait_seconds: float = 0):
    # Both the hourly Airflow task and the optional worker use this same lock.
    params = ("airspace_parquet_loader",)
    deadline = time.monotonic() + wait_seconds
    while True:
        acquired = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(current_schema()))", params
        ).fetchone()[0]
        if acquired:
            break
        if time.monotonic() >= deadline:
            raise LoaderBusy()
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    try:
        yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(hashtext(%s), hashtext(current_schema()))", params)


def initialize(conn) -> None:
    # Existing observation DDL is still idempotent; bookkeeping is additive.
    with conn.transaction():
        base.apply_schema(conn)
        conn.execute(TRACKING_SQL.read_text(encoding="utf-8"))


def load_tracked_file(conn, path: Path, source_key: str, relative_path: str,
                      expected: tuple[int, int]) -> tuple[int, int]:
    with conn.transaction():
        if fingerprint(path) != expected:
            raise RuntimeError("File changed before reading; defer to next run.")
        read, inserted = base.load_file(conn, path)
        # A failed COPY, merge, file check or receipt write rolls back EVERYTHING.
        if fingerprint(path) != expected:
            raise RuntimeError("File changed during reading; defer to next run.")
        conn.execute("""
            INSERT INTO aircraft_loaded_files
                (source_key, relative_path, size_bytes, mtime_ns, rows_read, rows_inserted, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, clock_timestamp())
            ON CONFLICT (source_key, relative_path) DO UPDATE SET
                size_bytes = EXCLUDED.size_bytes, mtime_ns = EXCLUDED.mtime_ns,
                rows_read = EXCLUDED.rows_read, rows_inserted = EXCLUDED.rows_inserted,
                loaded_at = EXCLUDED.loaded_at
        """, (source_key, relative_path, *expected, read, inserted))
        conn.execute("""
            INSERT INTO aircraft_load_state (source_key, revision, last_committed_at, last_file)
            VALUES (%s, 1, clock_timestamp(), %s)
            ON CONFLICT (source_key) DO UPDATE SET
                revision = aircraft_load_state.revision + 1,
                last_committed_at = EXCLUDED.last_committed_at, last_file = EXCLUDED.last_file
        """, (source_key, relative_path))
    return read, inserted


def run_load(parquet_dir: Path | None = None, *, source_key: str | None = None,
             max_files: int = 0, max_seconds: float = 0, force: bool = False,
             count_total: bool = False, wait_seconds: float = 0, connection=None) -> dict:
    directory = Path(parquet_dir if parquet_dir is not None else base.PARQUET_DIR)
    source_key = source_key or os.getenv("LOADER_SOURCE_KEY", "spark_aircraft_processed")
    if (not source_key.strip() or len(source_key) > 200 or max_files < 0
            or not math.isfinite(max_seconds) or max_seconds < 0
            or not math.isfinite(wait_seconds) or not 0 <= wait_seconds <= 300):
        raise ValueError("Invalid loader source or budget.")
    if not directory.is_dir():
        raise FileNotFoundError("Processed Parquet directory is missing.")
    started = time.monotonic()
    result = dict(files_found=0, files_loaded=0, files_skipped=0, files_failed=0,
                  files_pending=0, rows_read=0, rows_inserted=0)
    with ExitStack() as stack:
        conn = connection if connection is not None else stack.enter_context(base.connect())
        stack.enter_context(loader_lock(conn, wait_seconds))
        initialize(conn)
        receipts = {name: (size, modified) for name, size, modified in conn.execute(
            "SELECT relative_path, size_bytes, mtime_ns FROM aircraft_loaded_files WHERE source_key = %s",
            (source_key,),
        ).fetchall()}
        pending = []
        for path in directory.glob("*.parquet"):
            if path.name.startswith((".", "_")) or not path.is_file():
                continue
            result["files_found"] += 1
            stamp = fingerprint(path)
            relative = path.relative_to(directory).as_posix()
            if not force and receipts.get(relative) == stamp:
                result["files_skipped"] += 1
            else:
                pending.append((path, relative, stamp))
        pending.sort(key=lambda item: (item[2][1], item[1]))  # oldest first; deterministic ties
        for number, (path, relative, stamp) in enumerate(pending):
            if ((max_files and number >= max_files)
                    or (max_seconds and number > 0 and time.monotonic() - started >= max_seconds)):
                result["files_pending"] = len(pending) - number
                break
            try:
                read, inserted = load_tracked_file(conn, path, source_key, relative, stamp)
            except Exception as exc:
                result["files_failed"] += 1
                # No raw DB exception/DSN/credentials in logs. Other files may proceed.
                log.error("File %s failed (%s, SQLSTATE=%s); no receipt committed",
                          path.name, type(exc).__name__, getattr(exc, "sqlstate", None))
                continue
            result["files_loaded"] += 1
            result["rows_read"] += read
            result["rows_inserted"] += inserted
            log.info("File %s rows=%d inserted=%d duplicate_keys=%d", path.name, read, inserted, read-inserted)
        if count_total:
            result["table_rows"] = conn.execute("SELECT count(*) FROM aircraft_observations").fetchone()[0]
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    log.info("Load complete: %s", json.dumps(result, sort_keys=True))
    return result


def main(argv=()) -> int:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--force", action="store_true", help="Replay every file using ON CONFLICT; never overwrites observations")
    cli.add_argument("--count-total", action="store_true", help="Opt in to a full observation-table count")
    cli.add_argument("--max-files", type=int, default=0, help="0 = no file budget")
    cli.add_argument("--max-seconds", type=float, default=0, help="0 = no time budget; checked between file transactions")
    cli.add_argument("--wait-seconds", type=float, default=60, help="Wait up to this long for another loader (0-300)")
    args = cli.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        result = run_load(**vars(args))
        return 1 if result["files_failed"] else 0
    except LoaderBusy:
        log.warning("Another loader is active. Retry after it completes.")
        return 75
    except Exception as exc:
        log.error("Loader failed (%s, SQLSTATE=%s). Check database settings, directory and permissions.",
                  type(exc).__name__, getattr(exc, "sqlstate", None))
        return 1
