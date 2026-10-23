"""Optional independent loader/scoring loops. Never calls OpenSky or trains a model."""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, replace

log = logging.getLogger("airspace.refresh")


def env_int(name: str, default: int, low: int, high: int) -> int:
    value = int(os.getenv(name, str(default)))
    if not low <= value <= high:
        raise ValueError(f"{name} is outside its supported range")
    return value


@dataclass
class ScoreWatch:
    token: tuple | None = None
    last_completed: float = 0.0

    def due(self, token: tuple, now: float, rescan_seconds: int) -> bool:
        return self.token != token or now - self.last_completed >= rescan_seconds

    def record(self, token: tuple, now: float, budget_reached: bool) -> None:
        # A partially drained backlog must be revisited even without new files.
        if not budget_reached:
            self.token, self.last_completed = token, now
        else:
            self.token = None


def score_once(watch: ScoreWatch) -> None:
    from .ml.config import Settings
    from .ml.db import Repository, connect
    from .ml.service import score

    settings = replace(Settings.from_env(), max_batches=env_int("LIVE_ML_MAX_BATCHES", 4, 1, 1000)).validate()
    rescan = env_int("LIVE_ML_RESCAN_SECONDS", 600, 30, 86400)
    # The pointer is small and private. Changes cause a scoring pass for the new version.
    model_token = hashlib.sha256((settings.model_dir / "active.json").read_bytes()).hexdigest()
    with connect(settings) as conn:
        newest = conn.execute("SELECT max(observation_time) AS newest FROM aircraft_observations").fetchone()["newest"]
        has_tracking = conn.execute("SELECT to_regclass('aircraft_load_state') AS relation").fetchone()["relation"]
        revisions = tuple((r["source_key"], r["revision"]) for r in conn.execute(
            "SELECT source_key, revision FROM aircraft_load_state ORDER BY source_key"
        ).fetchall()) if has_tracking else ()
        token = (model_token, newest, revisions)
        if not watch.due(token, time.monotonic(), rescan):
            log.info("ML idle: no new committed files or active-model change")
            return
        result = score(Repository(conn), settings)
        watch.record(token, time.monotonic(), result["batch_budget_reached"])


def main(argv=None) -> int:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("mode", choices=("loader", "ml"))
    cli.add_argument("--interval", type=int, default=30, help="Wait after each cycle; cycles never overlap")
    cli.add_argument("--once", action="store_true")
    args = cli.parse_args(argv)
    if not 10 <= args.interval <= 3600:
        cli.error("--interval must be between 10 and 3600 seconds")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    watch = ScoreWatch()
    log.info("%s worker started; interval=%ss after each cycle", args.mode, args.interval)
    while not stopped.is_set():
        code = 0
        try:
            if args.mode == "loader":
                from .incremental_loader import LoaderBusy, run_load
                try:
                    result = run_load(max_files=env_int("LIVE_LOAD_MAX_FILES", 100, 1, 100000),
                                      max_seconds=env_int("LIVE_LOAD_BUDGET_SECONDS", 45, 1, 3600))
                    code = 1 if result["files_failed"] else 0
                except LoaderBusy:
                    log.info("Hourly/manual loader is active; retry next cycle")
                    code = 75
            else:
                score_once(watch)
        except Exception as exc:
            log.error("%s cycle failed (%s, SQLSTATE=%s); retry next cycle. Check configuration and service logs.",
                      args.mode, type(exc).__name__, getattr(exc, "sqlstate", None))
            code = 1
        if args.once:
            return code
        stopped.wait(args.interval)
    log.info("%s worker stopped", args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
