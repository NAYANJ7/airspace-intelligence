"""Usage: python -m src.ml {init-db,train,score,activate,inspect-model}."""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import replace
from datetime import datetime, timezone

from .config import MLError, Settings


def timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError("Use an ISO timestamp with timezone, e.g. 2026-10-17T00:00:00Z.") from None


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Create only the separate ML tables and indexes")
    train = commands.add_parser("train", help="Fit a new version and activate it after successful persistence")
    train.add_argument("--lookback-days", dest="train_lookback_days", type=int)
    train.add_argument("--max-rows", dest="train_max_rows", type=int)
    train.add_argument("--as-of", type=timestamp)
    train.add_argument("--no-activate", action="store_true", help="Keep the current model active")
    score = commands.add_parser("score", help="Score unprocessed eligible rows; never trains")
    score.add_argument("--lookback-hours", dest="score_lookback_hours", type=int)
    score.add_argument("--batch-size", type=int)
    score.add_argument("--max-batches", type=int)
    score.add_argument("--as-of", type=timestamp)
    score.add_argument("--model-version", help="Optional immutable version; defaults to active.json")
    activate = commands.add_parser("activate", help="Activate/roll back to an existing compatible model")
    activate.add_argument("--model-version", required=True)
    inspect = commands.add_parser("inspect-model", help="Validate the bundle and print non-secret metadata")
    inspect.add_argument("--model-version")
    return cli


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger("airspace.ml")
    try:
        settings = Settings.from_env()
        overrides = {name: getattr(args, name) for name in Settings.__dataclass_fields__
                     if getattr(args, name, None) is not None}
        settings = replace(settings, **overrides).validate()
        if args.command == "inspect-model":
            from .model import load
            _, metadata = load(settings.model_dir, args.model_version)
            print(json.dumps(metadata, indent=2))
            return 0
        from .db import Repository, connect
        with connect(settings) as connection:
            repo = Repository(connection)
            if args.command == "init-db":
                repo.initialize()
                log.info("ML tables ready. Historical observations and DQ tables are unchanged.")
            elif args.command == "train":
                from .service import train
                train(repo, settings, args.as_of, not args.no_activate)
            elif args.command == "score":
                from .service import score
                score(repo, settings, args.as_of, args.model_version)
            elif args.command == "activate":
                from .model import activate, load
                repo.require_schema()
                _, metadata = load(settings.model_dir, args.model_version)
                repo.register_model(metadata)
                activate(settings.model_dir, args.model_version)
                log.info("Active model: %s", args.model_version)
        return 0
    except MLError as exc:
        log.error("%s", exc)
        return 1
    except Exception as exc:
        # Do not print arbitrary DB/OS exceptions: DSNs and server details may
        # contain secrets. Error class / SQLSTATE are enough for first diagnosis.
        log.error("ML command failed (%s, SQLSTATE=%s). Check dependencies, database access/schema, "
                  "timeouts and model-directory permissions. No raw connection details were logged.",
                  type(exc).__name__, getattr(exc, "sqlstate", None))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
