"""Separate training and incremental scoring workflows."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from . import features, model
from .config import Settings
from .db import Repository

log = logging.getLogger("airspace.ml")


def train(repo: Repository, settings: Settings, as_of: datetime | None = None,
          make_active: bool = True) -> dict:
    repo.require_schema()
    until = as_of or datetime.now(timezone.utc)
    since = until - timedelta(days=settings.train_lookback_days)
    rows = repo.training_rows(since, until, settings.train_max_rows)
    batch = features.build(rows)
    log.info("Training extraction: selected=%d usable=%d rejected=%d", len(rows), len(batch.rows), batch.skipped)
    pipeline, metadata = model.fit(batch, settings, since, until)
    metadata = model.save(settings.model_dir, pipeline, metadata)
    repo.register_model(metadata)
    if make_active:
        model.activate(settings.model_dir, metadata["model_version"])
    log.info("Model trained: version=%s rows=%d active=%s training_flag_rate=%.4f",
             metadata["model_version"], len(batch.rows), make_active, metadata["training_flagged_fraction"])
    return metadata


def score(repo: Repository, settings: Settings, as_of: datetime | None = None,
          version: str | None = None) -> dict:
    repo.require_schema()
    pipeline, metadata = model.load(settings.model_dir, version)
    version = metadata["model_version"]  # Freeze one model for the entire invocation.
    until = as_of or datetime.now(timezone.utc)
    since = until - timedelta(hours=settings.score_lookback_hours)
    result = {"model_version": version, "selected": 0, "scored": 0, "inserted": 0,
              "conflicts": 0, "rejected": 0, "flagged_evaluated": 0, "batch_budget_reached": False}
    with repo.scoring_lock(version):
        repo.register_model(metadata)
        after = None
        for number in range(1, settings.max_batches + 1):
            rows = repo.pending_rows(since, until, version, settings.batch_size, after)
            if not rows:
                break
            after = (rows[-1]["observation_time"], rows[-1]["icao24"])
            batch = features.build(rows)
            scores, decisions = model.predict(pipeline, batch.matrix)
            records = [
                (r["icao24"], r["observation_time"], version, float(scores[i]), float(decisions[i]),
                 bool(decisions[i] < 0), int(batch.missing_counts[i]))
                for i, r in enumerate(batch.rows)
            ]
            inserted = repo.persist(records)  # one transaction per batch; retries use ON CONFLICT
            result["selected"] += len(rows)
            result["scored"] += len(records)
            result["inserted"] += inserted
            result["conflicts"] += len(records) - inserted
            result["rejected"] += batch.skipped
            result["flagged_evaluated"] += int((decisions < 0).sum())
            log.info("Scoring batch=%d selected=%d inserted=%d conflicts=%d rejected=%d",
                     number, len(rows), inserted, len(records) - inserted, batch.skipped)
            if len(rows) < settings.batch_size:
                break
        else:
            result["batch_budget_reached"] = True
            log.warning("Batch budget reached; unscored rows may remain. Rerun or raise ML_MAX_BATCHES. "
                        "Rows outside the lookback require an explicit wider backfill.")
    log.info("Scoring complete: %s", result)
    return result
