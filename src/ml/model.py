"""Reproducible CPU model, immutable bundles, and an atomic active pointer."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.impute import MissingIndicator, SimpleImputer
from sklearn.pipeline import FeatureUnion, Pipeline

from .config import MLError, Settings
from .features import FEATURE_NAMES, FEATURE_SCHEMA, FeatureBatch

VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")


def runtime_versions() -> dict:
    return {
        "python": ".".join(platform.python_version_tuple()[:2]),
        **{name: importlib.metadata.version(name) for name in (
            "scikit-learn", "numpy", "scipy", "joblib", "threadpoolctl",
        )},
    }


def model_path(root: Path, version: str) -> Path:
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        raise MLError("Invalid model version: use only letters, digits, underscores and hyphens.")
    return root / version


def fit(batch: FeatureBatch, settings: Settings, since: datetime, until: datetime) -> tuple[Pipeline, dict]:
    if len(batch.rows) < settings.min_train_rows:
        raise MLError(
            f"Only {len(batch.rows)} usable training rows; need {settings.min_train_rows}. "
            "Wait for more data, widen --lookback-days, or review eligibility. Active model is unchanged."
        )
    if any(np.unique(batch.matrix[:, i]).size < 2 for i in (0, 1)):
        raise MLError("Training requires variation in altitude and speed. Active model is unchanged.")
    pipeline = Pipeline([
        ("preprocess", FeatureUnion([
            ("medians", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("missing_flags", MissingIndicator(features="all")),
        ])),
        ("forest", IsolationForest(
            n_estimators=settings.n_estimators, max_samples=min(settings.max_samples, len(batch.rows)),
            contamination=settings.contamination, random_state=settings.random_state,
            n_jobs=settings.n_jobs, max_features=1.0, bootstrap=False,
        )),
    ])
    pipeline.fit(batch.matrix)
    scores, decisions = predict(pipeline, batch.matrix)
    trained = datetime.now(timezone.utc)
    version = f"iforest-{trained:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:12]}"
    digest = hashlib.sha256()
    for row in batch.rows:
        digest.update((row["icao24"] + "|" + row["observation_time"].isoformat() + "\n").encode())
    digest.update(batch.matrix.astype("<f8").tobytes())
    metadata = {
        "format_version": 1, "model_version": version, "trained_at": trained.isoformat(),
        "feature_schema": FEATURE_SCHEMA, "feature_names": list(FEATURE_NAMES),
        "population": "AIRBORNE with on_ground=false and valid core speed/altitude",
        "preprocessing": "training medians + fixed missing flags; all-missing features imputed to zero",
        "runtime": runtime_versions(), "training_rows": len(batch.rows), "skipped_rows": batch.skipped,
        "training_data_sha256": digest.hexdigest(),
        "requested_since": since.isoformat(), "requested_until_exclusive": until.isoformat(),
        "observed_since": min(r["observation_time"] for r in batch.rows).isoformat(),
        "observed_until": max(r["observation_time"] for r in batch.rows).isoformat(),
        "sampling": "daily quotas, latest eligible rows within each day; deterministic order; no top-up",
        "row_limit": settings.train_max_rows,
        "parameters": pipeline.named_steps["forest"].get_params(),
        "offset": float(pipeline.named_steps["forest"].offset_),
        "training_flagged_fraction": float(np.mean(decisions < 0)),
        "training_score_quantiles": {
            str(q): float(np.quantile(scores, q)) for q in (0.5, 0.9, 0.99)
        },
        "interpretation": "Statistical anomalies, not confirmed flight-safety incidents or probabilities.",
    }
    return pipeline, metadata


def predict(pipeline: Pipeline, matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(matrix) == 0:
        return np.empty(0), np.empty(0)
    transformed = pipeline.named_steps["preprocess"].transform(matrix)
    forest = pipeline.named_steps["forest"]
    raw = forest.score_samples(transformed)
    decisions = raw - forest.offset_
    if not np.all(np.isfinite(raw)) or not np.all(np.isfinite(decisions)):
        raise MLError("Model produced non-finite scores; batch was not written.")
    return -raw, decisions


def _write_json(path: Path, value: dict) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def save(root: Path, pipeline: Pipeline, metadata: dict) -> dict:
    """Publish a unique completed directory. Never overwrite an existing version."""
    root.mkdir(parents=True, exist_ok=True)
    destination = model_path(root, metadata["model_version"])
    if destination.exists():
        raise MLError("Model version already exists; refusing to overwrite artifacts.")
    temporary = Path(tempfile.mkdtemp(prefix=".training-", dir=root))
    try:
        artifact = temporary / "model.joblib"
        joblib.dump(pipeline, artifact, compress=3)
        with artifact.open("rb+") as handle:
            os.fsync(handle.fileno())
        saved = {**metadata, "artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest()}
        _write_json(temporary / "metadata.json", saved)
        temporary.rename(destination)
        return saved
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def load(root: Path, version: str | None = None) -> tuple[Pipeline, dict]:
    """Load only trusted local artifacts; checksum is integrity, not authentication."""
    try:
        if version is None:
            version = json.loads((root / "active.json").read_text(encoding="utf-8"))["model_version"]
        directory = model_path(root, version)
        metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
        if (
            metadata["format_version"] != 1 or metadata["model_version"] != version
            or metadata["feature_schema"] != FEATURE_SCHEMA
            or metadata["feature_names"] != list(FEATURE_NAMES)
        ):
            raise MLError("Model feature/format version mismatch. Train a compatible model.")
        if metadata["runtime"] != runtime_versions():
            raise MLError(
                "Model runtime versions differ. Use the training Python/dependencies, or retrain here. "
                "For Airflow, train with the Compose ml service (Python 3.12)."
            )
        artifact = directory / "model.joblib"
        if hashlib.sha256(artifact.read_bytes()).hexdigest() != metadata["artifact_sha256"]:
            raise MLError("Model artifact checksum mismatch; refusing to score.")
        # Joblib/pickle can execute code: never load third-party or untrusted bundles.
        pipeline = joblib.load(artifact)
        if not isinstance(pipeline, Pipeline) or not isinstance(pipeline.named_steps.get("forest"), IsolationForest):
            raise MLError("Artifact does not contain the expected Isolation Forest pipeline.")
        return pipeline, metadata
    except FileNotFoundError:
        raise MLError("No complete model found. Train first: python -m src.ml train") from None
    except (KeyError, TypeError, ValueError) as exc:
        raise MLError("Model metadata is invalid. Restore a trusted bundle or retrain.") from exc


def activate(root: Path, version: str) -> None:
    load(root, version)  # verify integrity and compatibility before changing the pointer
    temporary = root / (".active-" + uuid.uuid4().hex + ".json")
    try:
        _write_json(temporary, {"model_version": version})
        os.replace(temporary, root / "active.json")
    finally:
        temporary.unlink(missing_ok=True)
