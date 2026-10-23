"""ML settings, using the project's existing .env / POSTGRES_* conventions."""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class MLError(RuntimeError):
    """An actionable message that is safe to print without connection secrets."""


@dataclass(frozen=True)
class Settings:
    model_dir: Path = PROJECT_ROOT / "artifacts" / "ml"
    train_lookback_days: int = 30
    train_max_rows: int = 100_000
    min_train_rows: int = 1_000
    score_lookback_hours: int = 72
    batch_size: int = 5_000
    max_batches: int = 20
    n_estimators: int = 200
    max_samples: int = 256
    contamination: float = 0.01
    random_state: int = 42
    n_jobs: int = 1
    query_timeout_s: int = 120

    def validate(self) -> Settings:
        for name in (
            "train_lookback_days", "train_max_rows", "min_train_rows",
            "score_lookback_hours", "batch_size", "max_batches", "n_estimators",
            "max_samples", "n_jobs", "query_timeout_s",
        ):
            if getattr(self, name) < 1:
                raise MLError(f"{name} must be a positive integer.")
        if self.min_train_rows < 20 or self.train_max_rows < self.min_train_rows:
            raise MLError("Require 20 <= min_train_rows <= train_max_rows.")
        if not math.isfinite(self.contamination) or not 0 < self.contamination <= 0.5:
            raise MLError("contamination must be in (0, 0.5].")
        if not 0 <= self.random_state <= 2**32 - 1:
            raise MLError("random_state must be in [0, 2**32 - 1].")
        return self

    @classmethod
    def from_env(cls) -> Settings:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env", override=False)
        base = cls()
        values = {}
        for name in cls.__dataclass_fields__:
            value = os.getenv("ML_" + name.upper())
            if value is None:
                continue
            try:
                if name == "model_dir":
                    path = Path(value).expanduser()
                    values[name] = path if path.is_absolute() else PROJECT_ROOT / path
                elif name == "contamination":
                    values[name] = float(value)
                else:
                    values[name] = int(value)
            except (ValueError, TypeError):
                raise MLError(f"Invalid ML_{name.upper()} setting.") from None
        return replace(base, **values).validate()
