"""Pure presentation transforms. Never change stored observations or model inputs."""
from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd


def valid_positions(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Return finite, in-range coordinates without substituting an older position."""
    clean = frame.copy()
    for column in ("latitude", "longitude"):
        if column not in clean:
            clean[column] = pd.Series(index=clean.index, dtype=float)
        clean[column] = pd.to_numeric(clean[column], errors="coerce")
    keep = clean.latitude.between(-90, 90) & clean.longitude.between(-180, 180)
    return clean.loc[keep].copy(), int((~keep).sum())


def numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame.get(column, pd.Series(index=frame.index, dtype=float))
    return pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)


def history_series(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    """UTC plot data; missing measurements and gaps break lines, rather than interpolating."""
    result = pd.DataFrame({
        "time": pd.to_datetime(frame["observation_time"], utc=True, errors="coerce"),
        "value": numeric_series(frame, column),
    }).dropna(subset=["time"]).sort_values("time")
    breaks = result.time.diff().gt(timedelta(minutes=3)) | result.value.isna() | result.value.shift().isna()
    result["segment"] = breaks.cumsum().astype(str)
    result["time"] = result.time.dt.tz_localize(None)
    return result.dropna(subset=["value"])


def activity_window(frame: pd.DataFrame, days: int | None) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        return result
    result["observation_date"] = pd.to_datetime(result["observation_date"], utc=True, errors="coerce").dt.tz_localize(None)
    result = result.dropna(subset=["observation_date"]).sort_values("observation_date")
    if days and not result.empty:
        result = result.loc[result.observation_date >= result.observation_date.max() - timedelta(days=days - 1)]
    return result


def csv_bytes(frame: pd.DataFrame) -> bytes:
    """Protect exported text from spreadsheet formula interpretation; numbers stay numbers."""
    result = frame.copy()
    for column in result.select_dtypes(include=["object", "string"]).columns:
        result[column] = result[column].map(
            lambda v: "'" + v if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")) else v
        )
    return result.to_csv(index=False).encode("utf-8-sig")


def quality_counts(checks: pd.DataFrame) -> dict[str, int]:
    counts = checks.get("status", pd.Series(dtype=str)).value_counts()
    return {key: int(counts.get(key, 0)) for key in ("error", "warn", "info", "ok")}
