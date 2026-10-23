"""One versioned feature contract shared by historical training and scoring.

Bounds reject corrupt inputs, not ordinary operational extremes. For example,
400 m/s is still eligible even though the existing DQ view flags that speed.
This module never modifies observations or data_quality_history.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Sequence

import numpy as np

FEATURE_SCHEMA = "airborne-kinematics-v1"
FEATURE_NAMES = (
    "altitude_m", "velocity_ms", "vertical_rate_ms", "geo_minus_baro_m",
    "track_sin", "track_cos",
)
SOURCE_COLUMNS = (
    "icao24", "observation_time", "flight_status", "on_ground",
    "geo_altitude", "baro_altitude", "velocity", "vertical_rate", "true_track",
)
ALTITUDE_MIN, ALTITUDE_MAX = -1500.0, 60000.0
VELOCITY_MAX, VERTICAL_RATE_MAX = 1500.0, 300.0
HEX6 = re.compile(r"^[0-9a-fA-F]{6}$")

# PostgreSQL NaN is greater than every finite float: BETWEEN excludes NaN and
# infinities as well as corrupt extremes. Keep these bounds aligned with build().
ELIGIBLE_SQL = f"""
    o.flight_status = 'AIRBORNE' AND o.on_ground IS FALSE
    AND o.icao24 ~ '^[0-9a-fA-F]{{6}}$'
    AND o.velocity BETWEEN 0 AND {VELOCITY_MAX}
    AND (o.geo_altitude BETWEEN {ALTITUDE_MIN} AND {ALTITUDE_MAX}
         OR o.baro_altitude BETWEEN {ALTITUDE_MIN} AND {ALTITUDE_MAX})
"""


@dataclass(frozen=True)
class FeatureBatch:
    rows: list[Mapping]
    matrix: np.ndarray
    missing_counts: np.ndarray
    skipped: int


def _number(value, low: float, high: float) -> float:
    if value is None or isinstance(value, (bool, np.bool_)):
        return math.nan
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return math.nan
    return number if math.isfinite(number) and low <= number <= high else math.nan


def build(rows: Sequence[Mapping]) -> FeatureBatch:
    """Core speed + one altitude required; optional measurements become NaN."""
    accepted, vectors = [], []
    for row in rows:
        when = row.get("observation_time")
        if (
            row.get("flight_status") != "AIRBORNE" or row.get("on_ground") is not False
            or not isinstance(row.get("icao24"), str) or not HEX6.fullmatch(row["icao24"])
            or not isinstance(when, datetime) or when.utcoffset() is None
        ):
            continue
        geo = _number(row.get("geo_altitude"), ALTITUDE_MIN, ALTITUDE_MAX)
        baro = _number(row.get("baro_altitude"), ALTITUDE_MIN, ALTITUDE_MAX)
        altitude = geo if math.isfinite(geo) else baro
        speed = _number(row.get("velocity"), 0, VELOCITY_MAX)
        if not (math.isfinite(altitude) and math.isfinite(speed)):
            continue
        vertical = _number(row.get("vertical_rate"), -VERTICAL_RATE_MAX, VERTICAL_RATE_MAX)
        track = _number(row.get("true_track"), 0, 360)
        radians = math.radians(track % 360) if math.isfinite(track) else math.nan
        vectors.append([altitude, speed, vertical, geo - baro, math.sin(radians), math.cos(radians)])
        accepted.append(row)
    matrix = np.asarray(vectors, dtype=np.float64).reshape((-1, len(FEATURE_NAMES)))
    return FeatureBatch(accepted, matrix, np.isnan(matrix).sum(axis=1), len(rows) - len(accepted))
