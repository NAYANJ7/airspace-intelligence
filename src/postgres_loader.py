"""
Phase 6: load processed Spark Parquet into PostgreSQL.

    Parquet (data/spark/aircraft_processed)  ->  this loader  ->  aircraft_observations

    python -m src.postgres_loader                      # on the Windows host (needs the .env POSTGRES_* values)
    docker compose run --rm loader                     # inside Docker

Idempotent: every Parquet file is bulk-COPYed into a temporary staging table,
then merged with INSERT ... ON CONFLICT (icao24, observation_time) DO NOTHING.
Re-running, restarting, or loading overlapping files never creates duplicates;
only rows whose (icao24, observation_time) key is not yet in the table are added.
Each file and its persistent receipt commit together; unchanged files are skipped.
The first upgraded run registers existing files using the same safe conflict merge.
A database advisory lock serializes hourly, manual and optional live loaders.
Use --force for a conflict-safe replay or --count-total for an explicit archive count.

This is a downstream step only: it never touches Kafka, Spark or the checkpoint,
and it only READS the Parquet files.
"""

import os
import sys
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

# Same variable spark_streaming.py uses for its output path.
PARQUET_DIR = os.getenv("SPARK_PROCESSED_PATH", "data/spark/aircraft_processed")
SCHEMA_FILE = Path(__file__).with_name("postgres_schema.sql")
TABLE = "aircraft_observations"
KEY_COLUMNS = ("icao24", "observation_time")

# Must match the processed Parquet schema (and postgres_schema.sql) exactly.
COLUMNS = [
    "icao24", "callsign", "origin_country", "observation_time", "ingested_at",
    "longitude", "latitude", "baro_altitude", "geo_altitude", "velocity",
    "true_track", "vertical_rate", "on_ground", "squawk", "spi",
    "position_source", "category", "observation_date",
    "altitude_km", "velocity_kmh", "flight_status", "observation_hour",
    "ingestion_latency_seconds",
]
TIMESTAMP_COLUMNS = ("observation_time", "ingested_at")


def connect() -> psycopg.Connection:
    """Credentials come from the environment (.env) - nothing is hardcoded."""
    missing = [v for v in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            "Missing environment variable(s): " + ", ".join(missing)
            + ". Set them in .env (see .env.example)."
        )
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
    )


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))


def read_parquet_file(path: Path) -> pa.Table:
    table = pq.read_table(path)
    missing = [c for c in COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"{path.name}: Parquet file is missing expected column(s): {missing}")
    table = table.select(COLUMNS)

    for i, name in enumerate(table.column_names):
        col = table.column(i)
        if name in TIMESTAMP_COLUMNS:
            # Spark writes UTC instants; pyarrow reads them back as naive - label them UTC.
            if col.type.tz is None:
                col = pc.assume_timezone(col, "UTC")
            col = col.cast(pa.timestamp("us", tz="UTC"), safe=False)
        elif pa.types.is_floating(col.type):
            # NaN is not the same as "no value" in SQL; store it as NULL.
            col = pc.if_else(pc.is_nan(col), pa.scalar(None, col.type), col)
        table = table.set_column(i, name, col)
    return table


def load_file(conn: psycopg.Connection, path: Path) -> tuple[int, int]:
    """Returns (rows_in_file, rows_actually_inserted)."""
    table = read_parquet_file(path)
    cols = ", ".join(COLUMNS)
    conflict = ", ".join(KEY_COLUMNS)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE stg (LIKE {TABLE}) ON COMMIT DROP")
            with cur.copy(f"COPY stg ({cols}) FROM STDIN") as copy:
                # Bound Python row-object allocations; PostgreSQL still uses bulk COPY.
                for batch in table.to_batches(max_chunksize=4096):
                    for row in zip(*(batch.column(i).to_pylist() for i in range(len(COLUMNS)))):
                        copy.write_row(row)
            cur.execute(
                f"INSERT INTO {TABLE} ({cols}) SELECT {cols} FROM stg "
                f"ON CONFLICT ({conflict}) DO NOTHING"
            )
            inserted = cur.rowcount
    return table.num_rows, inserted


def main(argv=()) -> int:
    # Kept as the existing Airflow and command-line entry point.
    # Explicit empty argv prevents Airflow's own CLI arguments being parsed here.
    from .incremental_loader import main as incremental_main
    return incremental_main(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
