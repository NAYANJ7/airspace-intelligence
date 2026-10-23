"""
Phase 4B: Spark Structured Streaming - Kafka (aircraft_states) -> Parquet.

    readStream.format("kafka")
        -> CAST(value AS STRING)
        -> from_json (explicit schema, built from normalizer.SCHEMA_COLUMNS -
           the actual field names Phase 3 produces, not invented ones)
        -> validation (icao24, observation_time, lat/lon range, velocity >= 0)
        -> transformations (altitude_km, velocity_kmh, flight_status,
           observation_hour, ingestion_latency_seconds)
        -> Parquet (data/spark/aircraft_processed/), checkpointed

Runs inside the "spark" Docker service (see docker-compose.yml), on the
same Docker network as "kafka". From there, Kafka's address is the
INTERNAL listener kafka:19092 - not localhost:9092, which only resolves
for processes on the Windows host, outside that network.

    docker compose exec spark python -m src.spark_streaming

Phase 8: this now runs by itself as the long-lived "spark" compose service
(command: python -m src.spark_streaming, restart: unless-stopped, stop_signal:
SIGINT = the Ctrl+C path below). Do NOT start a second copy with `exec`: the live
checkpoint must have exactly one writer, so a second copy refuses to start.

Runs until Ctrl+C / `docker compose stop`.
"""

import os
import sys
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = "aircraft_states"  # matches TOPIC in kafka_producer.py
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

PROCESSED_OUTPUT_PATH = os.getenv("SPARK_PROCESSED_PATH", "data/spark/aircraft_processed")
CHECKPOINT_PATH = os.getenv("SPARK_CHECKPOINT", "data/checkpoints/aircraft_stream")

_LOCK_HANDLE = None  # kept open for the life of the process: closing it would release the lock


def acquire_single_instance_lock() -> bool:
    """Only one streaming query may use the live checkpoint. An advisory lock in the
    container's temp dir stops a second `docker compose exec ... spark_streaming` from
    starting next to the spark service. Returns False if another instance holds it."""
    global _LOCK_HANDLE
    try:
        import fcntl
    except ImportError:  # not POSIX (e.g. run directly on Windows): no guard available
        return True
    handle = open(os.path.join(tempfile.gettempdir(), "spark_streaming.lock"), "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    except OSError as e:  # locking unsupported here: do not block Spark from starting
        print(f"WARNING: single-instance lock unavailable ({e}); continuing without it.", file=sys.stderr)
        handle.close()
        return True
    _LOCK_HANDLE = handle
    return True


# Field names and order match src/normalizer.py's SCHEMA_COLUMNS exactly -
# that is the actual schema Phase 3 serializes to JSON, not a guess.
# Every field is nullable=True, INCLUDING icao24 - on purpose. from_json's
# contract is that a non-nullable field failing to parse nulls out the
# WHOLE struct, not just that field, which would silently drop otherwise-
# good rows. Business-rule enforcement (icao24 required, etc.) happens
# explicitly in build_validation_filter() below instead.
AIRCRAFT_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("observation_time", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True),
        StructField("category", IntegerType(), True),
        StructField("observation_date", DateType(), True),
    ]
)


def build_validation_filter():
    """Exactly 5 rules: icao24 and observation_time required; lat/lon/velocity
    checked only when present, never required."""
    return (
        col("icao24").isNotNull()
        & col("observation_time").isNotNull()
        & (col("latitude").isNull() | col("latitude").between(-90, 90))
        & (col("longitude").isNull() | col("longitude").between(-180, 180))
        & (col("velocity").isNull() | (col("velocity") >= 0))
    )


def add_transformations(df):
    """altitude_km, velocity_kmh, flight_status, observation_hour,
    ingestion_latency_seconds - null-safe (arithmetic on a null column stays
    null; it does not error or invent a value)."""
    return (
        df.withColumn("altitude_km", spark_round(col("geo_altitude") / 1000, 3))
        .withColumn("velocity_kmh", spark_round(col("velocity") * 3.6, 2))
        .withColumn("flight_status", when(col("on_ground"), "ON_GROUND").otherwise("AIRBORNE"))
        .withColumn("observation_hour", hour(col("observation_time")))
        .withColumn(
            "ingestion_latency_seconds",
            spark_round(col("ingested_at").cast("double") - col("observation_time").cast("double"), 3),
        )
    )


def process_batch(batch_df, batch_id) -> None:
    """Minimal per-batch visibility (not flooding the terminal with rows),
    then write to Parquet."""
    if batch_df.isEmpty():
        print(f"Batch ID: {batch_id} - 0 records (empty batch, nothing written)")
        return

    count = batch_df.count()
    print(f"Batch ID: {batch_id} - {count} records processed and written to Parquet")
    batch_df.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)


def main() -> int:
    if not acquire_single_instance_lock():
        print(
            "ERROR: a Spark streaming query is already running in this container (the 'spark' "
            "compose service). Refusing to start a second query on the same checkpoint.",
            file=sys.stderr,
        )
        return 2

    print("=" * 50)
    print("OPEN SKY SPARK STREAMING")
    print("=" * 50)
    print(f"Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    spark = (
        SparkSession.builder.appName("OpenSkySparkStreaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print(f"Checkpoint location: {CHECKPOINT_PATH}")
    print(f"Output location: {PROCESSED_OUTPUT_PATH}")
    print()
    print("Starting Kafka -> Spark stream...")
    print("=" * 50)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # PERMISSIVE from_json (the default) turns malformed JSON into an
    # all-null struct rather than raising - it's then dropped by the
    # validation filter below (icao24 IS NULL), same as any other invalid
    # record, instead of crashing the stream.
    parsed = raw.selectExpr("CAST(value AS STRING) AS json_value").select(
        from_json(col("json_value"), AIRCRAFT_SCHEMA).alias("data")
    ).select("data.*")

    valid = parsed.filter(build_validation_filter())
    enriched = add_transformations(valid)

    try:
        query = (
            enriched.writeStream.foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="30 seconds")
            .start()
        )
    except Exception as e:
        print(
            f"FATAL: could not start the streaming query "
            f"(is Kafka reachable at {KAFKA_BOOTSTRAP_SERVERS}, and does topic "
            f"'{KAFKA_TOPIC}' exist?): {e}",
            file=sys.stderr,
        )
        spark.stop()
        return 1

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nCtrl+C received - stopping Spark stream...")
    except Exception as e:
        print(f"Streaming query failed: {e}", file=sys.stderr)
    finally:
        if query.isActive:
            query.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())