-- Phase 6: analytics/serving table for the processed Spark Parquet
-- (data/spark/aircraft_processed). Columns mirror that Parquet schema exactly.
-- Safe to run repeatedly: the loader applies this file on every run.

CREATE TABLE IF NOT EXISTS aircraft_observations (
    icao24                    TEXT             NOT NULL,
    callsign                  TEXT,
    origin_country            TEXT,
    observation_time          TIMESTAMPTZ      NOT NULL,   -- OpenSky snapshot time (UTC)
    ingested_at               TIMESTAMPTZ,                 -- when our pipeline processed it (UTC)
    longitude                 DOUBLE PRECISION,
    latitude                  DOUBLE PRECISION,
    baro_altitude             DOUBLE PRECISION,            -- metres
    geo_altitude              DOUBLE PRECISION,            -- metres
    velocity                  DOUBLE PRECISION,            -- m/s
    true_track                DOUBLE PRECISION,            -- degrees
    vertical_rate             DOUBLE PRECISION,            -- m/s
    on_ground                 BOOLEAN,
    squawk                    TEXT,
    spi                       BOOLEAN,
    position_source           SMALLINT,
    category                  SMALLINT,
    observation_date          DATE,
    altitude_km               DOUBLE PRECISION,            -- Spark-derived
    velocity_kmh              DOUBLE PRECISION,            -- Spark-derived
    flight_status             TEXT             NOT NULL,   -- Spark-derived: AIRBORNE / ON_GROUND
    observation_hour          SMALLINT,                    -- Spark-derived (0-23)
    ingestion_latency_seconds DOUBLE PRECISION,            -- Spark-derived

    -- One row per aircraft per OpenSky snapshot. Verified on the real data:
    -- 0 duplicates of (icao24, observation_time) across all Parquet rows.
    CONSTRAINT aircraft_observations_pkey PRIMARY KEY (icao24, observation_time),
    CONSTRAINT aircraft_observations_flight_status_chk
        CHECK (flight_status IN ('AIRBORNE', 'ON_GROUND'))
);

-- The primary key already serves "history of one aircraft" (icao24 first).
-- Time-range queries across all aircraft:
CREATE INDEX IF NOT EXISTS idx_aircraft_obs_time
    ON aircraft_observations (observation_time);

-- "Observations by origin country", optionally within a time window:
CREATE INDEX IF NOT EXISTS idx_aircraft_obs_country_time
    ON aircraft_observations (origin_country, observation_time);