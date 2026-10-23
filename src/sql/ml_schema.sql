-- Phase 11 only. Existing observations, analytics and DQ tables are untouched.
-- Run explicitly with: python -m src.ml init-db
CREATE TABLE IF NOT EXISTS aircraft_ml_models (
    model_version TEXT PRIMARY KEY,
    trained_at TIMESTAMPTZ NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB NOT NULL,
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS aircraft_anomaly_predictions (
    icao24 TEXT NOT NULL,
    observation_time TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL REFERENCES aircraft_ml_models(model_version),
    anomaly_score DOUBLE PRECISION NOT NULL,
    decision_function DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    imputed_feature_count SMALLINT NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (icao24, observation_time, model_version),
    FOREIGN KEY (icao24, observation_time)
        REFERENCES aircraft_observations(icao24, observation_time),
    CHECK (anomaly_score >= 0 AND anomaly_score < 'Infinity'::double precision),
    CHECK (decision_function > '-Infinity'::double precision
           AND decision_function < 'Infinity'::double precision),
    CHECK (is_anomaly = (decision_function < 0)),
    CHECK (imputed_feature_count BETWEEN 0 AND 6)
);

CREATE INDEX IF NOT EXISTS idx_aircraft_anomaly_model_time
    ON aircraft_anomaly_predictions(model_version, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_aircraft_anomaly_flag_time
    ON aircraft_anomaly_predictions(model_version, observation_time DESC)
    WHERE is_anomaly;

COMMENT ON TABLE aircraft_anomaly_predictions IS
    'Isolation Forest statistical anomalies, not confirmed flight-safety incidents; separate from DQ results.';
COMMENT ON COLUMN aircraft_anomaly_predictions.anomaly_score IS
    'Negative sklearn score_samples: larger means more unusual; NOT a probability.';
COMMENT ON COLUMN aircraft_anomaly_predictions.decision_function IS
    'sklearn score_samples minus learned offset; negative means flagged.';
