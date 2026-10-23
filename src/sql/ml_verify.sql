-- Read-only verification; safe on the existing database.
SELECT model_version, trained_at,
       metadata->>'training_rows' AS training_rows,
       metadata->>'feature_schema' AS feature_schema
FROM aircraft_ml_models ORDER BY trained_at DESC;

SELECT model_version, count(*) AS predictions,
       count(*) FILTER (WHERE is_anomaly) AS statistical_anomalies,
       min(observation_time) AS first_observation,
       max(observation_time) AS last_observation,
       max(scored_at) AS last_scored
FROM aircraft_anomaly_predictions GROUP BY model_version ORDER BY last_scored DESC;

-- Must return 0: database primary key makes duplicates impossible.
SELECT count(*) AS duplicate_keys FROM (
    SELECT icao24, observation_time, model_version
    FROM aircraft_anomaly_predictions
    GROUP BY icao24, observation_time, model_version HAVING count(*) > 1
) duplicates;

SELECT p.icao24, o.callsign, p.observation_time, p.model_version,
       p.anomaly_score, p.decision_function, p.imputed_feature_count,
       o.geo_altitude, o.baro_altitude, o.velocity, o.vertical_rate
FROM aircraft_anomaly_predictions p
JOIN aircraft_observations o USING (icao24, observation_time)
WHERE p.is_anomaly
ORDER BY p.observation_time DESC, p.anomaly_score DESC LIMIT 20;
