-- Read-only operational checks; no archive-wide observation count.
SELECT source_key, count(*) AS tracked_files, sum(rows_read) AS rows_in_tracked_files,
       max(loaded_at) AS last_file_receipt
FROM public.aircraft_loaded_files GROUP BY source_key;

SELECT source_key, revision, last_committed_at, last_file
FROM public.aircraft_load_state ORDER BY source_key;

SELECT now() AS checked_at,
       (SELECT max(observation_time) FROM public.aircraft_observations) AS latest_observation,
       now() - (SELECT max(observation_time) FROM public.aircraft_observations) AS observation_age,
       (SELECT max(scored_at) FROM public.aircraft_anomaly_predictions) AS latest_scoring_write,
       (SELECT max(check_time) FROM public.data_quality_history) AS latest_quality_audit;
