-- Phase 7: query-plan review. Read-only: EXPLAIN ANALYZE of the queries that matter most.
--
--   python -m src.run_sql explain
--
-- Look for: "Index Scan"/"Index Only Scan" on the index named in each title, the row counts,
-- and "Execution Time". Plans depend on table size, so rerun this on the full database.
-- (Autovacuum normally analyzes the table after each load; ANALYZE aircraft_observations
-- forces fresh statistics if a plan looks odd.)

-- @ plan 1: history of one aircraft -> expect the primary key index (icao24, observation_time)
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.observation_time, o.callsign, o.latitude, o.longitude, o.altitude_km, o.velocity_kmh, o.flight_status
FROM aircraft_observations o
WHERE o.icao24 = '39de4e'
ORDER BY o.observation_time;

-- @ plan 2: aircraft_history('39de4e') function (should be inlined into the same plan)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM aircraft_history('39de4e');

-- @ plan 3: observations in a time window -> expect idx_aircraft_obs_time
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*)
FROM aircraft_observations
WHERE observation_time >= (SELECT max(observation_time) FROM aircraft_observations) - interval '2 minutes';

-- @ plan 4: one country in a time window -> expect idx_aircraft_obs_country_time
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*)
FROM aircraft_observations
WHERE origin_country = 'India'
  AND observation_time >= (SELECT max(observation_time) FROM aircraft_observations) - interval '5 minutes';

-- @ plan 5: v_current_status (latest observation per aircraft in a 15-minute window)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_current_status;

-- @ plan 6: v_latest_aircraft (latest observation of every aircraft, whole table)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_latest_aircraft;

-- @ plan 7: v_daily_activity (whole-table aggregate; a scan is expected, no index can help)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_daily_activity;

-- @ plan 8: v_top_origin_countries (whole-table aggregate)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_top_origin_countries LIMIT 10;

-- @ plan 9: v_most_observed_aircraft (whole-table aggregate per aircraft)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_most_observed_aircraft LIMIT 10;

-- @ plan 10: v_data_quality_checks (all 15 checks)
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_data_quality_checks;
