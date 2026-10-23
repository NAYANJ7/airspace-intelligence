-- Phase 7: analytics layer on top of aircraft_observations (PostgreSQL is the source of truth).
--
-- Only views and one function are created here: no extra tables, nothing is copied.
-- Every statement is CREATE OR REPLACE, so this file is safe to run again at any time.
--
--   python -m src.run_sql analytics        (inside Docker, see the run commands)
--   psql ... -f src/sql/analytics.sql      (also works; "-- @" lines are plain comments)
--
-- Blocks start with a "-- @ <title>" line; the runner executes one block at a time.
--
-- Notes that matter when reading the numbers:
--  * Ingestion only sends an aircraft when its state CHANGED (see deduplicator.py), so one
--    poll snapshot is not "all aircraft" - parked/unchanged aircraft are missing from it.
--    That is why "current" below uses each aircraft's latest observation in a time window
--    instead of a single snapshot.
--  * All time grouping is done in UTC, independent of the server time zone.

-- @ v_plausibility_limits: the one place where "implausible" is defined
-- Used by the velocity/altitude statistics below AND by data_quality.sql, so both agree.
--   velocity 400 m/s = 1,440 km/h: above any airliner ground speed (jet-stream records ~375 m/s)
--   altitude -500..25,000 m: below the lowest airports / above the highest airliners AND
--     above the stratospheric balloons that do appear in OpenSky (about 21 km)
--   on-ground speed 120 m/s = 432 km/h: faster than any take-off or landing roll of an airliner
CREATE OR REPLACE VIEW v_plausibility_limits AS
SELECT 400.0::double precision    AS max_velocity_ms,
       -500.0::double precision   AS min_altitude_m,
       25000.0::double precision  AS max_altitude_m,
       120.0::double precision    AS max_ground_speed_ms;

-- @ v_latest_aircraft: 1. latest observation for every aircraft (icao24)
-- Includes aircraft last seen days ago; filter on observation_time for "recent".
CREATE OR REPLACE VIEW v_latest_aircraft AS
SELECT DISTINCT ON (icao24)
       icao24,
       callsign,
       origin_country,
       observation_time,
       latitude,
       longitude,
       altitude_km,        -- geometric altitude / 1000 (NULL when the aircraft sent no GNSS altitude)
       baro_altitude,      -- barometric altitude in metres
       velocity_kmh,
       true_track,
       vertical_rate,
       on_ground,
       flight_status
FROM aircraft_observations
ORDER BY icao24, observation_time DESC;

-- @ v_current_status: 2. airborne vs on-ground right now, with percentages
-- "Right now" = every aircraft whose latest observation is within 15 minutes of the newest
-- observation in the table (as_of). Relative to the data, not to now(), so the view still
-- works when ingestion has been stopped. Caveat: aircraft that stop changing (parked) stop
-- being re-sent, so after a long run the on-ground share is understated.
CREATE OR REPLACE VIEW v_current_status AS
WITH bounds AS (
    SELECT max(observation_time) AS as_of FROM aircraft_observations
),
recent AS (
    SELECT DISTINCT ON (o.icao24) o.icao24, o.flight_status
    FROM aircraft_observations o, bounds b
    WHERE o.observation_time >= b.as_of - interval '15 minutes'
    ORDER BY o.icao24, o.observation_time DESC
)
SELECT r.flight_status,
       count(*)                                                AS aircraft,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)      AS pct,
       (SELECT as_of FROM bounds)                              AS as_of
FROM recent r
GROUP BY r.flight_status
ORDER BY r.flight_status;

-- @ v_daily_activity: 3 + 10. flight activity and aircraft counts per observation date
-- One view serves both: observations, distinct aircraft, poll snapshots and first/last time.
CREATE OR REPLACE VIEW v_daily_activity AS
SELECT observation_date,
       count(*)                                               AS observations,
       count(DISTINCT icao24)                                 AS aircraft,
       count(DISTINCT observation_time)                       AS snapshots,
       round(count(*)::numeric / count(DISTINCT icao24), 1)   AS obs_per_aircraft,
       count(*) FILTER (WHERE flight_status = 'AIRBORNE')     AS airborne_obs,
       count(*) FILTER (WHERE flight_status = 'ON_GROUND')    AS ground_obs,
       min(observation_time)                                  AS first_observation,
       max(observation_time)                                  AS last_observation
FROM aircraft_observations
GROUP BY observation_date
ORDER BY observation_date;

-- @ v_hourly_activity: 4. observation activity by hour (UTC)
CREATE OR REPLACE VIEW v_hourly_activity AS
SELECT observation_date,
       observation_hour,
       count(*)                          AS observations,
       count(DISTINCT icao24)            AS aircraft,
       count(DISTINCT observation_time)  AS snapshots
FROM aircraft_observations
GROUP BY observation_date, observation_hour
ORDER BY observation_date, observation_hour;

-- @ v_top_origin_countries: 5. most active origin countries
CREATE OR REPLACE VIEW v_top_origin_countries AS
SELECT origin_country,
       count(*)                                                  AS observations,
       count(DISTINCT icao24)                                    AS aircraft,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)        AS pct_of_observations
FROM aircraft_observations
GROUP BY origin_country
ORDER BY observations DESC, origin_country;

-- @ aircraft_history: 6. chronological observations for one aircraft
-- Usage: SELECT * FROM aircraft_history('39de4e');   (served by the primary key index)
CREATE OR REPLACE FUNCTION aircraft_history(p_icao24 text)
RETURNS TABLE (
    observation_time timestamptz,
    callsign         text,
    latitude         double precision,
    longitude        double precision,
    altitude_km      double precision,
    velocity_kmh     double precision,
    vertical_rate    double precision,
    true_track       double precision,
    flight_status    text
)
LANGUAGE sql STABLE AS $$
    SELECT o.observation_time, o.callsign, o.latitude, o.longitude, o.altitude_km,
           o.velocity_kmh, o.vertical_rate, o.true_track, o.flight_status
    FROM aircraft_observations o
    WHERE o.icao24 = lower(p_icao24)
    ORDER BY o.observation_time
$$;

-- @ v_flight_metrics_by_status: 7 + 8. velocity and altitude statistics, airborne vs on-ground
-- Implausible values (v_plausibility_limits) are left out of avg/max and counted in the
-- *_excluded columns, so a single corrupt ADS-B message cannot make "max velocity" nonsense.
CREATE OR REPLACE VIEW v_flight_metrics_by_status AS
SELECT o.flight_status,
       count(*)                                                                         AS observations,
       count(*) FILTER (WHERE o.velocity > l.max_velocity_ms)                           AS velocity_excluded,
       round((avg(o.velocity_kmh) FILTER (WHERE o.velocity <= l.max_velocity_ms))::numeric, 1)  AS avg_velocity_kmh,
       round((max(o.velocity_kmh) FILTER (WHERE o.velocity <= l.max_velocity_ms))::numeric, 1)  AS max_velocity_kmh,
       count(*) FILTER (WHERE o.geo_altitude NOT BETWEEN l.min_altitude_m AND l.max_altitude_m) AS altitude_excluded,
       round((avg(o.altitude_km) FILTER (WHERE o.geo_altitude BETWEEN l.min_altitude_m AND l.max_altitude_m))::numeric, 3) AS avg_altitude_km,
       round((max(o.altitude_km) FILTER (WHERE o.geo_altitude BETWEEN l.min_altitude_m AND l.max_altitude_m))::numeric, 3) AS max_altitude_km
FROM aircraft_observations o
CROSS JOIN v_plausibility_limits l
GROUP BY o.flight_status
ORDER BY o.flight_status;

-- @ v_flight_metrics_by_country: 7 + 8. velocity and altitude statistics per origin country (airborne only)
-- Ground rows are excluded on purpose: taxiing at 0 km/h and 0 km would drag every average down.
CREATE OR REPLACE VIEW v_flight_metrics_by_country AS
SELECT o.origin_country,
       count(*)                                                                         AS airborne_observations,
       count(DISTINCT o.icao24)                                                         AS aircraft,
       round((avg(o.velocity_kmh) FILTER (WHERE o.velocity <= l.max_velocity_ms))::numeric, 1)  AS avg_velocity_kmh,
       round((max(o.velocity_kmh) FILTER (WHERE o.velocity <= l.max_velocity_ms))::numeric, 1)  AS max_velocity_kmh,
       round((avg(o.altitude_km) FILTER (WHERE o.geo_altitude BETWEEN l.min_altitude_m AND l.max_altitude_m))::numeric, 3) AS avg_altitude_km,
       round((max(o.altitude_km) FILTER (WHERE o.geo_altitude BETWEEN l.min_altitude_m AND l.max_altitude_m))::numeric, 3) AS max_altitude_km
FROM aircraft_observations o
CROSS JOIN v_plausibility_limits l
WHERE o.flight_status = 'AIRBORNE'
GROUP BY o.origin_country
ORDER BY airborne_observations DESC, o.origin_country;

-- @ v_most_observed_aircraft: 9. aircraft with the most observations
CREATE OR REPLACE VIEW v_most_observed_aircraft AS
SELECT icao24,
       mode() WITHIN GROUP (ORDER BY callsign)                                         AS callsign,
       max(origin_country)                                                             AS origin_country,
       count(*)                                                                        AS observations,
       min(observation_time)                                                           AS first_seen,
       max(observation_time)                                                           AS last_seen,
       round(extract(epoch FROM max(observation_time) - min(observation_time)) / 60.0, 1) AS span_minutes,
       -- observations as a share of ALL poll snapshots: 100 = reported in every single poll
       round(100.0 * count(*) / (SELECT count(DISTINCT observation_time) FROM aircraft_observations), 1) AS pct_of_snapshots
FROM aircraft_observations
GROUP BY icao24
ORDER BY observations DESC, icao24;

-- ---------------------------------------------------------------------------------------
-- Sample queries (the runner prints these results; they only read data)
-- ---------------------------------------------------------------------------------------

-- @ sample 1: latest aircraft snapshot (5 most recently seen aircraft)
SELECT icao24, callsign, origin_country, observation_time, latitude, longitude, altitude_km, velocity_kmh, flight_status
FROM v_latest_aircraft
ORDER BY observation_time DESC, icao24
LIMIT 5;

-- @ sample 2: airborne vs on-ground (current)
SELECT * FROM v_current_status;

-- @ sample 3 + 10: activity by date
SELECT * FROM v_daily_activity;

-- @ sample 4: activity by hour
SELECT * FROM v_hourly_activity;

-- @ sample 5: top 10 origin countries
SELECT * FROM v_top_origin_countries LIMIT 10;

-- @ sample 6: history of one aircraft (first 5 observations of 39de4e)
SELECT * FROM aircraft_history('39de4e') LIMIT 5;

-- @ sample 7 + 8: velocity and altitude by flight status
SELECT * FROM v_flight_metrics_by_status;

-- @ sample 7 + 8: velocity and altitude by country (top 8 by observations)
SELECT * FROM v_flight_metrics_by_country LIMIT 8;

-- @ sample 9: most frequently observed aircraft (top 8)
SELECT * FROM v_most_observed_aircraft LIMIT 8;
