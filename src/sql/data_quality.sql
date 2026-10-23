-- Phase 7: lightweight data-quality checks on aircraft_observations (SQL only).
--
-- Needs analytics.sql applied first (it defines v_plausibility_limits, the single place
-- where the "implausible" thresholds live):  python -m src.run_sql analytics data_quality
--
-- Every check returns a COUNT (not just PASS/FAIL) plus the rule and what was observed.
--   severity: error = data is wrong / constraint broken, warn = suspicious, info = for awareness
--   status:   'ok' when the count is 0, otherwise the severity
-- Some checks cannot fail on this table because the schema already forbids the problem
-- (NOT NULL, PRIMARY KEY, CHECK). They are still run, as a safety net if a constraint is
-- ever dropped or the table is rebuilt; the detail column says which constraint covers them.
--
-- All blocks are CREATE OR REPLACE views (safe to rerun) followed by SELECTs that read them.

-- @ v_snapshot_gaps: poll snapshots with more than 3 minutes between them
-- Polling is about every 60-75 s, so a longer pause means ingestion (or Kafka/Spark) was not
-- running. These gaps are expected after restarts and are reported, not repaired.
CREATE OR REPLACE VIEW v_snapshot_gaps AS
WITH snapshots AS (
    SELECT observation_time FROM aircraft_observations GROUP BY observation_time
),
ordered AS (
    SELECT observation_time,
           lag(observation_time) OVER (ORDER BY observation_time) AS previous_time
    FROM snapshots
)
SELECT previous_time                                                        AS gap_start,
       observation_time                                                     AS gap_end,
       round(extract(epoch FROM observation_time - previous_time) / 60.0, 1) AS gap_minutes
FROM ordered
WHERE previous_time IS NOT NULL
  AND observation_time - previous_time > interval '3 minutes'
ORDER BY gap_start;

-- @ v_data_quality_checks: all checks in one result (one pass over the table + the key check)
CREATE OR REPLACE VIEW v_data_quality_checks AS
WITH t AS (
    SELECT
        count(*)                                                                    AS total,
        count(*) FILTER (WHERE o.icao24 IS NULL)                                    AS null_icao24,
        count(*) FILTER (WHERE o.observation_time IS NULL)                          AS null_obs_time,
        count(*) FILTER (WHERE o.latitude  NOT BETWEEN -90  AND 90)                 AS bad_lat,
        min(o.latitude)  AS lat_min,  max(o.latitude)  AS lat_max,
        count(*) FILTER (WHERE o.longitude NOT BETWEEN -180 AND 180)                AS bad_lon,
        min(o.longitude) AS lon_min,  max(o.longitude) AS lon_max,
        count(*) FILTER (WHERE o.velocity < 0)                                      AS neg_vel,
        count(*) FILTER (WHERE o.velocity > l.max_velocity_ms)                      AS fast_vel,
        count(DISTINCT o.icao24) FILTER (WHERE o.velocity > l.max_velocity_ms)      AS fast_vel_aircraft,
        max(o.velocity)                                                             AS vel_max,
        count(*) FILTER (WHERE o.geo_altitude  NOT BETWEEN l.min_altitude_m AND l.max_altitude_m
                            OR o.baro_altitude NOT BETWEEN l.min_altitude_m AND l.max_altitude_m) AS bad_alt,
        min(LEAST(o.geo_altitude, o.baro_altitude))                                 AS alt_min,
        max(GREATEST(o.geo_altitude, o.baro_altitude))                              AS alt_max,
        count(*) FILTER (WHERE o.flight_status NOT IN ('AIRBORNE', 'ON_GROUND'))    AS bad_status,
        count(*) FILTER (WHERE o.on_ground IS NULL
                            OR (o.flight_status = 'ON_GROUND') <> o.on_ground)      AS status_inconsistent,
        count(*) FILTER (WHERE o.on_ground AND o.velocity > l.max_ground_speed_ms)  AS ground_but_fast,
        count(DISTINCT o.icao24) FILTER (WHERE o.on_ground AND o.velocity > l.max_ground_speed_ms) AS ground_but_fast_aircraft,
        count(*) FILTER (WHERE o.latitude IS NULL OR o.longitude IS NULL)           AS no_position,
        count(*) FILTER (WHERE o.ingestion_latency_seconds < 0
                            OR o.ingestion_latency_seconds > 300)                   AS bad_latency,
        max(o.ingestion_latency_seconds)                                            AS latency_max,
        max(o.observation_time)                                                     AS newest
    FROM aircraft_observations o
    CROSS JOIN v_plausibility_limits l
),
dup AS (
    -- NULL keys are excluded here: the two NULL checks above already report them
    SELECT count(*) AS dup_keys
    FROM (SELECT 1 FROM aircraft_observations
          WHERE icao24 IS NOT NULL AND observation_time IS NOT NULL
          GROUP BY icao24, observation_time HAVING count(*) > 1) d
),
gaps AS (
    SELECT count(*) AS n, max(gap_minutes) AS longest FROM v_snapshot_gaps
)
SELECT c.check_name,
       c.severity,
       c.issue_count,
       c.rows_checked,
       CASE WHEN c.rows_checked > 0 THEN round(100.0 * c.issue_count / c.rows_checked, 3) END AS issue_pct,
       CASE WHEN c.issue_count = 0 THEN 'ok' ELSE c.severity END                             AS status,
       c.detail
FROM t, dup, gaps, v_plausibility_limits l,
LATERAL (VALUES
    (1,  'null_icao24',               'error', t.null_icao24,        t.total, 'NOT NULL constraint on icao24 (cannot fail while the constraint exists)'),
    (2,  'null_observation_time',     'error', t.null_obs_time,      t.total, 'NOT NULL constraint on observation_time (cannot fail while the constraint exists)'),
    (3,  'duplicate_observation_key', 'error', dup.dup_keys::bigint, t.total, 'keys (icao24, observation_time) occurring more than once; PRIMARY KEY prevents this'),
    (4,  'invalid_latitude',          'error', t.bad_lat,            t.total, format('rule: -90..90; observed %s .. %s', t.lat_min, t.lat_max)),
    (5,  'invalid_longitude',         'error', t.bad_lon,            t.total, format('rule: -180..180; observed %s .. %s', t.lon_min, t.lon_max)),
    (6,  'negative_velocity',         'error', t.neg_vel,            t.total, 'rule: velocity >= 0 m/s (the Spark job also drops these)'),
    (7,  'implausible_velocity',      'warn',  t.fast_vel,           t.total, format('rule: velocity <= %s m/s; %s aircraft affected; observed max %s m/s', l.max_velocity_ms, t.fast_vel_aircraft, t.vel_max)),
    (8,  'implausible_altitude',      'warn',  t.bad_alt,            t.total, format('rule: geo and baro altitude within %s .. %s m; observed %s .. %s m', l.min_altitude_m, l.max_altitude_m, t.alt_min, t.alt_max)),
    (9,  'unexpected_flight_status',  'error', t.bad_status,         t.total, 'rule: AIRBORNE or ON_GROUND only; CHECK constraint prevents others'),
    (10, 'status_vs_on_ground',       'error', t.status_inconsistent, t.total, 'flight_status must follow on_ground; rows with on_ground NULL are counted (status silently defaults to AIRBORNE)'),
    (11, 'on_ground_but_fast',        'warn',  t.ground_but_fast,    t.total, format('on_ground = true with velocity > %s m/s; %s aircraft affected', l.max_ground_speed_ms, t.ground_but_fast_aircraft)),
    (12, 'missing_position',          'info',  t.no_position,        t.total, 'latitude or longitude NULL (no position fix); expected for a small share of OpenSky states'),
    (13, 'high_ingestion_latency',    'warn',  t.bad_latency,        t.total, format('rule: 0 .. 300 s between OpenSky snapshot and ingestion; observed max %s s', t.latency_max)),
    (14, 'stale_data',                'warn',  CASE WHEN now() - t.newest > interval '15 minutes' THEN 1 ELSE 0 END::bigint, NULL::bigint,
                                               format('newest observation %s UTC, %s ago (threshold 15 minutes)', to_char(t.newest AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), date_trunc('second', now() - t.newest))),
    (15, 'snapshot_gaps',             'info',  gaps.n,               NULL::bigint,
                                               format('%s pause(s) over 3 minutes between poll snapshots; longest %s minutes (list: v_snapshot_gaps)', gaps.n, gaps.longest))
) AS c(ord, check_name, severity, issue_count, rows_checked, detail)
ORDER BY c.ord;

-- ---------------------------------------------------------------------------------------
-- Results (read-only)
-- ---------------------------------------------------------------------------------------

-- @ results: all data-quality checks
SELECT check_name, severity, issue_count, rows_checked, issue_pct, status, detail
FROM v_data_quality_checks;

-- @ results: snapshot gaps (known pauses in ingestion; reported, never backfilled)
SELECT gap_start, gap_end, gap_minutes FROM v_snapshot_gaps;
