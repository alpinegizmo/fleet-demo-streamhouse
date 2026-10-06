-- Business-domain derivations: raw telemetry and parcel data in, one topic
-- per business domain out (Fleet Maintenance / Safety / Delivery Operations),
-- each consumed independently downstream. Submitted as a single statement
-- set, so all four INSERTs run as one Flink job (fleet-business) that
-- reads each source topic once.

EXECUTE STATEMENT SET
BEGIN

-- Fleet Maintenance: engine running hot.
INSERT INTO `maintenance.alerts`
SELECT
  object_id AS vehicle_id,
  geozone_ids[1] AS geozone,
  inputs.calculated_inputs.temperature AS engine_temp_c,
  CASE WHEN inputs.calculated_inputs.temperature > 105 THEN 'high' ELSE 'medium' END AS severity,
  CONCAT('Vehicle ', object_id, ' engine temperature ', CAST(inputs.calculated_inputs.temperature AS STRING), 'C near ', geozone_ids[1]) AS message,
  `datetime` AS event_time
FROM `vehicle.telemetry`
WHERE inputs.calculated_inputs.temperature > 100;

-- Safety: fixed-threshold heuristic on the device's own overspeed flag and
-- harsh-braking score.
INSERT INTO `driver.risk.events`
SELECT
  object_id AS vehicle_id,
  geozone_ids[1] AS geozone,
  `position`.speed AS speed_kmh,
  inputs.device_inputs.overspeeding_events AS overspeed_status,
  inputs.device_inputs.ecodrive_braking_events AS harsh_braking_score,
  CASE
    WHEN inputs.device_inputs.overspeeding_events = 'OVERSPEED' AND inputs.device_inputs.ecodrive_braking_events > 0.7 THEN 'high'
    WHEN inputs.device_inputs.overspeeding_events = 'OVERSPEED' OR inputs.device_inputs.ecodrive_braking_events > 0.7 THEN 'medium'
    ELSE 'low'
  END AS risk_level,
  `datetime` AS event_time
FROM `vehicle.telemetry`
WHERE inputs.device_inputs.overspeeding_events = 'OVERSPEED' OR inputs.device_inputs.ecodrive_braking_events > 0.7;

-- Safety, statistical version: each vehicle judged against its own recent
-- driving instead of one global cutoff. Every reading is compared with the
-- mean and standard deviation of that vehicle's previous 30 speeds (in
-- event-time order); a reading more than 3 standard deviations from that
-- baseline is an anomaly. Needs 10 previous readings first, and a non-zero
-- spread (a parked vehicle's constant 0 km/h has none).
--
-- The baseline must exclude the reading being judged - otherwise a spike
-- inflates its own baseline, and in a window of n readings no single value
-- can ever score more than sqrt(n - 1) standard deviations. Streaming OVER
-- windows in Flink must end at CURRENT ROW, so the inner query aggregates
-- the window including the current reading (count, sum, sum of squares)
-- and the middle query subtracts it back out:
--   mean  = (sum - x) / (n - 1)
--   sigma = sqrt((sum_sq - x^2) / (n - 1) - mean^2)
--
-- forecast_speed_kmh / lower_bound / upper_bound keep the column names of
-- the Confluent Cloud AI_DETECT_ANOMALIES version this replaces: the
-- "forecast" here is simply the baseline mean, and the bounds are mean ± 3σ
-- (the lower bound floored at 0 km/h).
INSERT INTO `driver.risk.events.anomaly`
SELECT
  vehicle_id,
  geozone,
  speed_kmh,
  mean_speed AS forecast_speed_kmh,
  GREATEST(mean_speed - 3 * sd_speed, 0) AS lower_bound,
  mean_speed + 3 * sd_speed AS upper_bound,
  CASE WHEN ABS(speed_kmh - mean_speed) > 4 * sd_speed THEN 'high' ELSE 'medium' END AS risk_level,
  event_time
FROM (
  SELECT
    vehicle_id,
    geozone,
    speed_kmh,
    event_time,
    (sum_speed - speed_kmh) / (readings - 1) AS mean_speed,
    SQRT(GREATEST(
      (sum_sq_speed - speed_kmh * speed_kmh) / (readings - 1)
        - POWER((sum_speed - speed_kmh) / (readings - 1), 2),
      0)) AS sd_speed
  FROM (
    SELECT
      object_id AS vehicle_id,
      geozone_ids[1] AS geozone,
      `position`.speed AS speed_kmh,
      `datetime` AS event_time,
      COUNT(`position`.speed) OVER w AS readings,
      SUM(`position`.speed) OVER w AS sum_speed,
      SUM(`position`.speed * `position`.speed) OVER w AS sum_sq_speed
    FROM `vehicle.telemetry`
    WINDOW w AS (PARTITION BY object_id ORDER BY event_ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
  )
  WHERE readings > 10
)
WHERE sd_speed > 0
  AND ABS(speed_kmh - mean_speed) > 3 * sd_speed;

-- Delivery Operations: falling behind (lots onboard, few delivered) or
-- running ahead.
INSERT INTO `delivery.performance.events`
SELECT
  vehicle_id,
  geozone,
  parcels_onboard,
  parcels_delivered_last_hour,
  CASE
    WHEN parcels_onboard > 70 AND parcels_delivered_last_hour < 5 THEN 'behind_schedule'
    WHEN parcels_delivered_last_hour >= 15 THEN 'ahead_of_schedule'
    ELSE 'on_schedule'
  END AS performance_status,
  `timestamp` AS event_time
FROM `parcel.volume`
WHERE parcels_onboard > 70 OR parcels_delivered_last_hour >= 15;

END;
