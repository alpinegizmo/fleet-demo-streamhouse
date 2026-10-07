-- Table definitions for all 11 Kafka topics. submit-jobs.sh loads this file
-- as the init script for every job, because Flink's default catalog is
-- in-memory and per-session: each job declares the tables it uses.
--
-- These DDL statements ARE the schema contract - there's no schema
-- registry. Every topic carries plain JSON; a field missing from a record
-- reads as NULL, and a record that isn't valid JSON is skipped
-- ('json.ignore-parse-errors'). The columns match the Confluent Cloud
-- version of this pipeline exactly, so the console reads both the same way.
--
-- ${PIPELINE} is the submitting job's name (e.g. fleet-business), used as
-- its Kafka consumer group: each job commits its own offsets on every
-- checkpoint, so a resubmitted job resumes where it left off.

SET 'pipeline.name' = '${PIPELINE}';
-- A topic partition that goes quiet must not hold back event time for the
-- whole job (vehicle.telemetry has 3 partitions; the others are sparse).
SET 'table.exec.source.idle-timeout' = '10 s';

-- --- Raw and context topics, produced by fleet-context-engine -------------

CREATE TABLE `vehicle.telemetry` (
  object_id STRING,
  `datetime` STRING,
  ignition_status STRING,
  trip_type STRING,
  `position` ROW<
    altitude DOUBLE,
    longitude DOUBLE,
    latitude DOUBLE,
    direction DOUBLE,
    satellites_count INT,
    speed DOUBLE
  >,
  inputs ROW<
    other ROW<
      country_code_geonames DOUBLE,
      virtual_gps_odometer DOUBLE
    >,
    calculated_inputs ROW<
      fuel_consumption DOUBLE,
      fuel_level DOUBLE,
      mileage DOUBLE,
      rpm DOUBLE,
      temperature DOUBLE,
      weight DOUBLE
    >,
    device_inputs ROW<
      priority STRING,
      movement STRING,
      hdop STRING,
      power_supply_voltage DOUBLE,
      battery_voltage DOUBLE,
      gps_speed DOUBLE,
      gsm_signal_strength DOUBLE,
      `operator` DOUBLE,
      engine_rpm DOUBLE,
      canbus_engine_coolant_temperature DOUBLE,
      fuel_level_can DOUBLE,
      speed_wheel DOUBLE,
      overspeeding_events STRING,
      ecodrive_braking_events DOUBLE,
      ecodrive_harsh_acceleration DOUBLE,
      ecodrive_idling_time DOUBLE,
      lcv_driver_doors STRING,
      lcv_left_back_doors STRING,
      lcv_right_back_doors STRING
    >,
    tires ARRAY<ROW<
      tire_id STRING,
      tire_pressure DOUBLE,
      tire_temperature DOUBLE,
      tire_air_leakage_rate DOUBLE,
      tire_pressure_threshold_detection INT,
      tire_status INT,
      tire_sensor_electrical_fault INT,
      tire_sensor_enable_status INT,
      tire_location INT,
      tire_extended_tire_pressure_support INT
    >>
  >,
  geozone_ids ARRAY<STRING>,
  -- Set only on records produced by the console's inject buttons, and
  -- carried through every derived topic below that descends from this one,
  -- so a recommendation can be traced back to the injection that caused it.
  -- NULL for everything else.
  injection_id STRING,
  -- Event time, parsed from the record's own ISO-8601 `datetime`. The
  -- 1-second watermark delay tolerates the simulator stamping all 12
  -- vehicles with the same whole second on every tick.
  event_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(`datetime`, 'T', ' '), 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS'),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'vehicle.telemetry',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE `weather.conditions` (
  geozone STRING,
  `condition` STRING,
  temp_c DOUBLE,
  wind_kph DOUBLE,
  updated_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'weather.conditions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE `traffic.incidents` (
  geozone STRING,
  `type` STRING,
  severity STRING,
  `timestamp` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'traffic.incidents',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE `parcel.volume` (
  vehicle_id STRING,
  geozone STRING,
  parcels_onboard INT,
  parcels_delivered_last_hour INT,
  `timestamp` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'parcel.volume',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- --- Business-domain topics, derived by 10-business.sql -------------------
--
-- Written keyed by vehicle_id ('key.fields'), so all of one vehicle's
-- events stay in order on one partition for downstream consumers.
--
-- The kafka_* columns on these and the AI tables expose each record's Kafka
-- metadata. They're VIRTUAL: readable (30-lakehouse.sql copies them into
-- Iceberg), but not part of the value written to Kafka.

CREATE TABLE `maintenance.alerts` (
  vehicle_id STRING,
  geozone STRING,
  engine_temp_c DOUBLE,
  severity STRING,
  message STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'maintenance.alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TABLE `driver.risk.events` (
  vehicle_id STRING,
  geozone STRING,
  speed_kmh DOUBLE,
  overspeed_status STRING,
  harsh_braking_score DOUBLE,
  risk_level STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'driver.risk.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TABLE `driver.risk.events.anomaly` (
  vehicle_id STRING,
  geozone STRING,
  speed_kmh DOUBLE,
  forecast_speed_kmh DOUBLE,
  lower_bound DOUBLE,
  upper_bound DOUBLE,
  risk_level STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'driver.risk.events.anomaly',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TABLE `delivery.performance.events` (
  vehicle_id STRING,
  geozone STRING,
  parcels_onboard INT,
  parcels_delivered_last_hour INT,
  performance_status STRING,
  event_time STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'delivery.performance.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

-- --- AI recommendation topics, derived by the AI job (phase 3) ------------

CREATE TABLE `ai.maintenance.recommendations` (
  vehicle_id STRING,
  geozone STRING,
  severity STRING,
  trigger_event STRING,
  recommendation STRING,
  injection_id STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'ai.maintenance.recommendations',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TABLE `ai.safety.recommendations` (
  vehicle_id STRING,
  geozone STRING,
  risk_level STRING,
  speed_kmh DOUBLE,
  recommendation STRING,
  injection_id STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'ai.safety.recommendations',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TABLE `ai.delivery.recommendations` (
  vehicle_id STRING,
  geozone STRING,
  performance_status STRING,
  parcels_onboard INT,
  recommendation STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'ai.delivery.recommendations',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${PIPELINE}',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'vehicle_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'value.json.ignore-parse-errors' = 'true'
);
