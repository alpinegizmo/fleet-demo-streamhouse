-- Lakehouse: continuously copies the derived topics (business events and
-- AI recommendations) into Apache Iceberg tables, the open-source stand-in
-- for Confluent Tableflow. The same data is then available two ways: as a
-- stream (the Kafka topic, for applications and agents) and as a table
-- (Iceberg, for analytics engines like DuckDB - see local/duckdb/).
--
-- Iceberg tables live in an S3-compatible object store (RustFS); a REST
-- catalog (iceberg-rest) tracks each table's current snapshot. The Iceberg
-- sink commits a new snapshot on every Flink checkpoint, so the tables lag
-- the topics by up to one checkpoint interval (30 s).
--
-- The raw/context topics are left out, as they were with Tableflow: high
-- volume, low analytical value.
--
-- Table maintenance runs inside this job, after its own commits (Iceberg's
-- v2 sink with post-commit maintenance). Without it, every checkpoint adds a
-- snapshot to every table forever, and each commit writes a new
-- metadata.json listing all of them: after 4 days the 7 tables held 65 GB
-- of metadata (the newest file 6.6 MB) against 250 MB of data, the catalog
-- and object store ran out of memory, and the job stopped committing.
--   - expire snapshots: keep 1 hour of history (at least the last 10), so
--     time travel works for recent changes and the snapshot list stays short
--   - rewrite data files: compact the many small files (one per checkpoint
--     per table) every 10 commits or 10 minutes
--   - delete orphan files: files left behind by failed commits, once a day
--     old (well past any in-flight checkpoint)
-- Each table also keeps only its last 10 metadata.json files (see the
-- write.metadata.* properties below); expiring snapshots alone doesn't
-- delete those.
SET 'table.exec.iceberg.use-v2-sink' = 'true';
SET 'flink-maintenance.expire-snapshots.enabled' = 'true';
SET 'flink-maintenance.expire-snapshots.max-snapshot-age-seconds' = '3600';
SET 'flink-maintenance.expire-snapshots.retain-last' = '10';
SET 'flink-maintenance.rewrite.enabled' = 'true';
SET 'flink-maintenance.delete-orphan-files.enabled' = 'true';
SET 'flink-maintenance.delete-orphan-files.min-age-seconds' = '86400';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://objectstore:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '${S3_ACCESS_KEY}',
  's3.secret-access-key' = '${S3_SECRET_KEY}',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.fleet;

-- Each Iceberg table has the topic's columns (including injection_id where
-- the topic has one) plus the record's Kafka coordinates. Delivery is
-- at-least-once, so a Flink restart can copy a few records twice;
-- (kafka_partition, kafka_offset) identifies duplicates.
--
-- The WITH options are Iceberg table properties, so they only take effect
-- when a table is created - `make reset` applies changes to them.

CREATE TABLE IF NOT EXISTS lake.fleet.maintenance_alerts (
  vehicle_id STRING,
  geozone STRING,
  engine_temp_c DOUBLE,
  severity STRING,
  message STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.driver_risk_events (
  vehicle_id STRING,
  geozone STRING,
  speed_kmh DOUBLE,
  overspeed_status STRING,
  harsh_braking_score DOUBLE,
  risk_level STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.driver_risk_events_anomaly (
  vehicle_id STRING,
  geozone STRING,
  speed_kmh DOUBLE,
  forecast_speed_kmh DOUBLE,
  lower_bound DOUBLE,
  upper_bound DOUBLE,
  risk_level STRING,
  event_time STRING,
  injection_id STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.delivery_performance_events (
  vehicle_id STRING,
  geozone STRING,
  parcels_onboard INT,
  parcels_delivered_last_hour INT,
  performance_status STRING,
  event_time STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.ai_maintenance_recommendations (
  vehicle_id STRING,
  geozone STRING,
  severity STRING,
  trigger_event STRING,
  recommendation STRING,
  injection_id STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.ai_safety_recommendations (
  vehicle_id STRING,
  geozone STRING,
  risk_level STRING,
  speed_kmh DOUBLE,
  recommendation STRING,
  injection_id STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TABLE IF NOT EXISTS lake.fleet.ai_delivery_recommendations (
  vehicle_id STRING,
  geozone STRING,
  performance_status STRING,
  parcels_onboard INT,
  recommendation STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6)
) WITH (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO lake.fleet.maintenance_alerts
SELECT vehicle_id, geozone, engine_temp_c, severity, message, event_time,
       injection_id, kafka_partition, kafka_offset, kafka_timestamp
FROM `maintenance.alerts`;

INSERT INTO lake.fleet.driver_risk_events
SELECT vehicle_id, geozone, speed_kmh, overspeed_status, harsh_braking_score, risk_level, event_time,
       injection_id, kafka_partition, kafka_offset, kafka_timestamp
FROM `driver.risk.events`;

INSERT INTO lake.fleet.driver_risk_events_anomaly
SELECT vehicle_id, geozone, speed_kmh, forecast_speed_kmh, lower_bound, upper_bound, risk_level, event_time,
       injection_id, kafka_partition, kafka_offset, kafka_timestamp
FROM `driver.risk.events.anomaly`;

INSERT INTO lake.fleet.delivery_performance_events
SELECT vehicle_id, geozone, parcels_onboard, parcels_delivered_last_hour, performance_status, event_time,
       kafka_partition, kafka_offset, kafka_timestamp
FROM `delivery.performance.events`;

INSERT INTO lake.fleet.ai_maintenance_recommendations
SELECT vehicle_id, geozone, severity, trigger_event, recommendation,
       injection_id, kafka_partition, kafka_offset, kafka_timestamp
FROM `ai.maintenance.recommendations`;

INSERT INTO lake.fleet.ai_safety_recommendations
SELECT vehicle_id, geozone, risk_level, speed_kmh, recommendation,
       injection_id, kafka_partition, kafka_offset, kafka_timestamp
FROM `ai.safety.recommendations`;

INSERT INTO lake.fleet.ai_delivery_recommendations
SELECT vehicle_id, geozone, performance_status, parcels_onboard, recommendation,
       kafka_partition, kafka_offset, kafka_timestamp
FROM `ai.delivery.recommendations`;

END;
