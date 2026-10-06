-- Attaches the Iceberg REST catalog as `lake`, so the tables Flink writes
-- (local/flink/sql/30-lakehouse.sql) can be queried directly:
--
--   SHOW ALL TABLES;
--   SELECT risk_level, count(*) FROM lake.fleet.driver_risk_events GROUP BY ALL;
--
-- DuckDB reads each table's current snapshot, which advances every Flink
-- checkpoint (30 s). Credentials match the objectstore service in
-- docker-compose.yml.

.bail on
LOAD iceberg;
LOAD httpfs;

CREATE SECRET lake_s3 (
  TYPE s3,
  KEY_ID 'fleet',
  SECRET 'fleet-secret',
  ENDPOINT 'objectstore:9000',
  URL_STYLE 'path',
  USE_SSL false,
  REGION 'us-east-1'
);

ATTACH 'warehouse' AS lake (
  TYPE iceberg,
  ENDPOINT 'http://iceberg-rest:8181',
  AUTHORIZATION_TYPE 'none'
);
.bail off
