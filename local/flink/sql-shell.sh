#!/usr/bin/env bash
# Interactive Flink SQL client with all 11 topic tables already defined
# (local/flink/sql/00-tables.sql), for exploring the pipeline by hand:
#
#   make sql        (or: docker compose run --rm flink-sql-init sql-shell.sh)
#
#   Flink SQL> SELECT vehicle_id, risk_level, speed_kmh FROM `driver.risk.events`;
#
# Queries run as real Flink jobs on the cluster. The shell uses its own Kafka
# consumer group, so it never disturbs the pipeline's jobs.

set -euo pipefail
export PIPELINE=fleet-sql-shell
envsubst '${PIPELINE}' < /opt/flink/sql/00-tables.sql > /tmp/sql-shell-init.sql
# Results as a scrolling table rather than the full-screen viewer, which
# needs a real terminal size.
echo "SET 'sql-client.execution.result-mode' = 'tableau';" >> /tmp/sql-shell-init.sql
exec /opt/flink/bin/sql-client.sh -i /tmp/sql-shell-init.sql "$@"
