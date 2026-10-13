# Sourced by submit-jobs.sh and sql-shell.sh. For every schemas/<topic>.avsc
# (mounted at /opt/flink/schemas), exports SCHEMA_<TOPIC> - e.g.
# SCHEMA_VEHICLE_TELEMETRY - holding that schema as one line of JSON, ready
# for the 'avro-confluent.schema' option of its table in 00-tables.sql, and
# lists them in SCHEMA_VARS for envsubst.
#
# Flink is handed the exact schema schema-init registered, rather than
# deriving one from the table's columns, for two reasons: Flink's Avro sink
# registers its schema on every write, and with the same schema that only
# resolves the existing ID (the registry is read-only - a schema that has
# drifted fails the job instead of quietly becoming a new version); and the
# record and field names stay the ones in schemas/, not Flink's generated
# ones.
#
# jq -a writes non-ASCII characters (the anomaly schema's "± 3σ") as \u
# escapes, so the schema survives whatever encoding the SQL client reads the
# file in; the registry compares parsed schemas, so escapes don't count as a
# change. Single quotes are doubled for the SQL string literal.

SCHEMA_VARS=''
for avsc in /opt/flink/schemas/*.avsc; do
  topic=$(basename "$avsc" .avsc)
  var="SCHEMA_$(tr '[:lower:].' '[:upper:]_' <<< "$topic")"
  export "$var=$(jq -c -a . "$avsc" | sed "s/'/''/g")"
  SCHEMA_VARS+=" \${$var}"
done
