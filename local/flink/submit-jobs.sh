#!/usr/bin/env bash
# Submits each SQL job in /opt/flink/sql (every file except 00-tables.sql,
# in filename order) to the Flink cluster, skipping any job that's already
# running - so re-running `docker compose up` doesn't start duplicates.
#
# A job's name comes from its filename: 10-business.sql -> fleet-business.
# That name is also its Kafka consumer group (see 00-tables.sql), so a
# resubmitted job resumes from the offsets it last committed instead of
# reprocessing every topic from the beginning.
#
# After editing a job's SQL, replace the running copy:
#   docker compose run --rm flink-sql-init submit-jobs.sh --replace [name...]
# e.g. `--replace ai` for 20-ai.sql only (no names = every job). That
# cancels the running job (its state, e.g. OVER-window history, is
# dropped) and submits the new version, which resumes from the last
# checkpoint's offsets.

set -euo pipefail

REPLACE=false
REPLACE_ONLY=()   # short names (business, ai, ...); empty = all jobs
if [[ "${1:-}" == "--replace" ]]; then
  REPLACE=true
  shift
  REPLACE_ONLY=("$@")
fi

should_replace() {
  $REPLACE || return 1
  [[ ${#REPLACE_ONLY[@]} -eq 0 ]] && return 0
  local n
  for n in "${REPLACE_ONLY[@]}"; do [[ "fleet-$n" == "$1" ]] && return 0; done
  return 1
}

SQL_DIR=/opt/flink/sql
REST="http://${FLINK_JOBMANAGER:-flink-jobmanager}:8081"

# SCHEMA_<TOPIC> for each table's 'avro-confluent.schema' (see schema-env.sh).
source /opt/flink/bin/schema-env.sh

ACTIVE_STATES='["CREATED", "INITIALIZING", "RUNNING", "RESTARTING"]'

running_jobs() {
  curl -sf "$REST/jobs/overview" \
    | jq -r --argjson active "$ACTIVE_STATES" '.jobs[] | select(.state | IN($active[])) | .name'
}

cancel_job() {
  local jid
  for jid in $(curl -sf "$REST/jobs/overview" \
      | jq -r --arg name "$1" --argjson active "$ACTIVE_STATES" '.jobs[] | select(.name == $name and (.state | IN($active[]))) | .jid'); do
    curl -sf -X PATCH "$REST/jobs/$jid?mode=cancel" > /dev/null
    until curl -sf "$REST/jobs/$jid" | jq -e '.state == "CANCELED"' > /dev/null; do sleep 1; done
    echo "$1: cancelled $jid"
  done
}

until curl -sf "$REST/overview" > /dev/null; do
  echo "waiting for the Flink jobmanager at $REST..."
  sleep 2
done

for job in "$SQL_DIR"/*.sql; do
  [[ "$(basename "$job")" == 00-* ]] && continue
  name="fleet-$(basename "$job" .sql | sed 's/^[0-9]*-//')"

  if running_jobs | grep -qx "$name"; then
    if should_replace "$name"; then
      cancel_job "$name"
    else
      echo "$name: already running, skipping (use --replace to resubmit)"
      continue
    fi
  fi

  echo "$name: submitting $(basename "$job")"
  export PIPELINE="$name"
  envsubst "\${PIPELINE} \${SCHEMA_REGISTRY_URL}$SCHEMA_VARS" < "$SQL_DIR/00-tables.sql" > "/tmp/$name-init.sql"
  envsubst '${PIPELINE} ${LLM_BASE_URL} ${LLM_MODEL} ${S3_ACCESS_KEY} ${S3_SECRET_KEY}' < "$job" > "/tmp/$name.sql"
  /opt/flink/bin/sql-client.sh -i "/tmp/$name-init.sql" -f "/tmp/$name.sql"
done

echo "running jobs:"
running_jobs
