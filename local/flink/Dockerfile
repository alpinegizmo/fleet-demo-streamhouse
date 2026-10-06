# Apache Flink 2.2 plus the connector jars this pipeline needs. Used for the
# jobmanager, the taskmanager, and the one-shot flink-sql-init service that
# submits the SQL jobs in sql/ (see submit-jobs.sh and docker-compose.yml).

FROM flink:2.2.1-java17

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka source/sink for every table in sql/00-tables.sql.
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

# Checkpoint storage, on a named volume shared by jobmanager and taskmanager.
# jq is for submit-jobs.sh.
USER root
RUN mkdir -p /flink-data && chown flink:flink /flink-data \
 && apt-get update -q && apt-get install -yq --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
USER flink

COPY submit-jobs.sh /opt/flink/bin/submit-jobs.sh
