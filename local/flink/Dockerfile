# Apache Flink 2.2 plus the connector jars this pipeline needs. Used for the
# jobmanager, the taskmanager, and the one-shot flink-sql-init service that
# submits the SQL jobs in sql/. docker-compose.yml mounts sql/ and
# submit-jobs.sh from the repo, so editing them needs no image rebuild
# (a rebuild recreates the jobmanager, which drops its running jobs).

FROM flink:2.2.1-java17

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka source/sink for every table in sql/00-tables.sql, and the OpenAI
# model provider behind CREATE MODEL / ML_PREDICT in sql/20-ai.sql (works
# with any OpenAI-compatible server, e.g. Ollama).
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
      ${MAVEN}/org/apache/flink/flink-model-openai/2.2.1/flink-model-openai-2.2.1.jar

# Checkpoint storage, on a named volume shared by jobmanager and taskmanager.
# jq is for submit-jobs.sh.
USER root
RUN mkdir -p /flink-data && chown flink:flink /flink-data \
 && apt-get update -q && apt-get install -yq --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
USER flink
