# Apache Flink 2.2 plus the connector jars this pipeline needs. Used for the
# jobmanager, the taskmanager, and the one-shot flink-sql-init service that
# submits the SQL jobs in sql/. docker-compose.yml mounts sql/ and
# submit-jobs.sh from the repo, so editing them needs no image rebuild
# (a rebuild recreates the jobmanager, which drops its running jobs).

FROM flink:2.2.1-java17

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka source/sink for every table in sql/00-tables.sql.
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

# The OpenAI model provider behind CREATE MODEL / ML_PREDICT in
# sql/20-ai.sql (works with any OpenAI-compatible server, e.g. Ollama).
# Deliberately in opt/, not lib/: it bundles an older, unrelocated Apache
# HttpClient 5 that breaks the AWS SDK's S3 client (iceberg-aws-bundle,
# below) when both share the cluster classpath. 20-ai.sql loads it with
# ADD JAR instead, into that job's own classloader.
RUN wget -q -P /opt/flink/opt \
      ${MAVEN}/org/apache/flink/flink-model-openai/2.2.1/flink-model-openai-2.2.1.jar

# Iceberg sink for sql/30-lakehouse.sql: the Flink runtime, the AWS bundle
# (S3FileIO, for the S3-compatible object store), and the Hadoop client
# classes Iceberg's Flink catalog still loads even with a REST catalog.
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar \
      ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar

# Checkpoint storage, on a named volume shared by jobmanager and taskmanager.
# jq is for submit-jobs.sh.
USER root
RUN mkdir -p /flink-data && chown flink:flink /flink-data \
 && apt-get update -q && apt-get install -yq --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
USER flink
