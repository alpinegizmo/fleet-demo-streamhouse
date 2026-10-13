// kafka-client.js
//
// The one place this process talks to Kafka for writes and admin: the
// telemetry simulator, the seed script, the demo-injection HTTP endpoints
// (/api/inject-*) and the reset button all go through here. Reading is
// handled separately by context-engine.js's background consumer, which
// shares the same client via getKafka() and decodes with decodeValue().
//
// Record values are Avro in Confluent's wire format (a 0 byte, the 4-byte
// schema ID, then the Avro body), checked against the topic's registered
// schema (schemas/<topic>.avsc, registered by schema-init) as they're
// written: a record that doesn't fit is rejected here, before it reaches the
// topic. Two things to know about that check: fields the schema doesn't
// have are dropped silently, not rejected (so a misspelled field name just
// vanishes), and only clients that go through a serializer are checked at
// all - Apache Kafka itself accepts any bytes. This process never registers
// schemas (autoRegisterSchemas: false): it uses the latest registered
// version, and a topic with none fails.
//
// Configured via KAFKA_BOOTSTRAP (e.g. kafka:9092 inside docker compose,
// localhost:9094 from the host) and SCHEMA_REGISTRY_URL
// (http://schema-registry:8081 inside compose, http://localhost:8085 from
// the host).

import { Kafka, logLevel } from 'kafkajs';
import { AvroDeserializer, AvroSerializer, SchemaRegistryClient, SerdeType, SerializationError } from '@confluentinc/schemaregistry';

// Every topic in the pipeline, in pipeline order. Created up front by the
// kafka-init service in docker-compose.yml (auto-create is off on the
// broker), so this list and that one must stay in sync.
export const ALL_TOPICS = [
  'vehicle.telemetry', 'weather.conditions', 'traffic.incidents', 'parcel.volume',
  'maintenance.alerts', 'driver.risk.events', 'driver.risk.events.anomaly', 'delivery.performance.events',
  'ai.maintenance.recommendations', 'ai.safety.recommendations', 'ai.delivery.recommendations',
];

let kafka = null;
let producerPromise = null;

export function getKafka() {
  if (!process.env.KAFKA_BOOTSTRAP) {
    throw new Error('Missing KAFKA_BOOTSTRAP environment variable (e.g. kafka:9092). See SETUP.md.');
  }
  if (!kafka) {
    kafka = new Kafka({
      clientId: 'fleet-context-engine',
      brokers: process.env.KAFKA_BOOTSTRAP.split(','),
      connectionTimeout: 10000,
      requestTimeout: 30000,
      retry: { retries: 8, initialRetryTime: 1000, maxRetryTime: 10000 },
      logLevel: logLevel.WARN,
    });
  }
  return kafka;
}

let serdes = null;

// One serializer and one deserializer serve every topic: the subject is
// derived from the topic (<topic>-value), and both cache schemas by subject
// and ID, so the registry is asked once per topic, not once per record.
function getSerdes() {
  if (!process.env.SCHEMA_REGISTRY_URL) {
    throw new Error('Missing SCHEMA_REGISTRY_URL environment variable (e.g. http://schema-registry:8081). See SETUP.md.');
  }
  if (!serdes) {
    const client = new SchemaRegistryClient({ baseURLs: [process.env.SCHEMA_REGISTRY_URL] });
    serdes = {
      serializer: new AvroSerializer(client, SerdeType.VALUE, { autoRegisterSchemas: false, useLatestVersion: true }),
      deserializer: new AvroDeserializer(client, SerdeType.VALUE, {}),
    };
  }
  return serdes;
}

// A record that doesn't fit its schema throws "<topic>: record rejected by
// its schema - Invalid message at <field>, expected ..., got ...". Anything
// else (registry unreachable, no schema registered) is passed on as is.
export async function encodeValue(topic, record) {
  const { serializer } = getSerdes();
  try {
    return await serializer.serialize(topic, record);
  } catch (err) {
    if (err instanceof SerializationError) throw new Error(`${topic}: record rejected by its schema - ${err.message}`);
    throw err;
  }
}

// Reads the writer's schema ID from the record and decodes with that schema
// (fetched from the registry once per ID).
export async function decodeValue(topic, buffer) {
  return getSerdes().deserializer.deserialize(topic, buffer);
}

function getProducer() {
  if (!producerPromise) {
    const producer = getKafka().producer({ allowAutoTopicCreation: false });
    producerPromise = producer.connect().then(() => producer);
  }
  return producerPromise;
}

// Keyed by vehicle (or geozone for the per-zone context topics) so every
// record for one vehicle lands on the same partition - Flink's per-vehicle
// OVER window and the consumer's latest-wins logic both rely on per-vehicle
// ordering.
function recordKey(record) {
  return record.object_id ?? record.vehicle_id ?? record.geozone ?? null;
}

// Serializes the whole batch before sending any of it, so one record that
// breaks the schema rejects the batch instead of half-producing it.
export async function produceRecords(topic, records) {
  const messages = await Promise.all(records.map(async (r) => ({ key: recordKey(r), value: await encodeValue(topic, r) })));
  const producer = await getProducer();
  return producer.send({ topic, messages });
}

export async function produceRecord(topic, record) {
  return produceRecords(topic, [record]);
}

// Hard-resets the pipeline's data: deletes every record in all topics up to
// the current high watermark (offset -1). Topics themselves, and the Flink
// jobs reading them, are untouched - they just see no new data until the
// simulator produces again.
export async function purgeAllTopics() {
  const admin = getKafka().admin();
  await admin.connect();
  try {
    const results = await Promise.allSettled(ALL_TOPICS.map(async (topic) => {
      const offsets = await admin.fetchTopicOffsets(topic);
      await admin.deleteTopicRecords({
        topic,
        partitions: offsets.map(({ partition }) => ({ partition, offset: '-1' })),
      });
    }));
    const failed = results
      .map((r, i) => ({ topic: ALL_TOPICS[i], r }))
      .filter(({ r }) => r.status === 'rejected')
      .map(({ topic, r }) => ({ topic, error: r.reason.message }));
    return { purged: ALL_TOPICS.length - failed.length, total: ALL_TOPICS.length, failed };
  } finally {
    await admin.disconnect();
  }
}
