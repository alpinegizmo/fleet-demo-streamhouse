// kafka-client.js
//
// The one place this process talks to Kafka for writes and admin: the
// telemetry simulator, the seed script, the demo-injection HTTP endpoints
// (/api/inject-*) and the reset button all go through here. Reading is
// handled separately by context-engine.js's background consumer, which
// shares the same client via getKafka().
//
// Records are plain JSON - no schema registry, no wire-format header. The
// schema contract for each topic lives in the Flink table DDL instead
// (local/flink/sql/), so a record whose shape doesn't match is skipped by
// Flink rather than rejected at produce time.
//
// Configured via KAFKA_BOOTSTRAP (e.g. kafka:9092 inside docker compose,
// localhost:9094 from the host).

import { Kafka, logLevel } from 'kafkajs';

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

export async function produceRecords(topic, records) {
  const producer = await getProducer();
  return producer.send({
    topic,
    messages: records.map((r) => ({ key: recordKey(r), value: JSON.stringify(r) })),
  });
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
