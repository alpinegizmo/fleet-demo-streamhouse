// query-store.js
//
// SQL over the live topics, for the MCP tools (mcp-server.js) and the
// console's chat agent (nl-query-agent.js) - the local stand-in for
// Confluent's Real-Time Context Engine.
//
// context-engine.js's Kafka consumer already sees every record on every
// topic; remember() keeps the most recent RECORDS_PER_TOPIC of each. A query
// gets a fresh in-memory DuckDB database with those records loaded as one
// table per topic (typed by topic-schemas.js), so it's full SQL - joins,
// aggregates, window functions - over the current state of the stream.
//
// The SQL comes from a language model or any MCP client, so each database
// is locked down before the query runs: no file or network access, settings
// frozen, memory and time capped, and the database is thrown away
// afterwards. The worst a query can do is read fleet data.

import { DuckDBInstance } from '@duckdb/node-api';
import { TOPICS } from './topic-schemas.js';

const RECORDS_PER_TOPIC = 2000;
const QUERY_TIMEOUT_MS = 10000;

const buffers = new Map(Object.keys(TOPICS).map((topic) => [topic, []]));

// Called by context-engine.js for every consumed record.
export function remember(topic, record, kafkaTimestampMs) {
  const buffer = buffers.get(topic);
  if (!buffer) return;
  buffer.push({ ...record, kafka_timestamp: new Date(Number(kafkaTimestampMs)).toISOString() });
  if (buffer.length > RECORDS_PER_TOPIC) buffer.splice(0, buffer.length - RECORDS_PER_TOPIC);
}

// A topic's table can be queried by its topic name ("vehicle.telemetry",
// double-quoted because of the dots) or by this alias (vehicle_telemetry),
// which is easier for small models to get right.
export function tableAlias(topic) {
  return topic.replaceAll('.', '_');
}

export function recordCount(topic) {
  return buffers.get(topic)?.length ?? 0;
}

export async function runQuery(sql, maxRows = 50) {
  const instance = await DuckDBInstance.create(':memory:', { threads: '2', memory_limit: '256MB' });
  const conn = await instance.connect();
  const timer = setTimeout(() => conn.interrupt(), QUERY_TIMEOUT_MS);
  try {
    await conn.run("SET TimeZone = 'UTC'");
    await conn.run('SET enable_external_access = false');
    await conn.run('SET lock_configuration = true');

    for (const [topic, { schema }] of Object.entries(TOPICS)) {
      const structure = JSON.stringify([{ ...schema, kafka_timestamp: 'TIMESTAMPTZ' }]);
      const load = await conn.prepare(`CREATE TABLE "${topic}" AS SELECT unnest(from_json($1::JSON, $2), max_depth := 2)`);
      load.bindVarchar(1, JSON.stringify(buffers.get(topic)));
      load.bindVarchar(2, structure);
      await load.run();
      await conn.run(`CREATE VIEW ${tableAlias(topic)} AS SELECT * FROM "${topic}"`);
    }

    const result = await conn.streamAndReadUntil(sql, maxRows);
    const rows = result.getRowObjectsJson().slice(0, maxRows);
    return { columns: result.columnNames(), rows, rowCount: rows.length, truncated: rows.length === maxRows };
  } finally {
    clearTimeout(timer);
    conn.closeSync();
    instance.closeSync();
  }
}
