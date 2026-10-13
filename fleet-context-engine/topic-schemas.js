// topic-schemas.js
//
// The shape of every topic, for the MCP tools (mcp-server.js), the chat
// agent's catalog (nl-query-agent.js) and the queryable copy of recent
// records behind them (query-store.js). Read at startup from the topics'
// Avro schemas in schemas/ - the same files schema-init registers - so
// there's no second copy to keep in sync.
//
// Each schema is turned into a DuckDB JSON "structure": a column maps to a
// DuckDB type, a nested record to a STRUCT, and an array to a one-element
// list. query-store passes it straight to DuckDB's from_json() to type the
// loaded records. A topic's description is its schema's top-level doc.
//
// Every topic also gets kafka_timestamp (when the record was written to
// Kafka), added by query-store.js - so questions like "in the last 5
// minutes" can be answered for topics whose own time field is a string.

import { readFileSync } from 'node:fs';
import { ALL_TOPICS } from './kafka-client.js';

const SCHEMA_DIR = new URL('../schemas/', import.meta.url);

// The Avro types schemas/ uses. A nullable field (["null", T]) maps to T:
// every DuckDB column is nullable.
const DUCKDB_TYPES = { string: 'VARCHAR', double: 'DOUBLE', int: 'INTEGER' };

function toDuckDB(type) {
  if (Array.isArray(type)) {
    const branches = type.filter((t) => t !== 'null');
    if (branches.length !== 1) throw new Error(`unsupported union ${JSON.stringify(type)}`);
    return toDuckDB(branches[0]);
  }
  if (typeof type === 'string') {
    if (!DUCKDB_TYPES[type]) throw new Error(`unsupported Avro type ${type}`);
    return DUCKDB_TYPES[type];
  }
  if (type.type === 'array') return [toDuckDB(type.items)];
  if (type.type === 'record') return Object.fromEntries(type.fields.map((f) => [f.name, toDuckDB(f.type)]));
  throw new Error(`unsupported Avro type ${JSON.stringify(type)}`);
}

// In pipeline order (ALL_TOPICS), which is the order listTopics shows.
export const TOPICS = Object.fromEntries(ALL_TOPICS.map((topic) => {
  const avro = JSON.parse(readFileSync(new URL(`${topic}.avsc`, SCHEMA_DIR), 'utf8'));
  return [topic, { description: avro.doc, schema: toDuckDB(avro) }];
}));

// Renders a schema as DuckDB-style column types, e.g.
// { position: 'STRUCT(speed DOUBLE, ...)', geozone_ids: 'VARCHAR[]' }.
export function describeColumns(schema) {
  const typeOf = (t) => {
    if (Array.isArray(t)) return `${typeOf(t[0])}[]`;
    if (typeof t === 'object') return `STRUCT(${Object.entries(t).map(([k, v]) => `${k} ${typeOf(v)}`).join(', ')})`;
    return t;
  };
  return Object.fromEntries(Object.entries(schema).map(([name, t]) => [name, typeOf(t)]));
}
