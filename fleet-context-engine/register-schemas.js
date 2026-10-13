// register-schemas.js
//
// One-shot (the schema-init service in docker-compose.yml): registers
// schemas/<topic>.avsc as the value schema of every topic, then makes the
// registry read-only. This is the only thing that ever writes to the
// registry - producers never auto-register, and Flink is given the same
// schema text, so its registration calls just resolve the existing ID (and a
// drifted copy is refused by the read-only registry instead of silently
// becoming a new version).
//
// Safe to re-run: an unchanged schema keeps its version, a compatible change
// becomes the next version, and an incompatible change (under FULL
// compatibility) exits non-zero - so nothing that depends on schema-init
// starts, and the registry keeps the old version.
//
// Configured via SCHEMA_REGISTRY_URL (e.g. http://schema-registry:8081).

import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ALL_TOPICS } from './kafka-client.js';

const REGISTRY = process.env.SCHEMA_REGISTRY_URL;
const SCHEMA_DIR = fileURLToPath(new URL('../schemas/', import.meta.url));

// FULL: a new version must be readable by the old schema and vice versa.
// Flink jobs read with the schema they were submitted with, and producers
// and jobs are redeployed independently, in either order.
const COMPATIBILITY = 'FULL';

async function registry(method, path, body) {
  const resp = await fetch(`${REGISTRY}${path}`, {
    method,
    headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
    body: body && JSON.stringify(body),
  });
  const json = await resp.json().catch(() => ({}));
  return { status: resp.status, json };
}

// For settings that must take effect: anything but 200 is fatal.
async function put(path, body) {
  const { status, json } = await registry('PUT', path, body);
  if (status !== 200) throw new Error(`PUT ${path} -> HTTP ${status} ${json.message ?? ''}`);
}

async function latestVersion(subject) {
  const { status, json } = await registry('GET', `/subjects/${subject}/versions/latest`);
  return status === 200 ? json.version : null;
}

async function main() {
  if (!REGISTRY) throw new Error('Missing SCHEMA_REGISTRY_URL (e.g. http://schema-registry:8081).');

  // Every topic needs exactly one schema file, and every file one topic.
  const files = readdirSync(SCHEMA_DIR).filter((f) => f.endsWith('.avsc')).map((f) => f.slice(0, -5));
  const missing = ALL_TOPICS.filter((t) => !files.includes(t));
  const extra = files.filter((f) => !ALL_TOPICS.includes(f));
  if (missing.length || extra.length) {
    throw new Error(`schemas/ doesn't match ALL_TOPICS - missing: [${missing}], unknown: [${extra}]`);
  }

  await put('/mode', { mode: 'READWRITE' });
  await put('/config', { compatibility: COMPATIBILITY });

  let failed = 0;
  for (const topic of ALL_TOPICS) {
    const subject = `${topic}-value`;
    const schema = readFileSync(`${SCHEMA_DIR}${topic}.avsc`, 'utf8');
    const before = await latestVersion(subject);
    const { status, json } = await registry('POST', `/subjects/${subject}/versions`, { schemaType: 'AVRO', schema });
    if (status !== 200) {
      failed++;
      console.error(`[schema-init] ${subject}: REJECTED (HTTP ${status}) ${json.message ?? ''}`);
      continue;
    }
    const after = await latestVersion(subject);
    const what = before === null ? 'registered' : after === before ? 'unchanged' : `new version (was v${before})`;
    console.error(`[schema-init] ${subject}: ${what} - v${after}, id ${json.id}`);
  }
  if (failed) {
    throw new Error(`${failed} schema(s) rejected; the registry is left read-write until a run succeeds`);
  }

  await put('/mode', { mode: 'READONLY' });
  console.error(`[schema-init] ${ALL_TOPICS.length} subjects, compatibility ${COMPATIBILITY}, registry now READONLY`);
}

main().catch((err) => {
  console.error('[schema-init] failed:', err.message);
  process.exit(1);
});
