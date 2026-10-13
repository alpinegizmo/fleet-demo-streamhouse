# Plan: add Confluent Schema Registry to the local fleet demo

Replace plain JSON on all 11 topics with Avro records whose schemas live in
Confluent Schema Registry, so the contract is registered on purpose, checked
when a record is produced, and visible in the UIs. Today the Flink DDL is the
only contract.

## Decisions (from interview)

| Topic | Decision |
|---|---|
| Format | **Avro**, Confluent wire format (magic byte + 4-byte schema ID). Open-source Flink's only registry-aware format is `avro-confluent`. `json-registry` exists only on Confluent Cloud, and OSS Flink has no Confluent-protobuf format. |
| Source of truth | **One `.avsc` file per topic in `schemas/`** (all 11). A one-shot `schema-init` service registers them, then switches the registry to `READONLY`. Producers never auto-register. Flink is given the exact same schema text, so its registration calls only resolve the existing ID; if that text ever drifts, the read-only registry refuses it and the job fails, instead of a silent version 2 (verified in phase 0). |
| Demo scope | **Registry in the console and UIs**: Kafbat UI connected to the registry (decoded messages, subjects, versions), a top-bar link, and a Schema Registry box on Behind the Scenes. No bad-record button, no scripted evolution, MCP doesn't query the registry. |
| License | **Confluent Schema Registry (`cp-schema-registry`), and say so.** README and deck: every component is open source except Schema Registry, which is under the Confluent Community License (source-available, free to use). |
| Types | **Faithful port.** Same fields and types as `00-tables.sql`: timestamps stay ISO strings, statuses stay strings. Every field is nullable with `"default": null`, except each topic's key field, which is required. |
| MCP schemas | **`topic-schemas.js` is derived from `schemas/*.avsc` at startup** (Avro → DuckDB structure). The topic descriptions move into each schema's top-level `doc`. No runtime dependency on the registry. |
| Node serdes | **`@confluentinc/schemaregistry`**, standalone, on top of the existing kafkajs. `autoRegisterSchemas: false`, `useLatestVersion: true`. |
| Bad records / upgrade | **Reset on upgrade, fail loudly.** Switching to this branch needs `make reset`: old JSON records would fail the Avro jobs. `avro-confluent` has no ignore-parse-errors, so an undecodable record fails the job. That makes it a visible bug, not a silent skip. Document how to spot and recover from one. |

Smaller defaults I chose (say if you disagree):

- **Compatibility `FULL`, set globally** by `schema-init`. Flink reads with a fixed reader schema taken at submit time, and producers and jobs get redeployed independently in either order. `FULL` keeps both orders safe; `BACKWARD` (the registry default) only covers consumers upgrading first.
- **Keys stay `raw` strings**, not registered. They're plain IDs, and Kafbat shows them fine. Only the value subjects (`<topic>-value`, TopicNameStrategy) go in the registry.
- **Required field = the record key**, the same field `recordKey()` in `kafka-client.js` picks: `object_id` (telemetry), `geozone` (weather, traffic), `vehicle_id` (parcel volume and all 7 derived topics). The matching Flink columns become `STRING NOT NULL`.
- **Registry host port 8085.** Its default 8081 is already the Flink UI. Inside compose it stays `schema-registry:8081`.
- **Registry storage is the `_schemas` topic** on the existing broker (replication factor 1). `make reset` wipes it along with the Kafka volume, and `schema-init` re-registers everything on the next `up`. *Reset real pipeline* purges only `ALL_TOPICS`, so the schemas survive it.
- **Pin `@confluentinc/schemaregistry` to 1.10.1.** 1.11.0 is six days old.
- **Avro names:** namespace `fleet`, one PascalCase record per topic (`VehicleTelemetry`, `MaintenanceAlert`, …). Nested records are named explicitly (`Position`, `Tire`, …).

## Target architecture (changes only)

```
 simulator / seed / inject ──► AvroSerializer (latest registered version, never registers)
           │                              │ GET /subjects/<topic>-value/versions/latest (cached)
           ▼                              ▼
         kafka  ◄──── _schemas topic ── schema-registry :8081 (host :8085) ◄── schema-init (one-shot)
           │                              ▲          ▲                         registers schemas/*.avsc,
           ▼                              │          │                         sets compatibility FULL
   flink (avro-confluent, schema text     │       kafka-ui (Kafbat): subjects, versions, decoded messages
          inlined from schemas/*.avsc) ───┘
           │
           ▼
   iceberg (unchanged: Flink writes Iceberg from table columns, not from Avro)
```

New and changed compose services:

| Service | Change |
|---|---|
| `schema-registry` | new: `confluentinc/cp-schema-registry:8.2.4`. `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092`, `..._KAFKASTORE_TOPIC_REPLICATION_FACTOR=1`, host port `8085:8081`, healthcheck on `/subjects`. Depends on `kafka` healthy. |
| `schema-init` | new one-shot on the app image: `node register-schemas.js`. `PUT /mode {"mode":"READWRITE"}`, `PUT /config {"compatibility":"FULL"}`, `POST /subjects/<topic>-value/versions` for each `.avsc`, then `PUT /mode {"mode":"READONLY"}`. Idempotent: an identical schema returns its existing ID. A changed but compatible schema becomes a new version. An incompatible one exits non-zero, so nothing downstream starts (and the registry is left read-write until the next successful run, which is acceptable: nothing else is running). |
| `seed`, `fleet-app`, `flink-sql-init` | also depend on `schema-init` completing successfully. Get `SCHEMA_REGISTRY_URL=http://schema-registry:8081`. |
| `x-flink` | mount `./schemas:/opt/flink/schemas:ro`. |
| `kafka-ui` | `KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081`. It stays read-only, which also covers schemas. |

## Schemas (`schemas/`)

- 11 files named after the topic: `vehicle.telemetry.avsc`, …, `ai.delivery.recommendations.avsc`. Each is a faithful transcription of its `CREATE TABLE` in `00-tables.sql`, minus the computed `event_ts`, the watermark and the `kafka_*` VIRTUAL metadata columns, none of which are in the value.
- Nullable fields: `["null", T]` with `"default": null`. That covers nested `ROW`s, the `tires` array and its elements, `geozone_ids` and its elements, and `injection_id`. This matches how Flink maps a nullable column. `doc` strings carry today's `topic-schemas.js` descriptions and the field notes from the DDL comments (e.g. `injection_id`).
- Flink type mapping for the transcription: `STRING`→`string`, `DOUBLE`→`double`, `INT`→`int`, `ROW<…>`→`record`, `ARRAY<…>`→`array`.

## `fleet-context-engine` changes

| File | Change |
|---|---|
| `package.json` | add `@confluentinc/schemaregistry@1.10.1`. |
| `kafka-client.js` | One `SchemaRegistryClient` plus one cached `AvroSerializer` per topic (`SerdeType.VALUE`, `autoRegisterSchemas: false`, `useLatestVersion: true`). `produceRecords` awaits `serialize(topic, record)` for each value. A record that doesn't match the schema throws there, so the simulator/seed/inject path logs a clear "rejected by schema" error instead of producing. Update the header comment ("Records are plain JSON…"). |
| `context-engine.js` | `decodeValue` becomes async `AvroDeserializer.deserialize(topic, buffer)` (reads the writer schema by ID from the header, cached). The consumer's `eachMessage` already awaits. Check that the decoded records still look like today's: unions unwrapped to plain values, `null`s where JSON had missing fields. |
| `topic-schemas.js` | Replace the hand-written `TOPICS` with a loader over `schemas/*.avsc` that maps Avro to the DuckDB structure (`string`→`VARCHAR`, `double`→`DOUBLE`, `int`→`INTEGER`, record→struct, array→`[T]`, `["null",T]`→T) and takes `description` from the schema's `doc`. `describeColumns` and the exported shape stay the same, so `query-store.js`, `mcp-server.js` and `nl-query-agent.js` don't change. |
| `register-schemas.js` | new: the `schema-init` entry point described above. |
| `seed.js`, `telemetry-simulator.js` | no logic change; they go through `produceRecords`. Check that every `int` field gets an integer (most already go through `Math.round`/`Math.floor`). |
| `Dockerfile` / `.dockerignore` | copy `schemas/` into the app image. |

## Flink changes

- `local/flink/Dockerfile`: add `flink-sql-avro-confluent-registry-2.2.1.jar` to **`opt/`, not `lib/`**, and load it with `ADD JAR` at the top of `00-tables.sql`, which every job and the SQL shell load as their init script. Phase 0 found it bundles an unrelocated zstd-jni 1.5.5-1. In `lib/` it sorts ahead of `iceberg-flink-runtime` (zstd-jni 1.5.7-3), so Iceberg's Parquet writer ran on the older native library. It happened to work, but it would break silently on an Iceberg upgrade. With `ADD JAR`, Iceberg keeps its own 1.5.7. Same pattern and reasoning as the OpenAI jar, and the Dockerfile comment should say so.
- `00-tables.sql`, for every table:
  - plain-format tables: `'format' = 'json'` + `'json.ignore-parse-errors'` become `'format' = 'avro-confluent'`, `'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'`, `'avro-confluent.schema' = '${SCHEMA_<TOPIC>}'`;
  - keyed tables: same, with the `value.` prefix (`'value.format' = 'avro-confluent'`, `'value.avro-confluent.url'`, `'value.avro-confluent.schema'`);
  - key columns `STRING NOT NULL`.

  The header comment ("These DDL statements ARE the schema contract…") is rewritten: the contract is now `schemas/`, and the DDL must match it.
- `submit-jobs.sh` and `sql-shell.sh`: before `envsubst`, export one variable per `.avsc` (`SCHEMA_VEHICLE_TELEMETRY`, …), each `jq -c`-minified with `'` doubled for the SQL string literal, and add them to the `envsubst` variable list. Factor this into a small sourced helper so the two scripts can't drift.
- `10-business.sql`, `20-ai.sql`, `30-lakehouse.sql`: no change expected. Iceberg tables keep their columns, and old Iceberg rows remain valid.

## Console and docs

- Console: a `schemaRegistry` entry in the top-bar tool links (Kafbat's `/ui/clusters/fleet/schemas`). A **static** Schema Registry box on Behind the Scenes, linked to the producers and Flink, which opens the same page. Static, because serializers cache schema IDs, so there's no per-record registry traffic to animate honestly. Its subtitle says that.
- README: the services table (new `schema-registry`, `schema-init`; Kafka row no longer says "plain JSON, no schema registry"). The pipeline section's "All table definitions … *is* the schema contract" paragraph. The Confluent Cloud mapping row becomes "Schema Registry + `avro-confluent`, schemas in `schemas/` ↔ Confluent Cloud Schema Registry + `avro-registry`". The license sentence. New **Known limitations**:
  - **No broker-side enforcement.** Apache Kafka accepts any bytes; only clients that use the serializer are checked. Confluent Server and Confluent Cloud can validate schema IDs at the broker. This is a good bridge for the course's Confluent Cloud pivot.
  - **A poison pill fails the job, forever.** With checkpointing on, Flink restarts it without limit (13 failures in ~4 min in phase 0). The console just goes quiet; the Flink UI shows the job RESTARTING with root cause `Unknown data format. Magic number does not match`. Recovery: delete records up to and past it (`kafka-delete-records.sh`) or `make reset`.
  - **Unknown fields are dropped silently.** The Avro serializer encodes only the schema's fields, so a misspelled field (`injectionId`) disappears without an error. Wrong types, a float in an `int`, and a missing or null key *are* rejected.
  - `make reset` is required when coming from the JSON version.
- Troubleshooting: "`schema-init` exited 1" means an incompatible schema change. Show how to see the reason (`docker compose logs schema-init`). A Flink job restarting with "Failed to deserialize consumer record" at the top can be a *write* failure: if a cause further down says `Could not register schema` / `read-only mode; error code: 42205`, the job's inlined schema no longer matches the registered one.
- `fleet-intelligence-deck.html` (the "Schemas" row and the Flink SQL bullet), `docs/architecture-diagram.html` plus a regenerated `.png`, `confluent-fleet-eda/SKILL.md` and `references/{confluent-cloud-mapping,flink-pipeline,mcp-context-engine}.md`, then repackage `confluent-fleet-eda.skill`.

## Phases

0. **Spike (throwaway).** ✅ Done 2026-10-13: see *Phase 0 results* below. Ran beside the live stack on scratch `spike.*` topics, with real records sampled from `weather.conditions` and `vehicle.telemetry` (the nested one) rather than weather alone.
1. **Registry and schemas.** ✅ Done 2026-10-13. `schema-registry`, `schema-init`, `register-schemas.js` (plain `fetch`, no new dependency), all 11 `.avsc` files. Kafbat lists 11 subjects at version 1 with compatibility `FULL`, and the registry is `READONLY`. The `.avsc` files were checked mechanically against `topic-schemas.js`: same columns, types, field order and descriptions. An unchanged re-run is a no-op; an incompatible change (`temp_c` from double to string) is rejected with HTTP 409 and the field path, `schema-init` exits 1, and the subject stays at v1. `fleet-app` is gated through `seed`, so only `seed` and `flink-sql-init` depend on `schema-init` directly.
2. **Node.** ✅ Done 2026-10-13, deployed with phase 3 (on its own it can't run: the app would write Avro while Flink still read JSON). Serdes in `kafka-client.js` (`encodeValue`/`decodeValue`, one serializer and deserializer for all topics) and `context-engine.js`; `topic-schemas.js` generated from `schemas/`. Checks: the generated `TOPICS` is identical to the hand-written one, order included, so `listTopics`, `getMetadata` and the chat agent's catalog are unchanged. All 4,932 real records sampled from the 11 live JSON topics round-trip through the live registry unchanged, and none has a field outside its schema. Bad records (float in an `int`, missing key, wrong type) are rejected before sending; a batch with one bad record writes nothing (end offset checked). Only `SerializationError`s are labelled "record rejected by its schema"; a missing `SCHEMA_REGISTRY_URL` or an unreachable registry say so. The consumer logs undecodable records (first per topic, then every 100th) and skips them. Decoded records are instances of generated classes (`WeatherCondition`, …), with the same values as plain objects; nothing in the app depends on the difference.
   - Cost: `@confluentinc/schemaregistry` takes the app image from 466 MB to 811 MB (250 packages: KMS clients for AWS/Azure/GCP and Vault, CEL, Protobuf, JSON Schema, none of them used). `@kafkajs/confluent-schema-registry` would be ~12 MB. Kept the official one (decided 2026-10-13).
3. **Flink.** ✅ Done 2026-10-13, from `make reset`. Jar in `opt/` with `ADD JAR`, all 11 tables on `avro-confluent` with their schema inlined and the key `NOT NULL`, `schema-env.sh` sourced by `submit-jobs.sh` and `sql-shell.sh`. The inlined JSON goes through `jq -a`, so the anomaly schema's `± 3σ` arrive as `\u` escapes whatever encoding the SQL client reads with (the JVM turned out to be UTF-8 anyway). Before the reset, `EXPLAIN STATEMENT SET` planned all three jobs with the new tables without submitting anything. After it:
   - all three jobs RUNNING, 21 checkpoints each and no failures after ~10 minutes; every subject still at version 1 and the registry still `READONLY`, so Flink registered nothing;
   - the console fills (12 vehicles, recommendations, all business events) and `fleet-app` logs no undecodable records or rejected produces;
   - inject engine spike → maintenance alert (107 °C, high) → AI recommendation with the same `injection_id` in 0.6 s; inject speed anomaly → both the per-vehicle anomaly (150 vs a 44.6 km/h baseline) and the fixed rule → AI safety recommendation, all with its `injection_id`;
   - MCP `queryData`, `make duckdb` (Iceberg, compacting as before) and `make sql` all return the injected rows. Note for anyone testing `make sql` by script: `LIMIT` doesn't end a streaming query on Kafka; use a `scan.bounded.mode` hint.
   - Kafbat needed `KAFKA_CLUSTERS_0_DEFAULTVALUESERDE: SchemaRegistry`: it lists the registry serde but prefers `String`, which shows Avro as bytes. It shows nullable fields in Avro's JSON encoding (`"geozone": {"string": "essendon-fields"}`).
4. **Console.** ✅ Done 2026-10-13. A **Schemas ↗** top-bar link to Kafbat's `/ui/clusters/fleet/schemas`. On Behind the Scenes, a wide Schema Registry box under the Kafka → Serve row, inside the governance band (which now reaches down to cover it), opening the same page. Dashed, never-animated stubs connect it to Sources, both Flink jobs and Serve, and deliberately not to Kafka: the broker checks nothing. Its subtitle, "11 Avro schemas · read-only · fetched once, not per record", stays visible at every width, unlike the other boxes' captions, which hide below ~1100 px. The tooltip explains why it's static. Checked in headless Chrome at 1024, 1280 and 1600 px.
5. **Docs and skill.** ✅ Done 2026-10-13.
   - **README:** the intro now names the license exception; services table (`schema-registry`, `schema-init`, Kafbat's registry view); a **Schemas** section in place of "the DDL *is* the contract", including how to change a schema; the console's new links; the Confluent Cloud mapping row; the four Known limitations; two Troubleshooting entries; `schemas/` in the layout.
   - **Deck:** the building-blocks Flink bullet, the "run it yourself" lede (open source except the registry) and the Confluent Cloud "Schemas" row.
   - **Diagram:** a Schema Registry box in place of the "plain JSON, no registry" note, and the registry in the compose label. The PNG was regenerated (headless Chrome, 1920×830).
   - **Skill:** `SKILL.md` and the three references updated, then `confluent-fleet-eda.skill` repackaged. Only the `.skill` is tracked: `confluent-fleet-eda/` is gitignored.

## Verification (end of phase 3, from `make reset && make up`)

Status 2026-10-13: the bad-record check was done in phase 2 (against the live registry, before Flink switched). Compatibility was checked with the registry's `/compatibility/subjects/weather.conditions-value/versions/latest` endpoint, which tests without registering. A new nullable `wind_gust_kph` field passes under FULL; `temp_c` changed to string fails with `MISSING_UNION_BRANCH` both ways. The incompatible path through `schema-init` (exit 1, nothing downstream starts) was verified in phase 1. A compatible change was *not* actually registered as v2, because v2 would stay in the registry until `make reset`. `make replace JOB=ai`: the new job checkpointed with no exceptions and recommendations kept flowing; all 11 subjects were still at `[1]` and the registry still READONLY.

- A hand-made bad record (wrong type for `position.speed`, a missing `object_id`), produced through `produceRecord` from a node one-liner, is rejected with a schema error and never reaches the topic.
- An *incompatible* edit to a `.avsc` (e.g. a field changed from `double` to `string`) makes `schema-init` fail on `make up`, and the running stack is untouched. A *compatible* edit (a new nullable field with a default) registers as version 2. Revert both afterwards. The repo isn't changed, but this proves the compatibility gate works.
- Restarting `fleet-app` and replacing a job (`make replace JOB=ai`) still work, with no duplicate subjects or versions.

## Phase 0 results

The spike ran a registry and a second Flink cluster (the demo's image plus the registry jar) on the demo's Docker network, with scratch `spike.*` topics. The demo stack was untouched. Every open question is settled:

| Question | Result |
|---|---|
| Does `avro-confluent.schema` accept our nullability layout? | **Yes.** Real telemetry records produced by Node and read by Flink: nested nullable `ROW`s, the nullable array of nullable `Tire` records, the nullable `geozone_ids` array, `NOT NULL` keys, the computed `event_ts` and watermark, `injection_id` null and set. Plain and keyed (`value.` prefix, `'value.fields-include' = 'ALL'`, raw key) tables both work. |
| Is our layout what Flink would derive? | **Yes, apart from the names.** A sink with no schema registered Flink's own derivation: identical structure, nullability and `null` defaults, but named `org.apache.flink.avro.generated.record`, `record_position`, … So the `.avsc` files can be transcribed mechanically from the DDL, and are free to use proper names. |
| Does Flink's `register()` on write create new versions? | **No, when the inlined text is identical.** All sink subjects stayed at version 1, and Flink's records carry the same schema ID as Node's. **With a subject in `READONLY` mode**, an identical schema still resolves, and a drifted one (only the `doc` changed) fails the job with `42205 read-only mode`. That's why `schema-init` now ends by switching the registry to `READONLY`. |
| Does the JS serializer apply `"default": null` for absent fields? | **Yes.** Records without `injection_id` (and with `position` absent entirely) encode and decode with `null`. The round trip matches the original JSON exactly, with unions unwrapped to plain values. The wire format is the payload prefix Flink expects (`0x00` + 4-byte ID), not header-based IDs. |
| What does the serializer reject? | Wrong type (`speed: "88.0"`), a float in an `int`, a missing or `null` key, and a subject that isn't registered (`40401`, because auto-register is off). It **accepts and silently drops** unknown fields, which is now in the README notes. |
| Classpath conflicts? | **One: zstd-jni** (registry jar 1.5.5-1 vs. Iceberg 1.5.7-3; the TaskManager loaded 1.5.5). Iceberg reads and writes still worked, but the jar now goes in `opt/` with `ADD JAR`, which keeps Iceberg on 1.5.7 (verified with the same Avro → Iceberg + Avro → Kafka job). The other overlaps are with `flink-dist` (commons-io, commons-lang3, snappy), which is normal for Flink's own format jars. |
| Poison pill? | A plain-JSON record on an Avro topic fails the job with `Unknown data format. Magic number does not match`, and it restarts without limit. That's the agreed fail-loudly behavior, now documented under Known limitations. |
| Memory | The registry runs at ~290 MB with `SCHEMA_REGISTRY_HEAP_OPTS=-Xms128m -Xmx256m`. Use that in compose. |

Seen in passing, and fixed before phase 1: `objectstore` was OOM-killed on 2026-10-10 and was still down, and `iceberg-rest` sat at ~1.5 GB. `fleet-lakehouse` shows RUNNING, but its last completed checkpoint was 2026-10-10 19:35:13, three seconds before the object store died, and it has restored 695 times since, so nothing has reached Iceberg since then. The cause was unmaintained Iceberg tables: 65 GB of `metadata.json` against 250 MB of data. `30-lakehouse.sql` now runs post-commit maintenance (Iceberg 1.12's v2 sink: snapshot expiry, compaction, orphan cleanup) and caps metadata files per table. After `make reset`, metadata files level off at 11 per table, compaction turned 17–24 files per table into 1 within 5 minutes, and `iceberg-rest` is at ~300 MB. Also fixed on the way: every `make up` recreated the Flink containers (dropping the jobs) because all three Flink services built the same image tag, and build attestations made every build's image ID new. Now only `flink-jobmanager` builds it, with `provenance: false`, and a no-change `make up` recreates nothing.
