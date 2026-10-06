# Plan: run the fleet demo entirely locally on Docker

Replace Confluent Cloud + OpenAI with open-source Apache Kafka + Apache Flink and a small
local OpenAI-compatible model, all started by one `docker compose up`.

## Decisions (from interview)

| Topic | Decision |
|---|---|
| Scope | **Replace** the Confluent Cloud path. Terraform, RTCE, Tableflow, OpenAI code is removed (still recoverable from git at `f8c3ee1`). |
| Kafka | Apache Kafka 4.x, single KRaft node, PLAINTEXT. No REST Proxy — kafkajs does produce/admin directly. |
| Encoding / governance | **Plain JSON, no schema registry.** Flink `'format' = 'json'`. The schema contract lives in the Flink DDL plus one JS schema file for MCP `getMetadata`. |
| Flink | Apache Flink **2.2** (the newest line with matching Kafka connector `5.0.0-2.2`, `iceberg-flink-runtime-2.2:1.12.0`, `flink-model-openai:2.2.x`). |
| AI in the stream | Flink-native `CREATE MODEL ... WITH ('provider'='openai')` + `ML_PREDICT`, pointed at the local model. |
| LLM runtime | **Docker Model Runner** (host-side, Metal GPU), wired in via compose's top-level `models:` element. |
| Model | `ai/qwen2.5:1.5B-F16` (~3 GB). Configurable; chat agent model separately overridable. |
| Tableflow replacement | Flink → **Iceberg** (REST catalog + S3-compatible object store) → **DuckDB**. |
| RTCE replacement | **Own MCP server** inside `fleet-context-engine` (`/mcp`, Streamable HTTP), same `listTopics` / `getMetadata` / `queryData` tools. |
| AI_DETECT_ANOMALIES replacement | Per-vehicle **z-score** with a Flink SQL OVER window, same output topic shape. |

Smaller defaults I chose (say if you disagree):
- **Port `seed-data/generate_seed_data.py` to Node** (`seed.js`, one-shot compose service on the app image) and delete the Python script — removes a second runtime and the wire-encoding logic.
- **Keep SIMULATED mode** in `fleet-context-engine` (no Kafka → in-memory sim) so the console can still be developed with plain `npm start`. The old cloud "LIVE" mode becomes a "LOCAL" mode against `kafka:9092`.
- **Purge via `admin.deleteRecords`** to the high watermark instead of the retention-shrink trick — instant and doesn't need a 15 s wait.
- **Drop the `vehicle_telemetry_ts` intermediate topic.** In OSS Flink the watermark can be declared directly on the `vehicle.telemetry` source table as a computed column, which was only needed in Confluent Cloud.

## Target architecture

```
                    ┌──────────── docker compose (project: fleet) ────────────────────────┐
 host: Docker Model │                                                                     │
 Runner (Metal) ◄───┼── flink-taskmanager (ML_PREDICT)      fleet-app (Node, :8787)       │
  qwen2.5:1.5B   ◄──┼────────────────────────────────────── /api/ask agent               │
                    │                                        /mcp  (MCP server)          │
                    │  kafka (KRaft) ◄──── simulator / seed / inject buttons (kafkajs)     │
                    │     │   ▲                              console HTML + fonts          │
                    │     ▼   │                                                          │
                    │  flink-jobmanager + taskmanager ──► iceberg-rest ──► object store   │
                    │     (sql-init one-shot submits jobs)                    ▲           │
                    │                                              duckdb (on demand)     │
                    └─────────────────────────────────────────────────────────────────────┘
```

Compose services:

| Service | Image | Notes |
|---|---|---|
| `kafka` | `apache/kafka:4.x` | KRaft single node; internal `kafka:9092`, host `localhost:9094` |
| `kafka-init` | same | one-shot: creates the 11 topics (explicit partitions, `auto.create.topics.enable=false` on the broker) |
| `flink-jobmanager`, `flink-taskmanager` | custom `local/flink/Dockerfile` FROM `flink:2.2-java17` + connector jars | checkpoints to a named volume; ~8 slots |
| `flink-sql-init` | same Flink image | one-shot: `envsubst` the SQL templates (model URL/name), then `sql-client.sh -f` each statement set; skips any job whose `pipeline.name` is already RUNNING (idempotent re-runs) |
| `iceberg-rest` | `apache/iceberg-rest-fixture` (JDBC/SQLite on a volume) | Iceberg REST catalog |
| `objectstore` + `objectstore-init` | MinIO pinned, or SeaweedFS (see risks) | S3 warehouse bucket |
| `fleet-app` | repo `Dockerfile` | `fleet-context-engine` + console + MCP; `models: [llm]` |
| `seed` | repo `Dockerfile`, `node seed.js` | one-shot backfill (profile `seed`, or run automatically once) |
| `duckdb` | small image w/ DuckDB CLI + iceberg/httpfs ext | profile `tools`: `docker compose run --rm duckdb` |

Compose model binding:
```yaml
models:
  llm:
    model: ${LLM_MODEL:-ai/qwen2.5:1.5B-F16}
    context_size: 4096
services:
  fleet-app:      { models: { llm: { endpoint_var: LLM_URL, model_var: LLM_MODEL } } }
  flink-sql-init: { models: { llm: { endpoint_var: LLM_URL, model_var: LLM_MODEL } } }
```
`LLM_URL` resolves to Model Runner's OpenAI-compatible base (`.../engines/v1`); everything appends `/chat/completions`. An `OPENAI_BASE_URL` override lets anyone point at Ollama / LM Studio / llama.cpp instead.

## Flink SQL (replaces `terraform/flink_statements.tf`)

New dir `local/flink/sql/`, one self-contained file per job group (OSS Flink's default catalog is in-memory and per-session, so each file declares the tables it uses as `CREATE TEMPORARY TABLE`):

- `00-tables.sql` — shared DDL include for all 11 topics: `'connector'='kafka'`, `'topic'`, `'properties.bootstrap.servers'='kafka:9092'`, `'format'='json'`, `'json.ignore-parse-errors'='true'`, `'scan.startup.mode'='group-offsets'` + `'properties.auto.offset.reset'='earliest'` (resume after restart). `vehicle.telemetry` gets `event_ts AS TO_TIMESTAMP(...)` + `WATERMARK FOR event_ts AS event_ts - INTERVAL '1' SECOND` (also fixes the same-second tie issue the cloud version patched out-of-band).
- `10-business.sql` — `EXECUTE STATEMENT SET`: the 3 business derivations (SQL near-identical to today, minus the `${local.db}` qualification) + the z-score anomaly job:
  ```sql
  AVG(speed_kmh)        OVER w AS mean_speed,
  STDDEV_POP(speed_kmh) OVER w AS sd_speed,
  COUNT(*)              OVER w AS n
  ... WINDOW w AS (PARTITION BY vehicle_id ORDER BY event_ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
  -- emit when n >= 10 AND ABS(speed - mean) > 3 * sd
  -- forecast_speed_kmh = mean, lower/upper = mean ∓ 3·sd  (same columns as today)
  ```
- `20-ai.sql` — `CREATE MODEL fleet_ai_advisor INPUT (prompt STRING) OUTPUT (content STRING) WITH ('provider'='openai', 'endpoint'='${LLM_URL}/chat/completions', 'api-key'='unused', 'model'='${LLM_MODEL}', 'system-prompt'='You are a fleet operations advisor. Answer in one short sentence.')`. Each of the 3 AI jobs becomes a prompt view + `ML_PREDICT(TABLE <view>, MODEL fleet_ai_advisor, DESCRIPTOR(prompt))` (OSS syntax differs from Confluent's `LATERAL TABLE(ML_PREDICT('name', expr))`). `error-handling-strategy` = `IGNORE` so a model hiccup drops one recommendation instead of failing the job.
- `30-lakehouse.sql` — `CREATE CATALOG lake WITH ('type'='iceberg','catalog-type'='rest','uri'='http://iceberg-rest:8181', 'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', ...)` and one `STATEMENT SET` copying the same 6 downstream topics Tableflow did into `lake.fleet.*`. Commits happen on checkpoint (interval 30 s → that's the lakehouse freshness lag).

That's 3 running Flink jobs (business, AI, lakehouse), down from 14 separate cloud statements.

## `fleet-context-engine` changes

| File | Change |
|---|---|
| `context-engine.js` | LIVE → LOCAL mode keyed on `KAFKA_BOOTSTRAP` only; kafkajs without SSL/SASL; `decodeValue` = `JSON.parse`; drop `vehicle_telemetry_ts`; "Tableflow/Snowflake" pulse hops become "Iceberg/DuckDB" and are driven by **real** Iceberg snapshot commits (poll the REST catalog's table metadata) instead of being inferred from AI-topic arrivals. |
| `confluent-client.js` → `kafka-client.js` | kafkajs producer (plain JSON) + `deleteRecords` purge. No REST, no schema IDs. |
| `telemetry-simulator.js`, new `seed.js` | Use `kafka-client.js`. |
| `rtce-client.js` → deleted; new `query-store.js` | Bounded ring buffer per topic (e.g. last 2,000 records) loaded into an in-process **DuckDB** (`@duckdb/node-api`) so `queryData` gets real SQL — and unlike RTCE, JOINs/aggregates work. Optionally `ATTACH` the Iceberg catalog for history (stretch goal). |
| new `topic-schemas.js` | Column/type descriptions per topic for `getMetadata` (the one place schemas live outside Flink DDL, since there's no registry). |
| new `mcp-server.js` | `@modelcontextprotocol/sdk` Streamable HTTP mounted at `/mcp` on the existing HTTP server; tools backed by `query-store.js`. |
| `nl-query-agent.js` | Call `${LLM_URL}/chat/completions` with `LLM_MODEL` (override: `CHAT_MODEL`); call tools in-process. Tuned for a 1.5B model: inline the topic list + schemas into the system prompt (cuts the listTopics/getMetadata round-trips), shorter tool descriptions, relaxed SQL rules now that DuckDB supports aggregates. |
| `http-server.js`, `index.js` | Mount `/mcp`; mode/log strings. |
| `.vscode/mcp.json` | → `http://localhost:8787/mcp`, no auth. |

## Console (`fleet-intelligence-console.html`)

Relabel only, no structural change: "OpenAI" → model name from `/api/health`; "RTCE" → "MCP (fleet-context-engine)"; "Tableflow / Confluent managed storage" → "Iceberg / local object store"; "Snowflake" → "DuckDB"; "LIVE — real Confluent Cloud + Flink + OpenAI" → "LOCAL — Kafka + Flink + <model>".

## Removed / rewritten

- Delete `terraform/`, `terraform-us-west-2/`, `terraform-us-west-2-v2/`, `seed-data/`, `fleet-context-engine/SETUP-RTCE-COPILOT.md`, `setup-demo.md`.
- Rewrite `README.md`, `fleet-context-engine/SETUP.md`, `.env.example`, `.dockerignore`, architecture diagram.
- `confluent-fleet-eda.skill` and the untracked `confluent-fleet-eda/` — encode the cloud-era decisions; delete or regenerate (open question below).

## Phases (each ends with a check you can see)

0. **Pre-flight — tear down the cloud stack first.** `terraform/terraform.tfstate` still tracks **38 live resources** (Standard cluster, 10-CFU Flink pool, Tableflow). Deleting the directory would orphan them and keep billing. Run `terraform destroy` there (Tableflow resources have `prevent_destroy = true`, which must be removed first) — *or* tell me to keep them and I'll leave `terraform/` out of the deletion. Work on a branch `local-docker`.
1. **Kafka + app.** Compose with `kafka`, `kafka-init`, `fleet-app`, `seed`; Node on plain JSON. ✔ Console shows vehicles moving from real Kafka topics; inject buttons produce.
2. **Flink core.** Flink image, `00-tables.sql`, `10-business.sql`, `flink-sql-init`. ✔ Derived + anomaly topics populate; Flink UI on `:8081` shows jobs RUNNING; Safety/Delivery/Maintenance tabs fill.
3. **Local LLM.** *Start with a 30-min spike:* `flink-model-openai` against Model Runner (jar dependencies, endpoint path, dummy api-key, response parsing). Then `20-ai.sql`. ✔ `ai.*` topics fill; measure per-request latency and topic lag under the simulator's load (see risks).
4. **Lakehouse.** Object store, `iceberg-rest`, `30-lakehouse.sql`, `duckdb` tool. ✔ `SELECT count(*) FROM lake.fleet.ai_safety_recommendations` in DuckDB grows every ~30 s.
5. **MCP + Ask.** `query-store.js`, `mcp-server.js`, agent rewire. ✔ Claude Code via `.vscode/mcp.json` lists/queries topics; chat widget answers with real values.
6. **Console relabel + docs + deletions**, `Makefile` (`up`, `down`, `reset` = `down -v`, `sql` = Flink SQL client shell, `duckdb`), new diagram/screenshot.

## Risks and mitigations

- **LLM throughput.** ~4 telemetry msgs/s fan out into risk/maintenance/delivery events, and every one is a model call. A 1.5B F16 model on Metal handles a few one-sentence calls per second; CPU wouldn't. Mitigation if lag grows: dedupe in SQL so the model is only called on a per-vehicle state change (or at most once per vehicle per minute via a tumbling window), and cap output with the model's `max-tokens` option.
- **1.5B tool calling** for `/api/ask` may be shaky. Mitigation: schemas in the system prompt (fewer steps), `CHAT_MODEL=ai/qwen2.5:3B-Q4_K_M` override documented.
- **`flink-model-openai` packaging** — may not be a fat jar (OpenAI Java SDK deps). Phase 3 spike resolves it; fallback is a small Flink async UDF.
- **Iceberg on Flink needing Hadoop classes** even with a REST catalog + S3FileIO — known friction; add `hadoop-client-api/runtime` jars to the image if so.
- **Object store choice.** MinIO's community edition stopped publishing maintained images in late 2025; pin the last image or use SeaweedFS's S3 gateway. Decided during Phase 4.
- **Docker Model Runner requirement.** Needs Docker Desktop (or Docker Engine with the model plugin) with Model Runner enabled; won't work on Colima/OrbStack. `OPENAI_BASE_URL` override is the escape hatch.
- **No Flink HA.** A jobmanager restart loses running jobs; `flink-sql-init` resubmits on `up`, and Kafka group offsets + Iceberg checkpoints make that resume cleanly rather than reprocess.
- **Footprint.** ~6–8 GB Docker memory (Kafka ~1 GB, Flink ~3 GB, catalog/object store/app ~1 GB) plus ~3 GB host RAM for the model.
- **Schema drift.** With no registry, a producer can emit a field the DDL doesn't know about and Flink silently ignores it (or drops the record with `ignore-parse-errors`). Keep `topic-schemas.js` and the DDL in one PR whenever a shape changes.

## Open questions (not blocking Phase 1)

1. Cloud stack: destroy it (Phase 0) or keep `terraform/` around until you've done that yourself?
2. `fleet-intelligence-deck.html` talks about Confluent Cloud throughout — update it to the local architecture, or leave it as-is?
3. `confluent-fleet-eda.skill` — delete, or regenerate for the local architecture once it's done?
