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
| LLM runtime | **Ollama, installed natively on the host** (Metal GPU), reached from containers at `host.docker.internal:11434`. Originally Docker Model Runner, but its llama.cpp process got `Operation not permitted` opening model files on this machine (Phase 3). |
| Model | `qwen2.5:7b` for recommendations, `qwen2.5:3b` for the chat agent. Started on `qwen2.5:1.5b`, but on harsh-braking safety events it answered "Reduce speed…" 20/20 times; 3b gave on-topic advice; 7b more concrete advice still, at 4.7 calls/s vs ~2.8 needed (3b: 6.9). 7b isn't used for chat: with the agent's long tool prompt it skipped the tools and invented answers. `LLM_MODEL` / `LLM_BASE_URL` switch to any model or OpenAI-compatible server. |
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
| `fleet-app` | repo `Dockerfile` | `fleet-context-engine` + console + MCP; `LLM_MODEL` / `LLM_BASE_URL` env |
| `seed` | repo `Dockerfile`, `node seed.js` | one-shot backfill (profile `seed`, or run automatically once) |
| `duckdb` | small image w/ DuckDB CLI + iceberg/httpfs ext | profile `tools`: `docker compose run --rm duckdb` |

LLM wiring: Ollama runs on the host (`brew install ollama && brew services start ollama && ollama pull qwen2.5:7b && ollama pull qwen2.5:3b`). Compose passes `LLM_BASE_URL` (default `http://host.docker.internal:11434/v1`) and `LLM_MODEL` to Flink, and the model name to the app for display.

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

0. **Pre-flight.** ✅ The Confluent Cloud environment was already gone; 4 service accounts + 2 API keys remained (org-level) for manual deletion.
1. **Kafka + app.** ✅ Compose with `kafka`, `kafka-init`, `fleet-app`, `seed`; Node on plain JSON. ✔ Console shows vehicles moving from real Kafka topics; inject buttons produce.
2. **Flink core.** ✅ Flink image, `00-tables.sql`, `10-business.sql`, `flink-sql-init`. ✔ Derived + anomaly topics populate; Flink UI on `:8081` shows jobs RUNNING; Safety/Delivery/Maintenance tabs fill.
3. **Local LLM.** ✅ Done. `flink-model-openai` 2.2.1 (one 30 MB shaded jar) works against Ollama's OpenAI-compatible API; `20-ai.sql` runs the 3 `ML_PREDICT` jobs async; inject → recommendation in ~1 s.
4. **Lakehouse.** ✅ Done. RustFS 1.0.1 (MinIO no longer publishes images) + `apache/iceberg-rest-fixture:1.10.1` + `30-lakehouse.sql` copying the 7 derived topics (incl. the anomaly topic) into `lake.fleet.*` with their Kafka partition/offset/timestamp; `docker compose run --rm duckdb` opens a shell with the catalog attached. Verified: Iceberg rows = topic offsets with no gaps or duplicates, ~1 checkpoint behind. `flink-model-openai` had to move out of `lib/` (loaded per job via `ADD JAR`) because its bundled HttpClient 5 broke the AWS SDK's S3 client. The Behind the Scenes tab's Iceberg/DuckDB nodes now pulse on real snapshot commits.
5. **MCP + Ask.** ✅ Done. `topic-schemas.js` (mirrors the Flink DDL), `query-store.js` (last 2,000 records per topic → a fresh, locked-down in-memory DuckDB per query: no file/network access, 256 MB, 10 s), `mcp-server.js` (stateless Streamable HTTP at `/mcp`; verified with the MCP Inspector CLI), `nl-query-agent.js` on Ollama with the catalog in its system prompt. `.vscode/mcp.json` → `http://localhost:8787/mcp`. Agent answers in 1–6 s on qwen2.5:3b; hardened against small-model habits (backtick identifiers, "let me fix that" without retrying, runaway generation).
6. **Console relabel + docs + deletions** ✅ Done. Terraform (3 dirs), `setup-demo.md`, `SETUP-RTCE-COPILOT.md` removed; README and `fleet-context-engine/SETUP.md` rewritten; `Makefile` (`up/down/reset/status/jobs/logs/replace/sql/duckdb/model`) and `make sql` shell; diagram redrawn, screenshot retaken. Found and fixed on the way: events/sec counted the startup replay; a shared 200-recommendation cap starved maintenance; the console's number tween extrapolated on negative `t`. Clean-clone test (fresh volumes, separate compose project): up in 41 s with cached images, every layer verified.

## Risks and mitigations

- **LLM throughput.** ~4 telemetry msgs/s fan out into risk/maintenance/delivery events, and every one is a model call. A 1.5B F16 model on Metal handles a few one-sentence calls per second; CPU wouldn't. Mitigation if lag grows: dedupe in SQL so the model is only called on a per-vehicle state change (or at most once per vehicle per minute via a tumbling window), and cap output with the model's `max-tokens` option.
- **The chat agent and the Flink AI job share one Ollama server.** A runaway chat generation once queued Flink's `ML_PREDICT` calls behind it for 5 minutes (and the AI job restarted on the timeouts). Now capped (`max_tokens` 400, 60 s timeout). For heavier interactive use, raise Ollama's parallelism (`OLLAMA_NUM_PARALLEL`) or point `CHAT_MODEL`/`LLM_BASE_URL` for the agent elsewhere. Answer quality on 3B is correct but sometimes vague; `CHAT_MODEL=qwen2.5:7b` is the next step up.
- **`flink-model-openai` packaging** — may not be a fat jar (OpenAI Java SDK deps). Phase 3 spike resolves it; fallback is a small Flink async UDF.
- **Iceberg on Flink needing Hadoop classes** even with a REST catalog + S3FileIO — known friction; add `hadoop-client-api/runtime` jars to the image if so.
- **Iceberg table maintenance isn't configured.** Every 30 s checkpoint commits a snapshot with small files per table, and nothing expires snapshots or compacts files, so metadata and file counts grow without bound over a long-running demo. Fine for a session; `docker compose down -v` resets it. (A real "where challenges remain" topic for the course: Tableflow does this for you; OSS Flink only offers it via the DataStream table-maintenance API.)
- **Ollama on the host.** The AI job and `/api/ask` need Ollama running (`brew services start ollama`). If it stops, the AI job keeps retrying and catches up when it returns (tested: 45 s outage, no recommendations lost). Ollama in a container would make the stack self-contained, but on a Mac that means CPU only.
- **No Flink HA.** A jobmanager restart loses running jobs; `flink-sql-init` resubmits on `up`, and Kafka group offsets + Iceberg checkpoints make that resume cleanly rather than reprocess.
- **Footprint.** Measured: ~5.7 GB Docker memory (Flink ~3.2 GB, Kafka ~1.4 GB, object store ~0.7 GB, catalog ~0.3 GB, app ~0.1 GB) plus ~8 GB host RAM for both models (qwen2.5:7b ~5.5 GB, qwen2.5:3b ~2.8 GB).
- **Host sleep pauses everything.** Docker's VM stops while the Mac sleeps; the pipeline resumes on wake (observed). Keep the Mac awake (`caffeinate -dims`) during a demo.
- **Schema drift.** With no registry, a producer can emit a field the DDL doesn't know about and Flink silently ignores it (or drops the record with `ignore-parse-errors`). Keep `topic-schemas.js` and the DDL in one PR whenever a shape changes.

## Open questions (not blocking Phase 1)

1. Cloud stack: destroy it (Phase 0) or keep `terraform/` around until you've done that yourself?
2. `fleet-intelligence-deck.html` talks about Confluent Cloud throughout — update it to the local architecture, or leave it as-is?
3. `confluent-fleet-eda.skill` — delete, or regenerate for the local architecture once it's done?
