# fleet-context-engine

The Node.js service behind the console. Normally it runs as the `fleet-app`
container (`make up` from the repo root, see the main [README](../README.md));
this page is about what it does and how to run it on its own.

## What it does

- **Reads the whole pipeline.** A background Kafka consumer (kafkajs) tails
  all 11 topics from the beginning and keeps an in-memory model of the fleet:
  vehicles, weather, incidents, business events, recommendations
  (`context-engine.js`). Every API call is an instant read from that model.
- **Feeds the pipeline.** `telemetry-simulator.js` produces fresh telemetry,
  weather, traffic and parcel data continuously; the console's demo buttons
  produce real records through `kafka-client.js`. `seed.js` is the one-shot
  backfill (`npm run seed`).
- **Serves the console** (`fleet-intelligence-console.html` + `fonts/`) and
  its JSON API on port 8787 (`http-server.js`).
- **Serves MCP** at `/mcp` (`mcp-server.js`) — three tools over the live
  topics, for Claude, Copilot, or any MCP client.
- **Answers chat questions** (`POST /api/ask`, `nl-query-agent.js`) with a
  local LLM calling those same tools.
- **Watches the lakehouse.** Polls the Iceberg REST catalog for new snapshots
  so the console's Behind the Scenes tab shows real commits.

## Modes

| Mode | When | Data |
|---|---|---|
| **LOCAL** | `KAFKA_BOOTSTRAP` is set (compose sets `kafka:9092`) | the real pipeline |
| **SIMULATED** | `KAFKA_BOOTSTRAP` unset | an in-memory simulator, clearly labelled in the console — enough for UI work with no Kafka at all |

## Running it outside Docker

```bash
cd fleet-context-engine
npm install
npm start                                      # SIMULATED
KAFKA_BOOTSTRAP=localhost:9094 npm start       # LOCAL, against the compose stack's Kafka
```

Against the compose stack, stop the `fleet-app` container first
(`docker compose stop fleet-app`) or set `HTTP_PORT` to another port, and
`AUTO_TELEMETRY=false` unless you want two simulators producing.
`.env.example` lists every variable; copy it to `.env`.

| Variable | |
|---|---|
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers; unset = SIMULATED |
| `HTTP_PORT` / `PORT` | listening port (default 8787) |
| `AUTO_TELEMETRY` | `false` stops the simulator |
| `SIM_PACE` | the simulator's starting pace: `normal` (every vehicle every 3 s), `slow` (15 s), `minimal` (30 s) |
| `LLM_BASE_URL`, `LLM_MODEL`, `CHAT_MODEL` | the chat agent's OpenAI-compatible endpoint and model (`CHAT_MODEL` wins over `LLM_MODEL`); `LLM_MODEL` is also shown in the console |
| `ICEBERG_REST_URL` | Iceberg REST catalog to watch for commits (optional) |

## HTTP API

All `GET` unless noted; the console polls these every ~3 s.

| Endpoint | |
|---|---|
| `/api/health` | `{ ok, mode }` |
| `/api/fleet-summary` | the KPI tiles |
| `/api/vehicles`, `/api/high-risk?domain=` | per-vehicle state |
| `/api/recommendations?domain=&limit=` | AI recommendations (newest 200 per domain kept) |
| `/api/weather`, `/api/traffic-incidents`, `/api/parcel-volume` | context topics |
| `/api/maintenance-alerts`, `/api/driver-risk-events`, `/api/driver-risk-events-anomaly`, `/api/risk-comparison` | business events |
| `/api/pipeline-events?since=` | Behind the Scenes animation feed |
| `/api/simulator`, `POST /api/simulator` | the simulator's pace: `{ enabled, pace, paces }`; POST `{ pace: "normal" \| "slow" \| "minimal" }` (the console's Background traffic switch) |
| `POST /api/inject-incident`, `/api/inject-traffic-incident`, `/api/inject-speed-anomaly` | demo buttons — produce real records |
| `POST /api/reset` | deletes every record in every topic (topics and Flink jobs untouched) |
| `POST /api/ask` | `{ conversation: [{role, content}, …] }` → `{ answer, trace }` |
| `POST /mcp` | MCP (Streamable HTTP, stateless) |

## The MCP tools

| Tool | |
|---|---|
| `listTopics` | every topic, its SQL table name, description, and how many recent records are queryable |
| `getMetadata` | one topic's columns and DuckDB types (nested `STRUCT`s included) |
| `queryData` | one DuckDB SQL query over every topic's most recent 2,000 records |

`queryData` is real SQL — joins across topics, aggregates, window functions.
Each topic is a table named like `vehicle_telemetry` (or `"vehicle.telemetry"`),
and every table has `kafka_timestamp` for time filters. Schemas come from
`topic-schemas.js`, which mirrors the Flink DDL in
`local/flink/sql/00-tables.sql` — change both together.

Because the SQL comes from models and arbitrary clients, every query runs in
a fresh in-memory DuckDB that is locked down first (`query-store.js`): no
file or network access, configuration frozen, 256 MB of memory, a 10 s
time limit, and the database is discarded afterwards.

Connect a client:

```bash
claude mcp add --transport http fleet http://localhost:8787/mcp      # Claude Code
npx @modelcontextprotocol/inspector --cli http://localhost:8787/mcp --transport http --method tools/list
```

VS Code uses `.vscode/mcp.json` at the repo root.
