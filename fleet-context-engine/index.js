// index.js
//
// Backend process for the fleet telemetry demo: runs the
// fleet context engine (context-engine.js) and exposes its state as a
// small HTTP API for fleet-intelligence-console.html to poll.
//
// context-engine.js reads the REAL local Kafka + Flink pipeline when
// KAFKA_BOOTSTRAP is set (docker-compose.yml sets it), and falls back to an
// in-memory simulator otherwise.
//
// Run it:
//   docker compose up            # whole stack, from the repo root
//   npm install && npm start     # this process only (SIMULATED, or set
//                                # KAFKA_BOOTSTRAP=localhost:9094)

// Must be the first import: context-engine.js reads KAFKA_BOOTSTRAP at
// module-load time (isLiveModeConfigured() runs immediately), so .env has
// to be loaded into process.env before that import below runs. A real
// `export`ed shell env var always wins over .env (dotenv default), so this
// is additive - nothing changes for anyone already using `export`.
import 'dotenv/config';

import { isLive } from './context-engine.js';
import { startHttpServer } from './http-server.js';
import { startTelemetrySimulator } from './telemetry-simulator.js';

console.error(`[fleet-context-engine] mode: ${isLive ? `LOCAL (real Kafka at ${process.env.KAFKA_BOOTSTRAP})` : 'SIMULATED (in-memory, KAFKA_BOOTSTRAP not set)'}`);

startHttpServer();

// In LIVE mode, the real topics are otherwise a one-shot seeded batch -
// this keeps real data flowing continuously so the console has something
// new to show between demo-button clicks. SIMULATED mode already ticks
// its own in-memory state on an interval, so it doesn't need this.
if (isLive) {
  startTelemetrySimulator();
}
