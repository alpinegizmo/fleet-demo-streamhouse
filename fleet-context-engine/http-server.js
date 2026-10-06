// http-server.js
//
// A small JSON API over the context engine, so the browser-based HTML
// console (fleet-intelligence-console.html) can show real pipeline data
// too. Browsers can't speak the Kafka wire protocol directly, so this
// process is the proxy - the browser only ever talks to this HTTP server
// over plain JSON.
//
// The same server hosts the MCP endpoint (/mcp, see mcp-server.js) that
// gives agents SQL over the live topics, and /api/ask, the console chat
// widget's agent, which uses those same tools in-process
// (nl-query-agent.js).
//
// This process also serves the console's static files (HTML + fonts, both
// one directory up - see STATIC_ROOT below) so the whole demo is a single
// origin. `open fleet-intelligence-console.html` as a file:// still works
// too (see the LIVE_API fallback in that file) and never hits this static
// serving path at all.

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { contextEngine, isLive } from './context-engine.js';
import { askAboutFleetData } from './nl-query-agent.js';
import { handleMcpRequest } from './mcp-server.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// fleet-intelligence-console.html and fonts/ live one level up from this
// file (repo root) in both the local checkout and the Docker image - see
// the Dockerfile, which preserves that same relative layout.
const STATIC_ROOT = path.join(__dirname, '..');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.woff2': 'font/woff2',
};

// Serves fleet-intelligence-console.html at "/" and font files under
// fonts/, and nothing else - STATIC_ROOT is the repo root, which also
// contains .env files and the rest of the repo, so this must be an
// allowlist rather than "anything under STATIC_ROOT" or those would be
// servable too. Returns false (falls through to the API routes / 404) if
// there's nothing to serve, rather than answering wrong requests itself.
function serveStatic(req, res, pathname) {
  let relPath;
  if (pathname === '/') {
    relPath = 'fleet-intelligence-console.html';
  } else if (/^\/fonts\/[\w.-]+\.woff2$/.test(pathname)) {
    relPath = pathname.slice(1);
  } else {
    return false;
  }

  const filePath = path.join(STATIC_ROOT, relPath);
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) return false;

  const ext = path.extname(filePath);
  res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
  fs.createReadStream(filePath).pipe(res);
  return true;
}

function withCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res, status, data) {
  withCors(res);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return {};
  }
}

// PORT or HTTP_PORT override the listening port (the Dockerfile sets PORT);
// 8787 is the default. docker-compose.yml maps it to APP_PORT on the host.
export function startHttpServer(port = process.env.PORT || process.env.HTTP_PORT || 8787) {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
      withCors(res);
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url, `http://localhost:${port}`);
    try {
      if (req.method === 'GET' && !url.pathname.startsWith('/api/') && serveStatic(req, res, url.pathname)) {
        return;
      }
      if (url.pathname === '/mcp') {
        return await handleMcpRequest(req, res, await readJsonBody(req));
      }
      if (req.method === 'GET' && url.pathname === '/api/health') {
        return sendJson(res, 200, { ok: true, mode: isLive ? 'local' : 'simulated' });
      }
      if (req.method === 'GET' && url.pathname === '/api/fleet-summary') {
        return sendJson(res, 200, await contextEngine.getFleetSummary());
      }
      if (req.method === 'GET' && url.pathname === '/api/vehicles') {
        return sendJson(res, 200, await contextEngine.listVehicles());
      }
      if (req.method === 'GET' && url.pathname === '/api/high-risk') {
        const domain = url.searchParams.get('domain') || undefined;
        return sendJson(res, 200, await contextEngine.listHighRisk(domain));
      }
      if (req.method === 'GET' && url.pathname === '/api/recommendations') {
        const domain = url.searchParams.get('domain') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getRecentRecommendations(domain, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/weather') {
        const geozone = url.searchParams.get('geozone') || undefined;
        return sendJson(res, 200, await contextEngine.getWeather(geozone));
      }
      if (req.method === 'GET' && url.pathname === '/api/traffic-incidents') {
        const geozone = url.searchParams.get('geozone') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getTrafficIncidents(geozone, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/parcel-volume') {
        const vehicleId = url.searchParams.get('vehicleId') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getParcelVolume(vehicleId, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/maintenance-alerts') {
        const vehicleId = url.searchParams.get('vehicleId') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getMaintenanceAlerts(vehicleId, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/driver-risk-events') {
        const vehicleId = url.searchParams.get('vehicleId') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getDriverRiskEvents(vehicleId, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/driver-risk-events-anomaly') {
        const vehicleId = url.searchParams.get('vehicleId') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getDriverRiskAnomalyEvents(vehicleId, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/risk-comparison') {
        return sendJson(res, 200, await contextEngine.getRiskComparison());
      }
      if (req.method === 'POST' && url.pathname === '/api/inject-incident') {
        const { vehicleId } = await readJsonBody(req);
        return sendJson(res, 200, await contextEngine.injectIncident(vehicleId));
      }
      if (req.method === 'POST' && url.pathname === '/api/inject-traffic-incident') {
        const { geozone } = await readJsonBody(req);
        return sendJson(res, 200, await contextEngine.injectTrafficIncident(geozone));
      }
      if (req.method === 'POST' && url.pathname === '/api/inject-speed-anomaly') {
        const { vehicleId } = await readJsonBody(req);
        return sendJson(res, 200, await contextEngine.injectSpeedAnomaly(vehicleId));
      }
      if (req.method === 'POST' && url.pathname === '/api/ask') {
        const { conversation } = await readJsonBody(req);
        if (!Array.isArray(conversation) || !conversation.length) {
          return sendJson(res, 400, { error: 'Missing "conversation" array in request body.' });
        }
        return sendJson(res, 200, await askAboutFleetData(conversation));
      }
      if (req.method === 'GET' && url.pathname === '/api/pipeline-events') {
        const since = Number(url.searchParams.get('since')) || 0;
        return sendJson(res, 200, await contextEngine.getPipelineEvents(since));
      }
      if (req.method === 'POST' && url.pathname === '/api/reset') {
        if (!isLive) {
          return sendJson(res, 400, { error: 'Not connected to Kafka (SIMULATED mode) - nothing to reset. Set KAFKA_BOOTSTRAP or run via docker compose.' });
        }
        return sendJson(res, 200, await contextEngine.resetPipeline());
      }
      sendJson(res, 404, { error: 'Not found' });
    } catch (err) {
      sendJson(res, 500, { error: err.message });
    }
  });

  server.listen(port, () => {
    console.error(`[fleet-context-engine] HTTP API listening on http://localhost:${port} (for fleet-intelligence-console.html)`);
  });

  return server;
}
