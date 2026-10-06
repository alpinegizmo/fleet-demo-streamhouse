// mcp-server.js
//
// MCP server for the live fleet data, mounted at /mcp on the same HTTP
// server as the console's API (http-server.js) - the local stand-in for
// Confluent's managed Real-Time Context Engine. Any MCP client (Claude
// Code, Claude Desktop, VS Code Copilot - see .vscode/mcp.json) gets the
// same three tools RTCE offered:
//
//   listTopics   - what data exists
//   getMetadata  - one topic's columns and types
//   queryData    - SQL over the topics' recent records (query-store.js)
//
// The console's own chat agent (nl-query-agent.js) calls the same tool
// implementations in-process, via FLEET_TOOLS below.
//
// Stateless Streamable HTTP: each POST gets a fresh server and transport,
// so there are no sessions to keep or expire.

import * as z from 'zod';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { TOPICS, describeColumns } from './topic-schemas.js';
import { runQuery, tableAlias, recordCount } from './query-store.js';
import { contextEngine } from './context-engine.js';

const MAX_ROWS = 200;

function topicSummary(topic) {
  return { topic, table: tableAlias(topic), description: TOPICS[topic].description, records: recordCount(topic) };
}

export const FLEET_TOOLS = {
  listTopics: {
    description: 'Lists every topic in the live fleet pipeline, with the SQL table name to use for it, a description, and how many recent records are currently queryable.',
    inputSchema: {},
    run: async () => ({ topics: Object.keys(TOPICS).map(topicSummary) }),
  },
  getMetadata: {
    description: "Returns one topic's columns and DuckDB types, including nested STRUCT fields (query them with dot notation, e.g. \"position\".speed). Call this before querying a topic whose columns you haven't seen - never guess column names.",
    inputSchema: {
      topic_name: z.string().describe('Topic name (e.g. vehicle.telemetry) or its table name (vehicle_telemetry)'),
    },
    run: async ({ topic_name }) => {
      const topic = Object.keys(TOPICS).find((t) => t === topic_name || tableAlias(t) === topic_name);
      if (!topic) return { error: `Unknown topic "${topic_name}". Call listTopics for the available topics.` };
      return {
        ...topicSummary(topic),
        columns: describeColumns({ ...TOPICS[topic].schema, kafka_timestamp: 'TIMESTAMPTZ' }),
      };
    },
  },
  queryData: {
    description: 'Runs one DuckDB SQL query over the most recent records of every topic (up to 2,000 per topic, so minutes of telemetry, longer for sparser topics). Each topic is a table, named by its table name (e.g. vehicle_telemetry, ai_safety_recommendations). Full SQL works: joins across topics, GROUP BY, aggregates, window functions. Every table has kafka_timestamp (TIMESTAMPTZ, UTC) for time filters, e.g. WHERE kafka_timestamp > now() - INTERVAL 5 MINUTE. Columns named position, type, timestamp, condition and datetime must be double-quoted.',
    inputSchema: {
      query: z.string().describe('A single DuckDB SQL SELECT statement'),
      max_result_rows: z.number().int().min(1).max(MAX_ROWS).optional().describe(`Maximum rows to return (default 50, max ${MAX_ROWS})`),
    },
    run: async ({ query, max_result_rows }) => {
      // Models often quote identifiers MySQL-style; DuckDB wants double quotes.
      const sql = query.replaceAll('`', '"');
      contextEngine.recordAgentQuery(sql);
      try {
        return await runQuery(sql, Math.min(max_result_rows ?? 50, MAX_ROWS));
      } catch (err) {
        return { error: err.message };
      }
    },
  },
};

function buildServer() {
  const server = new McpServer({ name: 'fleet-context-engine', version: '1.0.0' });
  for (const [name, tool] of Object.entries(FLEET_TOOLS)) {
    server.registerTool(name, { description: tool.description, inputSchema: tool.inputSchema }, async (args) => {
      const result = await tool.run(args ?? {});
      return { content: [{ type: 'text', text: JSON.stringify(result) }], isError: Boolean(result?.error) };
    });
  }
  return server;
}

// Handles one HTTP request to /mcp. `body` is the already-parsed JSON body.
export async function handleMcpRequest(req, res, body) {
  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json', Allow: 'POST' });
    res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32000, message: 'Method not allowed (stateless server: POST only).' }, id: null }));
    return;
  }
  const server = buildServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on('close', () => {
    transport.close();
    server.close();
  });
  await server.connect(transport);
  await transport.handleRequest(req, res, body);
}
