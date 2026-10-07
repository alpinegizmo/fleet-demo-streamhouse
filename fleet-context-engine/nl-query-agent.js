// nl-query-agent.js
//
// Answers a natural-language question about the fleet (the console's "Ask
// the live data" chat widget, POST /api/ask) by giving a local LLM
// tool-calling access to the same three tools the MCP server exposes
// (mcp-server.js) - listTopics, getMetadata, queryData. Every answer is
// grounded in a real SQL query over the live topics; nothing is cached or
// precomputed.
//
// Talks to any OpenAI-compatible chat completions API: LLM_BASE_URL
// (default: Ollama on the host) and CHAT_MODEL, falling back to LLM_MODEL -
// the same model Flink uses for recommendations unless overridden.

import * as z from 'zod';
import { FLEET_TOOLS } from './mcp-server.js';
import { TOPICS, describeColumns } from './topic-schemas.js';
import { tableAlias } from './query-store.js';

export const TOOLS = Object.entries(FLEET_TOOLS).map(([name, tool]) => ({
  type: 'function',
  function: {
    name,
    description: tool.description,
    parameters: z.toJSONSchema(z.object(tool.inputSchema)),
  },
}));

// Small models make fewer, better tool calls when the tables and columns
// are already in front of them, so the system prompt carries the whole
// catalog; getMetadata/listTopics remain available but are rarely needed.
function catalog() {
  return Object.entries(TOPICS).map(([topic, { description, schema }]) => {
    const cols = Object.entries(describeColumns({ ...schema, kafka_timestamp: 'TIMESTAMPTZ' }))
      .map(([name, type]) => `${name} ${type}`).join(', ');
    return `- ${tableAlias(topic)} (topic ${topic}): ${description}\n  columns: ${cols}`;
  }).join('\n');
}

export const SYSTEM_PROMPT = `You answer questions about a live fleet of delivery vehicles in Melbourne by querying its real-time data with the queryData tool. The data is the current state of a Kafka/Flink pipeline - raw vehicle telemetry, weather, traffic, parcel loads, Flink-derived business events, and AI-generated recommendations - not a cached or batch copy. Every answer must come from a query you just ran.

Tables (DuckDB SQL; nested STRUCT fields use dot notation, e.g. "position".speed; double-quote columns named position, type, timestamp, condition, datetime):
${catalog()}

Each table holds recent records only (up to 2,000 per topic), and each record is one event, so a vehicle appears many times - use the latest row per vehicle (e.g. arg_max(col, kafka_timestamp)) when asked about current state. Joins, GROUP BY and aggregates all work. If a query fails, read the error, fix the SQL, and try again.

This is a chat UI. Answer in 1-3 short sentences of plain prose: no markdown, no lists. Cite the actual values you found (vehicle IDs, numbers, times). If a query returns nothing, say so plainly; never invent data.`;

export async function askAboutFleetData(conversation, { maxSteps = 6 } = {}) {
  const baseUrl = process.env.LLM_BASE_URL;
  const model = process.env.CHAT_MODEL || process.env.LLM_MODEL;
  if (!baseUrl || !model) {
    throw new Error('Set LLM_BASE_URL and LLM_MODEL (or CHAT_MODEL) to use /api/ask - see docker-compose.yml.');
  }
  if (!Array.isArray(conversation) || !conversation.length) {
    throw new Error('askAboutFleetData requires a non-empty conversation array.');
  }

  // Cap how much prior chat history gets resent each turn, so a long demo
  // session doesn't grow the request unbounded.
  const messages = [{ role: 'system', content: SYSTEM_PROMPT }, ...conversation.slice(-12)];
  const trace = [];
  let nudged = false;

  for (let step = 0; step < maxSteps; step++) {
    // max_tokens and the timeout matter beyond this one request: the Flink
    // AI job shares the same model server, and a runaway generation (small
    // models occasionally loop) queues its ML_PREDICT calls behind it.
    const resp = await fetch(`${baseUrl}/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model, messages, tools: TOOLS, temperature: 0.2, max_tokens: 400 }),
      signal: AbortSignal.timeout(60000),
    });
    if (!resp.ok) throw new Error(`LLM request failed: ${resp.status} ${await resp.text()}`);
    const msg = (await resp.json()).choices[0].message;
    messages.push(msg);

    if (!msg.tool_calls?.length) {
      // The system prompt says every answer must come from a query; this
      // enforces it. An answer only counts once a queryData call has
      // succeeded. Without that, models answer anyway: qwen2.5:7b, given
      // this prompt, skipped the tools every time and invented vehicles
      // ("V123456") and counts, and small models say "let me fix that query"
      // after an error and then stop. Push back once; if the model still
      // won't query, say so rather than pass on an ungrounded answer.
      const grounded = trace.some((t) => t.tool === 'queryData' && !t.result?.error);
      if (grounded) return { answer: msg.content, trace };
      if (!nudged) {
        nudged = true;
        messages.push({
          role: 'user',
          content: trace.length
            ? 'Run the corrected query now with the queryData tool, then answer from its result.'
            : "You haven't queried the data yet, so that answer isn't based on it. Call queryData now, then answer from its result.",
        });
        continue;
      }
      return { answer: "I couldn't get that from the live data. Try asking it a different way.", trace };
    }

    for (const call of msg.tool_calls) {
      let result;
      try {
        const tool = FLEET_TOOLS[call.function.name];
        if (!tool) throw new Error(`Unknown tool ${call.function.name}`);
        result = await tool.run(JSON.parse(call.function.arguments || '{}'));
      } catch (err) {
        result = { error: err.message };
      }
      trace.push({ tool: call.function.name, arguments: call.function.arguments, result });
      messages.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(result) });
    }
  }

  return { answer: "I wasn't able to find a confident answer within the allotted number of steps.", trace };
}
