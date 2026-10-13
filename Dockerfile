# Runs fleet-context-engine (Kafka consumer + HTTP API) and, from the same
# process, serves fleet-intelligence-console.html + fonts/ - see the
# STATIC_ROOT comment in fleet-context-engine/http-server.js for why both
# live under one process. The same image also runs the one-shot seed
# (`node seed.js`) and schema-init (`node register-schemas.js`) - see
# docker-compose.yml.
#
# Normally built and run by `docker compose up` from the repo root.

FROM node:20-slim

WORKDIR /app

COPY fleet-context-engine/package*.json ./fleet-context-engine/
RUN cd fleet-context-engine && npm ci --omit=dev

COPY fleet-context-engine ./fleet-context-engine
COPY schemas ./schemas
COPY fleet-intelligence-console.html ./
COPY fonts ./fonts

# The Java-compatible default partitioner is what we want (matches Flink).
ENV KAFKAJS_NO_PARTITIONER_WARNING=1
ENV PORT=8787
EXPOSE 8787

WORKDIR /app/fleet-context-engine
CMD ["node", "index.js"]
