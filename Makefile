# Shortcuts for the local fleet demo. Everything here is plain docker compose
# underneath - see docker-compose.yml.

# Flink's ML_PREDICT recommendations, and the console's chat agent.
LLM_MODEL ?= qwen2.5:7b
CHAT_MODEL ?= qwen2.5:3b

.PHONY: help up down reset status logs jobs replace sql duckdb model

help:
	@echo "make up            build and start everything (needs Ollama running: make model)"
	@echo "make down          stop everything, keep data"
	@echo "make reset         stop everything and delete all data (topics, checkpoints, Iceberg tables)"
	@echo "make status        containers and Flink jobs"
	@echo "make logs          follow all logs (Ctrl-C to stop)"
	@echo "make replace JOB=ai   redeploy one Flink job after editing local/flink/sql/<nn>-JOB.sql"
	@echo "make sql           Flink SQL shell with every topic as a table"
	@echo "make duckdb        DuckDB shell on the Iceberg tables"
	@echo "make model         pull the models into Ollama ($(LLM_MODEL), $(CHAT_MODEL))"

up:
	docker compose up -d --build
	@echo
	@echo "console:      http://localhost:$${APP_PORT:-8787}"
	@echo "MCP:          http://localhost:$${APP_PORT:-8787}/mcp"
	@echo "Flink UI:     http://localhost:8081"
	@echo "Kafka UI:     http://localhost:8080"
	@echo "object store: http://localhost:9001/rustfs/console/  (fleet / fleet-secret)"

down:
	docker compose down

reset:
	docker compose down -v

status:
	@docker compose ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'
	@echo
	@$(MAKE) --no-print-directory jobs

jobs:
	@docker compose exec flink-jobmanager bash -c \
	  "curl -s localhost:8081/jobs/overview | jq -r '.jobs[] | select(.state != \"CANCELED\") | \"\(.name)\t\(.state)\"'"

logs:
	docker compose logs -f

replace:
	@test -n "$(JOB)" || { echo "usage: make replace JOB=business|ai|lakehouse"; exit 1; }
	docker compose run --rm flink-sql-init submit-jobs.sh --replace $(JOB)

sql:
	docker compose run --rm flink-sql-init sql-shell.sh

duckdb:
	docker compose run --rm duckdb

model:
	@command -v ollama > /dev/null || { echo "Ollama not found: brew install ollama && brew services start ollama"; exit 1; }
	ollama pull $(LLM_MODEL)
	ollama pull $(CHAT_MODEL)
