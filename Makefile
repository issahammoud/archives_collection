.PHONY: all build run stop clean logs help dev test test-cov collect collect-status collect-stop migrate migrate-create

export COMPOSE_PROJECT_NAME := archives_collection

all: run

build:
	@echo "-> Building all services"
	docker compose build

build-nc:
	@echo "-> Building all services (no cache)"
	docker compose build --no-cache

run:
	@echo "-> Starting all services"
	docker compose up -d

dev:
	@echo "-> Starting all services in development mode"
	docker compose up

restart:
	@echo "-> Restarting all services"
	docker compose restart

stop:
	@echo "-> Stopping all services..."
	docker compose down

clean: stop
	@echo "-> Cleaning up Docker resources..."
	docker system prune -f

logs:
	@echo "-> Showing all containers logs..."
	docker compose logs -f

logs-backend:
	@echo "-> Showing backend logs..."
	docker compose logs -f backend

logs-frontend:
	@echo "-> Showing frontend logs..."
	docker compose logs -f frontend

logs-celery:
	@echo "-> Showing celery logs..."
	docker compose logs -f celery

shell-backend:
	@echo "-> Opening shell in backend container..."
	docker compose exec backend bash

shell-frontend:
	@echo "-> Opening shell in frontend container..."
	docker compose exec frontend sh

test:
	@echo "-> Running backend tests..."
	docker compose exec backend python -m pytest tests/ -v

test-cov:
	@echo "-> Running backend tests with coverage..."
	docker compose exec backend python -m pytest tests/ -v --cov=app --cov-report=term-missing

# Database migration commands
migrate:
	@echo "-> Running database migrations..."
	docker compose exec backend alembic upgrade head

migrate-create:
	@echo "-> Creating new migration..."
	@if [ -z "$(MSG)" ]; then \
		echo "Error: MSG is required. Usage: make migrate-create MSG='migration message'"; \
		exit 1; \
	fi
	docker compose exec backend alembic revision --autogenerate -m "$(MSG)"

migrate-downgrade:
	@echo "-> Rolling back last migration..."
	docker compose exec backend alembic downgrade -1

migrate-history:
	@echo "-> Showing migration history..."
	docker compose exec backend alembic history

# Collection commands
# Usage: make collect BEGIN=2024-01-01 END=2024-01-31 [ARCHIVES='["lemonde","lesechos"]']
BEGIN ?= $(shell date -d "yesterday" +%Y-%m-%d)
END ?= $(shell date +%Y-%m-%d)
ARCHIVES ?= null

collect:
	@echo "-> Starting collection from $(BEGIN) to $(END)..."
	@curl -s -X POST "http://localhost:8000/api/v1/tasks/collection/start" \
		-H "Content-Type: application/json" \
		-d '{"archives": $(ARCHIVES), "begin_date": "$(BEGIN)", "end_date": "$(END)"}'
	@echo ""

collect-status:
	@echo "-> Checking collection status..."
	@if [ -z "$(TASK_ID)" ]; then \
		echo "Error: TASK_ID is required. Usage: make collect-status TASK_ID=<task_id>"; \
		exit 1; \
	fi
	@curl -s "http://localhost:8000/api/v1/tasks/collection/status/$(TASK_ID)"
	@echo ""

collect-stop:
	@echo "-> Stopping collection..."
	@if [ -z "$(TASK_ID)" ]; then \
		echo "Error: TASK_ID is required. Usage: make collect-stop TASK_ID=<task_id>"; \
		exit 1; \
	fi
	@curl -s -X POST "http://localhost:8000/api/v1/tasks/collection/stop?task_id=$(TASK_ID)"
	@echo ""

# Backup commands
backup:
	@echo "-> Creating database backup..."
	./scripts/backup_db.sh

help:
	@echo "Available commands:"
	@echo "  make build                              Build all services"
	@echo "  make run                                Run all services (detached)"
	@echo "  make dev                                Run all services (attached)"
	@echo "  make stop                               Stop all services"
	@echo "  make clean                              Remove all containers and prune"
	@echo "  make logs                               Tail all logs"
	@echo "  make logs-backend                       Tail backend logs"
	@echo "  make logs-frontend                      Tail frontend logs"
	@echo "  make logs-celery                        Tail celery logs"
	@echo "  make shell-backend                      Open shell in backend container"
	@echo "  make shell-frontend                     Open shell in frontend container"
	@echo "  make test                               Run backend tests"
	@echo "  make test-cov                           Run backend tests with coverage"
	@echo "  make migrate                            Run database migrations"
	@echo "  make migrate-create MSG='message'       Create new migration"
	@echo "  make migrate-downgrade                  Rollback last migration"
	@echo "  make migrate-history                    Show migration history"
	@echo "  make backup                             Create database backup"
	@echo "  make collect                            Start data collection"
	@echo "  make collect-status TASK_ID=<id>        Check collection task status"
	@echo "  make collect-stop TASK_ID=<id>          Stop a running collection task"
	@echo "  make help                               Show this message"
	@echo ""
	@echo "Collection options:"
	@echo "  BEGIN=YYYY-MM-DD                        Start date (default: yesterday)"
	@echo "  END=YYYY-MM-DD                          End date (default: today)"
	@echo "  ARCHIVES='[\"lemonde\",\"lesechos\"]'     JSON array of archives (default: all)"
	@echo ""
	@echo "Access points:"
	@echo "  Frontend:   http://localhost:3000"
	@echo "  Backend:    http://localhost:8000"
	@echo "  API Docs:   http://localhost:8000/docs"
