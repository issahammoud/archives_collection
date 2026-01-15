.PHONY: all build run stop clean logs help dev test test-cov collect collect-status collect-stop

ifeq (, $(shell command -v nvidia-smi))
  DETECTED_GPU_MODE := NONE
else
  DETECTED_GPU_MODE := GPU
endif

EMBEDDING_MODE ?= $(DETECTED_GPU_MODE)

ifeq ($(EMBEDDING_MODE),GPU)
  BASE_IMAGE := vllm/vllm-openai:v0.8.5
else
  BASE_IMAGE := python:3.12-slim
endif


export UID := $(shell id -u)
export GID := $(shell id -g)
export EMBEDDING_MODE
export BASE_IMAGE
export COMPOSE_PROJECT_NAME := archives_collection

_LABEL := com.docker.compose.project=$(COMPOSE_PROJECT_NAME)

all: run


build:
	@echo "-> Building all services (embedding mode=$(EMBEDDING_MODE), base=$(BASE_IMAGE))"
	@echo "EMBEDDING_MODE=$(EMBEDDING_MODE)" > .env.local
	@echo "BASE_IMAGE=$(BASE_IMAGE)"        >> .env.local
	mkdir -p ~/archives_collection_images/
	docker compose build

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
	@echo "-> Cleaning up Docker resources for project '$(COMPOSE_PROJECT_NAME)'..."
	@if [ -n "$$(docker ps -aq)" ]; then \
	  docker rm -vf $$(docker ps -aq); \
	fi
	@if [ -n "$$(docker images -aq)" ]; then \
	  docker rmi -f $$(docker images -aq); \
	fi
	docker system prune -f
	rm -rf .env.local

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

help:
	@echo "Available commands:"
	@echo "  make build [EMBEDDING_MODE=<gpu|none>] Build all services"
	@echo "  make run                               Run all services (detached)"
	@echo "  make dev                               Run all services (attached)"
	@echo "  make stop                              Stop all services"
	@echo "  make clean                             Remove all containers, images, and networks"
	@echo "  make logs                              Tail all logs"
	@echo "  make logs-backend                      Tail backend logs"
	@echo "  make logs-frontend                     Tail frontend logs"
	@echo "  make logs-celery                       Tail celery logs"
	@echo "  make shell-backend                     Open shell in backend container"
	@echo "  make shell-frontend                    Open shell in frontend container"
	@echo "  make test                              Run backend tests"
	@echo "  make test-cov                          Run backend tests with coverage"
	@echo "  make collect                           Start data collection (see options below)"
	@echo "  make collect-status TASK_ID=<id>       Check collection task status"
	@echo "  make collect-stop TASK_ID=<id>         Stop a running collection task"
	@echo "  make help                              Show this message"
	@echo ""
	@echo "Collection options:"
	@echo "  BEGIN=YYYY-MM-DD                       Start date (default: yesterday)"
	@echo "  END=YYYY-MM-DD                         End date (default: today)"
	@echo "  ARCHIVES='[\"lemonde\",\"lesechos\"]'    JSON array of archives (default: all)"
	@echo ""
	@echo "Collection examples:"
	@echo "  make collect                           Collect yesterday to today, all archives"
	@echo "  make collect BEGIN=2024-01-01 END=2024-01-31"
	@echo "  make collect ARCHIVES='[\"lemonde\"]' BEGIN=2024-06-01 END=2024-06-30"
	@echo ""
	@echo "Access points:"
	@echo "  Frontend:   http://localhost:3000"
	@echo "  Backend:    http://localhost:8000"
	@echo "  API Docs:   http://localhost:8000/docs"
