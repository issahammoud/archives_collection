.PHONY: all build run stop clean logs help dev test test-cov

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
	@echo "  make help                              Show this message"
	@echo ""
	@echo "Access points:"
	@echo "  Frontend:   http://localhost:3000"
	@echo "  Backend:    http://localhost:8000"
	@echo "  API Docs:   http://localhost:8000/docs"
