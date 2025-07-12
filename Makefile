.PHONY: all build run stop clean jupyter logs help

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
	@echo "→ Building all services (embedding mode=$(EMBEDDING_MODE), base=$(BASE_IMAGE))"
	@echo "EMBEDDING_MODE=$(EMBEDDING_MODE)" > .env.local
	@echo "BASE_IMAGE=$(BASE_IMAGE)"        >> .env.local
	mkdir -p ~/archives_collection_images/
	docker compose build

run:
	@echo "→ Starting all services"
	docker compose up -d

stop:
	@echo "→ Stopping all services…"
	docker compose down

clean: stop
	@echo "→ Cleaning up Docker resources for project '$(COMPOSE_PROJECT_NAME)'…"
	@if [ -n "$$(docker ps -aq)" ]; then \
	  docker rm -vf $$(docker ps -aq); \
	fi
	@if [ -n "$$(docker images -aq)" ]; then \
	  docker rmi -f $$(docker images -aq); \
	fi
	docker system prune -f
	rm -rf .env.local

jupyter:
	@echo "→ Run a jupyter notebook. Connect to localhost:8888 from your editor..."
	docker compose exec webapp bash -c 'export HOME=/tmp && jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --IdentityProvider.token="" 2>&1'

logs:
	@echo "→ Showing all containers logs..."
	docker compose logs -f

help:
	@echo "Available commands:"
	@echo "  make build [EMBEDDING_MODE=<gpu|none>] Build all services"
	@echo "  make run                               Run all services"
	@echo "  make stop                              Stop all services"
	@echo "  make clean                             Remove all containers, images, and networks"
	@echo "  make logs                              Tail all logs"
	@echo "  make jupyter                           Open Jupyter in webapp_container"
	@echo "  make help                              Show this message"
