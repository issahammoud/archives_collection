DOCKER_COMPOSE = docker compose
UID := $(shell id -u)
GID := $(shell id -g)


all: run


build:
	@echo "Building Docker image..."
	UID=$(UID) GID=$(GID) $(DOCKER_COMPOSE) build

run:
	@echo "Running the service..."
	UID=$(UID) GID=$(GID) $(DOCKER_COMPOSE) up

stop:
	@echo "Stopping the service..."
	UID=$(UID) GID=$(GID) $(DOCKER_COMPOSE) down

clean:
	@echo "Cleaning up Docker resources..."
	@if [ -n "$$(docker ps -aq)" ]; then docker rm -vf $$(docker ps -aq); fi
	@if [ -n "$$(docker images -aq)" ]; then docker rmi -f $$(docker images -aq); fi
	docker system prune -f

log:
	@echo "Showing webapp logs, webapp container should be running..."
	docker exec -it webapp_container tail -f /tmp/logs.log
help:
	@echo "Available commands:"
	@echo "  make build               - Build the Docker image"
	@echo "  make run                 - Run the service"
	@echo "  make stop                - Stop the service"
	@echo "  make clean               - Clean up Docker resources"
	@echo "  make help                - Show this help message"
