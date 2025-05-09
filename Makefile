set-id:
	@echo "UID=$(shell id -u)" > .env.local
	@echo "GID=$(shell id -g)" >> .env.local


all: run


build: set-id
	@echo "Building Docker image..."
	mkdir -p ~/archives_collection_images/
	docker compose build

run:
	@echo "Running the service..."
	docker compose up -d

stop:
	@echo "Stopping the service..."
	docker compose down

clean:
	@echo "Cleaning up Docker resources..."
	@if [ -n "$$(docker ps -aq)" ]; then docker rm -vf $$(docker ps -aq); fi
	@if [ -n "$$(docker images -aq)" ]; then docker rmi -f $$(docker images -aq); fi
	docker system prune -f

jupyter:
	@echo "Run a jupyter notebook. Connect to localhost:8888 from your editor..."
	docker exec -it webapp_container bash -c 'export HOME=/tmp && jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --IdentityProvider.token="" 2>&1'
logs:
	@echo "Showing all containers logs..."
	docker compose logs -f

help:
	@echo "Available commands:"
	@echo "  make build               - Build the Docker image"
	@echo "  make run                 - Run the service"
	@echo "  make stop                - Stop the service"
	@echo "  make clean               - Clean up Docker resources"
	@echo "  make jupyter             - Run a jupyter notebook"
	@echo "  make logs                - Show containers logs"
	@echo "  make help                - Show this help message"
