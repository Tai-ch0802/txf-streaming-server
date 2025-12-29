.PHONY: up down clean logs help

# Default target
help:
	@echo "Available commands:"
	@echo "  make up      - Start Kafka services in background"
	@echo "  make down    - Stop Kafka services"
	@echo "  make clean   - Stop services and remove volumes (reset data)"
	@echo "  make tail    - Tail Kafka logs"

up:
	docker-compose -f ops/docker-compose.yaml up -d

down:
	docker-compose -f ops/docker-compose.yaml down

clean:
	docker-compose -f ops/docker-compose.yaml down -v

tail:
	docker-compose -f ops/docker-compose.yaml logs -f
