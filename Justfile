set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Process queued company events.
worker:
    uv run --locked python -m app.consumer

# Serve company recommendations.
api:
    uv run --locked uvicorn app.api:app

# Stop the services and keep the local data.
stop:
    docker compose down
