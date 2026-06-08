#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-ducklauncher-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-ducklauncher}"
# Default to 5433 to avoid conflicting with a local Homebrew Postgres on 5432 (common on macOS).
POSTGRES_PORT="${POSTGRES_PORT:-5433}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18}"

export DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
export TEST_DATABASE_URL="${DATABASE_URL}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required. Install Docker Desktop or use Homebrew Postgres instead:" >&2
  echo "  brew install postgresql@18 && brew services start postgresql@18" >&2
  echo "  createdb ducklauncher" >&2
  exit 1
fi

if lsof -iTCP:5432 -sTCP:LISTEN 2>/dev/null | grep -qv docker; then
  echo "Note: something other than Docker is listening on port 5432 (often Homebrew Postgres)."
  echo "This script uses port ${POSTGRES_PORT} for the container to avoid that conflict."
  echo
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  if docker ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    echo "PostgreSQL already running in container: ${CONTAINER_NAME}"
  else
    echo "Starting existing container: ${CONTAINER_NAME}"
    docker start "${CONTAINER_NAME}" >/dev/null
  fi
else
  echo "Creating PostgreSQL container: ${CONTAINER_NAME}"
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_USER="${POSTGRES_USER}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" >/dev/null
fi

echo "Waiting for PostgreSQL to accept connections..."
for _ in $(seq 1 30); do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
    echo
    echo "PostgreSQL is ready."
    echo "DATABASE_URL=${DATABASE_URL}"
    echo
    echo "Run tests:"
    echo "  export DATABASE_URL='${DATABASE_URL}'"
    echo "  uv run pytest tests/ -v"
    echo
    echo "Stop:"
    echo "  docker stop ${CONTAINER_NAME}"
    echo
    echo "Remove:"
    echo "  docker rm -f ${CONTAINER_NAME}"
    exit 0
  fi
  sleep 1
done

echo "PostgreSQL did not become ready in time." >&2
docker logs "${CONTAINER_NAME}" | tail -20 >&2 || true
exit 1
