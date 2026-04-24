#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "DEPLOY NODE.JS PIXEL SERVER (PROD)"
echo "======================================"

SERVER_APP_NAME="pixel-server"
WORKER_APP_NAME_PREFIX="pixel-worker"
IMAGE_NAME="pixel-server:latest"
HOST_PORT=9000
CONTAINER_PORT=9000
WORKER_COUNT="${WORKER_COUNT:-2}"
QUEUE_ROOT_DIR="$(pwd)/data"
QUEUE_DIR_IN_CONTAINER="${BIGQUERY_QUEUE_DIR:-data/bigquery-queue}"
BIGQUERY_ENABLED="${BIGQUERY_ENABLED:-true}"
BIGQUERY_DATASET="${BIGQUERY_DATASET:-playable_tracking}"
BIGQUERY_TABLE="${BIGQUERY_TABLE:-pixel_events_ver_2}"
BIGQUERY_BATCH_SIZE="${BIGQUERY_BATCH_SIZE:-100}"
BIGQUERY_QUEUE_SHARDS="${BIGQUERY_QUEUE_SHARDS:-4}"
BIGQUERY_RETRY_DELAY_MS="${BIGQUERY_RETRY_DELAY_MS:-30000}"
BIGQUERY_WORKER_POLL_MS="${BIGQUERY_WORKER_POLL_MS:-1000}"
BIGQUERY_WORKER_LEASE_MS="${BIGQUERY_WORKER_LEASE_MS:-120000}"
BIGQUERY_ERROR_LOG_INTERVAL_MS="${BIGQUERY_ERROR_LOG_INTERVAL_MS:-10000}"

# =========================
# REQUIRED FILES
# =========================
KEY_FILE="$(pwd)/app/credentials/pixel-writer-key.json"

if [[ ! -f "$KEY_FILE" ]]; then
  echo "Missing pixel-writer-key.json"
  exit 1
fi

mkdir -p "$QUEUE_ROOT_DIR"

# =========================
# CHECK DOCKER
# =========================
command -v docker >/dev/null 2>&1 || {
  echo "Docker is not installed"
  exit 1
}

# =========================
# DETECT PUBLIC IP
# =========================
SERVER_IP=$(
  curl -s --max-time 3 https://api.ipify.org || \
  curl -s --max-time 3 https://ifconfig.me || \
  curl -s --max-time 3 https://icanhazip.com || \
  echo ""
)

# =========================
# BUILD IMAGE
# =========================
echo "Building Docker image..."
docker build -t "$IMAGE_NAME" .

# =========================
# STOP OLD CONTAINERS
# =========================
if docker ps -a --format '{{.Names}}' | grep -q "^${SERVER_APP_NAME}$"; then
  docker rm -f "$SERVER_APP_NAME"
fi

for i in $(seq 1 "$WORKER_COUNT"); do
  WORKER_NAME="${WORKER_APP_NAME_PREFIX}-${i}"
  if docker ps -a --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
    docker rm -f "$WORKER_NAME"
  fi
done

# =========================
# RUN CONTAINER
# =========================
echo "Starting container..."

docker run -d \
  --name "$SERVER_APP_NAME" \
  --restart always \
  -p ${HOST_PORT}:${CONTAINER_PORT} \
  -e NODE_ENV=production \
  -e PORT=${CONTAINER_PORT} \
  -e BIGQUERY_ENABLED=${BIGQUERY_ENABLED} \
  -e BIGQUERY_DATASET=${BIGQUERY_DATASET} \
  -e BIGQUERY_TABLE=${BIGQUERY_TABLE} \
  -e BIGQUERY_BATCH_SIZE=${BIGQUERY_BATCH_SIZE} \
  -e BIGQUERY_QUEUE_DIR=${QUEUE_DIR_IN_CONTAINER} \
  -e BIGQUERY_QUEUE_SHARDS=${BIGQUERY_QUEUE_SHARDS} \
  -e BIGQUERY_RETRY_DELAY_MS=${BIGQUERY_RETRY_DELAY_MS} \
  -e BIGQUERY_WORKER_POLL_MS=${BIGQUERY_WORKER_POLL_MS} \
  -e BIGQUERY_WORKER_LEASE_MS=${BIGQUERY_WORKER_LEASE_MS} \
  -e BIGQUERY_ERROR_LOG_INTERVAL_MS=${BIGQUERY_ERROR_LOG_INTERVAL_MS} \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/pixel-writer-key.json \
  -v "${QUEUE_ROOT_DIR}":/app/data \
  -v "$KEY_FILE":/app/credentials/pixel-writer-key.json:ro \
  "$IMAGE_NAME"

for i in $(seq 1 "$WORKER_COUNT"); do
  WORKER_NAME="${WORKER_APP_NAME_PREFIX}-${i}"
  docker run -d \
    --name "$WORKER_NAME" \
    --restart always \
    -e NODE_ENV=production \
    -e BIGQUERY_ENABLED=${BIGQUERY_ENABLED} \
    -e BIGQUERY_DATASET=${BIGQUERY_DATASET} \
    -e BIGQUERY_TABLE=${BIGQUERY_TABLE} \
    -e BIGQUERY_BATCH_SIZE=${BIGQUERY_BATCH_SIZE} \
    -e BIGQUERY_QUEUE_DIR=${QUEUE_DIR_IN_CONTAINER} \
    -e BIGQUERY_QUEUE_SHARDS=${BIGQUERY_QUEUE_SHARDS} \
    -e BIGQUERY_RETRY_DELAY_MS=${BIGQUERY_RETRY_DELAY_MS} \
    -e BIGQUERY_WORKER_POLL_MS=${BIGQUERY_WORKER_POLL_MS} \
    -e BIGQUERY_WORKER_LEASE_MS=${BIGQUERY_WORKER_LEASE_MS} \
    -e BIGQUERY_ERROR_LOG_INTERVAL_MS=${BIGQUERY_ERROR_LOG_INTERVAL_MS} \
    -e BIGQUERY_WORKER_NAME="${WORKER_NAME}" \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/pixel-writer-key.json \
    -v "${QUEUE_ROOT_DIR}":/app/data \
    -v "$KEY_FILE":/app/credentials/pixel-writer-key.json:ro \
    "$IMAGE_NAME" \
    node src/worker.js
done

# =========================
# HEALTH CHECK
# =========================
sleep 5
if curl -fs "http://127.0.0.1:${HOST_PORT}/health" >/dev/null; then
  echo "DEPLOY SUCCESS"
else
  docker logs "$SERVER_APP_NAME"
  exit 1
fi

# =========================
# PRINT URL
# =========================
echo "======================================"
if [ -n "$SERVER_IP" ]; then
  echo "Health URL:"
  echo "http://${SERVER_IP}:${HOST_PORT}/health"
  echo "Pixel URL:"
  echo "http://${SERVER_IP}:${HOST_PORT}/p.gif?e=test&sid=demo"
else
  echo "Server running on port ${HOST_PORT}"
fi
echo "======================================"
