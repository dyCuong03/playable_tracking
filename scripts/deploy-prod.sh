#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "DEPLOY NODE.JS PIXEL SERVER (PROD)"
echo "======================================"

SERVER_APP_PREFIX="pixel-server"
LEGACY_SERVER_APP_NAME="pixel-server"
WORKER_APP_NAME_PREFIX="pixel-worker"
LEGACY_WORKER_APP_NAME="pixel-worker"
REDIS_APP_NAME="pixel-redis"
NGINX_APP_NAME="pixel-nginx"
NETWORK_NAME="pixel-server-net"
IMAGE_NAME="pixel-server:latest"
HOST_PORT=9000
CONTAINER_PORT=9000
WORKER_COUNT="${WORKER_COUNT:-2}"
APP_REPLICAS="${APP_REPLICAS:-4}"
WEB_CONCURRENCY="${WEB_CONCURRENCY:-4}"
TRACKING_RESPONSE_MODE="${TRACKING_RESPONSE_MODE:-empty}"
QUEUE_ROOT_DIR="$(pwd)/data"
QUEUE_DIR_IN_CONTAINER="${BIGQUERY_QUEUE_DIR:-data/bigquery-queue}"
BIGQUERY_ENABLED="${BIGQUERY_ENABLED:-true}"
BIGQUERY_DATASET="${BIGQUERY_DATASET:-playable_tracking}"
BIGQUERY_TABLE="${BIGQUERY_TABLE:-pixel_events_ver_2}"
BIGQUERY_BATCH_SIZE="${BIGQUERY_BATCH_SIZE:-100}"
BIGQUERY_QUEUE_READ_BATCH="${BIGQUERY_QUEUE_READ_BATCH:-1000}"
BIGQUERY_MAX_RETRIES="${BIGQUERY_MAX_RETRIES:-5}"
BIGQUERY_QUEUE_SHARDS="${BIGQUERY_QUEUE_SHARDS:-4}"
BIGQUERY_RETRY_DELAY_MS="${BIGQUERY_RETRY_DELAY_MS:-30000}"
BIGQUERY_WORKER_POLL_MS="${BIGQUERY_WORKER_POLL_MS:-1000}"
BIGQUERY_WORKER_LEASE_MS="${BIGQUERY_WORKER_LEASE_MS:-120000}"
BIGQUERY_ERROR_LOG_INTERVAL_MS="${BIGQUERY_ERROR_LOG_INTERVAL_MS:-10000}"
REDIS_URL="${REDIS_URL:-redis://${REDIS_APP_NAME}:6379}"
REDIS_QUEUE_STREAM="${REDIS_QUEUE_STREAM:-pixel:events}"
REDIS_QUEUE_GROUP="${REDIS_QUEUE_GROUP:-pixel-workers}"
REDIS_REJECTED_STREAM="${REDIS_REJECTED_STREAM:-pixel:rejected}"
REDIS_QUEUE_MAXLEN="${REDIS_QUEUE_MAXLEN:-1000000}"
NGINX_CONFIG_TEMPLATE="$(pwd)/scripts/nginx-pixel.conf.template"
NGINX_CONFIG_RENDERED="${QUEUE_ROOT_DIR}/nginx-pixel.conf"

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
if docker ps -a --format '{{.Names}}' | grep -q "^${REDIS_APP_NAME}$"; then
  docker rm -f "$REDIS_APP_NAME"
fi

if docker ps -a --format '{{.Names}}' | grep -q "^${NGINX_APP_NAME}$"; then
  docker rm -f "$NGINX_APP_NAME"
fi

if docker ps -a --format '{{.Names}}' | grep -q "^${LEGACY_SERVER_APP_NAME}$"; then
  docker rm -f "$LEGACY_SERVER_APP_NAME"
fi

if docker ps -a --format '{{.Names}}' | grep -q "^${LEGACY_WORKER_APP_NAME}$"; then
  docker rm -f "$LEGACY_WORKER_APP_NAME"
fi

for i in $(seq 1 "$APP_REPLICAS"); do
  SERVER_APP_NAME="${SERVER_APP_PREFIX}-${i}"
  if docker ps -a --format '{{.Names}}' | grep -q "^${SERVER_APP_NAME}$"; then
    docker rm -f "$SERVER_APP_NAME"
  fi
done

for i in $(seq 1 "$WORKER_COUNT"); do
  WORKER_NAME="${WORKER_APP_NAME_PREFIX}-${i}"
  if docker ps -a --format '{{.Names}}' | grep -q "^${WORKER_NAME}$"; then
    docker rm -f "$WORKER_NAME"
  fi
done

if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  docker network create "$NETWORK_NAME"
fi

docker run -d \
  --name "$REDIS_APP_NAME" \
  --restart always \
  --network "$NETWORK_NAME" \
  --ulimit nofile=200000:200000 \
  redis:7-alpine \
  redis-server --save "" --appendonly no

UPSTREAM_SERVERS=""
for i in $(seq 1 "$APP_REPLICAS"); do
  SERVER_APP_NAME="${SERVER_APP_PREFIX}-${i}"
  printf -v UPSTREAM_SERVERS '%s        server %s:%s max_fails=3 fail_timeout=5s;\n' "$UPSTREAM_SERVERS" "$SERVER_APP_NAME" "$CONTAINER_PORT"
done

awk -v upstream="$UPSTREAM_SERVERS" '{gsub(/__UPSTREAM_SERVERS__/, upstream)}1' "$NGINX_CONFIG_TEMPLATE" > "$NGINX_CONFIG_RENDERED"

# =========================
# RUN CONTAINER
# =========================
echo "Starting container..."

for i in $(seq 1 "$APP_REPLICAS"); do
  SERVER_APP_NAME="${SERVER_APP_PREFIX}-${i}"
  docker run -d \
    --name "$SERVER_APP_NAME" \
    --restart always \
    --network "$NETWORK_NAME" \
    --ulimit nofile=200000:200000 \
    -e NODE_ENV=production \
    -e PORT=${CONTAINER_PORT} \
    -e WEB_CONCURRENCY=${WEB_CONCURRENCY} \
    -e TRACKING_RESPONSE_MODE=${TRACKING_RESPONSE_MODE} \
    -e BIGQUERY_ENABLED=${BIGQUERY_ENABLED} \
    -e BIGQUERY_DATASET=${BIGQUERY_DATASET} \
    -e BIGQUERY_TABLE=${BIGQUERY_TABLE} \
    -e BIGQUERY_BATCH_SIZE=${BIGQUERY_BATCH_SIZE} \
    -e BIGQUERY_QUEUE_READ_BATCH=${BIGQUERY_QUEUE_READ_BATCH} \
    -e BIGQUERY_MAX_RETRIES=${BIGQUERY_MAX_RETRIES} \
    -e BIGQUERY_QUEUE_DIR=${QUEUE_DIR_IN_CONTAINER} \
    -e BIGQUERY_QUEUE_SHARDS=${BIGQUERY_QUEUE_SHARDS} \
    -e BIGQUERY_RETRY_DELAY_MS=${BIGQUERY_RETRY_DELAY_MS} \
    -e BIGQUERY_WORKER_POLL_MS=${BIGQUERY_WORKER_POLL_MS} \
    -e BIGQUERY_WORKER_LEASE_MS=${BIGQUERY_WORKER_LEASE_MS} \
    -e BIGQUERY_ERROR_LOG_INTERVAL_MS=${BIGQUERY_ERROR_LOG_INTERVAL_MS} \
    -e REDIS_URL=${REDIS_URL} \
    -e REDIS_QUEUE_STREAM=${REDIS_QUEUE_STREAM} \
    -e REDIS_QUEUE_GROUP=${REDIS_QUEUE_GROUP} \
    -e REDIS_REJECTED_STREAM=${REDIS_REJECTED_STREAM} \
    -e REDIS_QUEUE_MAXLEN=${REDIS_QUEUE_MAXLEN} \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/pixel-writer-key.json \
    -v "${QUEUE_ROOT_DIR}":/app/data \
    -v "$KEY_FILE":/app/credentials/pixel-writer-key.json:ro \
    "$IMAGE_NAME"
done

docker run -d \
  --name "$NGINX_APP_NAME" \
  --restart always \
  --network "$NETWORK_NAME" \
  --ulimit nofile=200000:200000 \
  -p ${HOST_PORT}:${CONTAINER_PORT} \
  -v "${NGINX_CONFIG_RENDERED}":/etc/nginx/nginx.conf:ro \
  nginx:1.27-alpine

for i in $(seq 1 "$WORKER_COUNT"); do
  WORKER_NAME="${WORKER_APP_NAME_PREFIX}-${i}"
  docker run -d \
    --name "$WORKER_NAME" \
    --restart always \
    --network "$NETWORK_NAME" \
    --ulimit nofile=200000:200000 \
    --no-healthcheck \
    -e NODE_ENV=production \
    -e BIGQUERY_ENABLED=${BIGQUERY_ENABLED} \
    -e BIGQUERY_DATASET=${BIGQUERY_DATASET} \
    -e BIGQUERY_TABLE=${BIGQUERY_TABLE} \
    -e BIGQUERY_BATCH_SIZE=${BIGQUERY_BATCH_SIZE} \
    -e BIGQUERY_QUEUE_READ_BATCH=${BIGQUERY_QUEUE_READ_BATCH} \
    -e BIGQUERY_MAX_RETRIES=${BIGQUERY_MAX_RETRIES} \
    -e BIGQUERY_QUEUE_DIR=${QUEUE_DIR_IN_CONTAINER} \
    -e BIGQUERY_QUEUE_SHARDS=${BIGQUERY_QUEUE_SHARDS} \
    -e BIGQUERY_RETRY_DELAY_MS=${BIGQUERY_RETRY_DELAY_MS} \
    -e BIGQUERY_WORKER_POLL_MS=${BIGQUERY_WORKER_POLL_MS} \
    -e BIGQUERY_WORKER_LEASE_MS=${BIGQUERY_WORKER_LEASE_MS} \
    -e BIGQUERY_ERROR_LOG_INTERVAL_MS=${BIGQUERY_ERROR_LOG_INTERVAL_MS} \
    -e BIGQUERY_WORKER_NAME="${WORKER_NAME}" \
    -e REDIS_URL=${REDIS_URL} \
    -e REDIS_QUEUE_STREAM=${REDIS_QUEUE_STREAM} \
    -e REDIS_QUEUE_GROUP=${REDIS_QUEUE_GROUP} \
    -e REDIS_REJECTED_STREAM=${REDIS_REJECTED_STREAM} \
    -e REDIS_QUEUE_MAXLEN=${REDIS_QUEUE_MAXLEN} \
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
  docker logs "$NGINX_APP_NAME"
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
