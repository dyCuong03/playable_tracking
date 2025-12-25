#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "🚀 DEPLOY NODE.JS PROJECT (PORT 9000)"
echo "======================================"

# =========================
# CONFIG
# =========================
APP_NAME="pixel-server"
IMAGE_NAME="pixel-server:latest"
HOST_PORT=9000
CONTAINER_PORT=9000

# =========================
# CHECK DOCKER
# =========================
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ Docker is not installed"
  exit 1
fi

docker --version

# =========================
# BUILD IMAGE
# =========================
echo "📦 Building Docker image..."
docker build -t "$IMAGE_NAME" .

# =========================
# STOP OLD CONTAINER (SAFE)
# =========================
if docker ps -a --format '{{.Names}}' | grep -q "^${APP_NAME}$"; then
  echo "🛑 Stopping existing container..."
  docker stop "$APP_NAME"
  docker rm "$APP_NAME"
else
  echo "ℹ️ First deploy – no container to stop"
fi

# =========================
# RUN CONTAINER
# =========================
echo "▶️ Starting container on port ${HOST_PORT}..."
docker run -d \
  --name "$APP_NAME" \
  --restart always \
  -p 0.0.0.0:${HOST_PORT}:${CONTAINER_PORT} \
  -e NODE_ENV=production \
  -e PORT=${CONTAINER_PORT} \
  "$IMAGE_NAME"

# =========================
# WAIT FOR APP
# =========================
sleep 3

# =========================
# HEALTH CHECK
# =========================
if curl -fs "http://127.0.0.1:${HOST_PORT}/health" >/dev/null; then
  echo "✅ Health check passed"
else
  echo "❌ Health check failed"
  docker logs "$APP_NAME"
  exit 1
fi

echo "======================================"
echo "✅ DEPLOY SUCCESS"
echo "🌐 Service running on:"
echo "👉 http://<SERVER_IP>:${HOST_PORT}/health"
echo "👉 http://<SERVER_IP>:${HOST_PORT}/p.gif"
echo "======================================"
