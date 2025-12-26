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
# DETECT PUBLIC IP (SAFE)
# =========================
echo "🌍 Detecting public IP..."

SERVER_IP=$(
  curl -s --max-time 3 https://api.ipify.org || \
  curl -s --max-time 3 https://ifconfig.me || \
  curl -s --max-time 3 https://icanhazip.com || \
  echo ""
)

if [ -z "$SERVER_IP" ]; then
  echo "⚠️  Cannot detect public IP (network blocked)"
  echo "⚠️  URLs will be shown without IP"
else
  echo "🌍 Server Public IP: $SERVER_IP"
fi

# =========================
# CHECK DOCKER
# =========================
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ Docker is not installed"
  exit 1
fi

# =========================
# BUILD IMAGE
# =========================
echo "📦 Building Docker image..."
docker build -t "$IMAGE_NAME" .

# =========================
# STOP OLD CONTAINER
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
  -v $(pwd)/logs:/app/logs \
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

# =========================
# PRINT READY URL
# =========================
echo "======================================"
echo "✅ DEPLOY SUCCESS"

if [ -n "$SERVER_IP" ]; then
  echo "🌐 Health URL:"
  echo "👉 http://${SERVER_IP}:${HOST_PORT}/health"
  echo "🌐 Pixel URL (READY TO USE):"
  echo "👉 http://${SERVER_IP}:${HOST_PORT}/p.gif?e=test&sid=demo&rnd=$(date +%s)"
else
  echo "🌐 Server is running on port ${HOST_PORT}"
  echo "👉 Cannot auto-detect IP, please check firewall / outbound network"
fi

echo "======================================"
