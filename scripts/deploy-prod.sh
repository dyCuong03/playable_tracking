#!/usr/bin/env bash
set -euo pipefail

# =========================
# CONFIG
# =========================
APP_NAME="pixel-server"
IMAGE_NAME="pixel-server:latest"
HOST_PORT=8080
CONTAINER_PORT=8080

# =========================
# GET PUBLIC IP
# =========================
SERVER_IP=$(
  curl -s https://api.ipify.org || \
  curl -s https://ifconfig.me || \
  curl -s https://icanhazip.com
)

if [ -z "$SERVER_IP" ]; then
  echo "❌ Cannot detect public IP"
  exit 1
fi

echo "🌍 Server Public IP: $SERVER_IP"

# =========================
# BUILD IMAGE
# =========================
echo "📦 Building Docker image..."
docker build -t "$IMAGE_NAME" .

# =========================
# STOP OLD CONTAINER
# =========================
if docker ps -a --format '{{.Names}}' | grep -q "^${APP_NAME}$"; then
  echo "🛑 Stopping old container..."
  docker stop "$APP_NAME"
  docker rm "$APP_NAME"
else
  echo "ℹ️ First deploy – no container to stop"
fi

# =========================
# RUN CONTAINER
# =========================
echo "▶️ Starting container..."
docker run -d \
  --name "$APP_NAME" \
  --restart always \
  -p 0.0.0.0:${HOST_PORT}:${CONTAINER_PORT} \
  -e NODE_ENV=production \
  "$IMAGE_NAME"

# =========================
# WAIT FOR SERVER
# =========================
echo "⏳ Waiting for server..."
sleep 3

# =========================
# VERIFY HEALTH
# =========================
if ! curl -fs "http://127.0.0.1:${HOST_PORT}/health" >/dev/null; then
  echo "❌ Health check failed"
  docker logs "$APP_NAME"
  exit 1
fi

# =========================
# PRINT READY-TO-USE URL
# =========================
TEST_URL="http://${SERVER_IP}:${HOST_PORT}/p.gif?e=test&sid=demo&rnd=$(date +%s)"

echo "======================================"
echo "✅ DEPLOY SUCCESS"
echo "🌐 Pixel URL READY TO USE:"
echo "👉 $TEST_URL"
echo "======================================"
