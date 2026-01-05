#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "DEPLOY NODE.JS (DOCKER-FIRST)"
echo "======================================"

APP_NAME="pixel-server"
IMAGE_NAME="pixel-server:latest"
HOST_PORT=9000
CONTAINER_PORT=9000

# =========================
# CHECK DOCKER
# =========================
command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but not installed."
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
# STOP OLD CONTAINER
# =========================
if docker ps -a --format '{{.Names}}' | grep -q "^${APP_NAME}$"; then
  docker rm -f "$APP_NAME"
fi

# =========================
# RUN CONTAINER
# =========================
docker run -d \
  --name "$APP_NAME" \
  --restart always \
  -p ${HOST_PORT}:${CONTAINER_PORT} \
  -e NODE_ENV=production \
  -e PORT=${CONTAINER_PORT} \
  "$IMAGE_NAME"

# =========================
# HEALTH CHECK
# =========================
sleep 5
if curl -fs "http://127.0.0.1:${HOST_PORT}/health" >/dev/null; then
  echo "DEPLOY SUCCESS"
else
  docker logs "$APP_NAME"
  exit 1
fi

# =========================
# PRINT URL
# =========================
if [ -n "$SERVER_IP" ]; then
  echo "Health URL:"
  echo "http://${SERVER_IP}:${HOST_PORT}/health"
  echo "Pixel URL:"
  echo "http://${SERVER_IP}:${HOST_PORT}/p.gif?e=test&sid=demo"
fi

echo "======================================"
