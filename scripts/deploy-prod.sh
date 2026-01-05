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

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "!! Required command '$cmd' is not installed or not in PATH."
    exit 1
  fi
}

npm_install_has_run=0

run_npm_install() {
  if [[ $npm_install_has_run -eq 0 ]]; then
    echo "?? Installing npm dependencies (this may take a moment)..."
    npm install
    npm_install_has_run=1
  fi
}

ensure_node_modules() {
  if [[ ! -d "node_modules" ]]; then
    echo "?? node_modules not found. Triggering npm install..."
    run_npm_install
  fi
}

ensure_bigquery_dependency() {
  if ! node -e "const pkg=require('./package.json');const deps={...(pkg.dependencies||{}),...(pkg.devDependencies||{})};if(!deps['@google-cloud/bigquery']){console.error('Missing dependency @google-cloud/bigquery in package.json');process.exit(1);}"; then
    echo "?? Dependency '@google-cloud/bigquery' is not declared in package.json. Run 'npm install @google-cloud/bigquery --save' and retry."
    exit 1
  fi

  if [[ ! -d "node_modules/@google-cloud/bigquery" ]]; then
    echo "?? '@google-cloud/bigquery' not found under node_modules. Running npm install..."
    run_npm_install
  fi

  if [[ ! -d "node_modules/@google-cloud/bigquery" ]]; then
    echo "?? Failed to install '@google-cloud/bigquery'. Please fix npm dependencies before deploying."
    exit 1
  fi
}

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
# CHECK TOOLING
# =========================
require_cmd docker
require_cmd npm
require_cmd node

# =========================
# VERIFY NPM DEPENDENCIES
# =========================
echo "🧪 Verifying npm dependencies..."
ensure_node_modules
ensure_bigquery_dependency

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
