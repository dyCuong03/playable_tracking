FROM node:20-slim

# =========================
# SERVER ENV (DEFAULT)
# =========================
ENV NODE_ENV=production
ENV PORT=9000

# =========================
# WORKDIR
# =========================
WORKDIR /app

# =========================
# INSTALL DEPENDENCIES
# =========================
COPY package*.json ./
RUN npm install --omit=dev

# =========================
# COPY SOURCE
# =========================
COPY src ./src

# =========================
# EXPOSE
# =========================
EXPOSE 9000

# =========================
# HEALTHCHECK
# =========================
# Intentionally NO HEALTHCHECK. A Docker HEALTHCHECK here cold-starts a Node
# process per container on every interval (× all replicas, forever), which pins
# dockerd CPU. Readiness is probed actively at deploy time (scripts/deploy-prod.sh)
# and the web tier is health-gated by nginx (max_fails/fail_timeout) at runtime.

# =========================
# START
# =========================
CMD ["node", "src/server.js"]
