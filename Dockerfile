# =========================
# Production Image
# =========================
FROM node:20-alpine

# Security & performance
ENV NODE_ENV=production

# App directory
WORKDIR /app

# Copy only dependency files first (better cache)
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production

# Copy application source
COPY src ./src

# Expose service port
EXPOSE 8080

# Healthcheck (Docker-level)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD wget -qO- http://127.0.0.1:8080/health || exit 1

# Start server
CMD ["node", "src/server.js"]
