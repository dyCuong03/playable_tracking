# =========================
# Production Image
# =========================
FROM node:20

# =========================
# Environment
# =========================
ENV NODE_ENV=production
ENV PORT=9000

# =========================
# App directory
# =========================
WORKDIR /app

# =========================
# Install dependencies
# =========================
COPY package*.json ./
RUN npm install --only=production

# =========================
# Copy source code
# =========================
COPY src ./src

# =========================
# Expose port
# =========================
EXPOSE 9000

# =========================
# Healthcheck
# =========================
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD curl -fs http://127.0.0.1:9000/health || exit 1

# =========================
# Start server
# =========================
CMD ["node", "src/server.js"]
