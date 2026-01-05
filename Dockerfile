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
# HEALTHCHECK (NO CURL)
# =========================
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD node -e "require('http').get('http://127.0.0.1:'+(process.env.PORT||9000)+'/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"

# =========================
# START
# =========================
CMD ["node", "src/server.js"]
