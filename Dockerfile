FROM node:20

ENV NODE_ENV=production
ENV PORT=9000

WORKDIR /app

COPY package*.json ./
RUN npm install --only=production

COPY src ./src

EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD curl -fs http://127.0.0.1:9000/health || exit 1

CMD ["node", "src/server.js"]