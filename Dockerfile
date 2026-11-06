FROM node:22-alpine

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund
COPY index.js ./

ENV NODE_RED_URL=http://host.docker.internal:1880
EXPOSE 1
ENTRYPOINT ["node", "index.js"]
