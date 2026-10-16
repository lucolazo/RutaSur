FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY shared ./shared
COPY services ./services
COPY scripts ./scripts
CMD ["node", "services/proyector/index.js"]
