FROM node:20-alpine AS explorer_builder

WORKDIR /app/explorer
COPY explorer/package.json /app/explorer/package.json
COPY explorer/package-lock.json /app/explorer/package-lock.json
RUN npm ci
COPY explorer /app/explorer
RUN npm run build

FROM nginx:1.27-alpine

RUN apk add --no-cache curl

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY www /usr/share/nginx/html
COPY --from=explorer_builder /app/explorer/dist /usr/share/nginx/html/explorer
