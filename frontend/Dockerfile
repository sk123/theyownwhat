# Build stage
FROM node:20-alpine as build-stage

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

# Production stage
FROM nginx:stable-alpine as production-stage

# Copy the built assets from the build stage
COPY --from=build-stage /app/dist /usr/share/nginx/html

# Copy the custom nginx configuration
# We expect this to be mounted or copied from the root
# But for a self-contained image, we could copy it here if we moved it.
# For now, let's assume we use the root's nginx.conf via compose.

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
