# Start from a node image to build the app
FROM node:20-alpine AS build

# Go to the app folder
WORKDIR /app

# Install the dependencies first (better layer cache)
COPY package.json package-lock.json* ./
RUN npm install

# Build the next app (standalone output)
COPY . .
RUN npm run build

# Use a slim node image for the runtime
FROM node:20-alpine AS run
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000

# Copy the standalone server and the static assets
COPY --from=build /app/.next/standalone ./
COPY --from=build /app/.next/static ./.next/static
COPY --from=build /app/public ./public

# Serve the app
EXPOSE 3000
CMD ["node", "server.js"]
