# syntax=docker/dockerfile:1
# Multi-stage build → single image: the Rust backend serves the static frontend.

# --- Frontend build (static SPA → .output/public) ---
FROM node:22-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run generate

# --- Backend build (release binary) ---
FROM rust:1.95-slim-bookworm AS backend
WORKDIR /app/backend
# Pre-fetch & compile dependencies on their own layer for caching.
COPY backend/Cargo.toml backend/Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src
COPY backend/ ./
RUN touch src/main.rs && cargo build --release

# --- Runtime ---
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=backend /app/backend/target/release/kotatsu /usr/local/bin/kotatsu
COPY --from=frontend /app/frontend/.output/public /app/static
ENV KOTATSU_BIND=0.0.0.0:8080 \
    KOTATSU_STATIC_DIR=/app/static
EXPOSE 8080
CMD ["kotatsu"]
