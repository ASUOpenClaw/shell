# ─── Stage 1: cargo-chef installer ───────────────────────────────────────────
FROM rust:bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# ─── Stage 2: dependency planner ─────────────────────────────────────────────
# Produces recipe.json — a fingerprint of all dependency metadata.
# This layer only rebuilds when Cargo.toml / Cargo.lock change.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ─── Stage 3: builder ────────────────────────────────────────────────────────
FROM chef AS builder

# Cook (compile) dependencies from the recipe.
# This layer is cached as long as recipe.json doesn't change,
# even when source files do — that's the 5x speedup.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Now compile the actual application on top of the cached deps.
COPY . .
RUN cargo build --release

# ─── Stage 4: runtime ────────────────────────────────────────────────────────
# debian:bookworm-slim — glibc compatible, Docker Hub only (no gcr.io needed).
# We copy the CA bundle from the builder so no apt-get is required at all.
FROM debian:bookworm-slim AS runtime

# Copy CA certificates from the builder (rust:bookworm has them pre-installed).
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/target/release/shell /shell

# Unprivileged user.
RUN useradd --no-create-home --shell /bin/false shell
USER shell

EXPOSE 8080

CMD ["/shell"]
