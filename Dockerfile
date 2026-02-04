FROM rust:1-bookworm AS builder

WORKDIR /app

# Cache dependencies first.
COPY Cargo.toml Cargo.lock ./
COPY crates/floe-core/Cargo.toml crates/floe-core/Cargo.toml
COPY crates/floe-cli/Cargo.toml crates/floe-cli/Cargo.toml

RUN cargo fetch --locked

# Copy the full source and build.
COPY crates crates

RUN cargo build -p floe-cli --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /usr/sbin/nologin floe

WORKDIR /work

COPY --from=builder /app/target/release/floe /usr/local/bin/floe

USER floe

ENTRYPOINT ["floe"]
CMD ["--help"]

