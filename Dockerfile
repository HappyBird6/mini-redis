FROM rust:1.94-bookworm

RUN rustup component add rustfmt clippy \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        redis-tools \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml rust-toolchain.toml ./
COPY src ./src
COPY tests ./tests

RUN cargo build

EXPOSE 6379

CMD ["cargo", "run"]
