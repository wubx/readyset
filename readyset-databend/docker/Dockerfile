# Build the readyset-databend single binary from source and package it.
#
#   docker build -f readyset-databend/docker/Dockerfile -t readyset-databend:dev .
#
# Run from the repository root (the build needs the whole Cargo workspace).
ARG RUST_IMAGE=rust:1.99.0-bookworm
ARG RUNTIME_IMAGE=ubuntu:24.04

FROM ${RUST_IMAGE} AS builder
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential clang cmake libclang-dev libssl-dev pkg-config protobuf-compiler; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    cargo build --locked --release -p readyset-databend --bin readyset-databend; \
    # The workspace release profile keeps debug info; the image does not need it.
    objcopy --strip-debug target/release/readyset-databend /readyset-databend

FROM ${RUNTIME_IMAGE}
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl libssl3t64; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --uid 10001 --home-dir /var/lib/readyset-databend readyset; \
    install -d -o readyset -g readyset /var/lib/readyset-databend
COPY --from=builder /readyset-databend /usr/local/bin/readyset-databend
USER readyset
VOLUME ["/var/lib/readyset-databend"]
EXPOSE 8001
HEALTHCHECK --interval=10s --timeout=3s --start-period=120s \
    CMD curl -fsS http://127.0.0.1:8001/livez || exit 1
ENTRYPOINT ["/usr/local/bin/readyset-databend"]
CMD ["--config", "/etc/readyset-databend/readyset-databend.toml"]
