FROM rust:1.90-bookworm AS builder

ARG VERSION=dev
ARG TARGETARCH
WORKDIR /src

# Keep dependency compilation cacheable while still building the exact locked tree.
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs VERSION ./
COPY crates ./crates
COPY proto ./proto
COPY assets ./assets
COPY src ./src
RUN DEVIN2API_BUILD_VERSION="$VERSION" cargo build --locked --release --bin devin-2api && \
    cp target/release/devin-2api /tmp/devin-2api && \
    expected="$VERSION" && { [ "$expected" != dev ] || expected="$(tr -d '[:space:]' < VERSION)"; } && \
    /tmp/devin-2api -version | grep -Fx "$expected"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /tmp/devin-2api /app/devin-2api
EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=2s --start-period=2s --retries=15 \
  CMD curl -fsS http://127.0.0.1:8080/healthz >/dev/null || exit 1
ENTRYPOINT ["/app/devin-2api"]
