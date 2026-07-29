FROM rust:1.88-bookworm AS build
ENV RUSTUP_TOOLCHAIN=1.88.0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin rutomq \
    && cp /src/target/release/rutomq /tmp/rutomq

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /tmp/rutomq /usr/local/bin/rutomq
EXPOSE 9092 8080
ENTRYPOINT ["/usr/local/bin/rutomq"]
