# Build ---------------------------------------------------------------------
ARG RUST_VERSION=1.93

FROM rust:${RUST_VERSION}-bookworm AS builder

# aws-lc-rs, the TLS backend, builds native code.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Compile the dependency graph against a stub first, so editing the source
# does not invalidate the layer that took all the time.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked --bin reverb-rs \
    && rm -rf src

# Only what the binary needs: tests and examples are built in CI, not here.
COPY src ./src

RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --bin reverb-rs \
    && strip target/release/reverb-rs \
    && cp target/release/reverb-rs /reverb-rs

# Run -----------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12 AS runtime

COPY --from=builder /reverb-rs /usr/local/bin/reverb-rs

# Distroless ships an unprivileged user; the default port needs no privileges.
USER nonroot:nonroot

EXPOSE 8080

# The binary probes itself, since this image has no shell or HTTP client.
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/reverb-rs", "--healthcheck"]

# Exec form, so the server is PID 1 and receives SIGTERM directly. It closes
# connections cleanly before exiting.
ENTRYPOINT ["/usr/local/bin/reverb-rs"]
