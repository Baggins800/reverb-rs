# A statically linked server on a base with nothing in it but CA certificates.
#
# Built natively for each architecture rather than emulated, so `uname -m`
# names the target. Works with both the classic builder and BuildKit.

ARG RUST_VERSION=1.93

FROM rust:${RUST_VERSION}-bookworm AS builder

# cmake for aws-lc-rs, which builds native code; musl for the static target.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake musl-tools musl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add "$(uname -m)-unknown-linux-musl"

# musl-tools ships the native musl-gcc, which is the one we need.
ENV CC_x86_64_unknown_linux_musl=musl-gcc \
    CC_aarch64_unknown_linux_musl=musl-gcc

WORKDIR /src

# Compile the dependency graph against a stub first, so editing the source
# does not invalidate the layer that took all the time.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked --bin reverb-rs \
        --target "$(uname -m)-unknown-linux-musl" \
    && rm -rf src

COPY src ./src

RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --bin reverb-rs \
        --target "$(uname -m)-unknown-linux-musl" \
    && strip "target/$(uname -m)-unknown-linux-musl/release/reverb-rs" \
    && cp "target/$(uname -m)-unknown-linux-musl/release/reverb-rs" /reverb-rs \
    # Fail the build rather than ship something that needs a loader.
    && ldd /reverb-rs 2>&1 | grep -q "statically linked\|not a dynamic executable"

# The static base carries CA certificates, /etc/passwd and time zones, and
# nothing else — no shell, no libc, no package manager.
FROM gcr.io/distroless/static-debian12 AS runtime

COPY --from=builder /reverb-rs /usr/local/bin/reverb-rs

USER nonroot:nonroot

EXPOSE 8080

# The binary probes itself, since this image has no shell or HTTP client.
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/reverb-rs", "--healthcheck"]

# Exec form, so the server is PID 1 and receives SIGTERM directly. It closes
# connections cleanly before exiting.
ENTRYPOINT ["/usr/local/bin/reverb-rs"]
