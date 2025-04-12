# Stage 1: Build the application
FROM rust:1.77-slim-bookworm as builder

# Set the working directory
WORKDIR /app

# Install build dependencies (if any, e.g., for specific crates)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy manifests first to leverage Docker layer caching for dependencies
COPY Cargo.toml Cargo.lock ./
# Build dependencies only (dummy build)
RUN mkdir src && \
    echo "fn main() {println!(\"Building dependencies...\");}" > src/main.rs && \
    cargo build --release --bin super-crawler && \
    rm -rf src

# Copy the actual source code
COPY src ./src

# Build the application binary
# Ensure the binary name matches what's in your Cargo.toml or the default
RUN cargo build --release --bin super-crawler

# Stage 2: Create the final runtime image
FROM debian:bookworm-slim as runner

# Set the working directory
WORKDIR /app

# Install runtime dependencies (if any, e.g., CA certificates)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/super-crawler /app/super-crawler

# Create the output directory - this will be mounted over by Fly.io volumes
RUN mkdir -p /app/output

# Expose the port the application listens on (adjust if different in main.rs)
EXPOSE 8080

# Command to run the application
# The `/app/output` path inside the container will be used for saving MDX files.
# Fly.io volumes will be mounted here.
CMD ["/app/super-crawler"] 