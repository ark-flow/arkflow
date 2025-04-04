# Build stage
FROM rust:1.85-slim as builder

WORKDIR /app
COPY . .

# Build project
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy compiled binary from builder stage
COPY --from=builder /app/target/release/arkflow /app/arkflow

 
# Set environment variables
ENV RUST_LOG=info


# Set startup command
CMD ["/app/arkflow" "--config", "/app/etc/config.yaml"]