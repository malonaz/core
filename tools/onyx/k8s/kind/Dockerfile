FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY user-service /app/user-service
USER 10001
ENTRYPOINT ["/app/user-service"]
