# syntax=docker/dockerfile:1.7
FROM scratch
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="Remount" \
      org.opencontainers.image.description="Durable computers for AI agents" \
      org.opencontainers.image.source="https://github.com/andrewgcodes/remount" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --chmod=0755 dist/remount-${TARGETOS}-${TARGETARCH} /usr/local/bin/remount
ENTRYPOINT ["/usr/local/bin/remount"]
