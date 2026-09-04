# The node image for the reference stack.
#
# It exists because packaging/container/Dockerfile is FROM scratch, and a
# scratch image cannot host a process-backend node: that backend runs each
# session as a process in the node's own filesystem, so a workspace whose
# `sh` does not exist fails with `executable file not found in $PATH`. The
# release image is correct for the control plane and the CLI, which need
# nothing but the binary; a node additionally needs a userland for the
# workspaces it will run.
#
# A docker-backend node has the opposite shape: its workspaces get their own
# image (images/workspace/Dockerfile), so the node itself needs no userland,
# but it does need access to a Docker daemon. That is a real privilege
# boundary and the reason this stack defaults to the process backend on an
# isolated network rather than mounting a socket.
#
#   docker build -f deploy/compose/node.Dockerfile -t remount-node:local .
#
# It reads the same dist/ binary the release image does, so the node and the
# control plane are the same build.

FROM debian:bookworm-slim

ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 dist/remount-${TARGETOS}-${TARGETARCH} /usr/local/bin/remount

LABEL org.opencontainers.image.title="Remount node" \
      org.opencontainers.image.description="Remount node with a userland for process-backend workspaces" \
      org.opencontainers.image.source="https://github.com/andrewgcodes/remount" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

ENTRYPOINT ["/usr/local/bin/remount"]
