# The reference Remount browser workspace image. See docs/images.md.
#
# It satisfies the workspace-image contract and adds exactly what a headless
# Chromium needs: the browser itself, a font set so text renders instead of
# tofu, root certificates, and curl for the health probe. Nothing else; an
# image is not a credential boundary and a browser is large enough already.
#
# BASE defaults to the same digest-pinned Debian the default workspace image
# builds on, so this Dockerfile builds in a clean checkout with no other image
# present. Pass --build-arg BASE=<a Remount workspace image> to get the harness
# tooling (node, uv, git, ripgrep) in the same image:
#
#   docker build -t remount-workspace:local images/workspace
#   docker build -t remount-browser:local \
#     --build-arg BASE=remount-workspace:local images/browser
ARG BASE=debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
FROM ${BASE}

ARG DEBIAN_FRONTEND=noninteractive
# chromium pulls its own runtime dependencies; fonts-liberation and
# fonts-dejavu-core are what keep a screenshot from being a grid of boxes.
# procps is here because an operator - and a conformance lane - has to be able
# to see and stop the browser process.
#
# socat is not optional on a container backend. Chromium's DevTools HTTP server
# binds 127.0.0.1 and ignores --remote-debugging-address, so the node, which
# reaches a workspace port at the container's address, cannot see the browser
# without a forwarder. The default launch starts one.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      chromium \
      ca-certificates curl socat \
      fonts-liberation fonts-dejavu-core \
      procps tini \
 && rm -rf /var/lib/apt/lists/*

# The health probe answers the only question worth asking before a
# computer.create: is a DevTools endpoint listening, and which browser is it?
# A silent port exits non-zero, because a check that cannot run is a failure
# and never an unearned pass.
COPY remount-browser-health /usr/local/bin/remount-browser-health
RUN chmod 0755 /usr/local/bin/remount-browser-health

# The node bind-mounts the workspace root here and runs every session with this
# working directory. The broker address arrives in /work/.remount/env, and
# browser profiles and downloads live under /work/.remount/, which every
# snapshot excludes.
WORKDIR /work
ENV LANG=C.UTF-8 \
    CHROME_BIN=/usr/bin/chromium

LABEL org.opencontainers.image.source="https://github.com/andrewgcodes/remount" \
      org.opencontainers.image.description="Remount reference browser workspace image" \
      org.opencontainers.image.licenses="Apache-2.0"

# The node starts containers with `sleep infinity`; a stopped default keeps the
# image usable by hand as well.
CMD ["sleep", "infinity"]
