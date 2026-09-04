# The default Remount workspace image. See docs/images.md for the contract.
#
# Debian slim plus the tools an agent harness reaches for first. Language
# runtimes are copied from their official images so the versions here are
# the ones those projects publish, not a distro's repackaging.
FROM node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 AS node
FROM ghcr.io/astral-sh/uv:0.9.5@sha256:f459f6f73a8c4ef5d69f4e6fbbdb8af751d6fa40ec34b39a1ab469acd6e289b7 AS uv

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git jq ripgrep openssh-client \
      python3 python3-venv procps tini \
 && rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
 && ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack

COPY --from=uv /uv /uvx /usr/local/bin/

# The node bind-mounts the workspace root here and runs every session with
# this working directory. The broker address arrives in /work/.remount/env.
WORKDIR /work
ENV LANG=C.UTF-8 \
    UV_LINK_MODE=copy \
    npm_config_update_notifier=false

LABEL org.opencontainers.image.source="https://github.com/andrewgcodes/remount" \
      org.opencontainers.image.description="Remount default workspace image" \
      org.opencontainers.image.licenses="Apache-2.0"

# The node starts containers with `sleep infinity`; a stopped default keeps
# the image usable by hand as well.
CMD ["sleep", "infinity"]
