# Workspace images

A docker workspace runs inside an image. This page is the contract for the
image Remount ships and for any image you substitute.

## The default image

`ghcr.io/andrewgcodes/remount-workspace:<version>` is built by CI from
[`images/workspace/Dockerfile`](../images/workspace/Dockerfile) on every
release tag and, as `:latest`, on every push to `main`. A release binary
defaults to the tag built alongside it; a development build (`remount version`
prints `dev`) defaults to `:latest`. Either way, `remount up --image` and a
workspace spec's `image` field override it.

| Layer | What is in it | Why |
|---|---|---|
| `debian:bookworm-slim` | glibc, coreutils, `sh`, `sleep`, `ps`, `tini` | the node starts the container with `sleep infinity` and runs commands with `docker exec`; a real init keeps zombies from accumulating |
| tools | `git`, `curl`, `jq`, `ripgrep` (`rg`), `openssh-client`, `ca-certificates` | what an agent harness reaches for in its first minute |
| Node 22 | `node`, `npm`, `npx`, `corepack`, copied from `node:22-bookworm-slim` | most harnesses ship on npm |
| Python 3 | `python3`, `venv`, `uv`, `uvx` (from `ghcr.io/astral-sh/uv`) | the other half of them |

Environment: `LANG=C.UTF-8`, `UV_LINK_MODE=copy` (the workspace root is a bind
mount), npm's update notifier off. The image runs as root; `--user` is a
backend option for a later phase and the isolation story is the backend's, not
the image's (see `remount doctor` and `docs/design.md`).

Size: about 410 MB uncompressed for `linux/amd64`. Both `linux/amd64` and
`linux/arm64` are published.

## What every image must provide

The node needs exactly this from an image, and nothing else:

1. `sleep` on `PATH` — the container's only process until a session opens.
2. `sh` — `remount sh` and recipe `prompt_template`s run through it.
3. A writable `/work` — the node bind-mounts the workspace root there and
   sets it as the working directory of every session. Do not bake files into
   `/work`; the mount hides them.
4. Nothing that reads `/work/.remount/env` at build time. That file is written
   by the node on every materialize and carries the broker address, which is
   different on every node and after every move.

`ubuntu:24.04`, `debian:bookworm-slim`, `alpine:3.20` and `node:22-bookworm`
all satisfy this and are exercised in the docker lane. `--image ubuntu:24.04`
gives you the pre-0.6 default unchanged.

## Building your own

Start `FROM ghcr.io/andrewgcodes/remount-workspace:<version>` and add what
your harness needs, or start from any distro image and respect the four rules
above. Verify with the same smoke test CI runs:

```sh
docker build -t my-workspace images/workspace
docker run --rm my-workspace sh -c 'git --version && node --version && uv --version && rg --version'
remount up --backend docker --image my-workspace
```

An image is not a credential boundary. Secrets never enter the image or the
workspace; the broker substitutes them at the network edge (ADR 10).
