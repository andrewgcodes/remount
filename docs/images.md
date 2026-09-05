# Workspace images

A docker workspace runs inside an image. This page is the contract for the
image Remount ships and for any image you substitute.

## The default image

The workspace-image workflow is configured to build
`ghcr.io/andrewgcodes/remount-workspace:<version>` from
[`images/workspace/Dockerfile`](../images/workspace/Dockerfile) on every
release tag and, as `:latest`, on qualifying pushes to `main` (documentation-only
pushes are excluded). Workflow configuration is not proof that an image is
published or accessible: check the registry, or build locally below. A release binary
defaults to the tag built alongside it; a non-release source build defaults to
`:latest`. Either way, `remount up --image` and a
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

The workflow targets `linux/amd64` and `linux/arm64`. Measure size on the exact
digest and platform; layer contents and size change with source revisions.
The Dockerfile pins its upstream image digests. Docker's `--init` option,
set by the backend, supplies the running container's init process.

## What every image must provide

The node needs exactly this from an image, and nothing else:

1. `sleep` on `PATH` — the long-running command beneath Docker's init until a session opens.
2. `sh` — `remount sh` and recipe `prompt_template`s run through it.
3. A writable `/work` (or the configured mount path) — the node bind-mounts the workspace root there and
   sets it as the working directory of every session. Do not bake files into
   `/work`; the mount hides them.
4. Nothing that reads `/work/.remount/env` at build time. That file is written
   by the node on every materialize and carries the broker address, which is
   different on every node and after every move.

Distro images can satisfy this core contract without providing a working
harness environment. Check shell behavior, libc, runtime versions and recipe
dependencies in the exact image you select; the core contract is not a claim
that every distro or architecture has passed the Docker lane.

## Building your own

Start `FROM ghcr.io/andrewgcodes/remount-workspace:<version>` and add what
your harness needs, or start from any distro image and respect the four rules
above. Verify with the same smoke test CI runs:

```sh
docker build -t my-workspace images/workspace
docker run --rm my-workspace sh -c 'git --version && node --version && uv --version && rg --version'
remount up --backend docker --image my-workspace
```

An image is not a credential boundary. Keep secrets out of image layers and
workspaces; broker-managed keys are substituted at the network edge (ADR 10).
Explicit harness-native login stores credentials in the workspace and changes
that trust model; see [harness integration](harness-integration.md).
