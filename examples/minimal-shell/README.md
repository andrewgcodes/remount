# minimal-shell

The smallest complete Remount program. It connects, creates a workspace, waits
for a node to claim it, runs one command with streamed output, writes a file,
reads it back, and destroys the workspace.

It imports only the two supported public packages, `remount.dev/remount/api`
and `remount.dev/remount/client`. It needs no credentials, no bindings and no
network access beyond the Remount server.

## Run it

Start a whole system — control plane, relay, artifact store and one
process-backend node — in one process. Standalone mode has no token and is
local and unisolated by design; it is for a laptop, not a shared machine.

```sh
go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
```

In a second shell:

```sh
REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/minimal-shell
```

Expected output, with the workspace and node ids differing every run. The
`pwd` line is the process backend's workspace tree on the host; an isolated
backend reports the workspace's own mount path instead.

```
workspace ws_… claimed by node n_…
hello from …/remount-data/node/ws/ws_…
exit 0
read back 27 bytes: written by the Remount SDK
workspace ws_… destroyed
```

`REMOUNT_TOKEN` is read when set, so the same program runs unchanged against a
server that requires one.

## What each step demonstrates

| Step | Why it is in the example |
|---|---|
| `client.New` | one reconnecting logical connection; the SDK redials and resumes streams |
| `CreateWorkspace` | a mutation, carrying an idempotency key so a retry is a no-op |
| `WaitClaimed` | a workspace is not addressable until a node has restored it and reported `ws.ready` |
| `Exec` + `client.Copy` | session output is a replayable log; an elided range surfaces as an `evicted` error, never as complete output |
| `WriteFile` / `ReadFile` | jailed filesystem operations against the workspace tree |
| `DestroyWorkspace` | the workspace is a resource with a lifecycle, not a leaked process |

## Proof

`integration/examples` boots this exact system in-process on a free loopback
port, builds and runs this program against it, and asserts on the output. No
network and no credentials are involved:

```sh
go test -count=1 -timeout 600s ./integration/examples/ -run TestMinimalShellExample
```
