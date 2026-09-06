# ADR 0088: Computer sessions ride the port substrate, not a workspace daemon

## Status

Accepted.

## Context

Gap 3 of `docs/engineering/gap-brief-2026-09-06.md` asks for first-class
browser/computer operations: take a screenshot, click, type, navigate, read
back what happened, and collect what the page downloaded — reachable through
the protocol and the SDKs rather than assembled by each caller.

The composition that already worked (`docs/harness-integration.md`, "Browser
and virtual desktop workloads") is a shell script inside the workspace: Xvfb,
Openbox, Chromium, x11vnc, xdotool, plus `remount port` for a VNC tunnel. It
proves the substrate is sufficient. It is not an API. Every caller reimplements
the supervisor, the readiness loop, the screenshot capture and the input
encoding, and nothing about it is observable to the control plane: a browser
that dies is just a process that exited.

The obvious alternative is a workspace-resident daemon that speaks a small
Remount-defined browser protocol. It is the wrong shape here. The workspace is
trusted with nothing (ADR 10). A daemon inside it would be one more thing to
ship in every image, one more version to negotiate, one more privileged
listener on the untrusted side of the boundary, and — for the backends that
already relay bytes — a second transport doing what the first one does.

Meanwhile `Backend.Prepare` already resolves a byte-accurate TCP path to any
workspace port on every backend: the process backend shares the node's network
namespace, docker resolves the container IP, gVisor dials the sandbox, and
firecracker installs a `Runner` that relays through its vsock guest bridge.
That is exactly one resolved path per backend, and `port.open` already uses it.

## Decision

**A computer is a Chrome DevTools Protocol conversation the node holds over the
port substrate.** It is not a new session kind and it needs nothing installed
in the workspace beyond a browser that can listen on a DevTools port.

- `internal/session.DialPort(spec)` opens the path a port session would use
  without creating a logged session: it dials `spec.Host:spec.Port` directly,
  or drives `spec.Runner` and presents the relayed stream as a `net.Conn`. The
  caller must have run `Backend.Prepare` on the spec first, so a node-held
  conversation and a client's `port.open` take the same bytes on every backend.
- `internal/computer` is a minimal CDP client over that dialer: Target, Page,
  Runtime, Input, Browser and Emulation, and nothing else.
- The node owns the registry, the authorization, the lifecycle and the events.
  A browser the node spawns is an ordinary managed exec session, so a client
  can attach to its output and every existing fencing path already stops it.

### Coordinates and screenshots

Coordinates are **CSS pixels with the origin at the top-left of the declared
viewport**, and the viewport is declared at `computer.create` and pinned with
`Emulation.setDeviceMetricsOverride`. Screenshots are **PNG clipped to that same
viewport** at `deviceScaleFactor: 1`. Declaring the rectangle once and clipping
to it is what makes a screenshot and the click that follows it agree; a capture
that silently grew past the viewport would move every coordinate the model just
computed.

One screenshot is capped at 8 MiB (`proto.ComputerMaxScreenshotBytes`) and a
larger capture is refused with `resource_exhausted`. A screenshot is a single
response body, not a session log: there is no cursor to resume from, so an
unbounded one is an allocation a peer chooses for us.

`computer.eval` exists because conformance has to assert what the page actually
became, not what a picture appears to show. Its value is page-controlled JSON:
decode it, never execute it.

### Input, and why it is deduplicated

`computer.input` carries an `ISeq` per computer and drops any sequence at or
below the last applied one, which is exactly what `session.Input` does for
keystrokes. The reason is the same: the transport can retry, and a retried
click is a second click. The SDK keeps the counter, so a handle that reconnects
and replays lands on a sequence the node has already applied and is dropped.

A batch is validated whole before any of it is dispatched, so a malformed
action cannot leave half a batch applied.

Typing uses `Input.insertText`, not synthesized keystrokes: it is the only path
that works the same in ordinary inputs and contenteditable regions, and it does
not depend on a keyboard layout the workspace image may not carry. Named keys
use `Input.dispatchKeyEvent` with a fixed table (Enter, Tab, Backspace, Delete,
Escape, Space, the four arrows, Home, End, PageUp, PageDown) that sends the
logical key, the physical code and the legacy virtual key code together,
because pages exist that read only one of the three. A key not in the table is
rejected rather than guessed.

### Errors: a stable code plus a stable reason

Every failure keeps an existing stable code and adds an `Error.Reason` from the
shared vocabulary added with `proto.ErrReason`. Callers still match on `Code`;
`Reason` narrows a code that covers several outcomes, the way `ExitInfo.Reason`
narrows a terminated session. No new error code was needed.

| Situation | Code | Reason |
|---|---|---|
| browser died mid-conversation | `closed` | `browser_crashed` |
| DevTools port never answered | `timeout` | `display_unavailable` |
| port answered but gave no usable page | `unsupported` | `display_unavailable` |
| DevTools WebSocket refused | `unreachable` | `display_unavailable` |
| malformed or out-of-viewport action | `bad_request` | `input_rejected` |
| navigation the egress policy refused | `denied` | `navigation_denied` |
| unusable browser profile directory | `conflict` | `profile_corrupt` |
| backend exposes no workspace port path | `unsupported` | `backend_unsupported` |
| download finished but could not be published | (no error; listed as `state: blocked`) | `download_blocked` |

An unrecognised reason must degrade to its code, never to an unhandled case.

A download that finished but could not be published is deliberately not an
error on any request: nothing was asked for at that moment. It is reported
where a caller will look for it, as a `computer.downloads` entry whose `state`
is `blocked` and whose `reason` says why. A failure the browser caused must be
visible in the listing rather than silently absent from it.

### The browser's egress is the workspace's egress

A browser the node spawns is started with `sessionEnv`, the same environment
every other session gets, so `HTTP_PROXY`/`HTTPS_PROXY` already point at the
workspace's broker and secrets stay outside the workspace exactly as they do
for a harness. Be precise about what that buys: Chromium consults those
variables only where its system-proxy resolver falls back to them, which is the
usual case on a headless Linux image and is not a guarantee. An image that must
*prove* every request went through the broker should pass `--proxy-server`
explicitly through `Launch.Program`; the enforced-gateway backends
(ADR 0061) are what make it unavoidable rather than advisory.

### The egress ceiling is per host, not per URL

The broker's CONNECT handler tunnels opaquely and terminates no TLS
(`internal/broker/broker.go`, "CONNECT is opaque by design"). It can therefore
see the host a browser asked for and nothing else: not the path, not the query,
not the response. **Navigation policy is per host and can never be per URL**,
and no amount of work in the computer layer changes that. A denial reaches the
client as the browser's own network error, which the node maps to
`denied`/`navigation_denied`. Anyone who reads a per-URL guarantee into this
feature is reading a guarantee the transport cannot make.

### Profiles do not persist, and that is the honest behaviour

A profile lives at `<workspace>/.remount/browser/<profile>` and downloads at
`<workspace>/.remount/downloads`. `.remount` is the directory `node.snapshot`
already excludes from every snapshot, so **a profile is node-local: it does not
survive a move, a sleep/wake, a failover or a `remount pull`.**

This is a deliberate choice rather than an oversight. `docs/harness-integration.md`
records what happens otherwise: a live Chromium profile contains singleton
symlinks and lock files that artifact validation correctly rejects, and the
recipe there already tells users to exclude the profile at workspace creation.
Restoring one host's browser profile onto another host is not meaningful. A
computer after a wake is a fresh browser on the same filesystem; durable state
belongs in ordinary portable files, which snapshots do carry.

Downloads take the other road for the same reason: a completed file is
published as an artifact through the path `volume.archive` uses — the same tree
boundary, the same deterministic tar.gz layout (`artifact.SnapshotFiltered`
over the download directory, keeping one entry), the same store limit, the same
tenant-scoped upload — and announced as `computer.download`. It reaches the
client by artifact id rather than by surviving in a snapshot.

### WebSocket over an arbitrary connection: no new dependency

CDP is JSON-RPC over WebSocket, and the connection here is a `net.Conn` handed
back by the port substrate rather than something dialed by address.
`github.com/coder/websocket`, already the transport's WebSocket library, takes
an `*http.Client`, so a `http.Transport` whose `DialContext` returns the port
connection is enough. The DevTools HTTP probe (`GET /json/version`) reuses the
same trick. **No dependency was added.**

The debugger URL that probe returns comes from inside the workspace. Only its
path is kept; its authority is discarded and replaced with the node's own
endpoint, because where the bytes go is the node's decision and never the
browser's.

## Consequences

- Every backend that can forward a workspace port can host a computer, with no
  per-backend code in the computer layer and nothing new inside the workspace.
- A browser that dies is observable: `computer.degraded` and `computer.closed`
  with `browser_crashed`, and every later operation fails with
  `closed`/`browser_crashed` instead of hanging.
- Release, sleep, quarantine and fence all route through
  `node.stopWorkspaceSessions`, which now closes computers and joins their
  goroutines *before* the sessions they talk to are terminated. Cancellation is
  not completion: teardown returns only once each download publisher and close
  watcher has stopped.
- `computer.create`, `computer.navigate` and `computer.close` are journaled
  mutations like any other, so a replay returns the first result.
- Publishing a download needs a host path for the download directory, so a
  backend whose filesystem is only reachable in-guest (firecracker) can drive a
  browser but cannot yet publish what it downloaded. That is reported as a
  `blocked` download with `backend_unsupported`, not hidden: the honest gap is
  the guest-side archive read, and closing it belongs with the guest bridge
  rather than here.
- The CDP client is proven against `internal/computer/fakecdp`, a scriptable
  DevTools endpoint that records methods and can crash on demand. That fake is
  the contract; a real-Chromium conformance lane is a separate, later gate and
  is deliberately not what these tests assert.
- The unimplemented half stays unimplemented on purpose: there is no CLI, no
  hand-written Python/TypeScript SDK method and no browser image here. The
  generated cross-language types (ADR 0078) do track the new bodies, because
  leaving them stale would break `cmd/protogen --check` rather than defer a
  decision.
