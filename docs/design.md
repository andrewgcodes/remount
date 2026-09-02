# Remount: the design

This is the architecture in full. The short version is in the
[README](../README.md); the wire format is in [the spec](../spec/PROTOCOL.md);
the reasoning behind individual choices is in [the ADRs](adr/).

---

## 1. The problem

An agent needs a computer. Today it gets one of two bad deals.

Either the computer is the machine the harness happens to be running on, in
which case the agent dies with the laptop lid, cannot be given a bigger box, and
holds your API keys in the same process it has root in.

Or the computer belongs to a sandbox vendor, in which case it is fast and clean
and completely trapped: you cannot move it to your GPU box, your on-prem host,
or another vendor, and the agent still holds your keys.

Remount is the third deal. The agent's computer is a value that can move, the
credentials are never in it, and losing the connection loses nothing.

## 2. Seven resources

| Resource | Is |
|---|---|
| **Node** | a machine running the supervisor |
| **Workspace** | a movable computer: a filesystem, plus processes while it is running |
| **Session** | a stateful attachment to a workspace: exec, pty or port |
| **Artifact** | an immutable content-addressed blob: a snapshot, a file, a dataset |
| **Binding** | a secret reference bound to destinations, a principal and a TTL |
| **Timer** | a durable wake: at a time, after a duration, or on an event |
| **Principal** | who acts: a human, an agent, a node |

Everything else in the system is a verb on one of these.

## 3. Shape

```
  CONTROL PLANE                      one process, or three
  ┌──────────────────────────────────────────────────────────┐
  │  identity   claim queue   timers   bindings   event log   │
  │  policy     leases        grants   artifacts              │
  │                                                           │
  │  carries COORDINATION. Never carries session bytes.       │
  └───────────────────────────┬──────────────────────────────┘
                              │
  RELAY: routes frames by destination id. Interprets nothing.
                              │
        ┌─────────────────────┴─────────────────────┐
        │ outbound wss:443                          │ outbound wss:443
  ┌─────┴──────────────────────┐            ┌───────┴───────┐
  │ NODE                       │            │ CLIENT        │
  │  supervisor   identity key │            │  harness, CLI │
  │  sessions     fs           │            │  or your loop │
  │  egress broker ◀── secrets │            └───────────────┘
  │  snapshots    backends     │
  │  ┌──────────────────────┐  │
  │  │ WORKSPACE (untrusted)│  │
  │  │  agent has root      │  │
  │  │  sees only ref:…     │  │
  │  └──────────────────────┘  │
  └────────────────────────────┘
```

Nothing listens. Nodes and clients dial out. ([ADR 12](adr/0012-outbound-only.md))

## 4. The three things that make it work

### 4.1 A session is a log

Output is an append-only log of sequenced chunks. A client is a cursor. Attach
from sequence N and tail live are the same code path, so reconnect is not a
feature but the absence of one.

When a requested sequence has genuinely been evicted, the node sends a `gap`
chunk naming the lost range and continues. It does not go silent, and it does
not kill the session, because for an agent the output is the product and a
transcript with an unmarked hole is worse than one with a marked one.

Input carries a client-side sequence and duplicates are dropped, so a keystroke
retried after a dropped connection is not typed twice.
([ADR 3](adr/0003-session-is-a-log.md))

### 4.2 The workspace never holds a credential

A workspace holds references. A binding maps a reference to a real secret,
scoped to destination hosts, a principal and a TTL. The node's broker
substitutes the real value at the network edge and records it.

The important rule is what happens when a reference goes somewhere it should
not: the request is **blocked**, not forwarded, and recorded as `leak_blocked`.
([ADR 7](adr/0007-secret-blind-workspaces.md))

### 4.3 The computer moves

A snapshot is a deterministic tarball of the filesystem. Files, not memory,
because files are portable across nodes, operating systems, architectures and
vendors, and memory images are not. Processes restart; identity, files and
policy travel. ([ADR 6](adr/0006-snapshots-are-files.md))

## 5. The workspace state machine

```
              ws.create
                 │
                 ▼
            ┌─────────┐   node wins CAS    ┌──────────┐  node reports  ┌─────────┐
            │ pending │ ─────────────────▶ │ claiming │ ─────────────▶ │ claimed │
            └─────────┘    ws.claim        └──────────┘    ws.ready    └─────────┘
                 ▲              ▲               │                          │
                 │              └───────────────┼── node offline ──────────┤
                 │  lease expired               │                          │
                 │  or released                 ▼  materialize failed      │ move
                 └──────────────────────────────┴──────────────────────────┤ sleep
                 │                                                          ▼
                 │              timer fires, event arrives, or        ┌────────┐
                 └────────────── someone calls wake ───────────────── │ paused │
                                                                      └────────┘
```

`claiming` is the state that took two bugs to discover. A node that wins a claim
still has to restore a filesystem, and a node that has gone offline still holds
its lease but is not answering. Both are "held but not serving", and clients
must wait for `claimed`. ([ADR 11](adr/0011-ready-handshake.md))

## 6. Placement is a race, not a decision

The control plane offers a pending workspace to every eligible online node.
Nodes race. The claim is a single compare-and-swap: the state was pending, now
it is claiming, and the generation increments.

Split brain is impossible by construction. A node acts only on its own
generation, and a grant naming an old generation is refused, so a workspace that
moved cannot be operated through a stale grant.

A claim carries a lease. The holder renews at no more than a third of the lease
interval, including while it is still materializing, because a slow restore must
not lose the workspace it is restoring.
([ADR 9](adr/0009-claim-queue.md))

## 7. Trust

| Component | Trusted with | If compromised |
|---|---|---|
| control plane | policy, bindings, grant signing key | everything; true of every system in this category |
| node supervisor and broker | real credentials, egress decisions | that node's workspaces, and its leases until TTL |
| relay | routing metadata | metadata, and today frame contents |
| **workspace** | **nothing** | **nothing worth having** |

Concretely enforced:

- **Filesystem jail.** Paths resolve through symlinks and anything landing
  outside the workspace root is denied, including a symlink inside the workspace
  that points out of it. Archive extraction refuses parent-directory components.
- **Egress default deny.** A destination is permitted only by a binding or the
  node's allow list.
- **No private addresses.** Any host resolving to loopback, private, link-local
  or multicast is refused unless explicitly allowed, which closes cloud metadata
  endpoints by default. The broker dials the validated IP literal, not the name,
  so DNS cannot change under the check.
- **Fail closed.** An expired lease blocks. There is no passthrough mode.

([ADR 10](adr/0010-trust-boundaries.md))

## 8. Failure model

One recovery path. "Graceful shutdown" is a snapshot followed by a crash.

| Failure | Behavior | At risk |
|---|---|---|
| client disconnects | session keeps running; reattach replays from last seq | nothing |
| relay dies | both sides redial; sessions resume by seq | nothing |
| node uplink flaps | sessions keep running; workspaces demote to `claiming`, promote back on reconnect | nothing |
| node dies | lease expires, workspace returns to pending with its last snapshot, another node claims | work since the last snapshot |
| node restarts | it re-adopts its local copies at the same generation, so outstanding grants stay valid | nothing |
| control plane restarts | held workspaces re-queue; nodes re-adopt local copies; sessions in flight are lost with the node process only if it also restarted | in-flight session output |
| materialize fails | node releases the claim; another node tries | nothing |
| workspace compromised | sees references only; egress mediated; leak attempts blocked and recorded | allow-listed destinations until revoked |
| node compromised | leases are short; certs and secrets bounded by TTL | that node's workspaces |
| relay compromised | metadata, and today frame contents | see gaps below |

Every row above has a test in `internal/sim`, driven through in-memory
transports with fault injection, so the failure paths run on every commit rather
than only in production.

## 9. What is built, and tested

| Area | State |
|---|---|
| Frame protocol, CBOR, version skew | built, tested |
| Exec, pty and port sessions | built, tested |
| Lossless reconnect with replay and gap reporting | built, tested under fault injection |
| Idempotent exec and input | built, tested |
| Filesystem: read, write, list, stat, mkdir, remove, rename | built, tested |
| Server-side regex search with binary and size skipping | built, tested |
| Atomic multi-edit | built, tested |
| Snapshots, content-addressed artifacts, cross-node restore | built, tested |
| Claim queue, leases, generations, re-adoption | built, tested |
| Durable timers, sleep and wake, webhook wake | built, tested |
| Egress broker with substitution, leak blocking, audit | built, tested against the live OpenAI API |
| Event log, SQLite and in-memory, subscriptions with backfill | built, tested |
| Grants: ed25519, expiry, generation binding | built, tested |
| Backends: process and docker | built, tested |
| CLI: server, up, standalone, ws, exec, sh, attach, fs, port, nodes, events, timers | built, exercised live |

### Verified end to end with a real agent

OpenAI's Codex CLI 0.152.1 was installed inside a Remount workspace, pointed at
the broker, and asked to create a file. It made real model calls, ran a real
shell command through the session layer, and wrote the file. The workspace's
`OPENAI_API_KEY` held only a placeholder. A scan of the workspace tree found
zero copies of the real key, while the same scan found a deliberately planted
canary, which is what makes the zero meaningful.

## 10. Not built yet

Named honestly, because a roadmap presented as a feature list is a lie.

- **microVM backends.** Firecracker on Linux, Apple Virtualization on macOS.
  Today the strongest isolation is a container. The `process` backend is
  `isolation: none` and is only appropriate on a machine you own.
- **Display and browser sessions.** The protocol has the session kind reserved
  and the shape is understood, but there is no implementation.
- **End-to-end encryption through the relay.** Frames are TLS to the relay, so a
  compromised relay can read them. Noise IK inside the frame stream is the fix
  and the protocol has room for it because the relay already ignores bodies.
- **Credential substitution inside CONNECT.** Would require a CA in the
  workspace, which we declined in v0. Model traffic uses the reverse-proxy path.
- **Web and phone UI.** The event log and attach are the only two things a UI
  needs, and both exist.
- **Direct peer-to-peer.** Every session byte goes through the relay today.
- **Subagent composition and RL fan-out.** Fork-from-snapshot is one call away
  given artifacts, but there is no API for it yet.

## 11. Performance

Targets to measure rather than promises. Current numbers are from a laptop with
both sides in one process, so they bound the protocol overhead, not a WAN.

| Thing | Target | Observed locally |
|---|---|---|
| exec round trip through the relay | < 30 ms intra-region | sub-millisecond in-process |
| workspace create and claim | < 500 ms warm | ~1 ms, process backend |
| snapshot, release, restore, re-claim on another node | < 2 s for a small tree | ~200 ms |
| paused workspace cost | storage only | storage only |
| session log memory | 2 MiB per session | 2 MiB, plus 128 MiB spill |
| binary size | < 30 MB | 13 MB, static, no CGO |

## 12. Principles, and what each one decided

| Principle | Decided |
|---|---|
| Data dominates | the spec is a data model plus an event log; code follows |
| Interfaces are the design | seven resources, one frame type, about thirty operations |
| Minimal core, conservative growth | no plugin ABI, no framework, no prompt format |
| Don't complect | identity, placement, policy and transport are separate; a session is not a connection |
| The log is truth | state is a cache of the event log |
| Crash-only | recovery is the normal path; graceful shutdown is snapshot then crash |
| End-to-end argument | relays are dumb, the control plane never sees stdout |
| Define errors out of existence | exec does not fail with "connection dropped"; idempotency keys everywhere |
| Small trusted computing base | the workspace is trusted with nothing |
| Brute force first | a Postgres-shaped claim queue in SQLite, not a scheduler |

## 13. Layout

```
cmd/remount            the single binary: server, node, client
internal/proto         frames, types, operation names, event names
internal/transport     Conn, Peer, WebSocket, in-memory pipe with fault injection
internal/session       the log, the cursor, exec/pty/port, the manager
internal/fsops         jailed filesystem operations, search, atomic edit
internal/workspace     Backend interface, process and docker backends
internal/artifact      content-addressed store, deterministic snapshots
internal/eventlog      the canonical log, memory and SQLite, subscriptions
internal/broker        the egress credential broker
internal/relay         frame routing by destination
internal/control       claim queue, leases, timers, bindings, grants
internal/node          the supervisor
internal/client        the Go SDK
internal/server        HTTP surface: /v1/link, /v1/artifacts, /v1/events
internal/sim           the whole system in one process, with fault injection
spec/PROTOCOL.md       the wire protocol
docs/adr/              why, and what it cost
```
