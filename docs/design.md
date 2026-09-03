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
  CONTROL PLANE               one authoritative writer
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

The server listens; nodes and clients do not. They both dial the server over
one outbound WebSocket path. The reference binary colocates relay, control,
event log and artifact store, and supports exactly one SQLite-backed control
writer. Do not place independent controllers behind a load balancer.
([ADR 12](adr/0012-outbound-only.md))

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

### 4.2 The workspace does not receive a reusable credential

A workspace receives references. A binding maps a reference to a real secret,
scoped to destination hosts, a principal and a TTL. The node's broker
substitutes the real value at the network edge and records it. The workspace
can still exercise whatever broker capability it has been granted; removing a
static key is not the same as removing API authority.

The important rule is what happens when a reference goes somewhere it should
not: the request is **blocked**, not forwarded, and recorded as `leak_blocked`.
([ADR 7](adr/0007-secret-blind-workspaces.md))

### 4.3 The computer moves

A snapshot is a deterministic tarball of the filesystem. Files, not memory,
because files can be portable across compatible nodes and backends, and memory
images are not. Processes, installed host tools and architecture-specific
binaries do not travel; identity, files and policy do. A user-requested live
snapshot is labeled `live` and never silently becomes failover state. An
authoritative checkpoint fences Remount-managed execution, archives under an
exclusive tree lock, uploads the artifact, and commits its digest in control
state before success. ([ADR 6](adr/0006-snapshots-are-files.md))

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

The drawing shows the common path only. `quiescing`, `checkpointing`,
`released`, `destroying`, `destroyed` and `failed`, including actor and
generation preconditions, are normative in the
[protocol transition table](../spec/PROTOCOL.md#51-normative-lifecycle-transition-table).
All mutations go through `internal/control/state_machine.go`; exhaustive,
property and fuzz tests reject unspecified or stale transitions.

## 6. Placement is a race, not a decision

The control plane offers a pending workspace to every eligible online node.
Nodes race. The claim is a single compare-and-swap: the state was pending, now
it is claiming, and the generation increments.

Within the supported single-writer topology, generation-bound grants and node
self-fencing prevent stale ownership from remaining serviceable. A node acts
only on its own generation, renews affirmative authority, and stops sessions,
broker access and filesystem service at its local lease deadline. A grant
naming an old generation is refused, so a workspace that moved cannot be
operated through a stale grant. Running independent control databases would
break this premise and is explicitly unsupported.

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
| **workspace** | its files, references and granted capabilities | workspace data and abuse of those capabilities; broker-held secrets remain outside it |

Concretely enforced:

- **Filesystem jail.** Paths resolve through symlinks and anything landing
  outside the workspace root is denied, including a symlink inside the workspace
  that points out of it. Archive extraction refuses parent-directory components.
- **Broker policy.** Explicit typed rules default deny and constrain protocol,
  host, port, method, path, request counts and body sizes. With no typed policy,
  local mode permits reverse-proxy destinations named by a used binding or the
  node allow list. This is mediation, not proof that direct sockets are blocked.
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
| control plane restarts | recorded holders remain reserved for a recovery grace period; the same node re-adopts at the same generation; ambiguous transitional states become `failed` | no workspace bytes from control restart alone; control events since the last durable commit |
| materialize fails | node releases the claim; another node tries | nothing |
| workspace compromised | cannot read broker-held keys; broker requests are scoped and recorded | its files, granted destinations, and—on cooperative built-in backends—direct network access |
| node compromised | leases are short; certs and secrets bounded by TTL | that node's workspaces |
| relay compromised | metadata, and today frame contents | see gaps below |

Core lifecycle rows above have tests in `internal/sim`, driven through
in-memory transports with fault injection. Package-level tests additionally
exercise races, policy limits, malformed inputs and failure injection. These do
not substitute for backend-specific hostile-workspace conformance tests.

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
| Typed egress broker with substitution, leak blocking, redirect reauthorization, budgets and audit | built, unit/race/simulation tested; live-provider evidence is point-in-time |
| Managed HTTPS package connector with read-only policy, digest provenance and scope-private immutable cache references | built, direct/race/hostile-workspace simulation tested |
| Event log, SQLite and in-memory, subscriptions with backfill | built, tested |
| Grants: ed25519, expiry, generation binding | built, tested |
| Backends: process and docker | built, tested |
| CLI: server, up, standalone, ws, exec, sh, attach, fs, port, nodes, events, timers | built, exercised live |
| Public Go API and reconnecting client | built; compiled and tested from a separate module |
| Live snapshots and authoritative checkpoints | built; consistency is explicit and control commit is required for authority |
| Resource quotas and retention | built for workspaces, sessions, requests, snapshots, artifacts, connector cache, events, timers and mutation records; diagnostics and metrics expose limits and GC |
| Release pipeline | pinned actions, cross-platform static binaries, SBOM, checksums, provenance attestation and keyless checksum signature |

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
- **A built-in enforced-egress backend.** The policy and backend controller
  contract exist and production profiles reject weaker descriptors, but the
  shipped process and Docker implementations are cooperative proxies. Neither
  is a firewall or a non-bypassable multi-tenant boundary.
- **Display and browser sessions.** The protocol has the session kind reserved
  and the shape is understood, but there is no implementation.
- **End-to-end encryption through the relay.** Frames are TLS to the relay, so a
  compromised relay can read them. Noise IK inside the frame stream is the fix
  and the protocol has room for it because the relay already ignores bodies.
- **Credential substitution inside CONNECT.** Would require a CA in the
  workspace, which we declined in v0. Model traffic uses the reverse-proxy path.
- **Web and phone UI.** Remount ships no end-user UI (ADR 0046). It provides
  the device-neutral Agent API: any client, the developer's own web app, the
  reference app in `examples/diy-devin`, a vendor mobile app, or the harness's
  own web UI reached through the preview proxy, is a client of the same Agent.
- **Direct peer-to-peer.** Every session byte goes through the relay today.
- **Automated controller high availability.** The supported topology is one
  SQLite writer. Active/passive failover is an operator procedure, not a
  leader-elected service.
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
| session log memory | 2 MiB per session | configurable 2 MiB memory plus 128 MiB spill by default |
| binary size | < 30 MB | 13 MB, static, no CGO |

## 12. Principles, and what each one decided

| Principle | Decided |
|---|---|
| Data dominates | the spec is a data model plus an event log; code follows |
| Interfaces are the design | seven resources, one frame type, about thirty operations |
| Minimal core, conservative growth | no plugin ABI, no framework, no prompt format |
| Don't complect | identity, placement, policy and transport are separate; a session is not a connection |
| Durable truth is explicit | transactional resource rows recover authority; the ordered log records audit/observation history |
| Crash-only | recovery is the normal path; graceful shutdown is snapshot then crash |
| End-to-end argument | relays are dumb, the control plane never sees stdout |
| Define errors out of existence | exec does not fail with "connection dropped"; idempotency keys everywhere |
| Small trusted computing base | the workspace is trusted with nothing |
| Brute force first | a Postgres-shaped claim queue in SQLite, not a scheduler |

## 13. Layout

```
cmd/remount            the single binary: server, node, client
api                    stable public resource types, constants and error taxonomy
client                 supported reconnecting Go SDK
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
internal/client        SDK implementation and wire/reconnect machinery
internal/server        HTTP surface: /v1/link, /v1/artifacts, /v1/events
internal/sim           the whole system in one process, with fault injection
spec/PROTOCOL.md       the wire protocol
docs/adr/              why, and what it cost
```
