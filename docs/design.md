# Remount: the design

This is the architecture overview. The short version is in the
[README](../README.md); the wire format is in [the spec](../spec/PROTOCOL.md);
the reasoning behind individual choices is in [the ADRs](adr/). Implementation
boundaries and dated proof are indexed in [current status](engineering/current-status.md).

---

## 1. The problem

An agent needs a computer, but its execution lifetime, filesystem, credentials
and client connection need not share one failure boundary. Remount separates
them: a workspace can be checkpointed and placed on a compatible node, brokered
provider keys stay outside it, and clients can reconnect to retained sessions.
This is execution and lifecycle infrastructure beneath a harness, with a durable
Agent layer for conversation and task coordination. It is not itself a model
or an end-user chat product.

Those properties have limits: replay is bounded, filesystem moves do not
transfer arbitrary running processes, and explicit harness-native login keeps
credentials in the workspace. Backend isolation and provider portability need
their own compatibility and conformance proof.

## 2. Core resources

| Resource | Is |
|---|---|
| **Node** | a machine running the supervisor |
| **Workspace** | a movable computer: a filesystem, plus processes while it is running |
| **Session** | a stateful attachment to a workspace: exec, pty or port |
| **Artifact** | an immutable content-addressed blob: a snapshot, a file, a dataset |
| **Binding** | a secret reference bound to destinations, a principal and a TTL |
| **Timer** | a durable wake: at a time, after a duration, or on an event |
| **Principal** | who acts: a human, an agent, a node |

These are the original primitives, not an exhaustive resource count. The
protocol also defines durable Agents, queues, approvals, budgets, tenants,
identity credentials, node pools, shared volumes and export cursors. An Agent
adds conversation and task lifecycle on top of a workspace; it is not the
workspace itself.

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

A filesystem snapshot is a deterministic archive or chunked manifest with
content-addressed blobs. Files can travel across compatible nodes and backends;
host-installed tools and architecture-specific binaries are not automatically
portable. Process/Docker/gVisor filesystem moves do not transfer memory;
execution must be resumed or restarted by a supported harness/Agent lifecycle.
Firecracker additionally implements full disk/state/memory
checkpoints, whose restore needs compatible host, VMM, guest and CPU metadata;
it is not arbitrary cross-platform process migration. See
[ADR 0063](adr/0063-firecracker-checkpoints.md) and
[ADR 0072](adr/0072-chunked-snapshots.md). A user-requested live
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
| node supervisor and broker | real credentials, egress decisions | its workspace data and every reusable credential visible to it; broker TTLs do not revoke an exfiltrated provider key |
| relay | routing metadata and plaintext payloads on stock connections | metadata and frame contents; the internal E2EE guard protects payloads only when explicitly integrated |
| **workspace** | its files, references and granted capabilities | workspace data and abuse of those capabilities; broker-held secrets remain outside it |

Concretely enforced:

- **Filesystem API jail.** Paths resolve through symlinks and anything landing
  outside the workspace root is denied, including a symlink inside the workspace
  that points out of it. Archive extraction refuses parent-directory components.
  This does not isolate a process-backend command from the host filesystem.
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
| client disconnects | session keeps running; reattach replays from last seq | output beyond configured retention is an explicit gap |
| relay dies | both sides redial; sessions resume by seq if authority remains valid | an outage beyond the lease causes node self-fencing; retained-output limits still apply |
| node uplink flaps | workspaces demote to `claiming`, promote back after reconciliation | sessions can continue only while the lease remains valid; longer outages fence execution |
| node dies | lease expires, workspace returns to pending with its last snapshot, another node claims | work since the last snapshot |
| node restarts | it re-adopts local copies when durable authority agrees; generation may change after lease expiry | running processes are not generally preserved; retained files/logs depend on their durable state |
| control plane restarts | recorded holders remain reserved for a recovery grace period; the same node re-adopts at the same generation; ambiguous transitional states become `failed` | no workspace bytes from control restart alone; control events since the last durable commit |
| materialize fails | readiness is withheld; cleanup/release or retained-source reconciliation follows the failure boundary | availability; an ambiguous destructive operation retains and fences its source |
| workspace compromised | cannot read broker-held keys; broker requests are scoped and recorded | its files, granted destinations, and—on cooperative built-in backends—direct network access |
| node compromised | quarantine fences broker/workspace authority; rotate any exposed upstream credentials | that node's workspaces and credentials; provider revocation is separate from broker lease expiry |
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
| Backends: process and docker | built, tested; local/cooperative egress only |
| Linux gVisor and Firecracker | implemented with runtime probes and host-gated conformance; see the live verification ledger |
| Durable Agents, children/forks, queues and approvals | implemented; HTTP/frame APIs, CLI and simulator coverage |
| Operator console | embedded at `/console/`; unit and real-server Playwright coverage |
| Python and TypeScript SDKs | built and locally tested, including strict-profile negotiation; public publication is separate |
| Warm-standby controller failover | implemented with conditional object-store lease, epoch fencing and reconciliation; one active SQLite writer, nonzero potential RPO |
| Relay payload confidentiality | internal E2EE guard and adversarial simulations implemented; stock client/node integration remains open |
| Identity, encryption at rest, governance, pools and shared volumes | implemented; capability and external-provider requirements remain deployment-specific |
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

## 10. Current limitations

Named honestly, because a roadmap presented as a feature list is a lie.

- **Host-qualified isolation.** Linux gVisor and Firecracker are implemented,
  not automatically available on every host. Registration probes and exact-host
  conformance must pass. Process and Docker remain cooperative, and Apple
  Virtualization is not implemented.
- **Display and browser sessions.** The protocol has the session kind reserved
  but no native implementation. External browser/X11/VNC stacks can run as
  ordinary exec workloads; see [virtual desktops](harness-integration.md#browser-and-virtual-desktop-workloads).
- **Stock relay-confidentiality integration.** `internal/e2ee` implements
  authenticated key exchange and sealed peer payloads, with mutation/replay/
  reconnect tests. The control-plane binding operation and stock client/node
  opt-in are still pending. Normal CLI/SDK connections therefore remain TLS
  to the relay, not end-to-end encrypted through it. See
  [the B6 implementation boundary](engineering/plan-b-b6-relay-confidentiality.md).
- **Credential substitution inside CONNECT.** Would require a CA in the
  workspace, which we declined in v0. Model traffic uses the reverse-proxy path.
- **End-user chat and phone UI.** Remount ships an operator console, not a
  general-purpose agent chat product. It provides the device-neutral Agent
  API: any client, the developer's own web app, the
  reference app in `examples/diy-devin`, a vendor mobile app, or the harness's
  own web UI reached through the preview proxy, is a client of the same Agent.
- **Direct peer-to-peer.** Every session byte goes through the relay today.
- **Multi-writer and zero-RPO control.** Optional warm standby can acquire an
  expired object-store lease and promote automatically after reconciliation.
  It is not consensus replication or multiple active SQLite writers; unshipped
  writes may be lost. See [controller availability](operations.md#controller-availability).
- **General RL orchestration.** Agent forks and policy-constrained children
  exist; a dedicated training or reinforcement-learning scheduler does not.

## 11. Performance

Performance measurements need a candidate, host, backend and workload. The
early sub-millisecond exec and 13 MB binary figures do not describe the current
durable session path or all distribution targets. Use the dated
[benchmark report](benchmarks.md) and its raw JSON, and remeasure the candidate
before making latency, throughput or size claims. Historical results are not
an SLA or current-release proof.

The default per-session hot tiers are 2 MiB memory and 128 MiB spill; immutable
blob tiers add retained replay under their own limits. A paused workspace has
no assigned node, but retained artifacts consume storage and a provisioned
idle node may continue incurring compute charges. Distribution builds are
static (`CGO_ENABLED=0`); measure each artifact produced by `make dist` rather
than treating a historical binary size as an invariant.

## 12. Principles, and what each one decided

| Principle | Decided |
|---|---|
| Data dominates | the spec is a data model plus an event log; code follows |
| Interfaces are the design | explicit resource schemas and one versioned frame envelope; the protocol catalog owns the operation inventory |
| Minimal core, conservative growth | no plugin ABI, no framework, no prompt format |
| Don't complect | identity, placement, policy and transport are separate; a session is not a connection |
| Durable truth is explicit | transactional resource rows recover authority; the ordered log records audit/observation history |
| Crash-only | recovery is the normal path; graceful shutdown is snapshot then crash |
| End-to-end argument | the relay routes session frames; control handlers do not interpret them, but the colocated server can see stock plaintext payloads |
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
internal/workspace     Backend interface, process, docker, gvisor and firecracker
internal/artifact      content-addressed store, deterministic snapshots
internal/eventlog      the canonical log, memory and SQLite, subscriptions
internal/broker        the egress credential broker
internal/relay         frame routing by destination
internal/control       claim queue, leases, timers, bindings, grants
internal/node          the supervisor
internal/client        SDK implementation and wire/reconnect machinery
internal/server        HTTP surface: /v1/link, /v1/artifacts, /v1/events
internal/identity      principals, tenants, signed credentials and enrollment
internal/control/replicate fenced warm-standby database publication and recovery
internal/e2ee          opt-in peer payload confidentiality (not stock CLI wiring)
internal/provision     provider-backed whole-node lifecycle drivers
internal/volume        immutable shared artifact versions
sdk/python, sdk/typescript public non-Go clients
web                    embedded operator console
internal/sim           the whole system in one process, with fault injection
spec/PROTOCOL.md       the wire protocol
docs/adr/              why, and what it cost
```
