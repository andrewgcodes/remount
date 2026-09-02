# Remount codebase deep dive and point-in-time engineering review

Date: 2026-09-02, America/Los_Angeles

Review basis: commit <code>51cebb971090354cd9625deee27f261d1a7069d0</code> plus the dirty working tree present during the review. Almost all implementation files were untracked relative to that initial commit, and another coding agent was changing the tree during the review. The findings below therefore describe a point-in-time source snapshot, not a reproducible committed revision. The last verification pass was performed after the diagnostic and metrics additions compiled.

This report is deliberately broader than a normal code review. It explains what Remount is at four resolutions, traces the important operations end to end, maps every package to its collaborators, records the runtime and persistence mechanics, and then lists defects and design gaps by severity.

## Executive summary

Remount is an agent-compute substrate, not an agent framework. It turns “the computer an agent is using” into a named workspace that can be claimed by a machine, operated remotely, snapshotted, moved to another machine, paused, and resumed. It also turns a running command into a replayable session log rather than treating it as a fragile socket. A third core idea is that the workspace receives secret references while a trusted node-side broker holds and substitutes the real credentials.

The motivating problem is sound:

- Agent harnesses are otherwise tied to the laptop, VM, or sandbox vendor on which they began.
- A lost client connection usually means lost output or a lost process.
- Putting model, source-control, package-registry, and cloud credentials directly inside a hostile or prompt-injectable agent environment gives that environment the keys it is supposed to protect.
- A long-idle environment should be reducible to durable files rather than consuming a live machine.
- The compute substrate should work for Claude Code, Codex, OpenCode, a shell, or a custom loop without embedding any one harness's prompt or memory model.

The architecture is compact and unusually coherent for an early implementation. The protocol, state machine, session log, content-addressed snapshots, claim queue, node broker, and in-memory fault-injection transport fit together cleanly. The ADRs and <code>MISTAKES.md</code> show that the implementation was shaped by real failure cases rather than only a diagram.

The current implementation is nevertheless an alpha-quality reference implementation, not a safe production or multi-tenant system. There are several release-blocking problems:

1. The relay has a reproducible concurrent-map/nil-map panic during reconnect.
2. The control plane has a reproducible data race in workspace claim/readiness handling.
3. Node identity does not prove possession of its private key, and the single shared bearer token is effectively fleet-admin authority.
4. Lease expiry is not self-enforced on a node, so the generation scheme does not prevent two copies from executing after a partition.
5. Move and sleep destroy the source workspace even if snapshot or upload fails.
6. Snapshot restore can follow an archive-created symlink outside the workspace root.
7. Lifecycle operations are not serialized, so destroyed workspaces can be resurrected and a wake timer can be consumed before sleep finishes.
8. A custom client can use <code>port.open</code> to make the trusted node dial arbitrary loopback, private-network, metadata, or public addresses outside broker policy.
9. Control-plane persistence failures are logged but successful mutations are still returned, so acknowledged state can disappear on restart.

The test suite is also currently red because a new node-written <code>.remount</code> directory was not incorporated into an older end-to-end assertion. The code formats cleanly and passes <code>go vet</code>, but neither the ordinary nor race suite is green.

The shortest accurate product description is:

> Remount is a single-binary control plane, relay, node supervisor, and CLI for portable filesystem workspaces and replayable remote processes, with an experimental credential-broker boundary.

## Level 1: high-level product model

### What it is

There are four runtime roles, even though three of them can live in one binary:

| Role | Responsibility |
|---|---|
| Control plane | Owns desired workspace state, placement, generations, leases, timers, configured bindings, grant signing, and the central event log. |
| Relay | Routes opaque-by-convention protocol frames between named peers. It shares the server process with the control plane today. |
| Node | Runs on a machine, claims workspaces, materializes filesystems, starts processes, retains session output, brokers credentials, and creates snapshots. |
| Client | A CLI, harness, or SDK consumer that creates workspaces and talks to the control plane or the node holding a workspace. |

The one compiled command, <code>remount</code>, can run the server, run a node, run both in standalone mode, or act as a client. The custom example <code>agentloop</code> demonstrates a minimal harness using the internal Go client.

The domain is expressed as seven resources:

| Resource | Meaning in this implementation |
|---|---|
| Node | A persistent node identity plus an outbound connection, labels, host facts, and a set of local workspace handles. |
| Workspace | A portable filesystem declaration and, while claimed, processes hosted on exactly one intended node. |
| Session | An exec process, PTY, or TCP connection whose output is an append-only sequence. |
| Artifact | An immutable blob named by its SHA-256 digest; workspace snapshots are deterministic tar-gzip artifacts. |
| Binding | A secret, allowed destinations, allowed principals, placeholder, and TTL configured at the control plane. |
| Timer | A persisted “resume this workspace” action triggered at a time or by an event type. |
| Principal | The claimed human/agent identity attached to workspace and audit records. It is a data field today, not an authenticated authorization subject. |

### Why it exists

The central abstraction is that a workspace, rather than the host machine or harness process, is the durable unit. That creates three product properties:

1. **Location independence.** A workspace can be snapshotted on one node and restored on a node with different placement labels, architecture, or backend.
2. **Connection independence.** A session continues on the node while the client reconnects. The client resumes at a sequence number rather than hoping a byte stream survived.
3. **Credential indirection.** The workspace stores a placeholder. The trusted node-side broker substitutes a real credential only for a matching destination.

Sleep is a consequence of the same design: snapshot the files, destroy the live backend, persist a wake condition, then materialize it later. Port forwarding is also modeled as a session, so the same chunk and input paths can carry TCP data.

### What it intentionally is not

The code does not implement prompting, conversation memory, agent planning, model selection, tool semantics, or subagent orchestration. It does not migrate process memory. “Move” means stop processes, snapshot files, destroy the old environment, and start a fresh environment from those files. It also does not yet implement Firecracker/Apple Virtualization, browser/display sessions, a web UI, direct peer-to-peer data transfer, or end-to-end encryption through the relay.

### Product promises versus current implementation

| Intended promise | Current reality |
|---|---|
| A workspace's files can move between nodes | Implemented and exercised for process and Docker backends. Processes restart; only files move. |
| Client disconnect does not lose a session | Mostly implemented for a client-uplink reconnect while retained output remains. Node-uplink reconnect is not transparently resumed by an already-connected client. |
| Output loss is explicit | The log emits a gap record after retained history is evicted, which is the right model. Spill write failures can currently make loss silent. |
| The workspace never holds a real credential | The normal reverse-proxy path keeps configured secrets in the broker, but network mediation is cooperative, the process backend is unisolated, Docker cannot use the host-loopback broker as currently configured, and an upstream can reflect a substituted secret in its response. |
| Egress is default-deny | The broker is default-deny. The workspace's actual network stack is not: both implemented backends report <code>EgressEnforced: false</code>. |
| Generations make split brain impossible | Stale client grants are rejected, but an old node does not fence its own processes when renewals fail. Split execution and duplicated external effects remain possible. |
| Everything is an event and the log is truth | There is a useful event log, but authoritative state is loaded from separate CBOR state tables, state/event writes are not atomic, and node events are best-effort. |
| Every mutation is retry-safe | Workspace creation and session open have partial idempotency. Most other mutations do not. |
| Nodes authenticate with persistent Ed25519 identity | The key is persisted and the public key is pinned, but the node never signs a challenge, so possession of the private key is not proven. |

## Level 2: medium-level architecture

### Runtime topology

    client CLI / harness
          |
          | outbound WebSocket, CBOR frames
          v
    +------------------------------------------------------+
    | server process                                       |
    |                                                      |
    |  relay -----------------------> connected node peer   |
    |    |                                |                 |
    |    +--> in-process control plane    |                 |
    |           |                         |                 |
    |           +--> SQLite               |                 |
    |           +--> artifact HTTP store  |                 |
    +-------------------------------------|-----------------+
                                          |
                                          v
                                +--------------------+
                                | node supervisor    |
                                | - workspace map    |
                                | - session manager  |
                                | - local artifacts  |
                                | - local event log  |
                                | - broker per ws    |
                                +---------+----------+
                                          |
                          +---------------+----------------+
                          |                                |
                    process backend                 Docker backend
                    host directory +                host directory mounted
                    ordinary OS processes           at /work + docker exec
                          |
                    loopback broker
                          |
                    permitted upstreams

Nodes and clients dial the server; a node does not expose a public Remount listener. The broker does listen on a node-local ephemeral loopback port. The CLI's <code>port</code> command also opens a local listener on the client machine.

There is a second, less obvious network path: a port session is created by the node process itself. With the process backend, a caller-provided <code>PortOpenReq.Host</code> is passed to <code>net.Dialer</code> in the trusted node namespace. This is not an ingress connection into a workspace and it does not pass through broker destination checks. Docker overwrites the host with the inspected container IP, but the process backend exposes the raw wire capability.

The relay and control plane are conceptually separate but share one process and one trust domain. Session bytes do not enter <code>control.HandleFrame</code>, but they do traverse the relay process and are not end-to-end encrypted. A compromise of that process can read or alter them.

### Ownership of data

| Data | Owner and location | Durability |
|---|---|---|
| Workspace desired/current state | Control plane maps, mirrored as CBOR blobs in SQLite <code>workspaces</code> | Durable if each best-effort save succeeds |
| Node registrations | Control map and SQLite <code>nodes</code> | Durable public key and last status |
| Timers | Control map and SQLite <code>timers</code> | Durable |
| Workspace-create idempotency | Control map and SQLite <code>idem</code> | Durable, globally keyed |
| Grant signing key | SQLite <code>keys</code> | Durable; raw private key in DB |
| Events | SQLite <code>events</code> at control; bounded in-memory log at node | Central control events durable; node-to-control mirroring is best-effort |
| Snapshot artifacts | SHA-256-named files in server and node artifact directories | Durable when uploaded and storage survives |
| Live workspace files | Node data directory; Docker also bind-mounts this directory | Durable only on that node until a snapshot is committed |
| Live session/process | Node memory and OS process/container | Lost if node process/host dies |
| Session output | In-memory chunk ring plus per-session spill file | Survives client/relay loss; not node loss |
| Real binding secrets | Control configuration and leased node memory | Not intended to enter workspace files |
| Node identity | Node <code>identity.json</code> | Durable but regenerated silently if invalid |

### Workspace state machine

The intended state machine is:

    create
      |
      v
    pending --claim CAS/gen++--> claiming --ready--> claimed
      ^                              |                  |
      |                              |                  |
      +---- release/lease expiry ----+------------------+
      |
    paused <---- snapshot + sleep ---- claimed
      |
      +---- timer/event/manual wake --> pending

    any state --destroy--> destroyed

The important distinction is “held” versus “serving”:

- <code>claiming</code> means a node owns the generation and lease but is restoring, or is temporarily offline.
- <code>claimed</code> means the node has reported materialization complete and grants may be issued.
- The control plane considers both states “held” for renewal and lease expiry.

The generation increments on a new claim. A client grant contains client ID, workspace ID, node ID, principal, generation, and expiration. The node verifies the control-plane signature and then compares the grant to its local workspace generation.

The implementation's transition validation is incomplete. <code>ws.ready</code> checks node and generation but not that the current state is <code>claiming</code>. Move and sleep accept a destroyed workspace because <code>ws.get</code> returns destroyed records and <code>release</code> treats any non-held state as already released. Lifecycle calls can run concurrently while <code>release</code> is waiting on a node RPC.

### Placement and claiming

Pending workspaces are offered to every connected eligible node. Eligibility checks:

- requested backend;
- OS and architecture;
- advertised CPU and memory;
- required capability strings;
- an explicit node pin;
- required node labels.

The first node whose <code>ws.claim</code> observes <code>pending</code> under the control mutex wins. This is intentionally a race rather than a scheduler. <code>Placement.Prefer</code> is defined but unused. CPU and memory checks compare a request with static host totals; there is no reservation, utilization accounting, priority, fairness, or capacity admission.

The memory check is also fail-open for an unknown value: a positive requirement is rejected only when the node reports a positive-but-insufficient capacity. A node reporting zero/unknown memory therefore satisfies every memory request.

On the node, <code>materializing</code> serializes duplicate offers for the same workspace and is included in lease renewal. After claim, the node either adopts an existing backend or creates one, downloads/restores a named artifact, leases bindings, starts a broker, writes <code>.remount/env</code>, installs the workspace in its local map, and calls <code>ws.ready</code>.

### Trust boundaries

The intended trusted computing base is the control plane plus node supervisor/broker. The workspace is assumed hostile.

The implemented trust mechanisms are:

- a shared bearer token in each hello and HTTP artifact/webhook request;
- relay enforcement of authoritative <code>From</code>;
- a persisted node ID and Ed25519 public-key pin;
- Ed25519-signed client grants scoped to client, workspace, node, generation, principal, and time;
- a path resolver for supervisor-side filesystem calls;
- broker allowlists, binding destination matching, lease expiration, public-IP validation, and DNS pinning;
- content-addressed artifact identifiers.

The model is appropriate for a single trusted operator experimenting on dedicated machines. It is not a tenant boundary. The bearer token authorizes every role, client principals are assertions, workspace ownership is never checked, node key possession is not proven, and the implemented backends do not force traffic through the broker.

### Failure model

The design handles several failures elegantly:

- A dropped client WebSocket fails pending RPCs and the client can redial.
- A live session log is independent of the client connection.
- Reattach from the next expected sequence replays retained output.
- A slow subscriber can catch up from the event store.
- A short node disconnect demotes workspaces to <code>claiming</code> while retaining generation and lease.
- A dead node eventually loses the control-plane lease and another node can restore the last uploaded snapshot.
- Duplicate workspace offers on one node are serialized.

The remaining hard failures are at ownership boundaries:

- The old node does not stop itself at lease expiry.
- A control-plane restart rewrites held workspaces to pending without a reconciliation protocol.
- Snapshot and release are not a two-phase commit.
- State and audit are separate writes.
- Node events have no durable outbox.
- The client reacts to its own connection loss, not to a node peer's loss on the shared relay.
- Port sessions originate from the trusted node network and are outside the broker's default-deny policy.

## Level 3: low-level end-to-end flows

### 1. Server startup

<code>remount server</code> parses a listen address, data directory, token, bindings file, and lease duration. <code>server.New</code>:

1. opens a pure-Go SQLite database;
2. creates the event table and state tables;
3. creates an artifact directory;
4. loads or creates the control-plane Ed25519 signing key;
5. loads workspaces, timers, nodes, and create-idempotency keys;
6. creates the relay;
7. attaches the relay to the control plane;
8. starts a one-second lease/timer/offer loop.

The HTTP mux exposes:

- <code>/v1/link</code>: WebSocket upgrade;
- <code>/v1/artifacts/{id}</code>: authenticated PUT/GET/HEAD;
- <code>/v1/events</code>: authenticated JSON webhook ingestion;
- <code>/healthz</code>: unconditional JSON liveness plus peer count.

There is no built-in TLS; production documentation expects a TLS reverse proxy.

### 2. Node startup and reconnect

<code>remount up</code> builds a backend registry and creates a node. The node creates <code>ws</code>, <code>spill</code>, and <code>artifacts</code> directories, then reads or generates <code>identity.json</code>.

The node opens a WebSocket and sends a hello containing:

- its persistent <code>n_...</code> ID;
- role <code>node</code>;
- shared token;
- protocol capability <code>v1</code>;
- its public key;
- labels;
- OS, architecture, CPU, memory, backend names, extra capability strings, and version.

The server validates the token and public-key pin, returns its grant-verification public key and lease duration, records the node online, and offers pending work.

After hello the node:

1. installs the new peer and clears its cached client grants;
2. calls <code>resync</code> for in-memory workspaces;
3. scans the local workspace directory and tries to reclaim directories not already in memory;
4. serves until the peer or parent context closes;
5. clears stream subscriptions;
6. retries with exponential backoff up to 30 seconds.

A separate 250-millisecond ticker renews when one third of the advertised lease has elapsed. The exponential reconnect backoff is never reset after a successful connection, so later outages eventually always wait the maximum.

### 3. Client connection

The Go client connects lazily. A hello presents role <code>client</code>, shared token, optional prior client ID, and caller-supplied principal. The control plane assigns a <code>c_...</code> ID if necessary.

Each successful client connection:

- replaces the previous peer;
- increments a local connection generation;
- clears cached grants;
- starts a supervisor;
- starts best-effort reattach goroutines for locally registered sessions.

An RPC that fails specifically with <code>transport.ErrClosed</code> is retried up to five times by default. Other errors return immediately.

### 4. Workspace create and claim

The client sends <code>ws.create</code> with a generated idempotency key and workspace specification. The control plane validates that named binding IDs exist, fills an empty workspace principal from the hello principal, writes a pending workspace, emits <code>ws.created</code>, and broadcasts offers.

Each eligible node can start <code>tryClaim</code>. The node reserves the workspace ID locally, calls <code>ws.claim</code>, and receives the workspace plus lease duration if it wins. A new claim increments the generation.

Materialization then:

1. selects the requested backend;
2. adopts or creates the backing environment;
3. fetches a restore artifact if present;
4. extracts it into the workspace directory;
5. requests real binding leases from control;
6. starts one loopback broker for the workspace;
7. writes a sourceable <code>.remount/env</code> containing only current broker metadata and placeholders;
8. puts the handle in <code>n.workspaces</code>;
9. emits restore/claim-related events;
10. calls <code>ws.ready</code>.

Only after ready changes the state to <code>claimed</code> will the control plane issue client grants.

### 5. Client-to-node authorization

For a node operation the client first calls <code>grant</code> at the control plane. The signed claims name the currently claimed node and generation and expire in one hour. The client caches the grant until one minute before expiry.

The actual request is routed directly to the node ID through the relay. The node:

1. finds the local workspace;
2. verifies the control-plane Ed25519 signature;
3. compares client, workspace, node, and generation claims;
4. caches the grant by <code>client|workspace</code>;
5. performs the operation.

Some follow-up request bodies omit the grant and depend on that connection-local node cache. That optimization is the source of several fresh-connection bugs described below.

### 6. Session open, output, replay, and exit

The node merges workspace and request environment values, resolves secret placeholders to broker URLs/references, prepends broker proxy variables, and lets the backend rewrite the working directory and program.

The session manager creates:

- a sortable <code>s_...</code> ID;
- an in-memory chunk log;
- an optional spill file;
- a session record;
- an optional idempotency mapping.

It starts one of three runners:

- exec: <code>exec.Cmd</code> with separate stdout/stderr pipes and an optional stdin pipe;
- PTY: <code>creack/pty</code>, one combined output stream, resize support;
- port: a TCP connection, with remote bytes represented as stdout chunks.

Pumps read 32 KiB at a time. The log splits larger appends to 32 KiB, assigns increasing sequence numbers, stores recent chunks in memory, and spills evicted chunks to disk. Exit is a final typed chunk and closes the log.

The node creates a cursor at the requested sequence. If that sequence predates retained output it sends a typed gap chunk naming the unavailable interval, skips to the oldest retained sequence, and continues. Otherwise it sends chunks until EOF or cancellation.

The client maintains <code>next</code>, discards duplicates, buffers future sequence numbers, and exposes an ordered channel. Chunks arriving before the open response go to an orphan buffer keyed by server session ID.

On client WebSocket failure, pending RPCs fail, the supervisor redials, and every local session issues <code>s.attach</code> from its current <code>next</code>. This is the core lossless-client-reconnect mechanism.

### 7. Filesystem operations

The node authorizes the workspace and calls its backend's <code>FS()</code>. <code>fsops</code> implements:

- bounded reads with offset;
- atomic replace through temp-file plus rename;
- append;
- sorted directory listing and stat;
- recursive mkdir and remove;
- rename;
- RE2 search, skipping <code>.git</code>, binary files, and files over 8 MiB;
- multi-edit that requires a unique match unless “all” is requested, then atomically rewrites.

Paths are normalized as workspace-absolute paths. The resolver evaluates the longest existing prefix and rejects a resolved prefix outside the root. This blocks simple <code>..</code> and static symlink escapes but is not safe against a concurrently mutating hostile filesystem.

### 8. Snapshot, move, and restore

A snapshot walks the host-side workspace tree, applies exclude globs, sorts paths, normalizes tar ownership, writes PAX tar through gzip, and streams it to the local artifact store. The store hashes as it writes and names the file <code>art_sha256:&lt;digest&gt;</code>. The first two digest characters form the storage subdirectory.

For a move:

1. control marks the workspace <code>released</code>;
2. control asks the node to release with snapshot enabled;
3. the node removes the workspace from its serving map;
4. the node kills its sessions;
5. the node snapshots and uploads;
6. the node closes the broker and destroys the backend;
7. control records the returned snapshot;
8. control applies new requirements/placement;
9. control sets pending and offers it;
10. a node claims, restores, and reports ready.

This is file migration, not live process migration. The ordering currently fails open if steps 5 or 6 fail.

### 9. Sleep, timers, events, and wake

Sleep first creates and persists a timer, then uses the same release/snapshot path as move, then marks the workspace paused.

The one-second control loop finds due time timers. Posted events also scan event-triggered timers. Firing marks a timer fired, emits <code>timer.fired</code>, and calls wake. Wake only changes <code>paused</code> to <code>pending</code>, after which the normal claim path restores it.

Node events are appended to a local 10,000-entry memory store and asynchronously posted to control. Control events and accepted posted events use SQLite sequence numbers. Event subscribers combine a bounded live channel with database backfill when they fall behind.

### 10. Credential broker

Each materialized workspace gets a broker listening on <code>127.0.0.1:0</code>. A binding lease contains the real secret, allowed destinations, principals, placeholder, and expiration.

There are three request styles:

- reverse-proxy HTTPS: <code>/d/host/path</code>;
- reverse-proxy HTTP: <code>/http/host/path</code>;
- standard forward proxy, including CONNECT.

For an inspected HTTP request the broker scans header values, including decoded Basic auth. A known placeholder:

- is blocked if sent to a host outside its binding;
- is blocked if its lease is expired;
- is replaced with the real secret if valid.

The request is permitted if a used binding covers the destination or the node-wide allowlist covers it. Resolution rejects public names that resolve to loopback/private/link-local/multicast addresses unless separately allowed, and the validated IP literal is dialed to avoid DNS rebinding.

CONNECT is a blind TCP tunnel. It checks destination permission but cannot inspect or substitute TLS contents.

### 11. Port forwarding

The CLI listens locally. For each accepted socket it opens a remote <code>port</code> session. Local-to-remote bytes use sequenced session input; remote-to-local bytes use stdout chunks. On Docker, the backend inspects the container IP before the node dials the port.

The public helper only supplies a port and therefore defaults to loopback, but the protocol also exposes a caller-selected host. For a process workspace, the node dials that host directly from its own namespace. Thus the comment “inside the workspace” is only a convention, not a security property; a custom client can use this path as server-side request forgery or as an egress-policy bypass.

### 12. Restart behavior

Node restart with the same control plane can re-adopt an on-disk workspace. A brief node uplink reconnect works especially well: the control plane demotes claimed to claiming but preserves node and generation; the node reports ready and outstanding grants remain valid.

Control-plane restart is different. Loading the DB changes every held workspace to pending and clears its node. A still-running node first tries <code>ws.ready</code> for its in-memory copy, receives a conflict, and destroys that local copy. This is a current data-loss path and contradicts the documented “re-adopt local copies” behavior.

## Package and dependency map

The code is about 12.5 thousand lines of Go across roughly forty Go files, plus approximately five thousand lines of documentation and project metadata. The largest implementation files are <code>internal/node/node.go</code>, <code>internal/control/control.go</code>, <code>cmd/remount/main.go</code>, and <code>internal/client/client.go</code>.

| Package/path | Owns | Important collaborators |
|---|---|---|
| <code>cmd/remount</code> | Command dispatch, flags, terminal raw mode/resize, local port listener, assembly of server/node/client | Imports client, control binding type, node, proto, server, transport, workspace |
| <code>internal/proto</code> | Frame shape, CBOR modes, operation bodies, resource types, event/error constants, diagnostics types | Base package for nearly everything |
| <code>internal/ids</code> | Time-sortable prefixed identifiers | Used by control, client, session |
| <code>internal/transport</code> | <code>Conn</code>, WebSocket connection, in-memory fault-injection pipe, <code>Peer</code> request correlation | Depends only on proto plus WebSocket library |
| <code>internal/relay</code> | Hello gate, peer registry, destination routing, control-originated RPC | Depends on transport/proto; exposes interfaces implemented by control |
| <code>internal/control</code> | Authentication, state maps, claim CAS, placement, lease/timer loop, grants, binding leases, central events, diagnostics | Uses relay interface, eventlog, SQLite handle, proto |
| <code>internal/eventlog</code> | Append/read/last abstraction, memory store, SQLite store, live subscription/backfill | Used by control and node |
| <code>internal/artifact</code> | SHA-256 blob store, deterministic tar-gzip snapshot and restore | Used by workspace, node, server |
| <code>internal/fsops</code> | Supervisor-side workspace file API | Returned by workspace handles, called by node |
| <code>internal/session</code> | Output log, spill format, cursors, exec/PTY/TCP runners, session manager | Used by node and workspace backends |
| <code>internal/workspace</code> | Backend/Handle contracts, process backend, Docker backend, environment construction | Composes fsops, artifact, session, proto |
| <code>internal/broker</code> | Per-workspace reverse/forward proxy, secret substitution, destination/IP checks, audit | Created by node |
| <code>internal/node</code> | Nearly all data-plane orchestration | Composes artifact, broker, control grant verification, eventlog, session, transport, workspace |
| <code>internal/client</code> | Typed SDK, reconnect/retry, grant cache, remote session reorder/replay | Uses transport/proto/ids |
| <code>internal/server</code> | HTTP assembly and artifact/webhook surface | Composes control, relay, eventlog, artifact, transport |
| <code>internal/metrics</code> | Process-global atomic counters/gauges and Prometheus rendering | Recently wired into several packages; not yet exposed as a scrape endpoint |
| <code>internal/sim</code> | Entire system in process over fault-injected pipes | Exercises server, nodes, clients, moves, reconnects, timers, broker, and Docker |
| <code>examples/agentloop</code> | Minimal harness loop | Uses the internal client because it is inside this module |
| <code>deploy/modal_app.py</code> | Single-container Modal demo with durable volume | Launches server and process-backend node as sibling processes |

The effective dependency layering is:

    proto / ids / metrics
          |
    transport, artifact, fsops, eventlog, session, broker
          |
    workspace, relay, client
          |
    control, node
          |
    server
          |
    cmd/remount and examples

There is no Go import cycle between control and relay because relay defines the controller/sender interfaces and control imports those interfaces.

One public-interface problem is hidden in that layout: the claimed “Go SDK” lives under <code>internal/client</code>. Go prevents another module from importing it. External consumers currently have the protocol, CLI, or copied code, but not a supported public Go package.

## Level 4: bare-metal mechanics

### Identifiers

IDs are a prefix plus 26 lowercase Crockford-like base32 characters. The first six raw bytes encode Unix milliseconds and the remaining ten are cryptographically random. IDs therefore sort approximately by creation time. Common prefixes include <code>n</code>, <code>c</code>, <code>ws</code>, <code>s</code>, <code>t</code>, and <code>b</code>. Artifacts are different: their ID is the full SHA-256 content digest.

### Wire encoding

All traffic uses one <code>Frame</code>:

| Field | Use |
|---|---|
| <code>V</code> | Protocol version |
| <code>T</code> | hello, req, res, chunk, ev, ping, or pong |
| <code>ID</code> | Request/response correlation |
| <code>Seq</code> | Per-session chunk sequence |
| <code>S</code> | Session ID |
| <code>WS</code> | Workspace ID |
| <code>To</code>, <code>From</code> | Logical routing; relay overwrites inbound From |
| <code>Op</code> | Operation or event name |
| <code>Body</code> | Nested CBOR payload |
| <code>Err</code> | Stable error code plus human message |

Encoding uses fxamacker's deterministic Core CBOR mode. Unknown CBOR fields are ignored. Arrays and maps are capped at 2^20 elements/pairs, and the transport caps the complete encoded frame at 4 MiB. WebSocket compression is disabled and every binary WebSocket message is exactly one frame.

<code>DecodeFrame</code> only requires a non-empty kind. Despite its comment, it does not reject unsupported versions or unknown kinds. Hello also returns <code>v1</code> without computing a real capability intersection.

### Transport concurrency

Every <code>transport.Peer</code> has:

- one read goroutine;
- one atomic request-ID counter;
- one mutex-protected map from request ID to a buffered one-result channel;
- a terminal done channel;
- a 30-second per-write timeout.

Responses matching the local pending map are delivered there. Unmatched responses are sent to the handler so a relay can forward transit traffic. Every other request/event/chunk is synchronously handed to the handler on the only read goroutine. Handlers therefore must not block; the control and node spawn request goroutines, while client chunk/event delivery currently can block.

The in-memory transport encodes and decodes every frame before delivery so tests do not accidentally share mutable pointers across the simulated wire. Hooks can drop frames, and closing either end closes a shared channel after queued messages drain.

### Relay mechanics

The first frame is read before a <code>Peer</code> exists and must be hello. After authentication, the relay:

- wraps the connection in a Peer;
- forces every incoming frame's From to the authenticated ID;
- replaces any older connection using the same ID;
- routes by To;
- keeps a “recent correspondents” graph so it can send <code>peer.gone</code>;
- has its own pending map for requests originating at control.

The body remains uninterpreted by the routing function, but it is ordinary plaintext in the relay's memory.

### Control locking and persistence

One mutex protects maps of workspaces, timers, nodes, clients, idempotency keys, bindings, and event-tail cancellations. Control requests are dispatched in independent goroutines. Many operations mutate state under the mutex, synchronously write SQLite while still holding it, unlock, and then append an event.

SQLite runs in WAL mode with <code>synchronous=NORMAL</code>, a five-second busy timeout, and one database connection to keep event sequence allocation serialized. State tables store whole deterministic-CBOR values. The event table has an autoincrement sequence and an index on <code>(stream, seq)</code>.

Because state tables are loaded directly, the event log is not actually used to reconstruct state. Because state and event writes are separate and state-save failures are logged rather than returned, neither side is guaranteed to describe the other after a crash or disk failure.

### Session log format

At node construction, each session is configured for:

- 2 MiB in-memory output;
- 128 MiB spill;
- maximum 32 KiB chunk data;
- maximum 16,384 in-memory chunks;
- 24-hour finished-session retention.

The spill format is a repeated record:

    8 bytes: big-endian sequence
    1 byte : stream kind
    4 bytes: big-endian payload length
    N bytes: payload

An index stores the file offset every 64 chunks. When the next record would exceed the spill capacity, the whole spill file is truncated and history starts over. The in-memory ring always retains at least one chunk.

The stream kinds are info, stdout, stderr, exit, and gap. The intended invariant is info at sequence zero and exit last. Output-pump goroutines currently start before the info append, so that invariant is not guaranteed.

### OS process behavior

Exec uses <code>exec.Command</code>, starts a new process group, and separately pumps stdout/stderr. Signals target the negative PID so the whole group receives them. PTY uses <code>creack/pty</code> and a combined stream. A session timeout sends KILL. Killing a workspace enumerates all its sessions, signals them, waits up to five seconds, removes manager entries, and releases spill files.

The process backend only changes cwd and environment. It is not a sandbox. The command can use absolute paths, inspect other processes allowed to the node user, and open arbitrary network connections.

Docker starts a long-lived container with <code>--init</code>, a read/write <code>/work</code> bind mount, optional CPU/memory flags, default Docker networking, and <code>sleep infinity</code>. Each session is rewritten as <code>docker exec</code>. No user, capability, seccomp, no-new-privileges, read-only-root, or network policy flags are applied.

### Filesystem behavior

The supervisor resolves paths by:

1. converting backslashes to slashes;
2. cleaning relative to a synthetic root slash;
3. joining under the real workspace root;
4. walking upward to the longest existing prefix;
5. evaluating symlinks in that prefix;
6. requiring the result to remain under the root;
7. appending the non-existent suffix.

Reads default to 4 MiB and cap at 32 MiB. Search skips files over 8 MiB, detects a NUL in the first 8 KiB as binary, caps scanner tokens at 1 MiB, returns 500 matches by default, and truncates displayed lines to 4 KiB.

Atomic write means the final file replacement is a rename, not that path authorization and replacement are one indivisible operation.

### Artifact behavior

<code>Store.Put</code> writes to a temporary file, hashes while copying, creates a two-character shard directory, and renames the temp file into the digest path. If that path already exists it assumes identical content and returns it. There are no upload or store quotas.

Snapshot walks and sorts the tree, records files/directories/symlinks, normalizes uid/gid/user/group, and writes PAX tar through gzip. It preserves regular-file mode and mtime on restore and applies directory modes last. Directory/symlink times are not restored. Special file headers may be written but are ignored on restore.

Restore checks literal parent components and a lexical root prefix. It does not prevent a prior tar entry from creating a symlink that a later regular-file entry follows.

### Broker networking

The broker's HTTP transport never chains through an ambient proxy. It requires TLS 1.2 for HTTPS, allows up to 64 idle connections, uses a 15-second dial and TLS-handshake timeout, and permits five minutes for response headers to support model APIs. Bodies can stream indefinitely.

It snapshots the current lease slice under a read lock for each request. Header substitution supports direct text and HTTP Basic values. Destination matching accepts exact hosts, wildcard subdomains, or <code>*</code>, optionally with ports.

The broker exports uppercase and lowercase HTTP(S) proxy variables plus <code>NO_PROXY=127.0.0.1,localhost</code>. Workspace/request environment is appended later and therefore can override those values.

### Important default limits

| Limit/default | Value |
|---|---:|
| Frame | 4 MiB |
| Session chunk | 32 KiB |
| Node session memory | 2 MiB/session |
| Node session spill | 128 MiB/session |
| Finished-session retention | 24 hours |
| File read through SDK | first 4 MiB unless caller uses lower-level offset API |
| Maximum single node file read | 32 MiB |
| Search file | 8 MiB |
| Search result count | 500 default |
| Control lease | 30 seconds default |
| Renewal interval | lease/3, minimum 250 ms |
| Grant TTL | 1 hour |
| Binding lease TTL | 600 seconds default |
| Client retries | 5 |
| Node local events | 10,000 |
| Non-following event read | 1,000 |

### Build and dependencies

The module is <code>remount.dev/remount</code> and declares Go 1.27.1. Direct libraries are deliberately few:

- coder/websocket;
- creack/pty;
- fxamacker/cbor;
- x/term;
- modernc/sqlite.

The main binary is built with <code>CGO_ENABLED=0</code>, trimpath, and stripped symbols. The reviewed local binary is a roughly 12 MiB Darwin arm64 Mach-O built from the dirty tree. The local <code>agentloop</code> binary is roughly 10 MiB and was manually built with CGO enabled. The Makefile cross-builds Linux and Darwin on amd64 and arm64.

## Verification performed

I did not modify any implementation file during the investigation; this report is my only intended repository addition. The local <code>.env</code> file was deliberately not read. Other files did change concurrently, so all test claims below refer to the source snapshot described at the top of this document.

### Commands and outcomes

| Check | Outcome |
|---|---|
| <code>gofmt -l .</code> | Clean at the verification snapshot |
| <code>go vet ./...</code> | Passed |
| <code>go test -count=1 -timeout 300s ./...</code> | Failed in <code>internal/sim/TestExecEndToEnd</code> |
| <code>go test -race -count=1 -timeout 900s ./...</code> | Failed at the same end-to-end assertion |
| <code>go test -race -count=20 -run '^TestReconnectMidStreamIsLossless$' ./internal/sim</code> | Reproduced relay panic: <code>assignment to entry in nil map</code> in <code>Relay.route</code> |
| <code>go test -race -count=10 -run '^TestNodeDeathMovesWorkspaceFromSnapshot$' ./internal/sim</code> | Reproduced data race: <code>wsClaim</code> read versus <code>wsReady</code> write |
| <code>go test -count=1000 -run TestExecEchoAndExitCode ./internal/session</code> | Passed; did not disprove the source-visible info-order race |
| <code>CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...</code> | Failed: shared session code uses Unix-only <code>syscall</code> fields/functions |
| <code>make conformance</code> | Returned success while running nothing; the target is phony but has no recipe |
| Docker smoke in full sim suite | Docker daemon was available; Docker workspace test ran and passed |

The ordinary failure is straightforward: materialization now creates <code>.remount/env</code>, so a root listing contains <code>.remount</code> and <code>src</code>, while the test still asserts that <code>src</code> is the only entry.

There are 66 named Go tests, but no direct package tests for client, control, metrics, node, relay, or server. Those packages are exercised indirectly by <code>internal/sim</code>. That is valuable integration coverage, but it makes exact state transitions and malicious-frame cases harder to isolate, and one early sim failure can hide later failures.

<code>staticcheck</code> and <code>govulncheck</code> were not installed in the review environment, so they were not run.

## Findings

The labels below mean:

- **Observed**: reproduced by a command during this review.
- **Source-confirmed**: follows directly from a reachable code path.
- **Architectural risk**: the implementation lacks the mechanism required to uphold a claim; an adversarial test should be added.

### P0: release blockers

#### P0.1 Relay routing can race and panic

**Observed.** <code>Relay.route</code> takes <code>r.mu.RLock</code> and writes new entries into <code>r.recent</code>. It later drops that lock, takes the write lock, and assumes both nested maps still exist. Concurrent peer removal can delete one between those critical sections. Reconnect stress produced:

    panic: assignment to entry in nil map
    internal/relay/relay.go:178

This is both an illegal mutation under a read lock and a check/use lifecycle race. The recent-correspondent update needs to occur entirely under the write lock, using fresh map initialization in the same critical section. A regression test should force route/remove interleavings.

#### P0.2 Control workspace state is read after unlock

**Observed.** In the conflict branch of <code>wsClaim</code>, the mutex is unlocked before the error formats <code>ws.State</code>. Concurrent <code>wsReady</code> writes that field. Repeated failover testing produced the race detector warning at the current <code>control.go</code> lines 795 and 834.

The state string must be copied while locked. More broadly, no live workspace/timer/node pointer should escape a control critical section.

#### P0.3 Archive restore can escape through symlinks

**Source-confirmed.** Restore lexically validates each archive name, then creates symlinks verbatim. A malicious tar can contain:

1. symlink <code>dir -&gt; /outside</code>;
2. regular file <code>dir/payload</code>.

The second name passes the lexical root-prefix check, but <code>os.OpenFile</code> follows the first entry's symlink and writes outside the workspace. A pre-existing symlink produces the same problem. Extraction runs on the host side for both process and Docker backends.

Use descriptor-relative extraction with no-follow semantics, reject unsafe symlink targets, enforce entry/byte limits, and extract into a private staging directory before commit. This is exploitable by any actor that can cause a node to restore a crafted content-addressed artifact.

#### P0.4 Release destroys the only good copy after checkpoint failure

**Source-confirmed.** Node release removes the workspace from its map and kills all sessions. If snapshot creation or upload fails, it only logs the error, then closes the broker, destroys the backend, and returns a successful response with no new snapshot. Control silently falls back to the previous snapshot and proceeds with move or pause.

For a workspace with no prior snapshot, this loses the entire workspace. With an older snapshot, it silently rolls back all later changes.

Release needs a two-phase protocol:

1. freeze/quiesce;
2. create, upload, and verify a candidate snapshot;
3. atomically commit that snapshot as the new control-plane checkpoint;
4. only then revoke service and destroy the source.

Any failure before commit must leave the source authoritative and return an error.

#### P0.5 Node authentication and authorization do not establish an identity boundary

**Source-confirmed.**

- A node generates an Ed25519 private key but only sends the public key. There is no nonce/challenge signature proving private-key possession.
- The same bearer token authenticates clients, nodes, artifacts, and webhooks.
- A client can request an existing <code>c_...</code> peer ID; the relay replaces and disconnects the previous connection, with no reconnect ticket proving continuity.
- A new node ID is trust-on-first-use. Concurrent first hellos can both pass before registration.
- A peer with the token may self-declare as a node, advertise arbitrary labels/capabilities, and race to claim work.
- Control only restricts a few node-only operations. A node peer can call ordinary client control operations, including create, move, destroy, list, and grant.
- No operation checks workspace ownership or tenant membership.
- Client principal and explicit workspace principal are caller-supplied assertions.
- Posted event principals can be spoofed when non-empty.

An attacker with the shared token can enroll a fake node, arrange or race for a workspace, request <code>binding.lease</code>, and receive raw configured secrets. The grant signature correctly prevents random forgery, but the control plane willingly signs grants for any authenticated peer.

Production use needs distinct credentials per actor/role, proof of node-key possession, authoritative principal mapping, resource ACLs, audience/scope enforcement, token rotation/revocation, and artifact/webhook credentials separated from fleet administration.

#### P0.6 Leases do not fence the old node

**Architectural risk, source-confirmed mechanism gap.** The control plane can expire a lease and assign a higher generation elsewhere, but the old node never records an authoritative local lease deadline and never stops work when renew fails. <code>ws.renew</code> returns an empty success even for individually rejected IDs, so the node cannot tell which claims control still accepts.

During a partition the old process tree and broker can continue making external changes while a new node restores the last snapshot. Stale client routing may be curtailed by the relay, but autonomous workspace processes are not. A generation number only fences requests that pass through <code>node.authorize</code>; it does not fence the workload itself.

The node needs a monotonic lease deadline derived from acknowledged renewals and must freeze/kill sessions, close broker capability, and reject every operation after uncertainty exceeds the deadline. Renewal responses should explicitly accept or reject each workspace generation.

#### P0.7 Control restart can destroy a live node's freshest copy

**Source-confirmed.** <code>Control.load</code> rewrites held workspaces to pending and clears Node. A still-running node reconnects and calls <code>resync</code> with <code>ws.ready</code>. The new control plane rejects it because node/generation no longer match. <code>resync</code> treats that conflict as loss of ownership and <code>dropWorkspace</code> destroys the local backend.

That copy can contain work newer than <code>LastSnapshot</code>, or be the only copy if no snapshot exists. The documented control-restart recovery therefore has a data-loss hole.

Restart reconciliation needs an explicit epoch and adopt/compare protocol. Control should not discard ownership facts until nodes report, and nodes must quarantine rather than destroy a disputed freshest copy.

#### P0.8 Lifecycle calls admit invalid and concurrent transitions

**Source-confirmed.**

- Move and sleep on a destroyed workspace resurrect it as pending or paused.
- <code>ws.ready</code> checks node/generation but not <code>state == claiming</code>, so a delayed ready can promote a workspace while release is in progress.
- Release keeps a live workspace pointer across a long network RPC. Concurrent destroy/move/sleep/ready calls can overwrite one another.
- <code>released</code> is not considered held, and restart loading does not reconcile it. A control crash after persisting release but before completing move/sleep can strand the workspace permanently.
- Sleep persists its timer before snapshot/release. A past or short timer, or a matching event arriving during release, can mark itself fired while wake is still a no-op; sleep then leaves the workspace paused forever with a consumed timer.
- A negative <code>AfterSec</code> passes the “all zero” validation but creates no deadline, after which the workspace can be paused forever.
- Idempotency keys on move and sleep are ignored, so a retry can create multiple releases/timers.

Workspace transitions need one serialized per-workspace operation/epoch, a legal transition table, compare-and-swap preconditions, and durable operation records.

#### P0.9 <code>port.open</code> bypasses broker policy and exposes node-network SSRF

**Source-confirmed.** <code>PortOpenReq</code> contains a caller-controlled <code>Host</code>. For the process backend, <code>Prepare</code> does not rewrite it. <code>Session.startPort</code> then calls <code>net.Dialer.Dial</code> in the trusted node process. The connection never passes through the broker's hostname, lease, private-address, or DNS-pinning rules.

Any token-bearing client that can obtain a workspace grant can use a custom protocol request to reach node loopback services, cloud metadata addresses, private VPC services, databases, or public destinations routable from the node. The Go convenience method hides the host field and defaults to <code>127.0.0.1</code>, but that is not enforcement at the wire boundary.

Port targets need an explicit capability and the same default-deny address policy as egress. “Workspace loopback” must be represented by the backend rather than confused with node loopback. Add tests for IPv4/IPv6 loopback, link-local metadata, RFC1918/ULA, DNS rebinding, and allowed targets.

#### P0.10 Acknowledged control mutations need not be durable

**Source-confirmed.** <code>saveWS</code>, <code>saveTimer</code>, and <code>saveNode</code> return no error. They log a failed SQLite <code>Exec</code>, while callers keep the in-memory mutation, emit follow-up events, and return success. State and event writes are separate even when both happen to succeed.

A full disk, closed database, I/O error, or crash between writes can make an acknowledged workspace, timer, assignment, or transition disappear or disagree with the audit log after restart. This is more than an observability gap: it violates the persistence contract on which move, sleep, failover, and grant routing rely.

Every state transition should be committed transactionally and return a persistence error to the caller. State plus its audit/outbox record need one durable boundary; in-memory publication should follow commit. Fault-injection tests should fail every write point and verify that neither success nor partially visible state leaks through.

### P1: serious correctness, security, and durability issues

#### P1.1 Session sequence zero is not guaranteed to be info

**Source-confirmed.** Exec, PTY, and port output pumps start before <code>Manager.Open</code> appends the info chunk. A fast process can write stdout or exit first; exit can close the log before info is appended. That violates a documented invariant and can make replay metadata absent or appear after output.

Construct and append info before any producer can append, or reserve sequence zero before starting the runner and fill immutable metadata through a separate record.

#### P1.2 Spill failures can silently lose output

**Source-confirmed.** The memory ring removes a chunk before spilling. Truncate and seek errors are ignored; write errors merely return from <code>spillLocked</code> and are not propagated or converted into a truthful gap. The chunk is already gone from memory.

Spill must return errors, retain the in-memory chunk until persistence succeeds, and emit an explicit gap/health finding if durability cannot be maintained. Reading spill also allocates every retained payload while holding the log mutex before applying the caller's maximum batch size.

#### P1.3 Slow consumers block the entire client connection

**Source-confirmed.** <code>Peer.readLoop</code> invokes handlers synchronously. Client session delivery sends into a 1,024-entry channel while holding the session mutex; event delivery sends into a 256-entry channel. Once either fills, no response, event, chunk, or ping on that WebSocket can be processed.

Queues need byte bounds and independent dispatch workers or explicit flow control. A slow stream must not block unrelated RPC correlation.

#### P1.4 Event-tail cancellation can send on a closed channel

**Source-confirmed.** The event handler copies <code>c.events</code> under the client mutex, unlocks, and sends. The cancellation goroutine can then set the field nil and close that same channel. The sender can panic.

Use a dedicated subscription object with owned context, close-once semantics, and delivery that never races channel closure.

#### P1.5 Node-uplink loss is not transparently reattached

**Source-confirmed and visible in the test design.** If the node's relay connection dies while the client's connection remains alive, the relay sends <code>peer.gone</code> to the client. The client ignores it. Its own connection supervisor does not run because its socket is healthy, so the original session never reattaches. The simulation test manually opens a second attachment after waiting for the node.

The client must associate sessions with remote peer state and reattach them when that peer returns, not only when its own WebSocket changes.

#### P1.6 Reattach errors are swallowed

**Source-confirmed.** <code>Session.reattach</code> discards the result of the 30-second attach RPC. The exposed output channel can then remain open forever with no error or terminal record.

Expose stream errors or retry with bounded backoff and an explicit terminal condition.

#### P1.7 Input deduplication is scoped incorrectly and commits before the write

**Source-confirmed.**

- The node stores one <code>lastISeq</code> per session, while each client-side attachment starts at sequence one. A new attachment's input is discarded after a previous attachment has sent higher values.
- Multiple clients collide in the same sequence space.
- The session advances <code>lastISeq</code> before writing. A failed or partial write consumes the sequence; retry is then treated as a duplicate even though all bytes may not have been applied.

Input needs an attachment/client identity and an acknowledged sequence protocol. The applied watermark must advance only according to well-defined write semantics.

#### P1.8 Fresh <code>WorkspaceInfo</code> and <code>ListSessions</code> calls omit their grant

**Source-confirmed.** <code>nodeCall</code> fetches a grant, but these two body builders ignore it. The node authorizes with a nil grant and succeeds only if some earlier request happened to populate the connection-local grant cache. A first call on a fresh connection returns unauthorized.

The request types need an explicit grant, or authorization must move to a frame-level envelope applied consistently to every node operation.

#### P1.9 Response correlation does not authenticate the responder

**Source-confirmed.** Both <code>transport.Peer</code> and the relay's control-request map match a response only by numeric ID. They do not verify expected From, To, or Op. The relay permits arbitrary authenticated peer-to-peer frames.

A malicious token-bearing peer can guess predictable request IDs and race a forged success/error response to a client or the control plane. Pending entries must retain expected peer/op and reject mismatches; the relay should enforce allowed communication edges by role.

#### P1.10 Retry safety is mostly declarative

**Source-confirmed.**

- Control persists idempotency only for workspace create.
- Session open deduplicates in node memory only, with no request fingerprint and no actor scope.
- Move/sleep keys are ignored.
- Filesystem write/edit keys are ignored; mkdir/remove/rename have none.
- Destroy, wake, event post, and port open have no dedupe.
- The SDK retries any request lost with <code>ErrClosed</code>, even when the effect may already have happened.
- Write/edit generate their key inside the grant-retry closure, so even a future dedupe implementation would see a new key on the second node attempt.

Define actor-scoped operation IDs, persist result/fingerprint, and specify retention. Do not automatically retry unsafe mutations until this exists.

#### P1.11 Explicit snapshot is not an authoritative checkpoint

**Source-confirmed.** <code>ws.snapshot</code> returns an artifact ID from the node but never updates control's <code>LastSnapshot</code>. A later node crash restores the older snapshot or nothing. The CLI gives the user no warning that the new artifact is not the failover checkpoint.

Snapshot commit must be a control-plane operation or have an explicit second commit step.

#### P1.12 “The event log is truth” is not implemented

**Source-confirmed.**

- Control loads CBOR state tables, not event replay.
- State persistence and event append are not one transaction.
- State-save and many emit errors are logged but not returned.
- Node events are posted in detached best-effort goroutines with no retry/outbox and disappear while offline.
- Some mutations emit no event, including mkdir and rename; renewals and several lifecycle bookkeeping transitions also have no corresponding state event.
- Cause IDs are structurally present but largely unused.
- Clients can submit arbitrary event types and principals.

The current log is a valuable audit stream, but it is neither complete nor authoritative. Product language should be narrowed until a transactional state/event model and durable node outbox exist.

#### P1.13 Broker policy is not enforced by either backend

**Source-confirmed.** Both backends advertise <code>EgressEnforced: false</code>. A workspace process can ignore/unset proxy variables and open a socket directly. User-supplied workspace/session environment values are merged after broker values and can override <code>HTTP_PROXY</code>, <code>HTTPS_PROXY</code>, and <code>NO_PROXY</code>.

The process backend is explicitly unisolated, so this is acceptable only when the agent already has the node user's trust. Docker uses ordinary default networking and likewise has no forced proxy/firewall.

The README heading “Secrets the agent can never leak” is too strong. The narrower truthful claim is that Remount does not inject configured secret values into normal workspace environment/files and can substitute them on the inspected broker path.

#### P1.14 Docker cannot reach the current loopback broker address

**Source-confirmed architecture mismatch.** The broker listens on the host's <code>127.0.0.1</code>, and that URL is passed into the container environment. Inside an ordinary container, <code>127.0.0.1</code> names the container, not the host. There is no host-network flag, host-gateway mapping, Unix socket mount, or broker sidecar.

Docker exec itself passes basic tests, but brokered credential use in Docker has no integration test and should fail in the standard network topology.

#### P1.15 Per-workspace brokers have no caller identity

**Source-confirmed.** Brokers listen on node loopback without authentication. Process-backend workloads share the node network namespace and can scan/connect to sibling brokers. A sibling that knows or guesses another binding placeholder can exercise that broker's leases and destinations.

Use a per-workspace authenticated Unix socket or network namespace/capability token, and prove sibling non-interference.

#### P1.16 Broker matching and lease details are inconsistent

**Source-confirmed.**

- A pattern with a port only rejects a mismatch if the request host also explicitly has a port. Thus <code>host:8443</code> can match a reverse-proxy request for <code>host</code>, which uses the scheme's default port.
- CONNECT treats any destination covered by a binding as allowed but does not check that lease's expiration.
- <code>/http</code> can substitute a credential and send it over plaintext.
- Lease and grant expiry use the node's wall clock even though server time is sent at hello and ignored.
- Unknown placeholders are forwarded unchanged when the host is otherwise allowlisted.

Normalize every destination to an effective port, apply expiry consistently, disallow secret substitution over plaintext by default, and base deadlines on bounded server/node skew.

#### P1.17 The absolute secret guarantee cannot be made for arbitrary upstreams

**Architectural risk.** The broker injects the real credential into an outbound request and streams the upstream response back without redaction. An allowed but malicious or compromised upstream can echo the Authorization header or secret in its response, causing the workspace to receive it.

This does not invalidate useful credential mediation, but “the workspace can never see the secret” requires trusted connector-specific response handling or a narrower claim.

#### P1.18 Artifact digest mismatch handling can delete a valid shared blob

**Source-confirmed.** Server PUT and node fetch both call <code>Store.Put</code>, compare the returned digest to the requested ID, and delete the returned digest on mismatch. If that content already existed legitimately, Put reused the shared blob; the mismatch path deletes the pre-existing valid artifact.

Put must report whether it created a new object, or mismatch cleanup must only remove a private temporary file. Content-addressed shared objects should never be deleted as validation rollback.

#### P1.19 Artifact and HTTP storage paths lack resource limits

**Source-confirmed.** Artifact PUT streams an unlimited request to local disk. Tar extraction has no maximum compressed bytes, expanded bytes, entry count, individual file size, path depth, or compression ratio. Events and idempotency rows have no retention. Artifacts have no reference tracking or garbage collection.

This permits disk exhaustion and decompression bombs from any token holder.

#### P1.20 Filesystem jail has hostile-tree TOCTOU gaps

**Source-confirmed.** Resolve authorizes a path, returns a string, and the later open/rename/remove acts on it. A process in the workspace can replace a checked directory with a symlink between those operations. This is especially dangerous for recursive remove and host-side Docker filesystem access.

Use descriptor-relative traversal (<code>openat2</code> where available, or a careful no-follow walk) and perform the final operation relative to the verified directory descriptor.

#### P1.21 High-level file reads silently truncate and edits are unbounded

**Source-confirmed.** <code>fsops.Read</code> reports size and EOF, but <code>client.ReadFile</code> returns only the first default 4 MiB and discards those fields. The CLI prints the truncated content as success. Conversely, <code>WriteFile</code> puts the entire payload in one frame even though the encoded frame is capped at 4 MiB; the CLI first buffers all stdin, then fails once framing overhead pushes the request beyond the cap. Edit reads the entire file into memory without a cap and can overwrite a concurrent update between read and rename. Search also ignores scanner errors, including overlong lines.

Expose streaming/pagination and chunked writes, surface truncation, bound edit size, and optionally use a content hash/version for compare-and-swap edits. Test round trips immediately below, at, and above the frame boundary.

#### P1.22 Node identity corruption is treated as a new identity

**Source-confirmed.** Any read, JSON, ID, or key-length problem causes <code>loadIdentity</code> to silently generate and overwrite a new identity with a non-atomic write. The old node's local workspaces can then conflict with control ownership and be destroyed by reclamation logic.

Corrupt identity must fail closed and require explicit recovery. Writes should be temp-file, fsync, rename, and directory fsync.

#### P1.23 Local cleanup can target the wrong backend

**Source-confirmed.** In one failed adoption/conflict path, <code>tryClaim</code> calls <code>Backends.Get("")</code>, meaning the first registered backend, instead of the workspace's actual backend. With process registered before Docker, it can delete a Docker workspace directory without removing the container, or otherwise orphan backend state.

Persist backend identity with the local workspace and never guess it during destructive cleanup.

#### P1.24 Ready failure leaves a materialized workspace in limbo

**Source-confirmed.** After successful materialization, <code>ws.ready</code> failure is logged and ignored; <code>materialize</code> returns nil and <code>tryClaim</code> clears its reservation. The node retains a local workspace that control may leave claiming or later reassign.

Ready should be retried/reconciled while the claim is fenced, or materialization should fail and quarantine without serving.

#### P1.25 Subscription ownership and workspace cleanup are wrong

**Source-confirmed.**

- A replacement subscriber cancels the old context, but the old goroutine's deferred cleanup only checks whether its own context is done, not whether the map still points to its subscriber object. It can delete the replacement.
- Workspace cleanup searches subscription keys of the form <code>client|session</code> for a suffix containing the workspace ID, so it does not find them.
- EOF can leave stale subscription entries.

Store a unique subscriber pointer/token, compare exact ownership on cleanup, and maintain a workspace-to-subscription index.

#### P1.26 Control/node shutdown permits work after the database closes

**Observed during repeated failover tests.** Background peer teardown logged <code>save workspace: sql: database is closed</code> and failed event emission after test cleanup. <code>Server.Close</code> clears/closes relay peers, stops control, and closes the log, but outstanding detached control/node goroutines can still arrive.

Use one root lifecycle context, wait for accepted connections and request goroutines, stop producers, then close persistence.

#### P1.27 The Modal demo's default token and topology are unsafe

**Source-confirmed.**

- Missing <code>REMOUNT_TOKEN</code> falls back to the public string <code>modal-demo-token</code>.
- Server and process-backend node share one container, Unix user, environment, and durable volume. A process-backend workspace can access the host container far beyond its workspace convention, including control/node data allowed to that user.
- Child process supervision and log rotation are minimal.

The demo should refuse an absent secret, use an isolated backend/node, and never place an untrusted process-backend workload beside the control plane.

#### P1.28 A blocked stdin write holds the session lifecycle mutex

**Source-confirmed.** <code>Session.Input</code> keeps <code>s.mu</code> across the underlying pipe, PTY, or TCP <code>Write</code>. If the child or remote endpoint stops reading, that write can block indefinitely. <code>Signal</code>, <code>Kill</code>, <code>Resize</code>, <code>ExitInfo</code>, and <code>finish</code> all need the same mutex, so the operation that should break the blockage cannot acquire it. Workspace teardown can then spend its timeout waiting for a kill that is itself stuck on the lock.

Protect pointer/state lookup with the mutex but perform cancellable I/O outside it. Define partial-write and acknowledgement semantics, and add a test with a child that never consumes stdin while kill/release races the write.

#### P1.29 Materialization can permanently omit required binding leases

**Source-confirmed.** During materialization, bindings are requested only if <code>n.peer</code> is non-nil. If it becomes nil in that window, the node skips the request, creates a broker with zero leases, writes a seemingly valid environment, installs the workspace, and can later become ready. The renewal loop refreshes only leases already near expiry; an empty slice never triggers acquisition.

Bindings should be a claim prerequisite. A workspace declaring them must remain fenced until all required valid leases are obtained, and reconciliation should compare configured binding IDs with active leases rather than merely refresh entries that happen to exist.

#### P1.30 Explicit snapshots are crash-inconsistent

**Source-confirmed.** Release stops sessions before taking its snapshot, but the direct <code>ws.snapshot</code> operation walks and reads the host tree while workspace processes can continue renaming and mutating it. The resulting archive can combine different points in time, omit raced paths, or fail midway. Content addressing verifies the bytes produced, not their filesystem consistency.

Define whether this API is only a best-effort live backup or an authoritative checkpoint. The latter needs backend quiescing/freezing or a real filesystem snapshot primitive, plus mutation-under-snapshot tests.

#### P1.31 Request dispatch and resource queues are not admission-controlled

**Source-confirmed.** Control launches one goroutine per request, node does the same, node events launch one detached post goroutine each, and pending RPC/subscription/reorder maps have no per-peer byte or count budget. Artifact, session, and event limits are independent and there is no principal/workspace/fleet admission policy.

An authenticated or compromised peer can exhaust goroutines, memory, descriptors, disk, or outbound connections and affect unrelated workspaces. Add bounded workers/queues, byte-aware stream flow control, overload errors, and tenant/workspace quotas.

#### P1.32 Non-following event reads silently stop after 1,000 records

**Source-confirmed.** <code>eventsTail</code> calls <code>log.Read(..., 1000)</code> once when <code>Follow</code> is false. Neither the response nor the client/CLI reports truncation or exposes a continuation cursor. An operator can therefore believe a partial audit history is complete.

Paginate until exhaustion or return an explicit next cursor/truncated flag. Test retrieval across more than 1,000 events and after store retention gaps.

#### P1.33 Synchronous session-start failures bypass the exit callback

**Source-confirmed.** <code>Manager.Open</code> installs the goroutine that invokes <code>OnExit</code> only after a successful start. If exec, PTY, or port startup fails synchronously, it appends an exit record and schedules reaping, then returns before installing that callback. The node's <code>s.exited</code> event and associated audit/metrics path can therefore be absent for failed starts.

Install completion observation before starting the runner, or route both startup and asynchronous completion through one close-once finalizer. Assert exactly one exit callback/event for every success and failure mode.

### P2: maintainability, observability, compatibility, and UX gaps

#### P2.1 Current test baseline is red

**Observed.** <code>TestExecEndToEnd</code> expects a one-entry workspace root. The new <code>.remount/env</code> feature makes that expectation stale. This is a test maintenance defect, but it also means CI cannot currently act as a trustworthy gate and can hide later race failures behind the first assertion.

#### P2.2 Direct tests are missing around the most concurrent packages

Client, control, node, relay, server, and metrics have no package-local tests. Simulation coverage is good for happy paths and selected faults, but deterministic unit tests are needed for:

- every legal/illegal state transition;
- duplicate and concurrent lifecycle operations;
- forged/misdirected frames;
- node proof-of-possession;
- lease fencing and clock skew;
- checkpoint failure injection;
- subscription replacement;
- channel backpressure;
- control restart reconciliation;
- malicious tar and hostile symlink trees.

Fuzz targets should include frame/body decoding, tar restore, path resolution, host matching, session reorder/gaps, and event subscriber catch-up.

#### P2.3 Diagnostics and metrics are only partially integrated

The review observed metrics being wired into relay, control, eventlog, session, broker, and artifact paths, plus new diagnostic protocol types and a control <code>diag</code> operation. At the cutoff:

- <code>node.diag</code> was defined but not dispatched;
- no client/CLI doctor command consumed the types;
- no <code>/metrics</code> handler exposed Prometheus text;
- control diagnostic artifact counts/bytes and version were not populated;
- the registry is process-global, so standalone server and node metrics are conflated.

This is promising scaffolding, not yet an operable surface.

#### P2.4 Health is liveness only

<code>/healthz</code> always reports ok and peer count. It does not check DB writability/integrity, lease-loop progress, artifact-store access, event append, or degraded state. Separate liveness, readiness, and deep diagnostic endpoints are needed.

#### P2.5 Server ownership/cleanup is incomplete

<code>Server.Serve</code> publishes <code>s.ln</code> and <code>s.http</code> without synchronization while <code>Addr</code> reads <code>s.ln</code>. Standalone mode discards the Serve error and polls that racy field, so a bind failure can become an empty server URL rather than a direct startup error. <code>Server.Close</code> does not itself shut down an active HTTP server/listener, and <code>Relay.Close</code> does not wake its own control-originated pending requests. In-memory mode creates a temporary artifact directory that is never removed. Initialization failure paths can leave DB/temp resources open.

At the transport layer, <code>wsConn.Close</code> constructs a two-second context and then never uses it; the WebSocket close call therefore does not have the timeout the code appears to intend. Use an explicit readiness/error channel, synchronized listener ownership, a root shutdown context, close signals for every waiter, and wait groups before closing persistence.

#### P2.6 Event subscription close does not wake a blocked reader

<code>Subscription.Close</code> marks closed and removes the subscriber but does not signal the live channel. A concurrent <code>Next</code> can remain blocked until its context is cancelled or another event arrives. <code>Log.Close</code> similarly does not terminate subscriptions explicitly.

#### P2.7 Protocol evolution is aspirational

Frame version is not validated, capability negotiation always returns v1, and operation/body compatibility is not described mechanically. The client and control plane also exchange a literal <code>"events.stop"</code> operation that has no protocol constant or normative spec entry. Add conformance vectors, version-skew tests, required/optional capability rules, a documented subscription-close operation, and a policy for incompatible mutation semantics.

#### P2.8 The public Go SDK does not exist

Move stable consumer APIs out of <code>internal</code>, separate wire types from convenient client abstractions, document compatibility, and add examples from a separate module to prove importability.

#### P2.9 Docker availability is advertised before it is known

The backend registry advertises “docker” even though daemon availability is checked lazily. The first availability error is cached forever. A node can repeatedly win an eligible claim and fail materialization even after Docker later becomes healthy.

Probe readiness before advertising or report dynamic backend health/capacity.

#### P2.10 Backend capability reporting is not connected to backend capabilities

The <code>workspace.Caps</code> type includes isolation, snapshot type, and egress enforcement, but node hello sends backend names plus manually configured extra strings. Placement cannot reliably require the actual backend security properties. A positive memory request also accepts a node reporting zero/unknown memory, because eligibility rejects only a positive known value that is too small. Security properties and resource requirements should fail closed when capacity is unknown.

#### P2.11 CLI cancellation and streaming can hang

The terminal driver calls the context-free <code>client.Copy</code> and ranges until the chunk channel closes. If the stream stalls after Ctrl-C or node-uplink loss, the command can hang. The stdin goroutine can also remain blocked in an OS read. Wake waits without its own timeout.

Use context-aware copying, close/detach the remote session on cancellation, and propagate stream errors.

#### P2.12 Port sessions and argument validation need tightening

Port values are not range-validated. A local disconnect sends EOF but does not always explicitly close the remote session, so a peer that keeps its side open can retain it. Per-connection sessions have no quotas.

#### P2.13 Binding configuration can silently create an empty secret

A binding value beginning with <code>$</code> is replaced with <code>os.Getenv</code>. A missing environment variable becomes an empty secret without an error. Configuration should validate non-empty secret material and duplicate binding IDs/destinations at startup.

#### P2.14 Documentation contains important contradictions

- README says secrets the agent “can never leak”; operations correctly admits that neither backend enforces egress.
- Design says split brain is impossible by construction; lack of node self-fencing disproves that.
- ADR 8 calls relay payloads “ciphertext-equivalent” and later states there is no end-to-end encryption.
- The event log is called the source of truth even though separate state tables are loaded.
- <code>MISTAKES.md</code> says the Modal image installs Node 22, but <code>deploy/modal_app.py</code> only installs curl and procps.
- Docs say control restart re-adopts node-local copies, but current resync can destroy them.

The operations guide is generally the most candid document and should be the baseline for narrowing top-level claims.

#### P2.15 Modal harness runtime fix is absent from code

The documented prior fix for a missing Node runtime is not present in the reviewed deployment image, so the Codex/npm demo path can regress exactly as described in <code>MISTAKES.md</code>.

#### P2.16 CI and release hardening are incomplete

CI runs format, vet, ordinary tests, race tests, and static cross-builds on macOS/Linux, which is a strong base. Missing pieces include:

- staticcheck and govulncheck;
- dependency/license/SBOM policy;
- fuzz and adversarial suites;
- benchmark/regression gates;
- artifact signing/provenance;
- actions pinned to immutable SHAs rather than tags;
- release installation tests from a separate module;
- full Apache-2.0 license text rather than an abbreviated notice.

The phony <code>conformance</code> target has no recipe, so <code>make conformance</code> exits zero without testing anything. It should either execute the promised hostile-workspace suite or be removed until that suite exists.

#### P2.17 Cross-platform claims need qualification

The file snapshot is portable in principle, but commands, installed tools, symlink semantics, permissions, and images are not automatically portable across OS/architecture. The build targets only Linux and Darwin; although a Windows resize stub exists, shared session code uses Unix-only <code>SysProcAttr.Setpgid</code>, <code>syscall.Kill</code>, and Unix signals. A verified <code>GOOS=windows go build ./...</code> fails on those symbols. “Files are portable between compatible backends” is more accurate than “the running computer works on any machine.”

#### P2.18 Resource lifecycle and quotas are absent

There are no tenant/workspace/session/artifact/event/idempotency quotas, no artifact GC, no timer pruning, no global stream budget, and no admission control. A session can reserve up to roughly 130 MiB of log storage, and retained sessions last 24 hours. These limits are individually bounded but fleet totals are not.

## What is especially good

This review found many problems because the code is ambitious, not because it is shapeless. Several choices are worth preserving:

- One frame type and a small operation vocabulary make the wire protocol understandable.
- The relay/control split is conceptually clean even though the trust claim needs narrowing.
- Treating a session as a replayable log is the right abstraction for agents.
- A typed gap is much better than silent transcript loss.
- <code>claiming</code> correctly separates ownership from service readiness.
- Materializing claims are renewed and duplicate local claims are reserved.
- The in-memory transport encodes/decodes real frames and supports fault injection.
- Content-addressed, file-only snapshots avoid coupling migration to a hypervisor.
- Broker DNS pinning and private-address checks address real SSRF/rebinding threats.
- The process backend honestly reports no isolation in the operations guide and backend caps.
- The Makefile keeps the main binary static with a pure-Go SQLite implementation.
- The ADRs record tradeoffs, and <code>MISTAKES.md</code> records concrete failure-derived lessons.
- The newly added production-readiness and incident-hardening documents independently identify several of the same race, lifecycle, fencing, session, subscriber, and egress issues.

## Recommended repair order

The order matters because later tests need a stable substrate.

1. **Freeze a commit and restore a trustworthy baseline.** Commit the actual tree, fix the <code>.remount</code> assertion, require ordinary and race suites to pass, and prevent concurrent unpublished rewrites while evaluating a release candidate.
2. **Fix known races and ownership bugs.** Relay recent-map locking, control live-pointer reads, exact subscriber ownership, stream/input lock ownership, and lifecycle shutdown.
3. **Formalize workspace transitions.** One per-workspace operation epoch/lock, strict legal transition table, restart recovery for <code>released</code>, idempotent results, and timer ordering.
4. **Make persistence and release fail safe.** Transactional acknowledged state/events, two-phase checkpoint commit, verified upload, source retained until commit, explicit operator-visible failure.
5. **Add node self-fencing and restart reconciliation.** Acknowledged deadlines, control epochs, quarantine rather than deletion, failover tests with autonomous side effects.
6. **Close identity/authorization and network escapes.** Per-role credentials, challenge signing, authenticated principals, workspace ACLs, response-source correlation, artifact/webhook separation, and capability-scoped <code>port.open</code> destinations.
7. **Harden archive and filesystem operations.** No-follow descriptor-relative extraction and file operations, staging, quotas, malicious corpus/fuzzing.
8. **Choose an honest network-security mode.** Either implement forced egress in a suitable backend or explicitly label proxy mediation as cooperative. Add per-workspace broker identity and make Docker reach it.
9. **Repair session integrity and flow control.** Reserve info sequence zero, propagate spill failures, bound queues by bytes, isolate stream delivery, correct input acknowledgments, resume on remote-peer recovery.
10. **Make events authoritative or rename the claim.** Transactional state/event writes, durable node outbox, complete mutation coverage, replay/reconciliation.
11. **Finish operability.** Metrics endpoint, health/readiness/doctor, quotas, GC, backups/restore drills, HA story, structured tracing.
12. **Stabilize product interfaces and documentation.** Public SDK, conformance suite, protocol versioning, narrowed security language, deployment fixes.

## A practical mental model for future work

When changing Remount, ask four ownership questions:

1. **Who owns the fact?** Control owns assignment; node owns current process/files; session log owns output order; artifact ID owns immutable bytes.
2. **What outlives the connection?** Workspace state, local files, processes, session logs, timers, and idempotency all do.
3. **What proves the actor and epoch?** Today this is incomplete. Every destructive or secret-bearing path should answer it explicitly.
4. **What happens if the acknowledgement is lost?** If replaying the request can duplicate, destroy, or roll back state, the operation is not yet safe.

The most important invariant is not merely “one workspace has one node in the control map.” It is:

> At most one generation may execute externally visible work; the freshest committed filesystem copy must never be destroyed until a newer durable checkpoint is authoritative.

The current code does not yet enforce that invariant. Making it true would turn a compelling prototype into a trustworthy substrate.

## Primary source map

- Product overview: <code>README.md</code>
- Architectural narrative: <code>docs/design.md</code>
- Normative protocol: <code>spec/PROTOCOL.md</code>
- Operational caveats: <code>docs/operations.md</code>
- Historical failures: <code>MISTAKES.md</code>
- Contributor invariants: <code>AGENTS.md</code>
- Production plan: <code>docs/engineering/production-readiness-request.md</code>
- Incident hardening plan: <code>docs/engineering/openai-huggingface-hardening-sdmr.md</code>
- Contemporaneous independent bug register: <code>docs/engineering/code-audit-2026-09-02.md</code>
- Runtime assembly: <code>cmd/remount/main.go</code>
- Core orchestration: <code>internal/control/control.go</code>, <code>internal/node/node.go</code>
- Data path: <code>internal/transport</code>, <code>internal/relay</code>, <code>internal/session</code>
- Files and movement: <code>internal/fsops</code>, <code>internal/artifact</code>, <code>internal/workspace</code>
- Trust edge: <code>internal/broker</code>
- Persistence/audit: <code>internal/eventlog</code>
- Failure simulation: <code>internal/sim/sim_test.go</code>
