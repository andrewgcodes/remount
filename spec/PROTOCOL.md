# The Remount Protocol, v1

This is the whole wire protocol. It should take about ten minutes to read and a
weekend to implement in any language. If something here is ambiguous, that is a
bug in this document.

## 1. Model

Three kinds of participant:

| Peer | Id prefix | Dials | Holds |
|---|---|---|---|
| **control** | the literal string `control` | nothing | policy, the claim queue, secrets, the event log |
| **node** | `n_` | outbound to the relay | workspaces, sessions, the egress broker |
| **client** | `c_` | outbound to the relay | nothing durable |

Nodes and clients both connect **outbound only**. The relay forwards frames
between them by destination id and never interprets a frame body. The control
plane is a peer named `control` that happens to live inside the relay process.

Eight resources: **Node**, **Workspace**, **Session**, **Artifact**,
**Binding**, **Timer**, **Principal**, **FleetOperation**.

## 2. Frames

One frame type, encoded as CBOR, carried over any ordered reliable byte
transport. The reference transport is one WebSocket message per frame at
`/v1/link`, with a 4 MiB cap.

```
Frame {
  v:    uint8       protocol version, 1
  t:    string      kind: hello | req | res | chunk | ev | ping | pong
  id:   uint64      correlates req/res, ping/pong
  seq:  uint64      chunk only: per-session output sequence
  s:    string      session id
  ws:   string      workspace id
  to:   string      destination peer
  from: string      source peer, set by the relay, never trusted from the sender
  op:   string      req: operation name; ev: event type
  body: bytes       CBOR payload, shape determined by t and op
  err:  Error?      res only, non-nil on failure
}

Error { code: string, msg: string, oldest: uint64 }
```

Rules that make version skew survivable:

1. Unknown fields are ignored. Unknown `t` values are ignored. Unknown `op`
   values get a `res` with code `unsupported`.
2. A peer never assumes a field it did not send will come back.
3. `from` is authoritative and is stamped by the relay. A sender that sets it is
   overwritten.

Error codes are stable: `bad_request`, `not_found`, `unsupported`,
`unauthorized`, `conflict`, `evicted`, `unreachable`, `internal`, `timeout`,
`closed`, `denied`, and `resource_exhausted`.

## 3. Hello

The first frame on every connection is `t: hello`. No other frame may precede
it. The relay answers with a `res` carrying the same `id`.

```
Hello  { peer, role: "node"|"client", token, caps: [string],
         pubkey: bytes (nodes: ed25519), labels: {string:string},
         principal: string (ignored as authority), node: NodeInfo?,
         issued_at: int64, nonce: bytes, proof: bytes }

HelloOK { peer, caps, server, now: int64, pubkey: bytes, lease_sec: int64,
          subject: string, tenant: string }
```

A node MUST present a stable `n_` id and its ed25519 public key. `proof` is an
Ed25519 signature over deterministic CBOR of the Hello with `proof` omitted;
`issued_at` must be fresh and `nonce` is single-use. Production enrollment
binds the key, labels, and backend descriptors to an operator-approved node
record. A client MAY present a previously assigned `c_` id to keep it across
reconnects, or leave it empty to be assigned one. Client-selected `principal`,
workspace principal, labels, and capabilities are never authorization input;
the control plane derives subject and tenant from the presented credential.

`HelloOK.pubkey` is the control plane's grant-signing key. Nodes verify grants
with it. `lease_sec` tells a node how often it must renew claims. A node MUST
renew at an interval no greater than one third of the lease.

## 4. Grants

A client may not talk to a node about a workspace without a grant. A grant is
issued by the control plane and verified by the node, so the node needs no
connection to the control plane to authorize a request.

```
GrantClaims { client, ws, node, principal, tenant, authz_revision,
              exp: int64, gen: uint64 }
Grant       { claims: GrantClaims, sig: bytes, node: string }
```

`sig` is ed25519 over the deterministic CBOR encoding of `claims`. A node MUST
reject a grant whose signature fails, whose `exp` has passed, whose `client`,
`ws` or `node` does not match the request, or whose `gen` differs from the
workspace's current generation, tenant, or ACL revision. The generation and
authorization-revision checks make stale grants useless after a move or policy
change.

Clients obtain grants with `op: grant` against `control` and cache them.

## 5. Workspaces and the claim queue

A workspace is a movable computer. Its lifecycle is a small state machine, and
every transition is an event.

```
              ws.create
                 │
                 ▼
            ┌─────────┐   node wins CAS    ┌──────────┐  node reports  ┌─────────┐
            │ pending │ ─────────────────▶ │ claiming │ ─────────────▶ │ claimed │
            └─────────┘    ws.claim        └──────────┘    ws.ready    └─────────┘
                 ▲                              │                          │
                 │  lease expires, or           │  node fails to           │ ws.move
                 │  node released it            │  materialize             │ ws.sleep
                 └──────────────────────────────┴──────────────────────────┘
                 │                                                          │
                 │                        timer or event fires              ▼
                 └──────────────────────────────────────────────────── ┌────────┐
                                                                       │ paused │
                                                                       └────────┘
```

**`claiming` is the important state.** A node that wins a claim still has to
create or restore the filesystem. Until it reports `ws.ready`, the workspace is
held but not serving. Clients wait for `claimed`. Grants are only issued for
`claimed`. A node that goes offline has its `claimed` workspaces demoted back to
`claiming`, because they are held but not answering.

The scheduler is a claim queue, not a placement engine. The control plane offers
a pending workspace to every eligible online node with a `ws.offer` event. Nodes
race. `ws.claim` is a single compare-and-swap under one lock: the state was
`pending`, now it is `claiming` and the generation is incremented. Exactly one
node wins. Split brain is impossible by construction, because a node acts only
on its own generation and every claim invalidates the previous one.

A claim carries a lease. The holder renews it. An expired lease returns the
workspace to `pending` with `restore_from` set to its last snapshot. There is no
separate failure path: a node dying and a node moving are the same event.

Each `WorkspaceSpec` carries an enforceable security contract:

```
SecuritySpec { profile, min_isolation, require_sibling_isolation,
               require_enforced_egress, secret_mode, network, audit }
NetworkPolicy { default: "deny"|"allow", rules: [EgressRule] }
EgressRule { id, protocol, hosts, ports, methods, path_prefixes,
             max_requests, max_request_bytes, max_response_bytes,
             shared_state }
```

Profiles are `local`, `isolated`, and `multi_tenant`. `isolated` and
`multi_tenant` require an enforced egress backend; `multi_tenant` additionally
requires microVM-strength isolation, sibling, network-namespace and device
isolation. Placement MUST fail closed when a backend descriptor does not prove
every requested capability. A node MUST revalidate the descriptor and install
the network policy before reporting `ws.ready`; advertising
`enforced_gateway` without implementing the network-controller contract is an
error, not evidence of enforcement.

**Re-adoption.** If a node reconnects and asks to claim a workspace it already
holds, the control plane returns it at the *same* generation and moves it to
`claiming`. Outstanding client grants stay valid, and the node re-announces
readiness when it has re-opened the workspace.

## 6. Control-plane operations

Sent to `control`. Client operations are marked C, node operations N.

| op | Who | Body → Response |
|---|---|---|
| `ws.create` | C | `WSCreateReq{spec, idem}` → `Workspace` |
| `ws.get` | C | `WSGetReq{id}` → `Workspace` |
| `ws.list` | C | → `WSListRes{workspaces}` |
| `ws.destroy` | C | `WSGetReq{id, idem}` → `{}` |
| `ws.move` | C | `WSMoveReq{id, requires?, placement?, idem}` → `Workspace` |
| `ws.sleep` | C | `WSSleepReq{id, after_sec\|at\|on, idem}` → `Timer` |
| `ws.wake` | C | `WSGetReq{id, idem}` → `Workspace` |
| `grant` | C | `GrantReq{ws}` → `Grant` |
| `node.list` | C | → `NodeListRes{nodes}` |
| `timer.list` | C | → `TimerListRes{timers}` |
| `fleet.quarantine` | C | `FleetQuarantineReq{selector, action, deadline?\|timeout_ms?, idem}` → `FleetOperation` |
| `fleet.get` | C | `FleetGetReq{id}` → `FleetOperation` |
| `fleet.list` | C | → `FleetListRes{operations}` |
| `events.tail` | C | `EventsTailReq{from, follow, ws, sub}` → history, or a stream of `ev` frames with `op: "log"` |
| `events.stop` | C | `EventsStopReq{sub}` → `{}` |
| `events.post` | C N | `EventPost{events}` → `{}` |
| `ws.claim` | N | `WSClaimReq{id}` → `WSClaimRes{workspace, lease_sec}` |
| `ws.ready` | N | `WSReadyReq{id, gen}` → `{}` |
| `ws.renew` | N | `WSRenewReq{ids, gen}` → `WSRenewRes{results}`; each result explicitly says continue/fence/destroy/reconcile |
| `ws.released` | N | `WSReleasedReq{id, gen, snapshot, reason}` → `{}` |
| `ws.snapshot.commit` | N | `WSSnapshotCommitReq{id, gen, snapshot}` → `{}` |
| `binding.lease` | N | `BindingLeaseReq{ws}` → `BindingLeaseRes{leases}` |
| `diag` | C | `DiagReq{verify}` → control diagnostics |

Every mutating request carries an `idem` key. Replaying a request with the same
key is a no-op that returns the original result. This is what makes a retry
after a dropped connection safe.

The control plane sends nodes one event: `ws.offer`, a hint that a workspace is
available to claim. It is a hint, not an instruction; a node that ignores it
loses nothing but the work.

## 7. Node operations

Sent to a node id, and every one carries a `Grant` on first use per connection.

| op | Body → Response |
|---|---|
| `s.open` | `SOpenReq{ws, kind, program, cwd, env, rows, cols, stdin, timeout_sec, idem}` → `SOpenRes{s, next}` |
| `s.attach` | `SAttachReq{s, from}` → `SOpenRes{s, next}` |
| `s.input` | `SInputReq{s, iseq, d, eof}` → `{}` |
| `s.resize` | `SResizeReq{s, rows, cols}` → `{}` |
| `s.signal` | `SSignalReq{s, signal}` → `{}` |
| `s.close` | `SCloseReq{s, kill}` → `{}` |
| `s.wait` | `SWaitReq{s, timeout_sec}` → `SWaitRes{exited, exit}` |
| `s.list` | `SListReq{ws}` → `SListRes{sessions}` |
| `port.open` | `PortOpenReq{ws, port, host}` → `SOpenRes` |
| `fs.read` | `FSReadReq{ws, path, offset, limit}` → `FSReadRes{d, size, eof}` |
| `fs.write` | `FSWriteReq{ws, path, d, mode, append, mkdirp, idem}` → `{}` |
| `fs.list` | `FSListReq{ws, path}` → `FSListRes{entries}` |
| `fs.stat` | `FSStatReq{ws, path}` → `FSStatRes{entry}` |
| `fs.mkdir` | `FSMkdirReq{ws, path, idem}` → `{}` |
| `fs.remove` | `FSRemoveReq{ws, path, recursive, idem}` → `{}` |
| `fs.rename` | `FSRenameReq{ws, old, new, idem}` → `{}` |
| `fs.search` | `FSSearchReq{ws, path, pattern, glob, max}` → `FSSearchRes{matches, truncated}` |
| `fs.edit` | `FSEditReq{ws, path, edits, idem}` → `FSEditRes{replacements}` |
| `ws.snapshot` | `WSSnapshotReq{ws, upload}` → `WSSnapshotRes{artifact, bytes}` |
| `ws.info` | `WSGetReq{id}` → `WSInfoRes{ws, backend, root, sessions, broker}` |
| `node.status` | → `NodeStatus` |
| `node.diag` | `NodeDiagReq{ws, verify}` → node diagnostics |
| `ws.release` | control only: `WSReleaseReq{ws, gen, snapshot, reason}` → `WSReleasedReq`; `preparing:true` means poll with the identical request |
| `ws.release.commit` | control only: `WSReleaseCommitReq{id, gen, snapshot}` → `{}` and authorizes source deletion |
| `ws.release.abort` | control only: `WSReleaseCommitReq{id, gen}` → `{}` and resumes the retained source |
| `ws.quarantine` | control only: `WSQuarantineReq{operation, ws, gen, action, backend, exclude, security}` → `WSQuarantineRes{fenced, gen, action, backend, snapshot?, warning?}` |
| `ws.quarantine.commit` | control only: `WSQuarantineCommitReq{operation, ws, gen, backend, snapshot}` → `{}` and authorizes deletion only after an exact durable phase-one proof |

Session kinds are `exec`, `pty` and `port`.

`fs.edit` is atomic across all edits in one request. Each edit's `old` must
match exactly once unless `all` is set. If any edit fails to apply, the file is
not written and the response is `conflict`.

Node mutations with an `idem` key are write-ahead journaled. The node persists
and fsyncs a `pending` intent before applying the effect, then persists and
fsyncs the completed response before acknowledging it. An intent that is still
`pending` after restart has an unknown outcome and MUST return `conflict`; it
MUST NOT be applied automatically. Reusing a key with different arguments also
returns `conflict`.

### 7.1 Fleet containment

A `FleetOperation` is a durable, selector-frozen incident response:

```
WorkspaceSelector { all, tenant, principal, run, node, model, backend,
                    labels, created_after, created_before }

FleetOperation { id, selector, action, requested_by, tenant, state,
                 created_at, deadline, updated_at, results }

FleetOperationResult { workspace, node, backend, generation, state,
                       fenced, acknowledged, snapshot, error, updated_at }
```

The allowed actions are `freeze`, `revoke_egress`, `checkpoint`, `stop`, and
`destroy`. Every action is containment: the node first removes the target from
its serving map, revokes broker access, invalidates grants/subscriptions and
stops sessions. `checkpoint` and `destroy` additionally require a verified
snapshot. `destroy` is two phase: control first durably commits the advanced
generation, terminal workspace state, and snapshot reference; only then may it
send `ws.quarantine.commit`. A node MUST match that commit against the exact,
durably journaled phase-one generation, action, backend and snapshot before it
deletes the retained source.

An empty selector is invalid; callers must supply at least one constraint or
set `all:true`. The matching workspace ids and their physical holder
generation are frozen when the operation is created, so workspaces created
later are not silently included. Tenant administrators are confined to their
tenant; global administrators may select across tenants.

Operation states are `pending`, `running`, `completed`, and `partial`; target
states are `pending`, `acknowledged`, and `failed`. A deadline or relative
`timeout_ms` may be supplied, but not both. The default is five minutes and the
maximum is 24 hours. At the deadline the operation becomes `partial` if targets
remain pending, making the bounded result visible to the caller. Control keeps
retrying pending targets after that report so an unreachable node can
eventually be fenced; a later terminal operation may also escalate an already
quarantined target while preserving its original physical generation.

`fleet.quarantine` is idempotent by `(authenticated tenant, subject, idem)`.
The selector and target list, operation id, and deadline survive control
restart. `fleet.get` and `fleet.list` expose per-target acknowledgement and
errors. A zero-target selection is a successful completed operation and emits
both requested and completed audit events.

## 8. Sessions are logs, not sockets

This is the part that makes reconnect work, so it is worth stating precisely.

A session's output is an **append-only log of chunks**. Each chunk has a
per-session monotonically increasing `seq`. A client is a **cursor** over that
log. The node keeps no per-client state beyond the cursor position it is
currently serving, so a client that vanishes costs nothing to forget.

Chunks arrive as frames with `t: chunk`, `s` set, `seq` set, and a body:

```
ChunkBody { st: uint8, d: bytes }

st = 1 stdout        the process wrote this
     2 stderr
     3 exit          d is CBOR ExitInfo{code, signal, error}
     4 info          d is CBOR SessionInfo; always seq 0
     5 gap           d is CBOR Gap{from, to}; these seqs are gone forever
```

Guarantees:

1. `seq` starts at 0 and increases by one per chunk. Seq 0 is always the `info`
   chunk, so a replay from 0 reconstructs the session header.
2. The `exit` chunk is the last chunk. After it the log is closed.
3. `s.attach` with `from: N` replays every chunk from N, then continues live.
   Replay and live tail are the same code path.
4. If N is older than what the node retained, the node MUST send a `gap` chunk
   naming the lost range and then continue from the oldest chunk it has. It MUST
   NOT silently skip, and it MUST NOT kill the session. The harness can then tell
   the model that output was elided, which is the only honest option.

Retention is a bounded in-memory ring plus a spill file. The reference node uses
2 MiB of memory and 128 MiB of spill per session, and evicts on both a byte cap
and a chunk count cap, because a pty emitting one byte at a time will blow a
chunk-count budget long before a byte budget.

Input is idempotent. `s.input` carries `iseq`, a client-side counter. A node
drops any `iseq` at or below the last one it applied. Without this, a keystroke
retried after a dropped connection is typed twice.

## 9. Secret-blind execution

A workspace holds **references**, never secrets. The node runs a broker on
loopback that the workspace reaches two ways:

- **Reverse-proxy path**: `GET $REMOUNT_BROKER/d/<host>/<path>` speaks TLS to
  `<host>` on the workspace's behalf. Credentials are substituted here.
- **Forward-proxy path**: `HTTP_PROXY` and `HTTPS_PROXY` are set. `CONNECT`
  requires either an explicit typed `connect` rule or, in legacy local mode,
  the node allow list. A binding alone never grants a tunnel. Credentials are
  **not** substituted, because doing so would require terminating TLS with a CA
  installed in the workspace.

A binding leased to a node looks like:

```
BindingLease { id, secret, destinations: [host patterns],
               shape, principals, placeholder, expires_at }
```

The broker's rules, in order, for every request:

1. For each header value containing a binding's placeholder:
   - If the destination does not match that binding's `destinations`, **block the
     request** and emit `egress.denied` with decision `leak_blocked`. The
     placeholder was aimed at the wrong host, which is an exfiltration attempt.
   - If the lease has expired, block and emit decision `expired`. Fail closed.
   - Otherwise substitute the real secret and emit `cred.used`.
2. If a typed `NetworkPolicy` exists, evaluate its rules in declaration order.
   The first rule matching protocol, canonical host, effective port, method and
   path-prefix segment is authoritative. It replaces, rather than widens into,
   the legacy binding/node allow list. Rules imply default deny when `default`
   is omitted; default allow is valid only for the `local` profile.
3. Enforce `max_requests` atomically per workspace generation. Buffer a bounded
   request before dialing so one-byte-over bodies cannot partially mutate an
   upstream. Bound streaming responses and terminate the stream on the first
   byte over `max_response_bytes`. Every redirect is rewritten through the
   capability-bearing broker URL and reauthorized as a new request.
4. In legacy local mode only, a reverse-proxy destination is permitted if a
   binding was used for it or it matches the node allow list. An opaque CONNECT
   tunnel still requires the node allow list and never inherits binding
   authority.
5. Refuse any destination that resolves to a loopback, private, link-local or
   multicast address unless that host is explicitly allowed. This closes cloud
   metadata endpoints by default.
6. Dial the validated IP literal, not the name, so DNS cannot change under the
   check.

Host patterns are an exact host, `*.suffix` matching subdomains only, or `*`.
A pattern may carry a port, which then must match.

Typed host patterns and request authorities are lower-cased and canonicalized;
userinfo, Unicode, malformed ports and ambiguous encodings are rejected. Path
prefixes match path segments (`/v2` does not match `/v2evil`). Encoded slash,
backslash, dot and percent forms that could be decoded differently downstream
are rejected. `shared_state` is one of `none`, `immutable_read`,
`scoped_write`, or `global_write`; control authorizes those as execute, read,
write, and admin respectively. `immutable_read` permits only `GET` and `HEAD`.
CONNECT cannot claim path, body-size, or shared-state enforcement because its
contents are opaque.

Every broker listener has a random 256-bit capability unique to one workspace
materialization, and audit records carry workspace, generation, rule, protocol,
decision, shared-state class and body lengths when known (or the first excess
byte on a streamed limit violation). Suspending or closing a broker closes
already-established CONNECT tunnels as well as refusing new requests. The
reference broker accepts at most 128 client connections and 64 concurrent
requests or tunnels per workspace; excess requests fail with 429.

The reference `process` and `docker` backends provide cooperative proxy
mediation only. They do not provide a non-bypassable network boundary and MUST
be rejected by production security profiles. A conforming
`enforced_gateway` backend must force all IPv4, IPv6, UDP, DNS and raw-socket
traffic through an out-of-workspace policy component, install policy before
readiness, and synchronously revoke it during fencing and shutdown.

Placeholders should be **shape-preserving**: same prefix and length as the real
secret, so client-side format validation in a harness does not reject the
placeholder before it ever reaches the broker.

## 10. Artifacts and snapshots

An artifact id is `art_sha256:<64 hex>`. A snapshot is a deterministic tar.gz of
the workspace filesystem: entries sorted by path, uid and gid zeroed, PAX
format. The same tree always produces the same id.

Snapshots contain **files only**, never process memory. That is what makes them
portable across nodes, operating systems, architectures and vendors. A move
across machines restarts processes; the filesystem, the identity and the policy
travel.

`PUT /v1/artifacts/{id}` stores a blob in a private temporary file, enforces the
configured compressed-size limit, and publishes it only after the digest
matches the id.
`GET /v1/artifacts/{id}` retrieves it. A node fetching an artifact verifies the
digest itself and refuses a mismatch.

## 11. The event log

Every consequential action is an event. Transactionally persisted resource
tables are the control plane's recovery source of truth; the append-only event
log is the canonical audit and subscription history.

```
Event { event_id, seq, received_at, observed_at, origin, actor, tenant,
        workspace, generation, operation_id, producer_seq,
        stream, principal, node, type, payload, cause }
```

`stream` is a workspace or node id, so a workspace's whole history is one filter.

`seq` is assigned by the control plane and is the only total order.
`received_at`, authenticated `origin`/`actor`, tenant, workspace and generation
are assigned or verified outside the workspace. `observed_at` is the producer's
clock and is not authoritative. Nodes post an ordered outbox with a monotonic
`producer_seq`; exact retries are deduplicated and skipped ranges create an
`event.producer_gap` record. For a workspace-scoped node event, `principal` is
derived from control-plane ownership; sender-supplied principal metadata is
discarded. A reader that cares about causality should use `cause` rather than
infer it from timestamps.
Canonical types: `node.enrolled`, `node.online`, `node.offline`, `ws.created`,
`ws.claiming`, `ws.claimed`, `ws.released`, `ws.moved`, `ws.paused`,
`ws.resumed`, `ws.snapshot`, `ws.restored`, `ws.destroyed`,
`ws.lease_expired`, `s.opened`, `s.exited`, `fs.write`, `fs.edit`, `fs.remove`,
`cred.used`, `egress.allowed`, `egress.denied`, `timer.set`, `timer.fired`,
`peer.gone`, `ws.fenced`, `event.producer_gap`,
`fleet.quarantine.requested`, `fleet.quarantine.target`, and
`fleet.quarantine.completed`.

`POST /v1/events` appends an event out of band. This is how a webhook wakes a
sleeping workspace.

## 12. What a minimal implementation must get right

If you implement this protocol, these are the parts that are easy to get subtly
wrong and that a conformance suite should check:

1. Seq 0 is the info chunk, and the exit chunk is last.
2. Replay from an evicted seq produces a `gap` chunk, not silence and not an
   error that kills the session.
3. Duplicate `iseq` on input is dropped.
4. A grant with the wrong generation is refused.
5. A placeholder sent to an unbound host blocks the request rather than
   forwarding it.
6. `ws.ready` gates `claimed`, so a client never reads from a node that is still
   restoring.
7. A node renews at no more than one third of the lease.
8. Two concurrent offers for the same workspace to the same node must not
   produce two materializations.
9. A fleet destroy never deletes a retained source until its checkpoint and
   advanced fence are durable in control and its node has an exact phase-one
   journal proof.
10. A fleet operation reports pending/unreachable targets by its deadline and
    resumes their reconciliation after control restart.

## 13. Versioning

`v` is the frame version. Peers negotiate capability strings in `hello`. New
operations are additive; a peer that does not know an `op` answers
`unsupported`, and the caller degrades. Removing or changing the meaning of an
existing field requires a new `v`.
