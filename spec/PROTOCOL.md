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

1. A receiver MUST reject a frame whose `v` is not exactly a version it
   negotiated. Version 1 peers advertise the mandatory `v1` capability in
   hello; an empty capability list is not an implicit wildcard.
2. Within a negotiated version, unknown fields are ignored. Unknown `t` values
   are ignored. Unknown `op` values get a `res` with code `unsupported`.
3. A peer never assumes a field it did not send will come back.
4. `from` is authoritative and is stamped by the relay. A sender that sets it
   is overwritten.

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
          subject: string, tenant: string, node_token: string }

NodeInfo { backends, backend_descriptors, connectors, os, arch, cpu, mem_mib,
           caps, snapshots, version }
```

A peer MUST offer `caps:["v1"]`; the server returns the actual ordered
intersection and rejects a hello with no supported baseline. A node MUST
present a stable `n_` id and its ed25519 public key. `proof` is an
Ed25519 signature over deterministic CBOR of the Hello with `proof` omitted;
`issued_at` must be fresh and `nonce` is single-use. Production enrollment
binds the key, labels, and backend descriptors to an operator-approved node
record. A client MAY present a previously assigned `c_` id to keep it across
reconnects, or leave it empty to be assigned one. Client-selected `principal`,
workspace principal, labels, and capabilities are never authorization input;
the control plane derives subject and tenant from the presented credential.

`HelloOK.pubkey` is the control plane's grant-signing key. Nodes verify grants
with it. `lease_sec` tells a node how often it must renew claims. A node MUST
renew at an interval no greater than one third of the lease. After dynamic
enrollment, `node_token` is a short-lived node-principal credential for HTTP
artifact transfers. It is refreshed on reconnect and never written into the
workspace or the node's persistent identity file.

### 3.1 Named capabilities

Within v1, a change an older peer could ignore without weakening any security
property is an additive field. A change an older peer *ignoring it* would
weaken is a **named capability**: an exact, case-sensitive identifier offered
in `Hello.caps` and echoed in `HelloOK.caps` only when both sides implement
it. The server never echoes an identifier it does not implement, and a peer
MUST NOT rely on a capability that was not echoed. Capabilities are returned
in the canonical order of the table below, `v1` first.

| Capability | Introduced for | An old peer ignoring it would |
|---|---|---|
| `v1` | the semantic baseline | not be a peer at all |
| `authz-push` | revocation epochs pushed on renew | keep honouring a revoked principal's grant until it expires |
| `controller-epoch` | controller failover fencing | accept a superseded controller's decisions |
| `session-cap` | principal-bound session capabilities | leave a revoked principal's session open |
| `chunked-artifacts` | verified chunked artifact transfer | restore a truncated artifact as complete |
| `tiered-session-logs` | durable sealed session ranges | lose completed output after node loss |
| `identity-admin` | production tenant/principal onboarding operations | assume an unavailable management API exists |
| `approvals` | held operations awaiting a decision | proceed while an approval is pending |
| `encrypted-artifacts` | artifacts encrypted at rest | write or read a plaintext snapshot |

A security profile requires the capabilities whose absence would break the
promise the profile makes. `local` requires none, so an older node keeps
working there. `isolated` and `multi_tenant` require every named capability
this release implements; a capability is added to that requirement in the
same release that implements it on both sides. This release implements
`authz-push`, `controller-epoch`, and `session-cap`; the remaining identifiers are reserved and
are neither offered nor required yet.

| Deployment security floor | Peer offers `v1` only | Peer offers this release's set |
|---|---|---|
| `local` | accepted | accepted |
| `isolated` | hello refused, `unsupported`, names the missing capabilities and the profile | accepted |
| `multi_tenant` | hello refused, `unsupported`, names the missing capabilities and the profile | accepted |

Without a deployment floor the same rule applies per workspace: a node that
negotiated fewer capabilities than a workspace's `security.profile` requires is
not eligible for it and the workspace stays `pending` until an eligible node
exists. A node connected to a control plane that echoed fewer capabilities than
a claimed workspace's profile requires MUST refuse to materialize it
(`unsupported`, naming the missing capabilities) and release the claim rather
than serve the workspace with the property missing. `NodeStatus.protocol`
reports what each node negotiated at its last hello.

### 3.2 Controller epochs (`controller-epoch`)

`HelloOK.controller_epoch` is the nonzero epoch of the conditionally leased
controller writer. After hello, every frame in either direction carries that
epoch. Events also persist it as `Event.controller_epoch`, and grants bind it
in `GrantClaims.controller_epoch`. A peer MUST reject a lower epoch before
touching workspace state. Nodes durably persist the greatest accepted epoch
before accepting a higher-epoch frame; equal epochs permit idempotent replay.
An old controller that loses its object-store lease refuses authentication and
every later decision even if its process and peer connections remain alive.

A promoted controller answers health as reconciling and refuses mutations
until it queries every restored assignment holder with the internal
`controller.state` operation. Repairs pass through the lifecycle transition
table. Completion persists `control.reconciled` per repair and one
`control.recovered` with the restored event sequence and lost-window estimate.

## 4. Grants

A client may not talk to a node about a workspace without a grant. A grant is
issued by the control plane and verified by the node, so the node needs no
connection to the control plane to authorize a request.

```
GrantClaims { client, ws, node, principal, tenant, authz_revision,
              controller_epoch, exp: int64, gen: uint64 }
Grant       { claims: GrantClaims, sig: bytes, node: string }
```

`sig` is ed25519 over the deterministic CBOR encoding of `claims`. A node MUST
reject a grant whose signature fails, whose `exp` has passed, whose `client`,
`ws` or `node` does not match the request, or whose `gen` differs from the
workspace's current generation, tenant, or ACL revision. The generation and
authorization-revision checks make stale grants useless after a move or policy
change.

Clients obtain grants with `op: grant` against `control` and cache them.

### 4.1 Revocation epochs (`authz-push`)

A grant is verified offline, so on its own a revoked principal would keep
access until the grant expired. With `authz-push` negotiated the workspace's
`authz_revision` is a **revocation epoch**: control advances it on every ACL
change (`ws.acl`, including one that re-states the current ACL) and on every
generation change, and refuses at once to mint a grant for a principal the new
ACL excludes. The ACL is how a principal's access to a workspace is revoked;
a deployment whose external authorizer withdraws a principal re-states the ACL
to force every grant to re-verify. Nodes learn the revision on renew: `WSRenewReq.authz[id]` carries
the revision the node currently enforces and `WSRenewResult.authz_revision`
carries the authoritative one. When they differ the node adopts the new
revision, so every outstanding grant minted under the old one fails the
`authz_revision` check in §4, and the result also names the principals revoked
since the node's revision in `revoked`. The node MUST end every live session of
a named principal with `exit{reason: "revoked"}`; other principals' sessions
continue and their clients fetch fresh grants transparently.

Control retains the last 64 revocations per workspace (`Workspace.revocations`)
and records the newest pruned revision as `revocation_floor`. A node whose
known revision is below the floor receives `authz_reset: true` instead of a
list and MUST end every session of the workspace; still-authorized principals
reopen with fresh grants. Fail closed is the rule throughout: a node that
cannot tell who was revoked revokes everyone.

The latency bound is one renew interval. A node renews at no more than a third
of the lease (§5), so with the default 30 s lease a revoked principal loses
access to the node within 10 s and is refused a new grant immediately. A node
that has not renewed within the lease is fenced anyway, so no revoked grant
outlives the lease. An old node that ignores these fields is exactly the
failure the capability names, which is why `isolated` and `multi_tenant`
require it (§3.1).

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
EgressRule { id, mode?: "allow"|"deny"|"approve", connector?, protocol, hosts, ports, methods, path_prefixes,
             max_requests, max_request_bytes, max_response_bytes, redact?,
             shared_state, repos?, push? }
RepoSpec    { url, ref?, depth? }
```

`WorkspaceSpec.repo` names a repository the node clones into the tree before
`ws.ready` (§10.2). It is mutually exclusive with `base` and `restore_from`.

Profiles are `local`, `isolated`, and `multi_tenant`. `isolated` and
`multi_tenant` require an enforced egress backend; `multi_tenant` additionally
requires microVM-strength isolation, sibling, network-namespace and device
isolation. Placement MUST fail closed when a backend descriptor does not prove
every requested capability. A node MUST revalidate the descriptor and install
the network policy before reporting `ws.ready`; advertising
`enforced_gateway` without implementing the network-controller contract is an
error, not evidence of enforcement.

`redact` is a bounded list of RE2 expressions applied to an HTTP response
before any response byte enters the workspace. It is unavailable for CONNECT
and managed connectors. A redacted response is buffered up to 16 MiB (or the
smaller `max_response_bytes`); encoded or larger responses fail closed instead
of crossing unchanged. A rewrite emits `egress.redacted` with a match count.

**Re-adoption.** If a node reconnects and asks to claim a workspace it already
holds, the control plane returns it at the *same* generation and moves it to
`claiming`. Outstanding client grants stay valid, and the node re-announces
readiness when it has re-opened the workspace.

### 5.1 Normative lifecycle transition table

The control plane applies every lifecycle change through one compare-and-
transition function. It validates the named operation, actor class, current
state, expected generation, and authoritative node before persistence. This
table is exhaustive; any pair not listed returns `conflict`. `fleet.*` is the
privileged containment path and may force any non-destroyed workspace to the
operator-actionable `failed` or terminal `destroyed` state.

| Operation | Actor | Legal transition(s) | Generation/node rule |
|---|---|---|---|
| startup recovery | recovery | `claimed→claiming`, `claiming→claiming`, `quiescing/checkpointing/destroying→failed`, `released→pending` | persisted assignment is retained |
| node disconnect | control | `claimed→claiming` | same generation and node |
| `ws.claim` | node | `pending→claiming`; same-node `claiming/claimed→claiming` re-adoption | a new claim increments generation; re-adoption does not |
| `ws.ready` | node | `claiming→claimed`; `claimed→claimed` duplicate | exact generation and node |
| release begin/commit | control | `claimed→quiescing→released` | exact generation and node throughout |
| release abort | control | `quiescing/destroying→claimed` after an acknowledged abort, otherwise `→failed` | exact generation and node |
| `ws.released` | node | `claiming/claimed→pending` | exact generation and node; it cannot override a control-owned transition |
| move | control | `released/paused/pending→pending` | expected generation |
| sleep/wake | control | `released/pending/paused→paused`; `paused→pending` | expected generation |
| destroy | control | `claimed/claiming→destroying→destroyed`; `pending/released/paused→destroyed` | expected generation; held states also require the node |
| lease expiry | control | `claiming/claimed→pending` | exact generation and node, then generation increments |
| fleet quarantine | control | `claiming/claimed/failed→quiescing`, then any non-destroyed state `→failed/destroyed` | target list freezes node and generation |

`destroyed` is absorbing. `failed` is durable and operator-actionable; ordinary
claim, ready, move, sleep, wake, and destroy calls cannot silently revive it.
Duplicate mutating operations are answered from the durable idempotency record.

### 5.2 Stable paths and task queues

`WorkspaceSpec.mount_path` is where the tree appears inside the workspace
(default `/work`). It must be absolute, clean, at most 1024 bytes and outside
the system directories (`/`, `/proc`, `/sys`, `/dev`, `/etc`, `/bin`, `/sbin`,
`/lib`, `/usr`, `/var`, `/run`, `/boot`); `ws.create` rejects anything else
with `bad_request` and stores the default as empty. A non-default path is a
placement requirement: only a node whose backend descriptor advertises
`runtime.mount_path` may claim the workspace, because the path is realised
inside the workspace's own mount namespace and never as a host symlink. The
path is part of the spec and survives snapshot, sleep, wake and move.

A **queue** is a durable list of tasks for one workspace:
`Queue{id, ws, tenant, owner, recipe?, items[{task, session?, exit, signal?,
attempts, finished_at}], cursor, status, sleep_after_sec?, sleep_until?,
created_at, updated_at}`. `status` is `running`, `done` or `failed`.
`queue.create` refuses an empty list, more than 256 tasks or a task over
16 KiB, and a workspace that already has an unfinished queue (`conflict`).
`queue.advance{index, exit, signal}` records one task's outcome and requires
`index == cursor`: a zero exit without a signal moves the cursor and marks the
queue `done` when it passes the last item; anything else leaves the cursor on
the task, increments its `attempts`, and marks the queue `failed` so a later
driver retries the same task. Queue state is never stored in the workspace
tree; a queue is deleted with its workspace. Both mutations carry `idem` and
commit the resource, the mutation record and their events in one transaction.

### 5.3 Node pools

A **pool** is tenant-scoped desired capacity:
`Pool{spec{name, vendor, min, max, labels?, backend,
idle_scale_down_ms?, region?, size?}, tenant, owner, current, created_at,
updated_at}`. Provider credentials and enrollment tokens are absent. `current`
is last observed provider inventory, not authority to place a workspace. Pool
names are unique inside a tenant, `0 <= min <= max`, `max > 0`, and the
`remount.pool` and `remount.node` labels are reserved for reconciliation. The
latter binds provider inventory to the predetermined node id accepted during
one-time enrollment; it is never trusted from an unauthenticated hello. Removing a pool with
non-zero inventory returns `conflict`; it never destroys machines implicitly.

### 5.4 Immutable shared-data volumes

A `Volume{id, tenant, owner, artifact, version, versions[], created_at,
updated_at}` names a bounded history of immutable artifacts. A workspace stores
resolved `WorkspaceSpec.volumes[] = VolumeMount{id, path, version, artifact}`;
clients supply only `id` and `path` at create/attach time and control pins the
current version. Paths are clean, absolute, outside system and `.remount`
trees, unique within a workspace, and at most 64 mounts are retained.

Nodes advertise `readonly-volumes` only for the process backend and only after
an actual read-only bind-mount probe succeeds. Docker, gVisor and Firecracker
must each gain an end-to-end visibility/read-only proof before advertising the
capability. Nodes fetch and verify each pinned artifact, attach it before
`ws.ready`, and synchronously detach it after the source checkpoint and control
commit during move or destroy. Workspace snapshots exclude mount paths, so a
later publish cannot change restored bytes. Existing mounts remain pinned;
new attaches see the new version. No live shared writes are provided.

`volume.publish` is two-hop: the client sends `VolumePublishPathReq` to the
holding node; the node archives the jailed path and holds the workspace tree
boundary while sending `VolumePublishReq` to control. Control verifies the
artifact, exact workspace generation and expected volume version before the
volume row and event commit atomically. At the 128-version bound an unpinned
old version may be pruned; if every old version is pinned the call fails with
`resource_exhausted`.

## 6. Control-plane operations

Sent to `control`. Client operations are marked C, node operations N.

| op | Who | Body → Response |
|---|---|---|
| `ws.create` | C | `WSCreateReq{spec, idem}` → `Workspace` |
| `ws.get` | C | `WSGetReq{id}` → `Workspace` |
| `ws.list` | C | → `WSListRes{workspaces}` |
| `ws.destroy` | C | `WSGetReq{id, idem}` → `{}` |
| `ws.move` | C | `WSMoveReq{id, requires?, placement?, idem}` → `Workspace` |
| `ws.sleep` | C | `WSSleepReq{id, after_sec\|at\|on, match?, idem}` → `Timer`; `match` is a bounded exact payload-field predicate used only with `on` |
| `ws.wake` | C | `WSGetReq{id, idem}` → `Workspace` |
| `ws.acl` | C | `WSACLReq{id, acl{readers, writers}, idem}` → `Workspace`; owner or admin only; replaces the ACL and advances `authz_revision` (§4.1) |
| `base.create` | C | `BaseCreateReq{name, artifact, workspace?, idem}` → `Base`; pins an uploaded artifact under a tenant-unique name (§10.1) |
| `base.list` | C | → `BaseListRes{bases}`; the caller's tenant only, unless admin |
| `base.remove` | C | `BaseRemoveReq{name, idem}` → `{}`; owner or admin only |
| `volume.create` | C | `VolumeCreateReq{id, artifact, idem}` → `Volume`; artifact is verified and becomes version 1 |
| `volume.get` | C | `VolumeGetReq{id}` → `Volume`; tenant scoped |
| `volume.list` | C | → `VolumeListRes{volumes}`; caller's tenant only unless admin |
| `volume.remove` | C | `VolumeRemoveReq{id, idem}` → `{}`; owner/admin only and refused while attached |
| `volume.attach` | C | `VolumeAttachReq{id, ws, generation, path, idem}` → `Workspace`; pins the current immutable version in a non-running workspace |
| `volume.detach` | C | `VolumeDetachReq{ws, generation, path, idem}` → `Workspace`; removes a pinned declaration from a non-running workspace |
| `volume.publish.commit` | N | `VolumePublishReq{id, ws, generation, artifact, expected_version, idem, grant}` → `Volume`; control re-verifies the forwarded signed grant and its live client identity before applying the generation/version CAS |
| `queue.create` | C | `QueueCreateReq{ws, recipe?, tasks, sleep_after_sec?\|sleep_until?, idem}` → `Queue`; one unfinished queue per workspace (§5.2) |
| `queue.get` | C | `QueueGetReq{id}` → `Queue`; owner, workspace principals or admin |
| `queue.list` | C | `QueueListReq{ws?}` → `QueueListRes{queues}`; the caller's tenant only, unless admin |
| `queue.advance` | C | `QueueAdvanceReq{id, index, session?, exit, signal?, idem}` → `Queue`; `index` must equal `cursor` or the call fails with `conflict` |
| `pool.create` | C | `PoolCreateReq{spec, idem}` → `Pool`; tenant administrator only (§5.3) |
| `pool.get` | C | `PoolGetReq{name}` → `Pool` |
| `pool.list` | C | → `PoolListRes{pools}`; caller's tenant only |
| `pool.remove` | C | `PoolRemoveReq{name, idem}` → `{}`; empty pools only |
| `agent.create` | C | `AgentCreateReq{name?, ws?\|workspace?, spec, policy, parent?, acp_session_id?, idem}` → `Agent`; makes the workspace unless `ws` adopts one (§6.1) |
| `agent.get` | C | `AgentGetReq{id}` → `Agent` |
| `agent.list` | C | `AgentListReq{status?, ws?, parent?}` → `AgentListRes{agents}`; only agents the caller may read |
| `agent.message` | C | `AgentMessageReq{id, text, kind?, idem}` → `AgentMessageRes{agent, message, degraded?, woken?}`; appends to the inbox, wakes a sleeping agent |
| `agent.cancel` | C | `AgentGetReq{id, idem}` → `Agent`; drops the inbox and cancels the current turn; the run stays open for the next message |
| `agent.sleep` | C | `AgentGetReq{id, idem}` → `Agent`; stops the run, checkpoints and pauses the workspace |
| `agent.fork` | C | `AgentForkReq{id, name?, task?, policy?, idem}` → `Agent`; snapshots the workspace and starts a child from the copy with the same harness session |
| `agent.destroy` | C | `AgentGetReq{id, idem}` → `{}`; destroys the workspace only if the agent created it |
| `agent.transcript` | C | `AgentTranscriptReq{id, from?, limit?}` → `AgentTranscriptRes{records, next, gap?, done?}`; a page of the durable transcript mirror from cursor `from`, never touching the workspace (§6.2) |
| `approval.list` | C | `ApprovalListReq{agent?, kind?, status?}` → `ApprovalListRes{approvals}`; pending only unless `status` is given |
| `approval.get` | C | `ApprovalGetReq{id}` → `Approval` |
| `approval.decide` | C | `ApprovalDecideReq{id, option?, denied?, content?, remember?, idem}` → `Approval`; the decision commits before it is handed to the run; egress `remember` is `none`, `host`, or `rule` |
| `budget.create` | C | `BudgetCreateReq{budget, idem}` → `Budget`; immutable tenant-scoped definition attached to a tenant, workspace, principal, or binding |
| `budget.list` | C | `BudgetListReq{}` → `BudgetListRes{budgets}`; caller's tenant only |
| `budget.remove` | C | `BudgetRemoveReq{id, idem}` → `{}`; stops new admission while retained reservations remain settleable and auditable |
| `budget.reserve` | N | `BudgetReserveReq{key, ws, gen, principal, bindings, provider, model, input_tokens, max_output_tokens, metered}` → `BudgetReservation`; current generation holder only, before secret substitution or upstream I/O |
| `budget.settle` | N | `BudgetSettleReq{reservation, mode, input_tokens?, output_tokens?}` → `BudgetSettlement`; owning node only, exact replay is a no-op |
| `usage.get` | C | `UsageReq{tenant?, ws?, principal?, binding?, window?}` → `UsageRes{usage}` |
| `tenant.create` | C | `TenantCreateReq{id, policy{quotas, retention, residency, oidc}, idem}` → `Tenant`; global operator only |
| `tenant.get` / `tenant.list` / `tenant.update` / `tenant.state` / `tenant.usage` | C | tenant-scoped administrative reads and expected-revision mutations |
| `principal.create` | C | `PrincipalCreateReq{tenant?, principal, roles, idem}` → `Principal`; tenant operator only; node is never assignable here |
| `principal.list` | C | `PrincipalListReq{tenant?}` → `PrincipalListRes{principals}`; exact tenant only |
| `principal.revoke` | C | `PrincipalRevokeReq{tenant?, principal, idem}` → `PrincipalRevokeRes{revision}`; advances the durable revision |
| `principal.token.issue` | C | `PrincipalTokenIssueReq{tenant?, principal, role, ttl_ms, idem}` → `PrincipalTokenIssueRes{access_token, expires_at}`; access-only, assigned role, 24h maximum |
| `principal.invite` | C | `PrincipalInviteReq{tenant, principal, ttl_ms, idem}` → `PrincipalTokenIssueRes`; creates a tenant-bound operator and returns its initial short-lived access bearer |
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
| `ws.renew` | N | `WSRenewReq{ids, gen, authz, controller_epoch}` → `WSRenewRes{results, controller_epoch}`; each result repeats the epoch and explicitly says continue/fence/destroy/reconcile and, for a continued lease, carries `authz_revision`, `revoked`, `authz_reset` (§3.2, §4.1) |
| `ws.released` | N | `WSReleasedReq{id, gen, snapshot, reason, failed?}` → `{}`; `failed:true` means materialization could not complete and control holds the workspace out of placement with a growing delay (1s doubling to 30s, reset by the next `ws.ready`) instead of re-offering it at once |
| `ws.snapshot.commit` | N | `WSSnapshotCommitReq{id, gen, snapshot}` → `{}` |
| `artifact.proof` | N | `ArtifactProofReq{ws, gen, method, artifact}` → `ArtifactProofRes{proof}`; issues a one-use, short-lived control signature only to the live assignment holder |
| `session.cap.issue` | N | `SessionCapabilityIssueReq{client, ws, gen, authz_revision, principal, tenant, roles}` → `SessionCapabilityIssueRes{capability, expires_at}`; derives authority from the connected client and live signed grant |
| `session.cap.renew` | N | `SessionCapabilityRenewReq{ws, gen, capability}` → `SessionCapabilityIssueRes{capability, expires_at}`; rotates an unexpired signed proof for the same principal while assignment and authorization remain live |
| `session.cap.check` | N | `SessionCapabilityCheckReq{ws, gen, capability}` → `SessionCapabilityCheckRes{principal, tenant}`; performed for every broker request and revalidates principal revision, ACL and the live assignment |
| `session.log.commit` | N | `SessionLogCommitReq{session, ws, gen, principal, kind, info, exit, max_chunk, segments, complete}` → `SessionLogRecord`; control derives `tenant` from the workspace and ignores any node-supplied tenant. A replacement must be a monotonic continuation: the stored segment list must be an exact prefix of the new one, and a record already marked `complete` is immutable. Every newly referenced segment is verified present and byte-exact in the tenant store before commit, and the assignment generation is revalidated after that verification |
| `session.log.get` | N | `SessionLogGetReq{session, ws, gen}` → `SessionLogRecord`; only the current holder of that exact workspace generation, and only for a record whose tenant matches the workspace |
| `session.log.delete` | N | `SessionLogGetReq{session, ws, gen}` → `{}`; current holder only. A live record — one not yet marked `complete` — cannot be deleted |
| `controller.state` | control only | `ControllerNodeState{node, epoch, workspaces[], releases[]}`; authenticated node-authoritative state used only while a promoted controller is reconciling |
| `binding.lease` | N | `BindingLeaseReq{ws, gen}` → `BindingLeaseRes{leases}` |
| `egress.approval` | N | `EgressApprovalReq{ws, gen, principal, rule, host, method, path_hash, body_hash, fingerprint, wait_ms?}` → `EgressApprovalRes{id, status, allowed?, expires_at?}`; only the current generation holder may create the durable approval |
| `agent.report` | N | `AgentReport{agent, run, ws, gen, seq, kind, ...}` → `{}`; one observation about a run, fenced to the node, generation and run, deduplicated by `seq` (§6.1); `kind: transcript` carries `chunks[]` for the mirror (§6.2) |
| `diag` | C | `DiagReq{verify}` → control diagnostics |
| `audit.export` | C | `AuditExportReq{tenant?, from, to}` → `AuditExportRes{manifest, signature, bundle}`; the range is INCLUSIVE of both endpoints and `from` must be at least 1. The tenant is resolved from the authenticated subject; a caller whose tenant is not `*` may not name another, `*` is never a valid export subject, and administrative authority is required. A range the log can no longer serve completely is `evicted` carrying the oldest retained sequence, never a shorter bundle. One bundle is bounded at 16 MiB and one range at 1,048,576 sequences (§11.1) |
| `audit.key` | C | `AuditKeyReq{}` → `AuditKeyRes{key_id, algorithm, public_key}`; publishes only the verification half of the control plane's durable Ed25519 audit signing key, so a bundle can be verified without reaching the control plane |

Every mutating request carries an `idem` key. Replaying a request with the same
key is a no-op that returns the original result. This is what makes a retry
after a dropped connection safe.

The control plane sends nodes one event: `ws.offer`, a hint that a workspace is
available to claim. It is a hint, not an instruction; a node that ignores it
loses nothing but the work.

### 6.1 Agents and approvals

An `Agent` is a durable control-plane resource: a workspace plus a harness
conversation plus a policy. The workspace holds the files and the harness
process; the control plane holds the inbox, the run history, the ACP session
id and the status. Nothing about the agent lives only in a process, so a node
loss, a move or a sleep never loses the conversation.

```
Agent { id, tenant, owner, name, ws, owns_ws, spec, mode, acp_session_id,
        capabilities, status, status_reason, inbox[], runs[], turns, parent,
        forked_from, policy, transcript_session, transcript_node,
        transcript_next, transcript_first, transcript_bytes,
        pending_approvals, url, created_at, updated_at, wake_timer,
        idle_since, failures, parent_notified }
```

`status` is derived, never stored as intent: `creating` until a node first
holds the workspace; `scheduled` while `policy.start_at` (Unix ms) lies in the
future and no run has started — the workspace is materialized, the inbox is
held, nothing launches; `running` while a prompt is in flight or queued;
`waiting_approval` while an approval is pending; `waiting_input` when the
harness finished a turn and is still up; `idle` when no harness is running and
nothing is queued; `sleeping` when the workspace is paused; `failed` after the
harness exited nonzero or a protocol error twice in a row; `finished` when a
policy (`max_turns`) ended it; `destroyed` forever. Transitions pass the
central table in `control/state_machine.go`; a terminal agent is never
resurrected.

The control plane reconciles durable intent to node operations on every tick:
a claimed workspace with a queued message and no live run gets `agent.run`
(`AgentRunReq{agent, run, attempt, ws, gen, tenant, owner, spec, policy,
mode, acp_session_id, messages}` → `AgentRunRes{transcript}`); a live run gets
queued messages by `agent.deliver`; `agent.cancel`/`agent.sleep` send
`agent.run.cancel`. A node answers with `agent.report` observations. A
message stays in the inbox until the node reports `turn_finished` for it, so
a harness that dies mid-turn is re-prompted with the same text on the retry.
A run whose node is gone past its lease, or whose workspace moved, is closed
with `stop_reason: node gone` and retried after a back-off; two consecutive
failures fail the agent. `agent.report` is refused with `conflict` for a
stale generation, `unauthorized` from a node that does not hold the run, and
is idempotent per `(run, seq)`.

Before it spawns the harness, the node runs the recipe's `install` script
once per workspace generation (marker `.remount/launch/<recipe>.installed`
holding the generation), the same step `remount run` performs from the
client. A move lands on a fresh generation and installs again; a retry on the
same node does not. The install runs as an exec session with the workspace's
broker environment, is bounded to fifteen minutes, and its tail is written to
the run's transcript on the stderr stream. A failed install finishes the run
with `exit_code: -1` and an error naming the exit status; the marker is not
written.

`agent.message` kinds are `follow_up` (default) and `steer`. ACP has no
mid-turn input, so a `steer` is queued as a follow-up and the response says
`degraded: true`. The inbox is bounded (64) and rejects with
`resource_exhausted`. A message to a `sleeping` agent wakes the workspace and
returns `woken: true`; the run starts once a node claims it and loads the same
ACP session.

`agent.fork` needs a claimed workspace: the control plane asks the holding
node for an uploaded snapshot on its own authority (`ws.snapshot{ws, gen,
upload, idem}`, fenced to the generation it believes the node holds, no client
grant), then creates a new agent whose workspace restores that snapshot and
whose `acp_session_id` is the parent's. A child's policy may only be narrower
than its parent's: `approve` may not widen and a bounded `max_turns` may not
grow or become unbounded.

`agent.create` with `parent` makes a child of a live agent the caller may
execute (a fork is a child too). A child inherits what it does not name —
`providers`, `primary`, `sandbox`, the workspace `bindings`, the security
profile — and may never hold more than the parent: a provider or binding the
parent lacks is `denied`, as is a wider policy. Trees are at most three deep.
When a child reaches `failed`, `finished` or `destroyed`, the control plane
appends one `kind: child` message to the parent's inbox whose text is a
`ChildSummary{child, name, status, reason, turns, ws, url}` JSON document,
emits `agent.child.finished` on the parent's stream, and marks the child
`parent_notified`, all in one transaction. A parent that is terminal or gone
is marked notified without a message; a parent whose inbox is full is retried
on the next reconcile. The parent handles the summary like any follow-up: it
wakes if asleep and runs a turn.

`policy.start_at` schedules the first run: the agent is created and its
workspace claimed at once, but the reconciler neither launches nor delivers
before that instant, and `agent.message` before it is queued. A start more than
366 days out is `bad_request`.

`policy.approve` is `never`, `on-request` (default) or `auto`. `auto` is
refused with `denied` for a local process workspace; it needs the docker
backend or a security profile above `local`.

An `Approval` is a question the harness asked that policy routed to a human:

```
Approval { id, tenant, owner, agent, ws, run, principal, rule, host, method,
           path_hash, body_hash, fingerprint, kind, title, tool_call,
           tool_kind, locations[], options[], detail, status, decision,
           delivered_at, created_at, updated_at, expires_at }
```

`kind` is `tool_call` (an ACP `session/request_permission`), `elicitation`
(an ACP elicitation) or `egress` (the broker asked). The node parks the
harness request and reports it (`agent.report{kind: permission|elicitation,
approval}`); the control plane owns the row and the decision; the node
answers the harness when the decision reaches it (`agent.approval.decided`).
The decision commits with its event before it is sent and is re-sent every
ten seconds until the node acknowledges (`delivered_at`) or the run ends. An
approval never outlives its run: ACP cannot re-ask, so a run ending expires
what it parked (`delivered_at: -1` for an undelivered decision) and the
harness asks again on its next turn. At most 64 approvals per agent may be
pending. `detail` is the harness's raw request (bounded to 64 KiB) and
appears in `approval.get`, never in an event payload.

`approval.decide` validates against the kind. `tool_call`: `option` must be
one the harness offered; an empty `option` without `denied` picks the first
`allow_once`/`allow_always` option and is `bad_request` when there is none.
`elicitation`: `content` must be a JSON object (the form fields) unless
`denied`, which discards any content; `option` is refused. `egress`: `option`
is `allow`, `deny` or absent (allow), `denied` also denies, and the recorded
decision always carries `option: allow|deny` and `remember: none|host|rule`.
Egress rows are idempotent on the request fingerprint and bound to the current
workspace generation. The broker waits at most 30 seconds, then returns `403`
with `X-Remount-Approval` and `Retry-After`; the row remains durable for ten
minutes. A resubmitted identical request passes after an allow decision.
`remember: host` also appends an allow rule and emits `policy.updated` in the
same transaction. A node may only park
`tool_call` and `elicitation`; `egress` rows come from the broker. A second
decision on any row is `conflict`; a replay with the same idempotency key
returns the row as decided.

### 6.2 Transcript mirror

The node writes the harness conversation to a session log (`kind: acp`) in the
workspace's node, where `s.attach` replays it. That log dies with the node and
sleeps with the workspace, so the node also ships every record to the control
plane as it is written: `agent.report{kind: transcript, chunks[]}` carries
`TranscriptChunk{seq, stream, at, data}` for each session-log record on the
`acp_in`, `acp_out` and `stderr` streams, after the same redaction and bound
(`MaxACPTranscriptFrame`) the session log applied. The node batches records
for at most 250 ms or 64 KiB and never lets a lifecycle report overtake a
queued record: `turn_finished` for a message follows every chunk of that
turn. One report carries at most `MaxTranscriptReportBytes` (256 KiB) and is
refused with `bad_request` beyond it.

The control plane appends the chunks to a per-agent log with a contiguous
agent-wide index that spans runs, retries and moves, and records the bounds
on the agent: `transcript_next` is the index the next record takes,
`transcript_first` the oldest index still held, `transcript_bytes` the data
held. A mirror is bounded (64 MiB per agent by default); past the bound the
oldest records are evicted and `transcript_first` moves up. Rows and bounds
commit in one transaction with the report's dedupe mark, so a replayed report
never duplicates a record. A transcript report is data, not a state change,
and emits no event.

`agent.transcript{id, from, limit}` returns `records[]` (`TranscriptRecord{
index, run, seq, stream, at, data}`) from `from` in index order, at most
`limit` (default and cap 1000), and `next`, the cursor to continue from. A
`from` below `transcript_first` returns `gap{from, to}` naming the evicted
range and the page starts at `to`; a reader never sees a shorter log without
being told. `done: true` is set when the agent is terminal and the page
reached `next`: nothing more will ever arrive. Reading the mirror needs read
authority on the agent and never wakes a sleeping workspace; the node-side
session log remains the source for byte-exact replay of a live run.

### 6.3 The agent HTTP API

Next to the frame protocol the control plane serves a JSON HTTP API for
agents, so a browser, an editor or a chat integration can drive one without
speaking frames. It is normative only in that every route is a translation of
one operation above: the request's bearer credential becomes a client, the
body becomes the protocol request, the protocol error code becomes a status.
Authorization, idempotency and events are the ones §6.1 and §6.2 define. The
route table, streaming formats and bounds are in `docs/api.md`.

```
POST   /v1/session                      mint the browser cookie from a header credential
DELETE /v1/session                      clear it
GET    /v1/usage                        usage.get
POST   /v1/agents                       agent.create      201 + Location
GET    /v1/agents                       agent.list
GET    /v1/agents/{id}                  agent.get
POST   /v1/agents/{id}/messages         agent.message
POST   /v1/agents/{id}/cancel           agent.cancel
POST   /v1/agents/{id}/sleep            agent.sleep
POST   /v1/agents/{id}/wake             agent.wake
POST   /v1/agents/{id}/destroy          agent.destroy     204
POST   /v1/agents/{id}/fork             agent.fork        201 + Location
GET    /v1/agents/{id}/transcript       agent.transcript (JSON page, SSE or WebSocket)
GET    /v1/agents/{id}/approvals        approval.list
GET    /v1/approvals/{id}               approval.get
POST   /v1/approvals/{id}               approval.decide
GET    /v1/agents/{id}/diff             git status + git diff in the workspace
GET    /v1/agents/{id}/terminal         WebSocket pty (new or attach+replay)
       /v1/agents/{id}/fs/{path}        GET/HEAD/PUT/DELETE, jailed by the node
       /v1/agents/{id}/ports/{port}/... authenticated reverse proxy into the workspace
GET    /a/{id}                          the stable agent URL
GET    /v1/identity/oidc?tenant=T       public non-secret device-flow configuration
POST   /v1/identity/oidc/exchange       verified ID token to Remount access/refresh pair
POST   /v1/identity/refresh             atomically rotate a single-use refresh bearer
```

Three rules are protocol, not presentation. A credential arrives in
`Authorization`, or as the WebSocket subprotocol `remount.bearer.<base64url>`,
or as the `remount_session` cookie; the cookie is accepted only on the
preview proxy and `GET /a/{id}`, and never from an untrusted `Origin`,
because preview content is same-origin with the API and written by the
untrusted workspace, so a cookie honoured on any route that reads a file,
opens a terminal or wakes an agent would let one workspace act as the
operator on every other. Reads never wake: the
transcript reads the mirror, and the filesystem and terminal refuse a
sleeping agent with `conflict` rather than waking it; `diff?wake=true` and
the preview proxy wake deliberately and emit `agent.woken` with `by: diff`
and `by: preview`. `/a/{id}` carries no capability: it redirects to the
operator UI when one is configured and otherwise answers `agent.get`, both
authenticating like every other route.

## 7. Node operations

Sent to a node id, and every one carries a `Grant` on first use per connection.

| op | Body → Response |
|---|---|
| `s.open` | `SOpenReq{ws, kind, program, cwd, env, rows, cols, stdin, timeout_sec, idem, run?}` → `SOpenRes{s, next}` |
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
| `fs.apply_tar` | `FSApplyTarReq{ws, artifact, format, idem}` → `FSApplyTarRes{files, dirs, bytes}`; `format` is the artifact's representation (§10) and an absent value means the legacy `tar` |
| `ws.snapshot` | `WSSnapshotReq{ws, upload, authoritative, idem}` → `WSSnapshotRes{artifact, bytes, consistency, authoritative}` |
| `volume.archive` | `VolumeArchiveReq{ws, path, upload, idem}` → `WSSnapshotRes`; creates a non-authoritative artifact for a jailed subdirectory |
| `volume.publish` | `VolumePublishPathReq{ws, path, volume, expected_version, idem}` → `Volume`; holds the tree boundary through the control-plane CAS |
| `ws.info` | `WSGetReq{id}` → `WSInfoRes{ws, backend, root, sessions, broker}` |
| `node.status` | → `NodeStatus` |
| `node.diag` | `NodeDiagReq{ws, verify}` → node diagnostics |
| `ws.release` | control only: `WSReleaseReq{ws, gen, operation, snapshot, reason, tenant, backend, spec}` → `WSReleasedReq`; `operation` is a control-generated lifecycle epoch, the recovery fields are control-derived and include exact pinned volumes, and `preparing:true` means poll with the identical request |
| `ws.release.commit` | control only: `WSReleaseCommitReq{id, gen, operation, snapshot}` → `{}` and authorizes source deletion only for the exact prepared lifecycle epoch |
| `ws.release.abort` | control only: `WSReleaseCommitReq{id, gen, operation}` → `{}`; restores the retained source and exact volume pins but keeps it inert |
| `ws.release.abort.commit` | control only: `WSReleaseCommitReq{id, gen, operation}` → `{}`; after control durably commits `claiming`, authorizes publication of that exact restored source. Control publishes `claimed` only after this acknowledgement |
| `ws.quarantine` | control only: `WSQuarantineReq{operation, ws, gen, action, backend, exclude, security, tenant, volumes}` → `WSQuarantineRes{fenced, gen, action, backend, snapshot?, warning?}`; recovery declarations identify exact pins to detach before phase-one acknowledgement |
| `ws.quarantine.commit` | control only: `WSQuarantineCommitReq{operation, ws, gen, backend, snapshot}` → `{}` and authorizes deletion only after an exact durable phase-one proof |

Session kinds are `exec`, `pty` and `port`.

`fs.edit` is atomic across all edits in one request. Each edit's `old` must
match exactly once unless `all` is set. If any edit fails to apply, the file is
not written and the response is `conflict`.

`fs.apply_tar` overlays an artifact (§10) onto the workspace tree. The node
fetches and fully validates the archive under the same limits as a restore
before it touches the tree; every regular file then lands by rename into its
final path, so a reader never sees a partially written file. Paths the archive
does not name are left in place, `.remount/` is refused, and an entry that
would replace a directory with a file, or write through a symlinked parent,
fails the whole request with `bad_request`. The response counts what was
written; the node emits one `fs.apply_tar` event and one `fs.write` per path.

The node reconstructs the archive from the `format` it is **told**; it never
guesses one from the artifact id. A `chunked-v1` request is resolved by
fetching and digest-verifying every referenced chunk before the tree boundary
is taken, then streaming the canonical tar through the same validation and
rename path as a legacy archive, so both representations share one overlay
commit point. A node that does not support the named representation MUST fail
with `unsupported` before touching the tree, and MUST NOT fall back to parsing
the object as some other format.

Node mutations with an `idem` key are write-ahead journaled. The node persists
and fsyncs a `pending` intent before applying the effect, then persists and
fsyncs the completed response before acknowledging it. An intent that is still
`pending` after restart has an unknown outcome and MUST return `conflict`; it
MUST NOT be applied automatically. Reusing a key with different arguments also
returns `conflict`.

An ordinary `ws.snapshot` is labeled `consistency:"live"`; it may observe a
concurrently changing tree and is never committed as failover state, even when
uploaded. `authoritative:true` requires `upload:true`, rejects new work, drains
node-mediated filesystem mutations, terminates and joins Remount-managed
process-backend sessions (or pauses a Docker container), and returns
`consistency:"quiesced"`. Only after the uploaded digest is verified and
`ws.snapshot.commit` succeeds does the operation succeed as authoritative.
The process backend cannot fence host processes that escaped Remount, so it is
only a local/unisolated profile; production profiles require a stronger
backend boundary.

### 7.1 Fleet containment

A `FleetOperation` is a durable, selector-frozen incident response:

```
WorkspaceSelector { all, workspace, tenant, principal, run, node, model, backend,
                    labels, created_after, created_before }

FleetOperation { id, selector, action, requested_by, tenant, state,
                 created_at, deadline, updated_at, results }

FleetOperationResult { workspace, node, backend, generation, state,
                       fenced, acknowledged, snapshot, error, updated_at }
```

The allowed actions are `freeze`, `revoke_egress`, `checkpoint`, `stop`, and
`destroy`. Every action is containment: the node first removes the target from
its serving map, revokes broker access, invalidates grants/subscriptions and
stops sessions. After any required checkpoint has joined, the node synchronously
detaches and verifies absence of every exact control-declared volume pin before
closing the tree or returning its durable phase-one proof. `checkpoint` and `destroy` additionally require a verified
snapshot. `destroy` is two phase: control first durably commits the advanced
generation, terminal workspace state, and snapshot reference; only then may it
send `ws.quarantine.commit`. A node MUST match that commit against the exact,
durably journaled phase-one generation, action, backend and snapshot before it
deletes the retained source. Control retains the physical node and pinned volume
references until the node's durable phase-two tombstone is acknowledged; their
removal and pinned `volume.detached` events commit atomically.

The node records quarantine progress in a typed durable journal before the
first fence. `preparing` is deliberately ambiguous after restart; later
`quiesced`, `checkpointed`, `detached`, `prepared`, and `destroy-authorized`
states contain enough exact request and result data to resume idempotently.
Only `committed` or `superseded` records are retention-prunable. A prepared
destroy operation cannot be superseded, so an older same-generation commit can
never delete a source retained by a later fleet cycle.

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
     3 exit          d is CBOR ExitInfo{code, signal, error, reason?}; reason "revoked" means the node ended it (§4.1)
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

### 8.1 Harness runs

`s.open` MAY carry `run: RunInfo{recipe, task_hash?, sandbox?, auth?}` to mark
the session as a harness launch (`remount run`). `recipe` is the recipe name
(`^[a-z0-9][a-z0-9_-]{0,63}$`); `task_hash` is a short digest of the task text,
never the text; `sandbox` is `read-only`, `workspace-write` or `full`; `auth`
is `api_key` (the harness reads a brokered placeholder from its environment)
or `workspace_resident` (the harness keeps its own login token in the
workspace, outside the broker's view). The node validates `run` fail-closed
(`bad_request`), copies it into `SessionInfo.run`, and emits `run.started`
once when the session is created — an idempotent replay of the open emits
nothing — and `run.finished` from the session's exit path, so a client that
detached still gets both records. When `auth` is `workspace_resident` the node
also emits `auth.workspace_resident`.

### 8.2 Tiered durable session logs (`tiered-session-logs`)

The ring and spill of §8 are node-local, so they do not survive node loss. A
node that offers the `tiered-session-logs` capability (§3.1) additionally
seals completed sequence ranges into immutable, tenant-local artifacts and
registers them with the control plane, so an exited session can still be
replayed byte-exactly after a move or a node restart.

A `SessionLogRecord` is the control-owned replay authority. It names the
session, its workspace and tenant, the producing principal, the session kind,
its `SessionInfo` and `ExitInfo`, the producer's `max_chunk` bound, and an
ordered `segments` list. Each `SessionLogSegment{first, next, artifact, bytes}`
covers the half-open chunk range `[first, next)` and names the artifact holding
it. Segments MUST be contiguous: every segment's `first` equals the previous
segment's `next`.

The three operations are node-to-control (§6):

- `session.log.commit` replaces the whole record. Control derives `tenant` from
  the workspace; a node cannot select it. A commit is accepted only from the
  node holding the exact workspace generation, and only as a **monotonic
  continuation** — the stored segment list must be an exact prefix of the
  replacement. A record marked `complete` is immutable, so a replayed identical
  commit returns the stored record and anything else is a `conflict`. Newly
  referenced segments are verified present and byte-exact in the tenant
  artifact store before the record is committed, and the assignment generation
  is revalidated afterwards, so a producer fenced during verification commits
  nothing.
- `session.log.get` returns the record to the current holder of that exact
  workspace generation, so a node that has just claimed a moved workspace can
  reconstruct a session it never ran.
- `session.log.delete` releases an expired record and the GC roots its
  segments held. A record that is not yet `complete` is live and cannot be
  deleted.

Retention is per tenant. While a record exists, every artifact it references is
a GC root, so the transitive closure of a retained session log is never
collected.

Replay from a tiered log obeys the same honesty rule as §8's guarantee 4, and
it is the rule that matters most here because the loss is now durable: if a
segment is unavailable — the blob is gone, or a cold cache cannot reach it —
the node MUST emit an explicit `gap` chunk naming the lost range, tagged with
the tier that failed, before continuing from the oldest chunk it can serve.
Unavailable archived output MUST NOT be reported as a complete replay. Control
records that unavailability as `session.log.unavailable`.

Events: `session.log.committed` carries the workspace, the segment count and
whether the record is `complete`; `session.log.deleted` carries the workspace
and the segment count that was released; `session.log.unavailable` records a
replay that could not be served in full.

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
BindingLeaseReq { ws, gen } // gen fences a source resolution that crosses a move
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

`connector: "package"` is a separate read-only capability. It requires HTTPS,
`immutable_read`, and `GET`/`HEAD`, and is reachable only as
`$REMOUNT_PACKAGE_CONNECTOR/<host>/<path>`. A package rule never authorizes the
generic reverse proxy, forward proxy, or CONNECT. Bodies, range requests,
WebDAV/mutating methods, and non-HTTPS redirects are denied. Redirects are
rewritten through the connector so each hop consumes and rechecks policy.

Package responses are staged, bounded, SHA-256 verified, and stored as
read-only content-addressed blobs. Mutable metadata and references are scoped
to an opaque tenant/workspace directory. `X-Remount-Expected-Digest` enables a
cache hit only after that same workspace has independently fetched the digest;
the header is stripped upstream. `X-Remount-Content-Digest` reports provenance
without exposing a cache path or hit state. Audits additionally carry the
connector and digest.

`connector: "git"` is the smart-HTTP capability, reachable only as
`$REMOUNT_GIT_CONNECTOR/<host>/<owner>/<repo>[.git]/{info/refs,git-upload-pack,git-receive-pack}`.
A git rule requires HTTPS, scopes by `repos` (exact `owner/name` or
`owner/*`) rather than `path_prefixes`, admits only `GET info/refs?service=`
and `POST` to the two pack endpoints with no other query, and never follows a
redirect. Dumb-HTTP object paths, the hosting service's API, raw-file and LFS
endpoints are outside the grammar and are denied before any credential is
substituted. `push:false` (the default) denies `git-receive-pack` at
advertisement time so `git push` fails before a packfile is sent. A workspace
whose `spec.repo` names a repository gets an implicit rule `repo` covering
exactly that repository (fetch and push) when its policy declares no typed git
rule; nothing else is opened. The push half is deliberate: an agent whose
purpose is to open a pull request needs it, and the binding that carries the
token is the operator's grant. An operator who wants a read-only checkout
declares a typed git rule for the repository with `push:false`, which then
governs alone. Audits carry `connector: git`, `op: fetch|push`
and `repo: owner/name`. The `/d/<host>/` reverse proxy remains usable as a
stopgap (`url.$REMOUNT_BROKER/d/github.com/.insteadOf`), but it is the generic
substitution path and enforces none of the grammar above.

The scheduler MUST require `package` in `NodeInfo.connectors` before assigning
a workspace containing such a rule, and `git` for a workspace with a git rule
or a `spec.repo`. Absence fails closed, including for older
nodes that do not send the additive field. Production enrollment binds this
list to operator-approved node information rather than trusting a node's
self-report.

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

An artifact id is `art_sha256:<64 hex>`. Snapshot responses and durable
workspace state carry an explicit `format`. Empty format is the legacy `tar`
format; `chunked-v1` is a manifest plus its referenced tenant-local chunks;
`firecracker-full-v1` is one opaque, content-addressed full-VM bundle containing
the root disk, VMM state, guest memory, and exact restore-compatibility fence.
A `tar` snapshot is a deterministic tar.gz of the workspace filesystem:
entries sorted by path, uid and gid zeroed, PAX format. The same tree always
produces the same id.

`tar` and `chunked-v1` snapshots contain **files only**, never process memory.
Their moves restart processes; the filesystem, identity and policy travel.
`firecracker-full-v1` is accepted only by an exactly compatible Firecracker
destination and carries the paused guest process state.

`ws.moved` is emitted when the move enters `pending`, before a destination is
chosen, so its `processes` field reports intent, not outcome. Every format
other than `firecracker-full-v1` records `processes:"restarted"`, which is
already final: those snapshots contain no process memory, so no destination can
change the answer. A `firecracker-full-v1` move records
`processes:"restore_pending"`, because whether the guest actually resumes
depends on the destination's Firecracker and CPU compatibility and on the
restore itself succeeding. `processes:"preserved"` MUST NOT be asserted from
the artifact format alone; it is reserved for a checkpoint that demonstrably
restored on the destination (ADR 0063). A reader that needs process continuity
must treat `restore_pending` as unknown, never as preserved.

`PUT /v1/artifacts/{id}` stores a blob in a private temporary file, enforces the
configured compressed-size limit, and publishes it only after the digest
matches the id. `GET` retrieves it and `HEAD` reports its plaintext size. The
authenticated subject's tenant selects the logical store; a tenant header is
never accepted. A known digest in another tenant is indistinguishable from an
unknown digest.

A dynamically enrolled node sends its short-lived node credential plus
`X-Remount-Workspace`, `X-Remount-Generation`, and
`X-Remount-Artifact-Proof`. Immediately before each request it obtains the
proof with `artifact.proof`. The signed claims bind a unique, single-use id,
authenticated node and tenant, workspace, current generation, exact HTTP
method and artifact id, issue time, and expiry. The artifact server verifies
the signature and exact request binding, then asks the control plane to
recheck the live assignment before touching storage. `claiming` permits
downloads needed for materialization; lifecycle checkpoint states permit
uploads needed to commit a release. A released, moved, expired, replayed, or
otherwise stale proof is refused as not found and never exposes whether the
tenant has the digest. A node fetching an artifact verifies the digest itself
and refuses a mismatch; a production node also authorizes a shared local-cache
hit with a tenant-bound `HEAD` before reusing it.

A client seeds a workspace from a local directory by producing the same
deterministic archive (the reference implementation's `localfs.Pack` honors
`.gitignore`, `.remountignore`, and a default exclude list, and never packs
`.remount/`), uploading it with `PUT`, and passing the id as
`WorkspaceSpec.restore_from`. Later local changes travel the same way and are
applied with `fs.apply_tar` (§7); a pull is a `ws.snapshot` with `upload:true`
followed by `GET`.

### 10.1 Bases

A **base** is a named, pinned artifact: `Base{name, tenant, owner, artifact,
workspace?, bytes, created_at}`. `base.create` verifies the artifact's digest
and records the pin; `WorkspaceSpec.base` then resolves to
`restore_from = base.artifact` at `ws.create` time, so the workspace itself
carries the artifact id and outlives the base. `base` and `restore_from` are
mutually exclusive in one request.

Names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` and are unique per tenant;
two tenants may each own a base called `golden`. Any member of the tenant may
list a base or create a workspace from it; only its owner or an admin may
remove it. An implementation bounds the number of pinned bases per tenant and
refuses the excess with `resource_exhausted`.

A pinned artifact is a GC root: artifact garbage collection must not unlink it
while the base exists. `base.remove` drops the pin only; workspaces created
from the base keep their own `restore_from` reference. A `ws.create` replay
whose idempotency key was first used with `base: NAME` returns the original
workspace even after that base is removed — the fingerprint covers the request
as sent, not the resolved artifact.

### 10.2 Repositories

`WorkspaceSpec.repo{url, ref, depth}` seeds a fresh workspace from a git
repository instead of an artifact. `url` is canonicalized to
`https://<host>/<owner>/<name>` (userinfo, query, fragment and non-HTTPS
schemes are refused); `ref` is a branch, tag or full commit SHA; `depth > 0`
requests a shallow clone. The node clones **before** `ws.ready`, through its
own broker with the workspace's binding placeholder, so the credential is
substituted at the node edge and never enters the tree, `.remount/env`, the
node log or an event payload. The tree's git configuration is delivered as
`GIT_CONFIG_*` environment (routing `https://<host>/` through
`$REMOUNT_GIT_CONNECTOR`, disabling credential helpers, terminal prompts and
every protocol but HTTP(S)); it is regenerated on every materialization
because the broker address changes on every move.

A clone that fails does not produce a workspace: the node destroys the fresh
tree, releases the claim with `failed:true`, and control backs the retry off.
A node that adopts a tree whose clone never completed (it died mid-fetch)
discards it rather than serving an empty checkout; the completion marker
lives under `.remount/` and, like `.remount/env`, never travels in a snapshot.
A restore (`restore_from`, a move, a wake) never re-clones: the snapshot
already carries the checkout. Success emits `repo.cloned{repo, ref, commit,
depth, backend}` where `commit` is `git rev-parse --verify HEAD` of the tree
as served.

## 11. The event log

Every consequential action is an event. Transactionally persisted resource
tables are the control plane's recovery source of truth; the append-only event
log is the canonical audit and subscription history.

```
Event { event_id, seq, received_at, observed_at, origin, actor, tenant,
        workspace, generation, session, operation_id, producer_seq,
        stream, principal, node, type, payload, cause, controller_epoch }
```

`stream` is a workspace or node id, so a workspace's whole history is one filter.
`session` is set by the node on every event attributed to one session
(`s.opened`, `s.exited`, and any later `s.*` type) so one command's history is
a second filter that needs no payload parsing. The control plane clears a
client-supplied `session`; only the node that runs a session may attribute to
it.

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
`ws.lease_expired`, `ws.acl`, `authz.revoked`, `s.opened`, `s.exited`,
`fs.write`, `fs.edit`, `fs.remove`, `fs.apply_tar`,
`cred.used`, `egress.allowed`, `egress.denied`, `egress.redacted`, `timer.set`, `timer.fired`,
`peer.gone`, `ws.fenced`, `ws.state_changed`, `event.producer_gap`,
`fleet.quarantine.requested`, `fleet.quarantine.target`,
`fleet.quarantine.completed`, `base.created`, `base.removed`, `volume.created`,
`volume.published`, `volume.attached`, `volume.detached`, `volume.removed`, `run.started`,
`run.finished`, `auth.workspace_resident`, `queue.created`,
`queue.advanced`, `pool.created`, `pool.removed`, `pool.scaled`,
`pool.provision_failed`, `repo.cloned`, `agent.created`, `agent.message`,
`agent.run.started`, `agent.run.finished`, `agent.session`, `agent.turn`,
`agent.tool_call`, `agent.waiting`, `agent.cancelled`, `agent.slept`,
`agent.woken`, `agent.forked`, `agent.failed`, `agent.finished`,
`agent.destroyed`, `agent.child.finished`, `approval.pending`,
`approval.decided`, `approval.expired`, `egress.pending`, `egress.allowed`,
`egress.denied`, `policy.updated`, `budget.created`, `budget.removed`,
`budget.reserved`, `budget.settled`, `budget.expired`, `budget.unmetered`,
`notifier.unavailable`, `notifier.dead_lettered`,
`notifier.dead_letters_pruned`, `identity.principal_created`,
`identity.roles_changed`, `identity.principal_revoked`,
`session.log.committed`, `session.log.deleted`, `session.log.unavailable`,
`audit.exported`, `audit.export_denied`, `retention.enforced`,
`retention.violation`, `residency.denied` and `export.cursor.advanced`.

Principal role events are tenant-scoped and carry the actor, principal,
revision and complete non-secret role set. Access, refresh, provider, device,
enrollment and bootstrap bearers never enter an event or durable role row.

Agent events are on the workspace stream and every one carries `agent`.
`agent.created` carries `ws`, `owns_ws`, `recipe`, `mode`, `task_hash`,
`policy` and `parent`; `agent.message` carries `message`, `kind`,
`text_hash` and `degraded`; `agent.run.started` carries `run`, `attempt`,
`node` and `transcript`; `agent.run.finished` carries `run`, `stop_reason`,
`error`, `cancelled` and `turns`; `agent.turn` carries `run`, `message`,
`stop_reason` and `tokens`; `agent.cancelled` carries `run`, `dropped` (a
count) and `by`; `agent.forked` carries `from` and `snapshot`;
`agent.child.finished` is on the parent's stream and carries `child`,
`status`, `reason`, `turns` and `message` (the inbox message id);
`approval.pending` carries `run`, `title`, `tool_call`, `tool_kind` and
`options` (a count); `approval.decided` carries `option`, `denied`, `by` and
`run`. No agent or approval event carries a prompt's text, an elicitation's
content, a permission request's detail or a provider key.

`run.started` carries `s`, `recipe`, `task_hash`, `sandbox` and `auth`;
`run.finished` carries `s`, `recipe`, `exit` and `signal`;
`auth.workspace_resident` carries `s` and `recipe`. All three set `session`.
None carries the task text, the harness argv or a provider key.

`queue.created` carries `queue`, `ws` and `items` (a count); `queue.advanced`
carries `queue`, `index`, `exit`, `signal`, `status` and `cursor`. Both are on
the workspace stream and neither carries a task's text.

`pool.created` and `pool.removed` contain only non-secret configuration.
`pool.scaled` carries `pool`, `from`, `to`, `reason` and, for a provider
mutation, the non-secret provider machine id.
`pool.provision_failed` carries a sanitized reason and `retry_at`; it never
carries a credential, enrollment token, provider response body or bootstrap
environment.

`export.cursor.advanced` carries a destination name, previous/next sequence
and revision. It commits with the tenant-scoped cursor row only after the
destination accepts the bounded batch; a retry may duplicate but never skip a
canonical event.

`repo.cloned` carries `repo` (canonical URL), `ref`, `depth`, `commit` and
`backend`; it never carries the binding, its placeholder or the broker URL.
`egress.allowed`/`egress.denied` from the git connector add `connector: git`,
`op: fetch|push` and `repo: owner/name`. A `ws.released` for a failed
materialization carries `retry_after_ms`.

`base.created` and `base.removed` are tenant-scoped rather than
workspace-scoped: `stream` is the base name, `tenant` is set, `workspace` is
empty, and the payload carries `name`, `artifact`, `workspace` (the snapshot's
source, if any) and `bytes`.

`fs.apply_tar` summarizes one overlay: `artifact`, `files`, `dirs`, `bytes`,
and `complete:false` when a refusal partway through left some files written.
Each written path also gets its own `fs.write` carrying `path` and `artifact`.

`ws.acl` records an ACL change with the new lists, the principals it revoked
and the resulting `authz_revision`. `authz.revoked` is the node's record of
acting on a pushed revision: the revision, the principals (or `reset`), and the
sessions it closed.

`cred.used` is the record of what a released credential bought. The node
emits it once per released binding after the upstream outcome is known: the
payload's `status` is the upstream response status when headers arrived, or
`0` with `error` set to a failure class (`dns`, `connection_refused`,
`connection_reset`, `timeout`, `tls`, `canceled`, `eof`, `redirect_rejected`,
`response_limit`, `non_public_address`, `connector`, `upstream`) when they did
not. `error` is a class, never the transport's error text.

`POST /v1/events` appends an event out of band. This is how a webhook wakes a
sleeping workspace. The Remount body is `{type, stream?, payload?, agent?}`;
unknown fields are `400`. The caller authenticates with a bearer the control
plane knows, or with an HMAC-SHA256 of the raw body under the configured
webhook secret in `X-Remount-Signature` or `X-Hub-Signature-256`
(`sha256=<hex>`).

Provider-native requests are selected by their signature headers, or by
`X-Remount-Provider` for the generic adapter. GitHub verifies
`X-Hub-Signature-256` over the raw body; Slack verifies `v0:` plus the request
timestamp and raw body and rejects timestamps outside its replay window;
Linear verifies the hexadecimal `Linear-Signature`; generic verifies its own
bearer. Selection fails closed. Valid requests produce canonical types under
`webhook.github.*`, `webhook.slack.*`, `webhook.linear.*`, or
`webhook.generic.*`; authentication material never enters the event payload.
`agent` maps the event onto an Agent: `{wake, message}` sends `message` to an
existing agent as a follow-up (waking it), where `wake` is an id or
`name:<template>` naming exactly one live agent (none is `not_found`, several
is `conflict`); `{create: AgentCreateReq}` creates one. `message`, `wake`,
`create.spec.task`, `create.name`, `create.workspace.name` and
`idempotency_key` are Go templates over the decoded payload with a missing key an error, so a payload
that changed shape is refused rather than rendered into a half-empty prompt. A
signed request acts as the configured webhook credential; without one it may
only append. The response is `202 {accepted, agent?, ws?, status?}` and the
event lands on the agent's workspace stream when `stream` is empty. Redelivery
with the same rendered `idempotency_key` returns the same agent or message.

An event-driven `Timer` may carry `match: {field: value}`. Every entry must
equal the decoded payload value before the timer fires. Dotted paths select
nested object fields; `repo` aliases `repository.full_name`, and `label`
matches one name in the GitHub issue or pull-request label list. Predicates are
bounded at admission, retained with the timer, tenant checked, and evaluated
only after the event append commits. A missing or malformed payload never
matches.

Outbound notification subscriptions are operator configuration, not protocol
requests. A tenant worker consumes committed event sequences through one
durable cursor and bounded batches. The only filters are `egress.pending`,
`run.finished`, `ws.fenced`, and `pool.*`; the only destinations are Slack
incoming webhooks and an allow-listed generic HTTPS endpoint. Acceptance is
at least once. The cursor advances only after every matching destination has
accepted the batch, or after sanitized dead-letter metadata and the matching
`notifier.dead_lettered` event both commit. Failure to deliver or durably
record fallback retains the cursor and emits `notifier.unavailable`. Generic
payloads contain versioned canonical metadata and sequence numbers but no raw
event payload; receivers deduplicate by subscription and sequence.

Event history is finite. The reference control plane retains it by configured
age and row budgets and records the oldest retained sequence as a durable
watermark. A historical or tail request older than that watermark fails with
`evicted` and `Error.oldest`; it never silently starts at a newer sequence.
Pagination continues until the requested range is exhausted rather than
silently stopping at an implementation page size.

### 11.1 Retention and compliance export

Retention deletes only a contiguous oldest prefix of the sequence, so a range
that is missing events is always reported as an explicit gap and never as a
shorter answer. Per-tenant retention is therefore two mechanisms rather than
one: the prefix is deleted at the oldest instant any tenant still requires,
and between a tenant's own cutoff and that floor the tenant's event payload is
replaced in place with the constant marker
`{"remount_redacted": "tenant_retention"}`. Sequence, time, type and tenant are
never changed, so contiguity holds; the redaction travels into an export as
ordinary content, so a bundle covering a redacted range is explicit about it.

Per-tenant artifact retention is a maximum age, so it may only accelerate
collection relative to the shared grace window and never delay it. It never
selects a referenced object at any age: a live workspace, base, fleet or
session-log closure outranks a retention policy, and an artifact still held by
one is reported as a violation rather than deleted.

A compliance bundle is JSON Lines: one canonical event object per line, then
one line holding `{"remount_audit_manifest": {...}, "signature": "..."}`. The
manifest's `payload_sha256` covers exactly the event bytes preceding that line.
Two exports of one range produce byte-identical event lines and the same hash;
only `created_at` differs. See ADR 0080.

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
11. Unsupported frame versions and hellos without the `v1` baseline fail
    closed; a v1 golden frame re-encodes byte-for-byte.
12. A live snapshot cannot replace authoritative recovery state, while a
    successful quiesced checkpoint can.

## 13. Versioning

`v` is the frame version. Peers negotiate exact, case-sensitive capability
strings in `hello`; this release requires `v1`. New optional operations and
fields are additive within v1, and a peer that does not know an `op` answers
`unsupported`. A change whose omission by an older peer would weaken a
security property is a named capability (§3.1), not an additive field. A
semantic change, removal, or incompatible field change to the baseline
requires a new frame version and an explicit dual-version migration window.
The repository keeps deterministic v1 golden fixtures under
`internal/proto/testdata` (a request frame and a hello offering every named
capability) to catch accidental wire drift.
