# Operations: running Remount for real

This is the guide for whoever gets paged. It covers deploying the server,
enrolling nodes, choosing backends, secrets, network policy, leases, backups,
monitoring and recovery. Every value here comes from the source; where a
default matters it is stated with the flag that changes it.

## The server

`remount server` runs the control plane, the relay and the artifact store in one
process on one port.

```sh
remount server --listen 0.0.0.0:7443 --data /var/lib/remount --token "$REMOUNT_TOKEN" \
  --bindings /etc/remount/bindings.json --lease 30
```

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `127.0.0.1:7443` | bind address; also `REMOUNT_LISTEN` |
| `--data` | `./remount-data` | state directory; also `REMOUNT_DATA` |
| `--token` | `$REMOUNT_TOKEN` | shared bearer token, required |
| `--insecure` | off | allow an empty token, for local experiments only |
| `--bindings` | none | JSON file of secrets the nodes may lease |
| `--provisioners` | none | provider registry JSON; credentials are named environment references |
| `--lease` | `30` | claim lease in seconds |
| `--mode` | `standalone` | `standalone`, `production-single-tenant`, or `production-multi-tenant` |
| `--max-concurrent-requests` | `128` | active control request handlers; excess work fails with `resource_exhausted` |
| `--max-tenant-workspaces` / `--max-subject-workspaces` | `1000` / `100` | non-destroyed workspace quotas |
| `--max-mutation-records` | `100000` | retained control idempotency results |
| `--max-timers` / `--max-workspace-timers` | `100000` / `128` | durable timer quotas |
| `--artifact-object-bytes` | `8 GiB` | largest compressed artifact request |
| `--artifact-store-bytes` / `--artifact-store-objects` | `64 GiB` / `100000` | retained blobs plus in-flight reservations |
| `--artifact-grace` / `--artifact-gc-interval` | `24h` / `10m` | reference-aware artifact collection |
| `--event-retention` / `--max-events` | `30d` / `1000000` | event age and row-count bounds |
| `--control-record-retention` | `30d` | replay/tombstone/terminal-record visibility window |

The server refuses to start without a token unless `--insecure` is set. The
token authenticates every node hello, every client hello, every artifact
upload and every webhook.

The data directory contains two things:

| Path | Contents |
|---|---|
| `control.db` | SQLite: the event log, workspaces, fleet operations, timers, nodes, idempotency keys, assignment history, and the grant signing key |
| `artifacts/` | content-addressed snapshot blobs, named by SHA-256 |

The grant signing key is generated on first start and stored in `control.db`.
Nodes learn its public half at hello and verify every client grant with it.
Losing `control.db` means losing that key, so back it up.

### TLS

The server speaks plain HTTP and WebSocket. Put a TLS-terminating reverse proxy
in front of it and give nodes and clients the `https://` URL. The CLI rewrites
`https://` to `wss://` for the link endpoint automatically.

```
client / node  --wss:443-->  nginx / caddy / cloud LB  --ws:7443-->  remount server
```

The proxy must pass WebSocket upgrades on `/v1/link` and must not buffer
`/v1/artifacts/` bodies, which can be gigabytes. Nodes only ever dial out, so a
public address on the server is the only inbound surface in the whole system.

Liveness is at `/healthz`; readiness is at `/readyz`. Both include serving and
security-mode state. Use readiness for traffic and deployment checks.

## Nodes

Enroll a machine with `remount up`. It dials the server, presents its identity
key and capabilities, and waits for work.

```sh
remount up --server https://remount.example --token "$REMOUNT_TOKEN" \
  --label zone=eu-west --label gpu=a100 --backend process,docker \
  --allow registry.npmjs.org --allow pypi.org --allow files.pythonhosted.org
```

| Flag | Default | Meaning |
|---|---|---|
| `--data` | `~/.remount/node` | identity, workspaces, spill, artifact cache; also `REMOUNT_NODE_DATA` |
| `--label k=v` | none | placement labels, repeatable |
| `--backend` | `process` | comma-separated: `process`, `docker` |
| `--image` | `ghcr.io/andrewgcodes/remount-workspace:<version>` | default image for docker workspaces; see `docs/images.md`; `ubuntu:24.04` still works |
| `--allow` | none | hosts reachable without a credential, repeatable |
| `--allow-private` | none | hosts allowed to resolve to private addresses, repeatable |
| `--max-concurrent-requests` | `128` | active node request handlers |
| `--max-sessions` / `--max-active-sessions` | `1024` / `256` | retained and live sessions per node |
| `--max-workspace-sessions` / `--max-principal-sessions` | `64` / `128` | retained session quotas |
| `--session-memory-bytes` / `--session-spill-bytes` | `2 MiB` / `128 MiB` | output retained per session in memory/on disk |
| `--session-chunk-bytes` / `--session-memory-chunks` | `32 KiB` / `16384` | per-chunk and in-memory index bounds |
| `--artifact-store-bytes` / `--artifact-store-objects` | `32 GiB` / `50000` | node snapshot cache capacity |
| `--artifact-retention` / `--artifact-gc-interval` | `24h` / `10m` | unreferenced node-cache collection |
| `--connector-cache-bytes` / `--connector-cache-objects` | `16 GiB` / `100000` | immutable managed-connector cache capacity |
| `--connector-workspace-bytes` / `--connector-workspace-objects` | `2 GiB` / `4096` | per-workspace connector references |
| `--connector-object-bytes` | `512 MiB` | maximum staged connector response |
| `--max-mutation-records` / `--mutation-retention` | `10000` / `30d` | node replay journal capacity/window |
| `--max-concurrent-snapshots` / `--snapshot-min-interval` | `4` / `1s` | snapshot concurrency and caller frequency |

The node data directory holds `identity.json`, which is the node's id and
ed25519 private key. Keep it; a node that loses it enrolls as a new node and the
control plane refuses the old id with a different key. It also holds `ws/` for
workspace roots, `spill/` for session output that overflowed memory, and
`artifacts/` as a local cache of snapshots. `mutations.cbor` is the node's
write-ahead idempotency journal. Back it up with the node data: deleting it can
remove the proof required to finish a prepared fleet destroy safely.

Nodes advertise OS, architecture, CPU count, memory and backends. Workspaces
specify requirements and placement; a node is eligible only when every
requirement and every `--label` in the placement matches.

## Choosing a backend

| Backend | Isolation | What the agent gets | Use when |
|---|---|---|---|
| `process` | none | a directory on the host, processes as the node's user | the machine is yours and you already trust the agent with it |
| `docker` | container, cooperative egress | a long-lived container with the workspace mounted at `/work` | local/single-owner containment where Docker's default boundary is sufficient |

Be honest with yourself about `process`. The agent runs as the same user as the
node with the host's full network and filesystem. The workspace root is only a
convention. That is fine for a laptop you are watching and a fleet box that
exists to run agents, and wrong for a shared machine or a multi-tenant host.
Neither backend enforces egress at the network layer; the broker is the
credential boundary, not a firewall. MicroVM backends are not built yet.

The docker backend checks the daemon lazily and reports `unsupported` if it is
missing. Filesystem operations and snapshots use the host-side mount, so they
are as fast as `process`.

## Snapshot consistency

`remount ws snapshot WS` is a live capture. It may observe concurrent process
writes, reports `consistency=live`, and never updates `last_snapshot` even when
uploaded. Use it for inspection/export where that tradeoff is acceptable.

`remount ws snapshot WS --authoritative` is the failover operation. The node
rejects new managed work, terminates and joins Remount-managed sessions for the
process backend (or pauses a Docker container), exclusively locks filesystem
mutations, uploads the artifact, and asks control to commit the digest for the
same node and generation. A missing artifact store, upload failure, stale
generation, or persistence failure returns an error and leaves the prior
authoritative checkpoint unchanged. Host processes that escaped the local
`process` backend remain outside its documented consistency boundary.

## Bindings

The bindings file is a JSON array. Each binding is a secret, the destinations
it may be sent to, and the placeholder the workspace holds.

```json
[
  {
    "id": "b_github",
    "secret": "$GITHUB_TOKEN",
    "destinations": ["api.github.com", "github.com"],
    "principals": ["a_repo_fixer"],
    "placeholder": "ghp_REMOUNTPLACEHOLDER0000000000000000",
    "ttl_sec": 600
  }
]
```

| Field | Meaning |
|---|---|
| `id` | referenced by workspaces as `ref:<id>` |
| `secret` | the value, or `$NAME` to read it from the server's environment at start |
| `destinations` | host patterns; `*.example.com` matches subdomains only, `:port` restricts the port |
| `principals` | optional; only workspaces acting as one of these may lease it |
| `placeholder` | what the workspace holds; default `ref:<id>`; a shape-preserving value keeps client-side key validation happy |
| `ttl_sec` | lease lifetime handed to nodes; default 600 |

Use `$ENV` references and keep the real values in a secret manager or systemd
credentials. The file then contains nothing worth stealing. Nodes receive
leases with the real value and refuse to substitute after the TTL, then renew
about a minute before expiry.

## Network policy

There are two deliberately distinct modes. An explicit workspace policy is a
first-match list of typed capabilities and defaults to deny. A workspace with
no typed policy uses the legacy local rule: a reverse-proxy request is allowed
when it uses a binding for that destination or matches the node's `--allow`
list. Typed rules replace that legacy authority; they are not merged with it.

For example, this permits two immutable registry reads per workspace
generation and one bounded write to a run-scoped API:

```sh
remount ws create --security local --network-default deny \
  --egress-rule '{"id":"registry-read","connector":"package","protocol":"https","hosts":["registry.example"],"ports":[443],"methods":["GET","HEAD"],"path_prefixes":["/v2"],"max_requests":2,"max_response_bytes":8388608,"shared_state":"immutable_read"}' \
  --egress-rule @scoped-write-rule.json
```

Rules match protocol (`http`, `https`, or opaque `connect`), canonical ASCII
host, effective port, method and path-prefix segment. They may cap request
count, request bytes and response bytes. Counts reset only when the workspace
generation changes. `immutable_read` is restricted to `GET`/`HEAD`;
`scoped_write` requires workspace write authority; `global_write` requires an
administrator. Every decision records the workspace, generation and matching
rule. A redirect is sent back through the broker and checked again.

A `connector:"package"` rule is deliberately narrower than ordinary host
access. Use `${REMOUNT_PACKAGE_CONNECTOR}/registry.example/v2/...`; the same
rule cannot be spent at `${REMOUNT_BROKER}/d/...` or through CONNECT. The
connector accepts only GET/HEAD over HTTPS and blocks bodies, range requests,
WebDAV methods and writable shared-state declarations. For a digest-verified
download, send `X-Remount-Expected-Digest: sha256:<hex>`. The response contains
`X-Remount-Content-Digest`, but never a cache path or hit indicator. Physical
blobs are immutable and content-addressed; cache metadata and permission
to reuse a blob are private to one tenant/workspace scope.

`--allow` is per node and applies only to the legacy local policy. A fleet box
that installs packages may need registry entries; production policy should put
those destinations in explicit workspace rules instead.

The broker refuses to connect to loopback, private and link-local addresses,
including cloud metadata endpoints, unless the host is listed in
`--allow-private`. That is how a local Ollama on the same machine is reached,
and how everything else on your network stays out of reach.

The broker rewrites credentials only on its reverse-proxy path
`/d/<host>/...`. CONNECT tunnels through `HTTPS_PROXY` are not inspected, so a
placeholder sent through a tunnel is never substituted. A binding never grants
CONNECT authority: typed mode needs an explicit `protocol:"connect"` rule and
legacy local mode needs `--allow`. CONNECT rules cannot claim path, byte-limit,
or shared-state enforcement. Suspending or fencing the workspace closes
established tunnels. Point harnesses at `${REMOUNT_BROKER}/d/<host>` for
anything that needs a key.

The broker capability authenticates one workspace and generation, but proxy
environment variables alone cannot stop a hostile process from opening a
direct socket. The built-in `process` and `docker` backends advertise
`cooperative_proxy`, so `isolated` and `multi_tenant` workspaces reject them.
There is currently no built-in production backend. An external backend may
advertise `enforced_gateway` only when its handle implements the network
controller that installs the generation-specific policy before readiness and
revokes it synchronously during fencing, quarantine, and node shutdown.

## Leases

A claim carries a lease. The node renews at one third of the lease interval,
with a floor of 250 milliseconds, and also renews for workspaces it is still
restoring. If a lease expires, the control plane returns the workspace to
pending with its last snapshot, emits `ws.lease_expired`, and another eligible
node claims it at a higher generation.

| `--lease` | Renew interval | Failover after node death | Risk |
|---|---|---|---|
| 5 | ~1.7s | about 5s | a GC pause or a slow disk expires leases spuriously |
| 30 (default) | 10s | about 30s | a good balance |
| 120 | 40s | about 2 minutes | slow failover, very tolerant of blips |

A short uplink blip does not expire anything. The workspace is demoted out of
`claimed` while the node is offline and promoted back when it reconnects and
reports ready, all within the same generation.

## Backup and restore

Back up `control.db` with SQLite's online backup API (for example
`sqlite3 control.db '.backup /backup/control.db'`) or stop the server before a
filesystem copy. Do not copy a live database file by itself while WAL activity
may be in progress. Copy `artifacts/` after the database backup; taking extra
unreferenced blobs is safe, while omitting a blob referenced by the database is
not. Retain both under one backup generation and record their checksums.

A snapshot artifact is a gzip tar of the workspace filesystem with sorted
entries, modes, mtimes and symlinks, minus the workspace's `exclude` globs.
It contains no process state and no secrets. Identical trees produce identical
ids, so repeated snapshots of an unchanged workspace cost nothing.

To restore a server, verify the database with `PRAGMA integrity_check`, verify
the artifact tree with `remount doctor --deep`, put both back, and start exactly
one controller. A recorded holder is preserved
through a recovery grace period rather than immediately re-queued: the same
node may re-adopt it at the same generation and prove readiness. A held
workspace whose lease later expires returns to `pending` from its last durable
snapshot. An interrupted transitional state is made `failed` and remains
operator-visible; Remount does not guess that an ambiguous source is disposable.
Durable fleet operations are loaded from the same database and continue their
pending target reconciliation.

To move a deployment, fence traffic to the old writer, copy both, start the new
server, verify `/readyz`, and only then point nodes at it. Never overlap two
independent writers. Node identity is on the node, not the server, so nodes keep
their ids.

Rehearse this procedure with measured recovery point and recovery time
objectives. A database-only restore can recover metadata but not referenced
workspace bytes; an artifact-only restore cannot recover assignments, signing
keys or policy. Node `identity.json`, workspace directories and
`mutations.cbor` need their own host-level backup when retaining the latest
uncheckpointed copy matters.

## Fleet containment

Use `fleet quarantine` when a principal, run, model, node, backend, tenant, or
label set may be compromised. The command freezes its target list at creation,
persists the operation, and reports each target independently.

```sh
# Stop one suspicious run and wait up to five minutes for acknowledgements.
remount fleet quarantine --run run_20260902 --action stop --idem incident-4821

# Revoke and fence everything currently assigned to one node.
remount fleet quarantine --node n_abc --action revoke_egress --idem node-abc-4821

# Preserve a checkpoint, then authorize deletion of every matching source.
remount fleet quarantine --tenant acme --label campaign=bad \
  --action destroy --deadline 10m --idem incident-4821-destroy --json

remount fleet get fleet_abc --json
remount fleet ls --json
```

At least one selector is required; `--all` is deliberately explicit. Available
selectors are `--tenant`, `--principal`, `--run`, `--node`, `--model`,
`--backend`, repeated `--label k=v`, `--created-after`, and `--created-before`.
Times are RFC 3339. The acknowledgement deadline defaults to five minutes and
may not exceed 24 hours. `--idem` should be a stable incident-specific value;
re-running the same command returns the original operation rather than creating
a second target set.

All actions perform strong containment: revoke broker/network capability,
advance the authoritative generation fence, stop execution, and retain the
local filesystem. `checkpoint` also records a verified snapshot. `destroy`
uses a second phase and deletes source bytes only after the checkpoint and
fence are durably committed in `control.db` and the node matches the exact
phase-one proof in `mutations.cbor`.

Operation state meanings:

| State | Meaning | Operator action |
|---|---|---|
| `pending` / `running` | containment is in progress | wait or inspect per-target state |
| `completed` | every target acknowledged | preserve the operation id with the incident record |
| `partial` | the deadline passed or at least one target failed | inspect `results[].error`; isolate unreachable hosts out of band |

A `partial` operation remains durable and control continues retrying targets
that are merely unreachable. If a checkpoint itself failed, the target is
`failed` and its source stays quarantined; correct the storage/network problem
and submit a new operation with a new idempotency key. A later action can
escalate a completed or partial quarantine, for example from `freeze` to
`destroy`, without losing the original physical generation.

Containment is safe under acknowledgement loss. The node removes a workspace
from service before slow snapshot I/O. Control advances the generation even if
the node cannot be reached, so new grants are invalid; the old node's local
lease deadline independently stops its execution and egress. An unreachable
target remains explicitly `pending` rather than being reported as fenced by
the node.

## Monitoring

Lifecycle resource rows are transactional recovery truth; the ordered event
log is the monitoring and audit surface.

```sh
remount events --follow --json | jq -c 'select(.type|test("lease_expired|egress.denied|node.offline"))'
```

| Event | Meaning | Action |
|---|---|---|
| `node.offline` | a node's uplink dropped; its workspaces are demoted | wait for `node.online`; investigate if it persists |
| `ws.lease_expired` | a node stopped renewing; the workspace was re-queued | check the node; expect `ws.claiming` elsewhere |
| `egress.denied` with `leak_blocked` | a placeholder was sent to a host it is not bound to | treat as an incident inside that workspace |
| `egress.denied` with `denied` | a host outside policy was requested | usually a missing `--allow`; sometimes an agent probing |
| `egress.denied` with `expired` | a lease TTL passed and renewal failed | check the node's link to the server |
| `cred.used` | a secret was substituted for a bound host | the audit trail; count these per principal |
| `ws.restored` | a workspace was materialized from a snapshot | expected after a move, sleep or failover |
| `fleet.quarantine.requested` | a durable selector and action were accepted | record the operation id in the incident |
| `fleet.quarantine.target` | one target changed containment state | inspect `operation_id`, acknowledgement, and error |
| `fleet.quarantine.completed` | the bounded operation result changed to completed or partial | inspect every result; partial is not a clean bill of health |

`remount nodes` shows liveness, labels and how many workspaces each node holds.
`remount ws ls` shows state, node and generation for every workspace. A
workspace in `pending` for more than a few seconds has no eligible node.

## Capacity, quotas, and retention

| Resource | Per unit | Notes |
|---|---|---|
| workspaces | 1,000 per tenant; 100 per subject | counts every non-destroyed workspace; creation fails closed at the limit |
| sessions | 1,024 retained; 256 active per node | additionally 64/workspace and 128/principal by default |
| session output in memory | 2 MiB and 16,384 chunks per session | oldest chunks spill to disk; chunks are at most 32 KiB |
| session output on disk | 128 MiB per session | under the node's `spill/`; oldest chunks are evicted with an explicit gap |
| finished session retention | 24 hours | the exit record and log stay attachable |
| broker client connections | 128 per workspace | listener stops accepting until a socket closes |
| broker concurrent requests/tunnels | 64 per workspace | excess requests receive 429; streams and CONNECT hold a slot |
| control/node requests | 128 active each | overload is rejected on a separately bounded response path |
| snapshots | 4 active per node; 1 second between explicit snapshots/workspace | lifecycle checkpoints wait; user snapshots fail fast |
| server artifacts | 64 GiB / 100,000 objects; 8 GiB each | in-flight reservations count; unreferenced blobs get a 24-hour grace period |
| node artifact cache | 32 GiB / 50,000 objects | reference-aware collection preserves live and quarantined workspace snapshots |
| connector cache | 16 GiB / 100,000 objects | 2 GiB / 4,096 refs per workspace and 512 MiB per response |
| control-plane events | 30 days and 1,000,000 rows | pruning removes an oldest contiguous prefix; old cursors receive `evicted` with the new watermark |
| control mutation records / timers | 100,000 each | 30-day replay/visibility window; timers additionally cap at 128/workspace |
| node mutation records | 10,000 per node | new mutations fail closed with `resource_exhausted` at the cap |
| fleet acknowledgement deadline | 5 minutes default, 24 hours maximum | pending unreachable targets remain visible and are retried |
| grants | 1 hour | clients refresh them transparently |

Session output that ages out of both tiers is reported to clients as an
explicit gap, never silently dropped.

Artifact, event and control-record collectors run in bounded batches every ten
minutes by default. They preserve referenced workspace/fleet authority and
producer high-water marks, update in-memory indexes only after database commit,
and expose runs, errors, objects/bytes or row counts through metrics. Limits are
also returned by `status`, `inspect`, `doctor` and the JSON diagnostic methods.
The managed connector cache is capacity-bounded but currently has no automatic
age-based eviction; size it for the deployment and treat exhaustion as an
operator-visible fail-closed condition.

## Restarts and upgrades

The control plane can be restarted at any time. On start it preserves recorded
holders for a recovery grace period, reloads fleet operations, and waits for
nodes. Nodes reconnect with backoff up to 30 seconds, prove the same assignment
and generation, and re-adopt what is on their disk. If authority disagrees, the
node fences execution and egress and retains the local filesystem for
reconciliation instead of deleting possibly fresher bytes.

A node can be restarted at any time. It re-adopts its local workspaces under
the same generation if the lease has not expired, and under a new generation
if it has. Files survive either way. Running processes do not.

Upgrade one binary at a time. Version 1 requires an exact frame version and the
`v1` hello capability; a peer that cannot negotiate it is rejected
before requests flow. Unknown additive operations return `unsupported`, and
unknown fields remain forward-compatible. Do not assume different semantic
frame versions can communicate merely because CBOR decoding succeeds.

## Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| `ws create` prints "not claimed" | no online node matches requires or placement | `remount nodes`; compare labels, backend, OS, CPU, memory |
| workspace sits in `pending` | same as above, or every eligible node is full of failed claims | `remount events --ws WS` for `ws.claiming` followed by `ws.released` |
| `403` from the broker | destination not in a binding or the node's `--allow` list | the `egress.denied` event names the host and reason |
| `403` with "not bound to" | the placeholder went to the wrong host | that is the leak block working; check what sent it |
| `502` from the broker with "non-public" | destination resolved to a private address | add the host to `--allow-private` if that is intended |
| node logs "uplink lost; reconnecting" forever | wrong `--server` URL or wrong token | `curl $SERVER/healthz`; compare tokens |
| node hello rejected "registered to a different key" | `identity.json` was lost or copied from another node | restore the file, or delete the node from the server's view |
| `ws move` hangs | the target placement has no eligible node | `remount nodes` and the placement flags you passed |
| reads fail with "not on this node" right after a move | client cached a stale grant | the client refreshes on the next call; retry |
| `remount sh` shows no prompt | the program exited or the workspace has no `sh` | `remount exec WS -- ls /bin` |

When in doubt, `remount events --ws WS` tells the whole story of a workspace
in order, with a sequence number you can quote.

---

## Controller availability

The reference implementation is a single SQLite writer. It does not implement
leader election, shared transactional storage, or fencing between controllers.
Putting two server instances behind a load balancer creates split authority
even if they start from copies of the same database.

For active/passive operation, keep the passive stopped, replicate a consistent
database-plus-artifact backup, and use an external lease/fencing mechanism that
proves the former writer is dead before starting the replacement. Then verify
`/readyz`, `remount doctor --deep`, node re-adoption and one real workspace
operation. Automated zero-downtime controller failover remains a published
product gap; scale nodes horizontally, not controllers.

---

## Deploying the control plane on serverless platforms

The control plane is a single stateful process. That constrains how it can be
hosted on anything that scales containers automatically, and getting it wrong
fails in a way that looks like a Remount bug but is not.

We deployed it on Modal and hit both traps.

**Pin it to exactly one container.** A web endpoint that scales out gives you N
independent control planes behind one URL, each with its own SQLite database.
The symptom is that a node enrolls successfully and then does not appear in
`remount nodes`, because the CLI and the node reached different containers.

**Give it a durable volume.** Without one, a recycled container takes the claim
queue and the artifact store with it. Workspaces held by a node in that
container lose their last snapshot.

The working Modal deployment is in [deploy/modal_app.py](../deploy/modal_app.py):

```python
volume = modal.Volume.from_name(APP_NAME + "-data", create_if_missing=True)

@app.function(min_containers=1, max_containers=1, volumes={"/data": volume})
@modal.concurrent(max_inputs=200)
@modal.web_server(7443, startup_timeout=180)
def control():
    ...
```

`max_containers=1` gives one control plane. `modal.concurrent` lets that one
container serve many simultaneous WebSocket connections. The volume makes the
claim queue and artifact store survive a restart.

The same reasoning applies to any platform that scales on request volume. Run
one control plane, give it persistent storage, and scale nodes instead.

The checked-in deployment also requires a named Modal secret containing
`REMOUNT_TOKEN`, pins Node 22 and Python 3.12, checks Node/npm/the Remount binary
before advertising readiness, supervises both child processes, and provides
`make modal-smoke`. The smoke uses `modal.Function.from_name` to resolve the
deployed function explicitly; testing the ephemeral `modal run` copy would be
a false positive. Modal Volumes commit periodically and at container shutdown,
but the single-writer and backup rules above still apply.

### What this deployment proved

A workspace was created on a Modal node running Debian on linux/amd64 under
gVisor, written to, then moved to an Apple Silicon Mac enrolled over the public
internet. A 300 KB random file hashed identically on both sides. Both machines
dialed out only; neither opened an inbound port.

```
$ remount nodes
ID                            ONLINE  OS/ARCH       CPU  MEM_MiB  LABELS
n_06g67p0x3w83z7ghqys952ebbr  true    linux/amd64   17   458958   vendor:modal
n_06g67p19jj05qnj026t5b8z5wm  true    darwin/arm64  18   24576    vendor:laptop

$ remount ws move $WS --node n_06g67p19jj05qnj026t5b8z5wm
moved: node=n_06g67p19jj05qnj026t5b8z5wm gen=2 restored_from=art_sha256:d57b4606dc26b…

$ remount exec $WS -- uname -srm
Darwin 25.3.0 arm64
```
## Provider-backed node pools

`remount server --provisioners /etc/remount/provisioners.json` enables the
drivers explicitly listed in that file. Credentials are never literal JSON
values: each driver names an environment variable such as `token_env`, and
server startup fails if it is absent. The bootstrap section is deployment-wide;
the backend, one-time enrollment token, trusted labels, and fixed node id are
filled per pool machine.

```json
{
  "bootstrap": {
    "server_url": "https://control.example",
    "binary_url": "https://control.example/remount",
    "data_dir": "/var/lib/remount"
  },
  "drivers": [
    {"vendor": "e2b", "token_env": "E2B_API_KEY", "template": "remount-node"}
  ]
}
```

Supported entries are `fly`, `e2b`, `modal`, `ix`, and `ssh`; vendor-specific
fields are validated at startup. Pools require one-time node enrollment, so a
server refuses provisioners unless a node authenticator and enrollment source
are configured. `pool.scaled` and `pool.provision_failed` are the durable
operator record. After a crash, provider inventory is re-read before any new
mutation. A machine is eligible for idle destruction only when its
`remount.node` label exactly matches an online control node reporting zero
workspace assignments.
