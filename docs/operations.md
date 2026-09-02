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
| `--lease` | `30` | claim lease in seconds |

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

Health is at `/healthz` and returns the number of connected peers.

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
| `--image` | `ubuntu:24.04` | default image for docker workspaces |
| `--allow` | none | hosts reachable without a credential, repeatable |
| `--allow-private` | none | hosts allowed to resolve to private addresses, repeatable |

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
| `docker` | container | a long-lived container with the workspace mounted at `/work` | you want a boundary between the agent and the host |

Be honest with yourself about `process`. The agent runs as the same user as the
node with the host's full network and filesystem. The workspace root is only a
convention. That is fine for a laptop you are watching and a fleet box that
exists to run agents, and wrong for a shared machine or a multi-tenant host.
Neither backend enforces egress at the network layer; the broker is the
credential boundary, not a firewall. MicroVM backends are not built yet.

The docker backend checks the daemon lazily and reports `unsupported` if it is
missing. Filesystem operations and snapshots use the host-side mount, so they
are as fast as `process`.

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

The default is deny. A workspace may reach a host only if a binding it holds
names that host, or the node's `--allow` list does. Everything else gets a 403
from the broker and an `egress.denied` event. This is deliberate: an agent that
cannot reach the internet cannot exfiltrate through it, and every exception is
written down.

`--allow` is per node, so a fleet box that installs packages needs the
registries, and a box that only runs a model client does not.

The broker refuses to connect to loopback, private and link-local addresses,
including cloud metadata endpoints, unless the host is listed in
`--allow-private`. That is how a local Ollama on the same machine is reached,
and how everything else on your network stays out of reach.

The broker rewrites credentials only on its reverse-proxy path
`/d/<host>/...`. CONNECT tunnels through `HTTPS_PROXY` are allowed to permitted
hosts but are not inspected, so a placeholder sent through a tunnel is never
substituted. Point harnesses at `${REMOUNT_BROKER}/d/<host>` for anything that
needs a key.

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

Back up `control.db` with a SQLite-aware tool, and `artifacts/` with anything
that copies files. The two are independent: `control.db` names snapshots by
digest, and `artifacts/` holds the bytes.

A snapshot artifact is a gzip tar of the workspace filesystem with sorted
entries, modes, mtimes and symlinks, minus the workspace's `exclude` globs.
It contains no process state and no secrets. Identical trees produce identical
ids, so repeated snapshots of an unchanged workspace cost nothing.

To restore a server, put both back and start it. A recorded holder is preserved
through a recovery grace period rather than immediately re-queued: the same
node may re-adopt it at the same generation and prove readiness. A held
workspace whose lease later expires returns to `pending` from its last durable
snapshot. An interrupted transitional state is made `failed` and remains
operator-visible; Remount does not guess that an ambiguous source is disposable.
Durable fleet operations are loaded from the same database and continue their
pending target reconciliation.

To move a deployment, copy both, start the new server, and point nodes at it.
Node identity is on the node, not the server, so nodes keep their ids.

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

Everything is an event, and the event log is the monitoring surface.

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

## Capacity

| Resource | Per unit | Notes |
|---|---|---|
| session output in memory | 2 MiB per session | ring buffer; oldest chunks spill to disk |
| session output on disk | 128 MiB per session | under the node's `spill/`; rotated when full |
| finished session retention | 24 hours | the exit record and log stay attachable |
| control-plane events | unbounded in SQLite | plan for the log's growth; it is the audit trail |
| node mutation records | 10,000 per node | new mutations fail closed with `resource_exhausted` at the cap |
| fleet acknowledgement deadline | 5 minutes default, 24 hours maximum | pending unreachable targets remain visible and are retried |
| grants | 1 hour | clients refresh them transparently |

Session output that ages out of both tiers is reported to clients as an
explicit gap, never silently dropped.

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

Upgrade one binary at a time. Unknown frame kinds and fields are ignored on
both sides, and the hello negotiates capabilities, so mixed versions talk.

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
volume = modal.Volume.from_name("remount-data", create_if_missing=True)

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
