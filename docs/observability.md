# Seeing what is happening

Three depths, one command each, all with `--json` so an agent can parse them
instead of reading a table.

| Depth | Command | Answers |
|---|---|---|
| Fleet | `remount status` | Is anything wrong right now? |
| One workspace | `remount inspect WS` | Where is it, what is it running, how big is it on disk? |
| Damage | `remount doctor [--deep]` | Has anything been lost, corrupted, or disagreed about? |
| Raw | `remount metrics [--node ID]` | The counters themselves |

## The fleet

```
$ remount status
control   up 4s   events 13   lease 30s   bindings 1
fleet     1/1 nodes online   1 workspaces   0 timers pending
state     claimed 1
egress    1 substituted   0 allowed   1 denied   1 leaks blocked
sessions  3 opened   3 exited   7 chunks   654B streamed   0 gaps
moves     1 claims   0 lease expiries   0 moves   0 snapshots   0 restores

NODE                          ONLINE  OS/ARCH       CPU  MEM      LABELS                WS
n_06g680pppj1hntkhc4r6rpx504  true    darwin/arm64  18   24.0GiB  map[standalone:true]  1

 WARN  egress.leak_attempts: 1 credential placeholders were sent to unbound hosts
        `remount events | grep leak_blocked` shows which workspace and which host
```

`--watch 5s` repeats it. Findings appear without being asked for, because a
number you have to interpret is worse than a sentence that tells you.

## One workspace, down to the disk

`remount inspect WS` asks the control plane where the workspace is, then asks
that node what it actually has. Both answers appear, and disagreement between
them is reported as an error rather than silently reconciled.

```
$ remount inspect ws_06g680q56h0fbn0y33h37nx4cc
workspace ws_06g680q56h0fbn0y33h37nx4cc  "obs-demo"
  state       claimed (generation 2)
  node        n_06g680pppj1hntkhc4r6rpx504
  lease       26s remaining
  bindings    b_openai

on node n_06g680pppj1hntkhc4r6rpx504 (darwin/arm64)
  backend     process
  root        /.../data/node/ws/ws_06g680q56h0fbn0y33h37nx4cc
  disk        9.0KiB in 3 files   node free 727.6GiB
  broker      http://127.0.0.1:56437

  sessions
  ID                            KIND  STATE   SEQ  OLDEST  PROGRAM
  s_06g680twh0gqddyem6pc7z60j4  exec  exit 0  2    0       sh -c echo hi > a.txt
```

`SEQ` is the next output sequence the session will emit. `OLDEST` is the
earliest one still replayable. When `OLDEST` is above zero, a client
reconnecting from further back gets a gap chunk, and `inspect` says so.

## Finding damage

`remount doctor` checks reachability, the database's own integrity, agreement
between the control plane and every node, placeability, and the counters that
indicate loss. `--deep` additionally re-hashes every artifact, which reads
every byte.

```
$ remount doctor --deep
checked: control.reachable, control.db_integrity, node.consistency,
         workspace.placeable, artifact.deep_verify, data.loss_signals

ERROR  artifact.digest art_sha256:0d887e29…: artifact: digest mismatch
 INFO  artifact.verified: re-hashed 1 artifacts, 1 damaged
 INFO  artifact.verified: node re-hashed 1 cached artifacts, 0 damaged

PROBLEMS FOUND
```

That output is from a real test: a byte was flipped in a stored snapshot. The
control plane's copy was damaged while the node's cache was still intact, and
the tool says exactly that. It exits non-zero, so it works as a health check.

**A check that cannot run is never reported as a pass.** If a node refuses the
deep call, doctor emits `node.diag_unavailable` and says its workspaces were
not checked. An earlier version skipped quietly and printed "healthy", which
is the most dangerous output a health tool can produce.

## Two scripts for machine readers

`scripts/collect.sh` runs every inspection command and emits one JSON document.
`scripts/explain.py` turns that document, or a raw event stream, into prose
that leads with the verdict.

```sh
scripts/collect.sh --deep > snapshot.json
scripts/explain.py snapshot.json
```

```
VERDICT: problems found. 2 error, 1 warning.
Fleet: 1 of 1 nodes online, 1 workspaces.
States: claimed 1
  node n_06g681e9xd46…: darwin/arm64 18 cpu, holding 1 [standalone=true]
Signals worth reading:
  1 credential placeholders sent to unbound hosts
  1 artifacts that failed digest verification
```

Event streams get their own summary, which is the fastest way to answer "what
did this agent try to do":

```sh
remount events --json | scripts/explain.py --events
```

```
14 events across 2 streams.
Most common: s.opened 3, s.exited 3, node.enrolled 1, ws.created 1

Egress denials: 1, of which 1 were leak attempts.
  ws_06g681esgk… -> evil.example.com GET /steal  (placeholder for b_openai sent to evil.example.com)

Credentials substituted: api.openai.com x1
```

`explain.py` exits 1 when the snapshot reports problems and 2 when the
collection itself was incomplete, so a partial answer is never mistaken for a
healthy one.

## Counters

Every counter is exported at `/metrics` in Prometheus text format, and through
`remount metrics`. The ones that indicate loss rather than throughput:

| Metric | Means |
|---|---|
| `remount_egress_leak_blocked_total` | a placeholder was sent to a host it is not bound to |
| `remount_session_gaps_total` | output a client asked for was already evicted |
| `remount_artifact_digest_mismatch_total` | a snapshot was corrupted |
| `remount_workspace_lease_expired_total` | a node stopped renewing and its work was re-queued |
| `remount_session_inputs_deduped_total` | a duplicate input arrived after a reconnect |
| `remount_frames_dropped_total` | a frame was addressed to a peer that had gone |
| `remount_connector_package_upstream_requests_total` | managed package reads that reached an approved registry |
| `remount_connector_package_cache_hits_total` | reads served from that workspace's own immutable reference |
| `remount_connector_package_response_bytes_total` | registry bytes staged and hashed before release |
| `remount_connector_package_failures_total` | connector execution, integrity, or policy failures |
| `remount_connector_package_quota_rejections_total` | package staging rejected before disk limits could be exceeded |
| `remount_connector_package_cache_integrity_failures_total` | a workspace cache reference failed digest verification and was withdrawn before any byte was released |
| `remount_connector_package_staging_reclaimed_total` | abandoned package staging files removed when the connector store restarted |
| `remount_workspace_quota_rejections_total` | workspace creation rejected at tenant/subject capacity |
| `remount_session_quota_rejections_total` | session open rejected at node/workspace/principal capacity |
| `remount_control_requests_rejected_total` / `remount_node_requests_rejected_total` | bounded request admission rejected overload |
| `remount_snapshot_quota_rejections_total` | explicit snapshot rejected by concurrency/frequency admission |
| `remount_artifact_quota_rejections_total` | blob staging rejected before byte/object limits could be exceeded |
| `remount_mutation_quota_rejections_total` / `remount_timer_quota_rejections_total` | durable control records reached their fail-closed cap |
| `remount_events_pruned_total` | canonical event rows removed from the oldest retained prefix |
| `remount_artifact_gc_runs_total` / `remount_artifact_gc_errors_total` | reference-aware collector health |
| `remount_artifact_gc_objects_total` / `remount_artifact_gc_bytes_total` | unreferenced snapshot cache reclaimed |
| `remount_event_gc_runs_total` / `remount_event_gc_errors_total` | age/row event-retention health |
| `remount_control_record_gc_runs_total` / `remount_control_record_gc_errors_total` | timer/idempotency/tombstone/fleet metadata retention health |
| `remount_workspace_tombstones_pruned_total` | old destroyed resource rows removed |
| `remount_fleet_operations_pruned_total` / `remount_assignment_records_pruned_total` | terminal incident metadata and superseded assignment history removed |

The leak, gap and digest-mismatch counters mean something was attacked, lost or
corrupted. Throughput counters are normal; rejection and GC-error counters
require capacity or reliability investigation when they climb.

Diagnostics expose the denominators as well as usage. Control reports event
oldest/latest sequence, event row limit, workspace quotas, timers and mutation
records, request concurrency, artifact objects/bytes and active reservations.
Nodes report retained/active sessions and all session limits, request/snapshot
admission, artifact capacity, connector byte/object limits, and durable mutation
records. Alert on sustained usage near a maximum and on any GC error; a quota
rejection is a controlled failure, not permission to silently evict authority.

Event retention never punches a hole in the middle of history. A reader below
the retained prefix receives the stable `evicted` error and current oldest
sequence, and node producer high-water marks survive pruning so retries do not
manufacture duplicate events or gaps.

## Traces

Every control-plane and node request can be recorded as a span and shipped to a
collector you run. Tracing is off until you name one:

```sh
remount server     --otlp-endpoint http://collector.internal:4318
remount up         --otlp-endpoint http://collector.internal:4318
remount standalone --otlp-endpoint http://collector.internal:4318
```

`REMOUNT_OTLP_ENDPOINT` sets the same thing. There is no Remount-operated
collector and no default endpoint: with the flag unset nothing is recorded and
nothing leaves the process. A URL with no path gets `/v1/traces` appended, which
is the OTLP/HTTP path for the trace signal; give a full path to override it.

The exporter speaks OTLP/HTTP with a JSON payload and no OpenTelemetry
dependency — the same reason `internal/metrics` renders Prometheus text by
hand. Any collector that accepts OTLP/HTTP JSON works; the OpenTelemetry
Collector's `otlp` receiver does, on port 4318.

One span is recorded per dispatched request, named by its `op`. Control stamps
the trace context onto requests it sends to nodes, so a node span appears as a
child of the control span that caused it. Attributes are identifiers only:

| Attribute | Means |
|---|---|
| `remount.peer` | the connected peer that sent the request |
| `remount.principal` | the principal the control plane resolved for that peer |
| `remount.ws` | the workspace the request names |
| `remount.session` | the session the request names, on node spans |
| `remount.code` / `remount.reason` | the stable error classification when the request failed |

No token, secret, request body or free text is recorded, and every attribute is
scrubbed through the same credential-shape redaction the broker and the agent
transcript use before it is buffered. Structured logs pass through that
redaction too, so a credential-shaped value that reached a log argument by
accident does not reach the log.

Two counters make the exporter itself observable:

| Metric | Means |
|---|---|
| `remount_trace_spans_exported_total` | spans the collector accepted |
| `remount_trace_export_failures_total` | spans that never arrived — dropped at a full queue, or lost to a failed batch |

Telemetry never applies back pressure to a request: a full export queue drops
the span and counts it here rather than stalling the operation that produced
it. A rising failure counter means the collector is unreachable or refusing
batches, not that the deployment is unhealthy.
