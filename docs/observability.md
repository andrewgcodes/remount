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

The first three mean something was lost or attacked. The last three are normal
in small numbers and mean something is wrong when they climb.
