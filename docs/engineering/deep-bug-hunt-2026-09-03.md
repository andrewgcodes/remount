# Remount Deep Bug Hunt — Consolidated Findings

**Audit date:** 2026-09-03  
**Audited revision:** `7e61cdeb32c8ad1c44e5dd71c7c2e17cf82b636c`  
**Repository state:** the audited revision matched `origin/main` when the review began  
**Disposition:** audit report only; no production fixes are included

## Executive summary

### Integration disposition — 2026-09-05

This report is historical evidence about `ac83dfa`, not a list of eight
confirmed defects in current main. The integration review checked main
`3620140`; the original observations and severities below are preserved.
This documentation PR does not implement the remaining fixes.

| Finding | Disposition at integration | Current evidence / next proof |
|---|---|---|
| RBH-001 | Original failure fixed separately in PR #24 | `internal/control/agent_report_publish_test.go` proves failed commits leave the report watermark and state unpublished and retryable. |
| RBH-002 | Original reset/lost-outbox mechanism addressed separately | `internal/node/event_loop_test.go:TestProducerSequenceContinuesAcrossRestart` and `internal/sim/node_event_restart_test.go:TestNodeEventsSurviveANodeRestart`; this is not a claim of lossless node delivery under every capacity or crash condition. |
| RBH-003 | Original cross-subject binding flaw fixed separately | `internal/control/auth_test.go:TestAClaimedClientPeerIDCannotBeReboundToAnotherSubject`. |
| RBH-004 | Remains a current code-path concern | `relay.route` still calls destination `Send` synchronously; reproduce blocked-destination isolation on the next fix candidate. |
| RBH-005 | Requires a complete current reproduction | `transport.Peer.nextID` remains per-instance, but reused numbers alone do not prove that an old response can complete a new request. Exercise delayed routing across replacement peers. |
| RBH-006 | Post-open validation gap remains | `FS.Read` requires a regular file before opening, but checks only `IsDir` on the opened handle. A deterministic object-replacement regression is still required. |
| RBH-007 | Original overflow mechanism superseded; terminal-status concern remains | Overflow now drops events and sets internal `Lagged` state instead of closing immediately. Public `TailEvents` still returns only a channel; failed reattachment can close it and CLI follow returns success. Update the reproduction accordingly. |
| RBH-008 | Historical Docker evidence retained; not requalified here | Docker preparation still wraps `docker exec`, while Unix session signals target the host process group. Repeat the child/grandchild experiment through the public API before claiming current container-tree termination. |

See also the [later review's inherited-finding disposition](review-handoff-2026-09-04.md)
and [current implementation status](current-status.md). Neither a historical
pass nor a historical failure automatically qualifies today's candidate.

### Original audit summary

The review retained eight current defects after comparing candidates against the
current tree, the historical 2026-09-02 reviews, the 2026-09-03 implementation
closure, existing tests, and Remount's documented invariants.

| ID | Severity | Finding | Confidence |
|---|---|---|---|
| RBH-001 | High | Failed agent-report persistence advances the in-memory report watermark | Confirmed by focused reproduction |
| RBH-002 | High | Node event producer sequence restarts and its outbox is lost on process restart | Confirmed by code path and restart experiment |
| RBH-003 | High | A client-selected peer ID can be rebound to a different authenticated subject | Confirmed by focused reproduction |
| RBH-004 | High | Relay routing synchronously blocks a source peer on a slow destination | Confirmed by focused reproduction |
| RBH-005 | Medium | Transport request IDs restart for every replacement peer | Confirmed by focused reproduction |
| RBH-006 | Medium | `FS.Read` does not reject every non-regular file after opening it | Confirmed by code path |
| RBH-007 | Medium | Event-tail overflow or reconnect failure is reported as a clean channel close | Confirmed by code path |
| RBH-008 | High | Signaling a Docker-backed session can leave in-container descendants running | Confirmed against local Docker |

No critical issue was retained. The findings are independent: fixing one does
not close the others. The highest-risk cluster is false completion or false
durability at process and persistence boundaries.

## Scope and method

The audit covered:

- control-plane authority, persistence, idempotency, and tenant separation;
- node restart behavior, generation fencing, event delivery, and durable state;
- relay and transport ordering, reconnect behavior, request correlation, and
  backpressure;
- filesystem jail behavior, object-type validation, and blocking special files;
- public SDK stream completion and observable failure semantics;
- process and Docker session cleanup;
- resource bounds, cancellation, race behavior, diagnostics, and deployment
  assumptions;
- historical findings, to avoid reporting already-closed defects as new.

Eight independent audit lanes reviewed the major subsystem boundaries. A final
synthesis pass re-read each cited symbol at the audited revision and challenged
the proposed findings against existing tests, lock boundaries, transaction
boundaries, generation checks, cleanup joins, and documented residual risks.

Focused throwaway regression tests were used to validate several findings.
Those tests and all attempted fixes were removed after reproduction; this
report is the only intended repository change.

## Baseline verification

The following baseline checks passed on the audited revision before the focused
reproduction work:

- `make lint`
- uncached full Go test suite
- `make race`
- `staticcheck ./...`
- `govulncheck ./...`

`govulncheck` reported: `No vulnerabilities found.`

Passing the baseline does not contradict the findings below. Most require a
specific persistence failure, reconnect incarnation, slow destination,
consumer overflow, path-replacement schedule, or Docker process boundary that
the baseline suite does not currently exercise.

## Secret and external-test availability

Stored secret names and categories were inspected without reading, printing, or
persisting secret values.

- No Remount-specific repository secret was visible in the inspected listing.
- Generic provider credentials were available for some external services, but
  no live provider deployment was necessary to establish the retained bugs.
- Remount-related environment names referenced by the repository include
  `REMOUNT_TOKEN`, `REMOUNT_WEBHOOK_SECRET`, `REMOUNT_WEBHOOK_TOKEN`,
  `REMOUNT_INTEGRATION_OPENAI_KEY`, `REMOUNT_INTEGRATION_IMAGE`,
  `MODAL_ENVIRONMENT`, `REMOUNT_MODAL_APP`, `REMOUNT_MODAL_VOLUME`, and
  `REMOUNT_MODAL_SECRET`.
- Local Docker was available at version `27.4.1` and was used only for the
  contained RBH-008 reproduction. The temporary container was removed.

No secret value was exposed, copied into a test, or written into this report.

---

## RBH-001 — Failed agent-report persistence advances the in-memory watermark

**Severity:** High  
**Status:** Confirmed; open at the audited revision  
**Primary location:** `internal/control/agents.go:1330-1474`

### Trigger

1. A node submits a new agent report with sequence `N`.
2. The control plane accepts it as newer than `run.LastReport`.
3. The report mutates the in-memory agent/run state.
4. `persistAgent` or transcript persistence fails.
5. The node retries the same report sequence `N`.

### Mechanism

The duplicate check occurs before mutation:

```go
if rep.Seq <= run.LastReport {
    c.mu.Unlock()
    return nil
}
```

The control plane then advances `run.LastReport` and performs additional
in-memory mutations before the durable operation:

```go
run.LastReport = rep.Seq
...
err = c.persistAgent(a, events...)
```

If persistence fails, the function returns the error without restoring the
pre-report state. A correct retry is therefore treated as an already-applied
duplicate even though the durable transaction never committed.

The problem extends beyond the watermark. Depending on report kind, the
uncommitted memory changes can include run state, inbox removal, turn counts,
failure counts, retry scheduling, delivery marks, approvals, terminal status,
and generated events.

### Impact

- A report can be lost permanently after a transient SQLite failure.
- Memory can claim progress that durable state and the event log do not contain.
- An inbox message can appear consumed in memory while remaining durable.
- Terminal or retry state can diverge until restart, creating inconsistent
  behavior before and after recovery.
- The node's retry protocol cannot repair the failure because the in-memory
  idempotency watermark incorrectly suppresses the retry.

This violates the resource-row/event atomicity and retry-idempotency invariants.

### Reproduction evidence

A focused test temporarily renamed the `agents` table, submitted report
sequence `2`, restored the table, and retried sequence `2`.

Observed pre-fix result:

```text
failed report advanced in-memory watermark ... LastReport:2
```

The first submission returned a persistence error, but the live run had already
advanced to sequence `2`; the retry was then ignored.

### Recommended remediation

Make acceptance of an agent report transactional from the caller's perspective:

1. stage the full post-report state on copies and publish it only after durable
   commit; or
2. snapshot every mutated control-plane structure and restore it on all
   validation and persistence failures.

Staging on copies is safer because nested helpers mutate multiple maps and
objects, and future fields are less likely to be omitted from rollback.

### Required regression coverage

- Force `persistAgent` to fail after a valid new report.
- Assert all in-memory state remains at the pre-report value.
- Restore persistence and retry the same sequence.
- Assert one durable state transition and exactly one corresponding event.
- Repeat for report kinds that mutate inbox, approval, retry, and terminal
  state.

---

## RBH-002 — Node event sequence and outbox are lost on process restart

**Severity:** High  
**Status:** Confirmed; open at the audited revision  
**Primary locations:** `internal/node/node.go:369-378`,
`internal/node/node.go:745-765`, `internal/node/node.go:820-855`

### Trigger

1. A node emits and successfully forwards one or more node-originated events.
2. The node process exits or restarts while retaining its node identity and
   workspace directory.
3. The restarted node emits another event.

The same problem loses unacknowledged events if the process exits after local
append but before the control plane acknowledges the batch.

### Mechanism

Each node constructs its event log as a fresh in-memory store:

```go
events: eventlog.New(eventlog.NewMemory(10000)),
```

`emitSession` relies on that store's local `Event.Seq`; it does not assign an
independently durable `ProducerSeq`. The forwarding loop subscribes from
sequence `1` and retains only the current batch in process memory.

The control plane, by contrast, durably remembers the last accepted producer
sequence for each authenticated node and rejects changed or out-of-order
duplicates. After restart, the node's first local event is sequence `1` again
while the control plane may already have accepted sequence `1` or higher.

### Impact

- Post-restart node events can be rejected as changed duplicates or otherwise
  fail to advance the producer stream.
- Events appended immediately before a crash are lost with the in-memory
  outbox.
- Filesystem, session, broker, and other node-originated audit history can
  become incomplete.
- The documented producer-gap mechanism cannot accurately represent the loss
  when the producer itself forgets its prior sequence and pending records.
- Operational investigations can observe resource state without the events
  that explain how it changed.

This violates the durable, ordered, reconstructible event-stream invariant.

### Reproduction evidence

A restart-focused simulation extended the existing local-workspace adoption
scenario with writes before and after node restart, then inspected control-plane
events. The experiment did not observe the expected two `fs.write` events.

The exact failure is also established structurally: the node retains the same
ID and data directory while its event store and sequence are recreated from
memory at every `Node` construction.

### Recommended remediation

- Back the node event outbox with the existing SQLite event-log implementation
  under the node data directory.
- Preserve monotonic producer sequence across process restarts.
- Retain unacknowledged events until the control plane confirms them.
- Bound retained rows and, if local eviction is unavoidable, resubscribe from
  the oldest retained sequence so the control plane receives an explicit
  producer gap.
- Close the store only after event-producing workers and session cleanup have
  joined.

### Required regression coverage

- Emit event `N`, restart the same node from the same data directory, emit
  event `N+1`, and assert both appear once in order.
- Restart after local append but before acknowledgement and assert replay.
- Prune a bounded local outbox and assert an explicit producer-gap event rather
  than silent loss.
- Run the restart scenarios under `-race`.

---

## RBH-003 — Client-selected peer IDs can move between authenticated subjects

**Severity:** High  
**Status:** Confirmed; open at the audited revision  
**Primary location:** `internal/control/control.go:1324-1350`

### Trigger

1. Subject Alice authenticates as client peer `c_shared`.
2. While that mapping remains live, subject Bob authenticates and requests the
   same client peer ID.

### Mechanism

For client hellos, the control plane accepts any `c_`-prefixed peer ID supplied
by the caller and unconditionally overwrites the subject map:

```go
id := h.Peer
if id == "" || !strings.HasPrefix(id, "c_") {
    id = ids.New("c")
}
...
c.subjects[id] = subject
```

There is no check that an existing live mapping belongs to the same subject and
tenant. Authentication validates the token, but it does not bind the requested
logical peer identity to the resulting principal.

### Impact

- A second authenticated user can replace the authorization identity associated
  with another live client peer ID.
- In-flight or subsequently routed operations attributed by peer ID can be
  resolved under the wrong subject.
- This creates cross-subject and potentially cross-tenant identity confusion.
- Audit attribution and authorization decisions can disagree with the
  connection that originally established the peer ID.

This is not an unauthenticated bypass; the attacker needs valid credentials for
some subject. The defect is failure to preserve identity ownership between
authenticated subjects.

### Reproduction evidence

A focused test authenticated `c_shared` as Alice, then authenticated the same
ID as Bob.

Observed pre-fix result:

```text
different subject reclaimed c_shared: <nil>
```

Reconnection by Alice should remain valid; simultaneous rebinding to Bob should
be rejected.

### Recommended remediation

While holding `control.mu`, treat an existing live client-peer mapping as an
identity binding:

- allow the same subject and tenant to reconnect;
- reject a different subject or tenant;
- release the binding only through the authoritative peer-disconnect path.

Relay replacement and control-plane `PeerGone` ordering must be tested together
so an old connection's teardown cannot delete a newer valid mapping.

### Required regression coverage

- Same peer ID and same subject reconnects successfully.
- Same peer ID and different subject is rejected.
- Same subject ID in a different tenant is rejected.
- After authoritative disconnect cleanup, a new binding follows the explicitly
  chosen peer-ID reuse policy.
- Race reconnect and teardown of two connections carrying the same ID.

---

## RBH-004 — Slow destinations block relay routing on the source read loop

**Severity:** High  
**Status:** Confirmed; open at the audited revision  
**Primary location:** `internal/relay/relay.go:250-294`

### Trigger

1. A source peer sends a frame through the relay to a connected destination.
2. The destination's socket or transport stops accepting writes.
3. The source sends additional traffic, including traffic for other
   destinations or the control plane.

### Mechanism

`route` looks up the destination and calls `dst.Send` synchronously:

```go
if err := dst.Send(ctx, f); err != nil && f.T == proto.KindReq {
    ...
}
```

`route` is invoked by a peer's read-loop handler. Until `dst.Send` succeeds,
times out, or fails, that source read loop cannot consume or dispatch another
frame.

The coupling is source-to-destination: one backpressured destination can stall
all frames arriving from the same source connection, even when later frames
target healthy peers.

### Impact

- A slow client can stall a node's multiplexed output and event traffic.
- A slow node can stall a client's unrelated operations.
- Heartbeats, request responses, session chunks, and control-plane traffic can
  be delayed behind an unrelated destination write.
- An authenticated peer can create a cross-session denial of service without
  exhausting an explicit admission limit.
- Backpressure is neither isolated nor surfaced through an immediate bounded
  rejection.

### Reproduction evidence

A focused relay test installed a destination connection whose `Send` blocked
and called `route` from a goroutine.

Observed pre-fix result:

```text
routing blocked the source read loop on a slow destination
```

The routing call did not return within the test's 100 ms observation window.

### Recommended remediation

Introduce bounded, per-destination delivery queues with one ordered writer per
active destination incarnation.

The design must:

- preserve frame order for one destination;
- prevent a blocked writer from blocking source read loops;
- bound queue depth and worker count;
- return an observable resource-exhausted/unreachable response for requests
  that cannot be queued;
- define explicit drop or replay behavior for chunks and events;
- stop the old destination worker on reconnect without dropping frames into
  the replacement incarnation;
- join workers during relay shutdown.

### Required regression coverage

- A blocked destination does not block the source handler.
- Traffic from the same source to a healthy destination continues.
- Queue overflow is bounded and observable.
- Per-destination order remains exact.
- Reconnection does not leak workers or deliver old-incarnation frames to the
  replacement connection.
- Relay close joins all workers under `-race`.

---

## RBH-005 — Request IDs restart for every replacement transport peer

**Severity:** Medium  
**Status:** Confirmed; open at the audited revision  
**Primary location:** `internal/transport/peer.go:28-65`,
`internal/transport/peer.go:168-205`

### Trigger

1. A logical peer connection sends request ID `N`.
2. The connection is replaced or reconnects under the same logical peer ID.
3. The new `transport.Peer` sends the same operation to the same destination.
4. A delayed response to the prior incarnation is routed to the replacement
   logical peer.

### Mechanism

Every `Peer` owns a zero-valued `atomic.Uint64`:

```go
nextID atomic.Uint64
```

New peers do not seed it. The first `Request`, `RequestFrame`, and `Ping` on
every connection therefore reuse low IDs beginning at `1`.

Response correlation checks ID, expected sender, and operation. That narrows
the collision, but it does not distinguish connection incarnations. A delayed
old response with the same sender and operation can satisfy a new pending
request after logical peer replacement.

### Impact

- A new request can receive a stale response from a prior connection
  incarnation.
- Callers can observe false success, the wrong response body, or the wrong
  error.
- The risk is greatest during reconnect races when the remote side already
  accepted the old request but its response is delayed through relay routing.
- Idempotency keys protect some mutations from duplicate execution but do not
  make an incorrectly correlated response safe.

### Reproduction evidence

A focused transport test created two fresh peers and captured their first
outgoing request frames.

Observed pre-fix result:

```text
replacement peers reused request id 1
```

### Recommended remediation

Make request IDs unique across peer incarnations for the lifetime in which a
delayed frame can remain routable. Suitable approaches include:

- a process-wide monotonic request-ID allocator; or
- a unique incarnation prefix/high-order range plus a per-peer counter.

Wraparound behavior must be explicit; IDs must never become zero when zero has
special assignment semantics.

### Required regression coverage

- Fresh peers receive disjoint request-ID ranges.
- A delayed response from incarnation A cannot complete a request on
  incarnation B.
- The test covers `Request`, assigned `RequestFrame`, and `Ping`.
- High-concurrency allocation remains collision-free under `-race`.

---

## RBH-006 — `FS.Read` incompletely validates the opened object type

**Severity:** Medium  
**Status:** Confirmed; open at the audited revision  
**Primary locations:** `internal/fsops/fsops.go:113-152`,
`internal/fsops/open_unix.go:11-13`

### Trigger

1. `FS.Read` performs `Lstat` and confirms that the path currently names a
   regular file.
2. Another actor replaces that path with a non-regular object before
   `openRead`.
3. The opened object is not a directory, for example a FIFO.

### Mechanism

The pre-open check correctly requires a regular file:

```go
if st, err := f.handle.Lstat(name); err != nil {
    ...
} else if !st.Mode().IsRegular() {
    ...
}
```

After opening, the code calls `fh.Stat` but rejects only directories:

```go
if st.IsDir() {
    return nil, proto.Err(proto.CodeBadRequest, "is a directory")
}
```

On Unix, `openRead` uses `O_NONBLOCK`. That reduces some FIFO-open blocking
cases, but the subsequent `io.ReadFull` can still operate on a special object,
and the post-open check does not enforce the same regular-file contract as the
pre-open check.

The opened handle is the authoritative object. The earlier pathname metadata
cannot establish its type after a replacement race.

### Impact

- A path replacement can make a nominal file read operate on a FIFO or another
  special object.
- Reads can block, return misleading empty/EOF results, or surface unstable
  platform-specific errors.
- If called while higher-level workspace serialization is held, the operation
  can delay unrelated filesystem or lifecycle work.
- The public contract says non-regular files are rejected, but the final object
  is not checked against that contract.

### Recommended remediation

After `fh.Stat`, require `st.Mode().IsRegular()` and return a typed
`bad_request` error for every other object type. Keep the pre-open check as an
early rejection, but treat the post-open check as authoritative.

### Required regression coverage

- Deterministically pass an opened FIFO through the post-open read path and
  assert `bad_request`.
- Cover sockets or device-like objects where safely available.
- Retain symlink and traversal tests to ensure the object-type fix does not
  weaken the jail.
- Run on Unix and retain a separate Windows expectation where object types
  differ.

---

## RBH-007 — Event-tail failure is indistinguishable from clean completion

**Severity:** Medium  
**Status:** Confirmed; open at the audited revision  
**Primary locations:** `internal/client/client.go:593-701`,
`cmd/remount/cmd_events.go:92-99`

### Trigger

Either of these conditions is sufficient:

1. incoming event delivery fills the subscription's 512-entry internal queue;
   or
2. reconnect reattachment fails while the client is still on the same
   connection generation.

### Mechanism

On local queue overflow, `enqueue` closes the subscription:

```go
default:
    s.close()
    go s.c.stopEventSubscription(s)
```

On reattach failure, `reattach` also closes it:

```go
if err != nil && s.c.generation() == generation {
    s.close()
}
```

The public API returns only `<-chan proto.Event`; it has no terminal error
channel or status result. `run` closes the output channel for overflow,
reattach failure, caller cancellation, and normal stop alike.

The CLI follows the channel and returns success when it closes:

```go
for e := range ch {
    print(e)
}
return nil
```

### Impact

- Consumers can treat a truncated audit stream as complete.
- `remount events --follow` can exit with status zero after local overload or a
  reconnect failure.
- Automation may checkpoint the last seen event and miss later records without
  an error or explicit gap.
- This contradicts Remount's invariant that incomplete output must never be
  reported as complete.

Server-side replayability does not solve the API problem: the consumer is not
told that it must replay.

### Recommended remediation

Expose terminal status explicitly. Options include:

- a subscription object with `Events()` and `Err()`/`Wait()` methods;
- paired event and terminal-error channels; or
- a typed terminal/gap record if the public event model intentionally carries
  transport state.

Caller cancellation should remain distinguishable from overflow, eviction,
authentication failure, and reconnect exhaustion. The CLI must return nonzero
for unexpected termination and include the last safe cursor.

### Required regression coverage

- Overflow the internal queue and assert a typed terminal error.
- Force reattach failure and assert the failure is observable.
- Cancel the caller context and assert cancellation remains identifiable.
- Verify a consumer can resume from the last delivered cursor without
  duplicate or silent loss.
- Compile and test the external public SDK module with `make public-api`.

---

## RBH-008 — Docker session signals can leave container descendants running

**Severity:** High  
**Status:** Confirmed against Docker 27.4.1; open at the audited revision  
**Primary locations:** `internal/session/process_unix.go:12-29`,
`internal/workspace/workspace.go:570-582`

### Trigger

1. A Docker-backed session starts a program through `docker exec`.
2. That program starts a descendant process inside the container.
3. The client calls the session signal or kill path.

### Mechanism

On Unix, Remount creates a host process group and signals that group:

```go
return syscall.Kill(-cmd.Process.Pid, sig)
```

For a Docker workspace, the host process is the `docker exec` CLI:

```go
spec.Program = append([]string{h.bin}, args...)
```

The host process group contains the CLI-side process, not the container's PID
namespace. Terminating the `docker exec` process does not reliably signal or
reap the command's descendants inside the already-running container.

The broader workspace pause/destroy paths operate on the container itself and
are not the same path. The defect concerns direct session signal/kill semantics
and the accounting that follows session exit.

### Impact

- A session can report exit while its in-container child continues running.
- Session capacity and active accounting can be released before the workload
  actually stops.
- The descendant can continue consuming CPU, memory, filesystem, and broker
  access associated with the workspace.
- Subsequent sessions can observe side effects from a process the caller
  believed was killed.
- Direct signal semantics differ materially between process and Docker
  backends without an explicit capability or error.

### Reproduction evidence

The audit started an isolated Alpine container, launched `sleep` as a child of a
`docker exec` shell, terminated the host `docker exec` process group, and then
checked the child PID inside the container.

Observed result:

```text
CONFIRMED: container child survived host docker-exec process-group termination
```

The temporary container was forcibly removed after the check.

### Recommended remediation

Use container-aware signaling for Docker sessions. The implementation needs a
stable way to identify the in-container process or process group created for a
session, then signal it through the container runtime or a small in-container
supervisor.

Until that exists, the Docker backend should not claim process-tree signal
semantics equivalent to the process backend. If only container-wide kill is
safe, expose that stronger scope explicitly rather than reporting a
session-local kill.

### Required regression coverage

- Start a Docker session with a child and grandchild.
- Signal and kill the session through the public API.
- Assert every in-container descendant exits before the exit chunk is
  observable and before capacity is released.
- Assert one session's signal does not kill unrelated sessions in the same
  workspace unless the API explicitly documents container-wide scope.
- Verify TERM, KILL, INT, and cancellation paths.

---

## Historical findings not reopened

The audit reviewed the historical code audit, adversarial review, hardening
request, closure ledger, and the merged hardening work represented by PR #6.
Issues were not carried into this report merely because an older document
listed them.

Examples treated as historical unless a current regression was independently
demonstrated include:

- per-peer lifecycle ordering;
- bounded harness waits;
- policy retry backoff;
- cancellation during recipe installation;
- Agent JSON cookie exposure;
- HTTP 429 retry behavior;
- previously closed lease, live-pointer, output-gap, broker, and lifecycle
  findings enumerated in the implementation closure.

The eight retained findings above cite current code paths and current
reproduction or proof. This report does not change the status of documented
architectural or external gates such as single-controller SQLite, production
backend qualification, portability limits, tracing/performance evidence, and
independent release review.

## Prioritized remediation order

This audit does not implement fixes. If remediation is scheduled, the suggested
order is:

1. **RBH-003** — close the cross-subject peer-identity binding flaw.
2. **RBH-001** — restore durable/in-memory atomicity for agent reports.
3. **RBH-002** — make the node event producer and outbox restart-durable.
4. **RBH-008** — prevent false Docker session termination.
5. **RBH-004** — isolate relay backpressure with bounded delivery.
6. **RBH-005** — separate request-ID incarnations before expanding reconnect
   behavior.
7. **RBH-007** — make event-tail truncation explicitly fail.
8. **RBH-006** — enforce post-open regular-file validation.

RBH-004 and RBH-005 should be designed together because queueing can extend the
lifetime of delayed frames and make incarnation-safe correlation more
important. RBH-001 and RBH-002 should be reviewed under the same durable
commit/replay model even though they affect different stores.

## Verification expectations for future fixes

Each fix should add the focused regression described in its section. The final
remediation series should also run, as applicable:

```text
make lint
make test
make race
make conformance
make fuzz FUZZTIME=5s
go mod verify
go mod tidy -diff
staticcheck ./...
govulncheck ./...
make dist
make public-api
```

Docker-specific verification must run against an actual Docker backend and
prove descendant cleanup, not merely host CLI exit. Restart/event fixes must
use the simulation layer or an equivalent multi-component test and assert
ordered durable events after a real reconstructed node instance.

## Final disposition

At revision `7e61cdeb32c8ad1c44e5dd71c7c2e17cf82b636c`, all eight findings remain
unfixed in production code. Four were reproduced with focused Go tests, one was
reproduced directly against Docker, and the remaining three are established by
current control-flow and state-lifetime analysis with concrete regression
strategies.

The repository's existing broad test, race, static-analysis, and vulnerability
baselines were green. The missing coverage is concentrated at adversarial
failure boundaries rather than ordinary success paths.
