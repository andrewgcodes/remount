# Mistakes

Everything that broke while building Remount, in the order it was found, with
the actual cause and the actual fix. This is the most useful document in the
repository. The design documents say what the system is; this one says how it
got that way.

Each entry: symptom, cause, fix, lesson, and how it was found where that
matters.

---

## 1. Every client-to-node request timed out

**Symptom.** Workspace creation worked. Reading a file from that workspace hung
for the full ten seconds and failed. Client-to-control traffic was fine;
client-to-node traffic never got a response, not even an error.

**Cause.** The relay wraps every connection in a `transport.Peer`. The peer's
read loop matched every `res` frame against its own pending-request map and
silently dropped any that did not match, on the theory that an unknown id was a
late reply to a timed-out request. A response from a node to a client passes
through the relay as a `res` with the client's id, which the relay-side peer
never issued. It was dropped before routing ever saw it.

**Fix.** `internal/transport/peer.go`. A `res` or `pong` that matches a pending
request is delivered to it; one that does not is handed to the handler. The
relay's handler routes it by destination. An endpoint's handler ignores it.

**Lesson.** A component that sits in the middle must not apply endpoint
semantics to what it forwards. The bug was invisible because the frame was
consumed by code that was correct for the other role.

**How found.** Instrumenting `relay.route` showed the request going out and no
response ever coming back in.

## 2. Sessions hung forever

**Symptom.** After the previous fix, `Exec` returned a session, and ranging over
its chunks never finished. The process had exited on the node.

**Cause.** The node subscribes the client to the session's log before it sends
the `s.open` response, so seq 0 can arrive at the client before the client knows
the session id. The client looked the id up, found nothing, and dropped it.
Every later chunk was held waiting for seq 0, which would never come.

**Fix.** `internal/client/client.go`. Chunks for an unknown session id go into a
bounded orphan buffer. When the open response arrives, `register` drains the
buffer into the session in seq order.

**Lesson.** When a stream and its metadata arrive on the same connection from
different senders, assume the stream can win the race.

**How found.** The test timed out with a goroutine parked on channel receive
inside `range s.Chunks()`.

## 3. The client never reconnected when idle

**Symptom.** Cut the client's connection mid-stream and the stream stopped for
good. Nothing errored.

**Cause.** Reconnection lived inside `call`, so it only happened when the caller
made a request. A caller that was only ranging over chunks made no requests, so
it sat forever on a channel that would never be written again.

**Fix.** `internal/client/client.go`. Every successful `Connect` starts a
`supervise` goroutine that waits for the connection to die and, if the client
is open and has live sessions, reconnects. Reconnect re-attaches every session
from its last delivered seq. Dials are serialized by a separate mutex so two
callers cannot race to open two connections.

**Lesson.** Reconnect is a property of the connection, not of the next request.

## 4. Node restart destroyed the local workspace

**Symptom.** Restart a node process. The workspace it held was still marked
claimed by that node id. The restarted node tried to claim, got a conflict, and
the adopt path deleted the directory. Files written before the restart were
gone.

**Cause.** The claim path treated "already claimed" as "someone else has it" and
disposed of the local copy. It did not consider "I already have it".

**Fix.** `internal/control/control.go`. If a node claims a workspace that is
already held by that same node, the control plane returns it at the same
generation. Outstanding client grants stay valid. The node adopts its local
directory rather than restoring.

**Lesson.** Identity persists across process restarts. A claim from the holder
is not a conflict.

## 5. "Workspace is not on this node" right after it became claimed

**Symptom.** `WaitClaimed` returned, the next `ReadFile` failed with
`not_found: workspace ws_… is not on this node`.

**Cause.** The control plane marked a workspace `claimed` the instant a node won
the race. The node then spent time restoring a snapshot into a directory that
did not exist yet. A client that waited for `claimed` saw it, sent a request,
and hit a node that had not finished.

**Fix.** A new state. `claiming` means a node owns the workspace but is not
serving it. The node sends `ws.ready` after materializing and only then does the
control plane set `claimed`. Grants are issued only for `claimed`. This is
`docs/adr/0011-ready-handshake.md`.

**Lesson.** Winning a race is not the same as being ready. Say "ready" only when
it is true.

**How found.** The sleep test under the race detector, where restores were slow
enough to widen the window.

## 6. An offline node still advertised its workspaces as ready

**Symptom.** Restart a node. `WaitClaimed` returned immediately because the
state was still `claimed` from before the restart. The next request hit a node
that had not re-adopted yet.

**Cause.** `PeerGone` marked the node offline and left its workspaces
untouched, on the reasoning that the lease was still valid.

**Fix.** `internal/control/control.go`. When a node disconnects, every
`claimed` workspace it holds is demoted to `claiming`. The lease still runs.
When the node returns, `resync` sends `ws.ready` for everything it is still
serving, and `reclaimLocal` re-claims anything only on disk.

**Lesson.** "Held" and "serving" are different facts. A lease says held.

## 7. The webhook wake never fired

**Symptom.** `POST /v1/events` returned 202. The timer waiting on that event
type never fired. A timer-based wake worked fine.

**Cause.** The HTTP handler passed `r.Context()` into the control plane and
returned. The request context was cancelled the moment the handler returned,
which was before the goroutine got to `log.Append`, which failed with
`context canceled` and never reached the timer check.

**Fix.** `internal/server/server.go` and `internal/control/control.go`. The
handler uses `context.WithoutCancel(r.Context())` and calls a dedicated
`PostEvents` method that appends, fires timers, and re-offers pending work.

**Lesson.** Work that outlives a request must not borrow the request's context.

**How found.** Instrumenting `fireTimer` showed one timer fire for the
duration-based sleep and none for the event-based one.

## 8. One workspace materialized twice on the same node

**Symptom.** After a timer wake, the node logged `materialize failed;
releasing … already exists on this node`, then a second claim at a higher
generation, then `ws.ready failed … stale ready`. The workspace bounced forever.

**Cause.** The timer wake and the periodic tick each sent an offer. Two
`tryClaim` goroutines ran on the same node. The first claim won the race. The
second arrived while the workspace was `claiming` by the same node, which the
re-adoption rule from mistake 4 accepted as a valid claim. Both goroutines
materialized. One created the directory; the other hit the conflict, and its
error path released the claim the first one was about to finish.

**Fix.** `internal/node/node.go`. `tryClaim` reserves the workspace id in
`n.materializing` under the node's mutex before talking to the control plane.
A second offer for a reserved id returns immediately.

**Lesson.** An idempotency rule that is correct across restarts can be wrong
across concurrent goroutines. Serialize at the smallest scope that owns the
resource.

**How found.** Node logs under the race detector, where restores were slow
enough for both offers to arrive before the first finished.

## 9. Leases expired during a slow restore

**Symptom.** Under the race detector, a workspace being restored from a
snapshot returned to pending before the node finished. It was then claimed
again, restored again, and expired again.

**Cause.** The renew loop iterated `n.workspaces`. A workspace being restored
was not in that map until `materialize` returned. Nothing renewed its lease
while the restore ran.

**Fix.** `internal/node/node.go`. `renew` includes every entry in
`n.materializing` along with the generation from the claim.

**Lesson.** A claim you are working on is a claim you hold. Renew from the
moment of the claim, not the moment of completion.

## 10. The node renewed every five seconds under a two-second lease

**Symptom.** With the sim world's two-second lease, workspaces churned between
claimed and pending under `-race`.

**Cause.** The renew interval was a constant. The control plane handed the node
the lease length in `HelloOK` and the node ignored it.

**Fix.** `internal/node/node.go`. The renew loop ticks every 250 ms and renews
when a third of the lease has passed since the last renew. The spec now states
that a node must renew at no more than one third of the lease.

**Lesson.** A constant that must be smaller than another value is not a
constant.

## 11. Data race on control-plane workspace state

**Symptom.** `WARNING: DATA RACE` between `wsReady` writing `ws.State` and
`wsClaim` reading it. Both functions held `c.mu` at those lines.

**Cause.** The idempotent path of `wsCreate` returned the live `*Workspace`
from the map after unlocking. `wsSleep` returned the live `*Timer`. The caller
serialized those pointers into a response without the lock while another
goroutine mutated the same object under it.

**Fix.** `internal/control/control.go`. Every value that leaves a locked region
is a copy: `cp := *ws` before `c.mu.Unlock()`.

**Lesson.** A mutex protects a critical section, not the data that escapes it.

**How found.** The race detector, on the third run. It was intermittent.

## 12. A dead port session was returned as a live one

**Symptom.** `OpenPort` succeeded immediately even though nothing was
listening. The retry loop waiting for a server to come up never retried. Input
then failed with `session has no stdin`.

**Cause.** `Manager.Open` returns the session even when the runner fails to
start, because for exec and pty the failure is recorded in the log and that is
the right behavior. For a port session the "runner" is a TCP dial, and a failed
dial means there is nothing to attach to.

**Fix.** `internal/node/node.go`. `portOpen` checks whether the session exited
immediately with an error and returns `unreachable` instead of a session id.

**Lesson.** The same success signal can mean different things for different
session kinds.

## 13. `--node` after the workspace id was silently ignored

**Symptom.** `remount ws move WS --node B` moved the workspace back to node A.
The move worked perfectly. The flag was not read.

**Cause.** Go's `flag` package stops parsing at the first argument that does not
start with a dash. `WS` came first, so `--node B` became a positional argument
nobody looked at.

**Fix.** `cmd/remount/main.go`. A `parse` function permutes flags ahead of
positionals before calling `fs.Parse`, honoring `--` as a terminator and
knowing which flags take a value.

**Lesson.** A CLI that silently accepts a flag and ignores it is worse than one
that rejects it.

**How found.** Live, watching a move land on the wrong node while every event
in the log said the move succeeded.

## 14. A move that no node could satisfy sat pending forever

**Symptom.** After fixing mistake 13, `ws move --node laptop` timed out after
two minutes with `context deadline exceeded`. The log showed snapshot,
release, and move, then nothing.

**Cause.** The workspace was created with `--label vendor=modal`. The move
added `Node: laptop` and kept `Allow: {vendor: modal}`. Eligibility requires
both. No node was both the laptop and labeled modal.

**Fix.** `cmd/remount/main.go`. Naming a node clears a conflicting label
constraint unless labels are also given. On timeout the CLI now prints the
requirements and placement it was waiting on.

**Lesson.** Constraints compose by intersection. A user who names a node means
that node.

**How found.** Live, against the Modal deployment.

## 15. A hosted control plane that scaled out

**Symptom.** A node enrolled and logged `uplink established`. `remount nodes`
did not list it. `remount ws ls` was empty a minute after creating a workspace.

**Cause.** The control plane ran as a Modal web endpoint with default scaling.
Different requests reached different containers, each running its own
`remount server` with its own SQLite database. There were several control
planes behind one URL.

**Fix.** `deploy/modal_app.py`. `max_containers=1`, `modal.concurrent` so that
one container serves many connections, and a `modal.Volume` at `/data` so a
restart keeps the claim queue and artifact store.

**Lesson.** A stateful singleton on a platform that scales by request volume
must be pinned explicitly. The platform will not guess.

**How found.** Live. The node's log and the CLI disagreed about the same fact.

## 16. The harness install produced nothing

**Symptom.** `npm install @openai/codex` inside a workspace on the Modal node
printed nothing and left no `node_modules`.

**Cause.** The Modal image was `debian_slim`, which has no Node runtime. The
shell command failed at `npm`, and the tail of its output was empty.

**Fix.** `deploy/modal_app.py`. The image installs Node 22 from NodeSource.

**Lesson.** A demo that depends on a runtime should assert the runtime exists
before doing anything that would look like success without it.

## 17. A config file that baked in the broker's address

**Symptom.** A harness configured on one node would have stopped working after
a move, because its config named a broker port that only existed on the old
node.

**Cause.** The broker listens on a loopback port chosen at materialize time. It
is different on every node and after every move. Anything that copied
`REMOUNT_BROKER` into a file that travels in the snapshot carried a stale
address.

**Fix.** `internal/node/node.go`. On every materialize the node writes
`.remount/env` into the workspace with the current broker address, workspace
id, and each binding's placeholder. The `.remount` directory is excluded from
every snapshot. A launcher script sources that file and regenerates config at
start-up.

```sh
#!/bin/sh
. ./.remount/env          # discovers REMOUNT_BROKER for whichever machine holds the workspace now
```

**Proof.** The same launcher ran the Codex CLI on a Modal Linux container, then
ran it again on an Apple Silicon Mac after a move, with no client-side
reconfiguration. The agent appended to the same file on both machines.

**Lesson.** Node-local truth must be rewritten by the node and must not travel.
The test for this is a move followed by a read of the file.

**How found.** Thinking through what "move a running agent" would require
before running it.

## 18. A deploy hung and blamed the platform's capacity

**Symptom.** After redeploying the hosted control plane, `/healthz` never
answered. `modal app logs` repeated: `Function 'control' is waiting to be
scheduled on a CPU worker. We are actively working on acquiring more capacity
for your workload.` It read as the platform being out of CPUs.

**Cause.** The function was pinned with `min_containers=1, max_containers=1`,
which mistake 15 required. A rolling redeploy cannot place the new revision
because the running container holds the only permitted slot. The platform
reports that wait as a capacity message.

**Fix.** Stop the app before deploying a new revision of a pinned singleton.
Deploying onto a stopped app became healthy in about five seconds.

**Lesson.** A pinned singleton and a rolling deploy are contradictory. Also, a
queue message describes a symptom, not a diagnosis; the platform cannot know
that its own scheduling ceiling is the thing blocking it.

**How found.** Testing the hypothesis directly: stop the app, deploy, watch
`/healthz`.

## 19. `modal run` silently used the wrong environment

**Symptom.** `Secret 'remount-openai' not found in environment 'main'`. The
secret had been created in `dev` minutes earlier and was definitely there.

**Cause.** `--env dev` was passed to `modal deploy` but not to `modal run`,
which defaults to `main`. The account had no access to `main` at all, so every
symptom pointed at permissions rather than at a missing flag.

**Fix.** Set `MODAL_ENVIRONMENT=dev` once for the shell instead of remembering
a flag on every subcommand.

**Lesson.** When a tool takes a per-invocation context flag, put it in the
environment. Forgetting it does not fail loudly; it succeeds against the wrong
scope and reports a confusing error about that scope.

## 20. A container fell back to a default credential

**Symptom.** Every node and client got `unauthorized: bad token` against a
freshly rented container. The token was correct on the caller's side.

**Cause.** The deployment script had `TOKEN = os.environ.get("REMOUNT_TOKEN",
"rent-token")` at module level. That line is evaluated inside the container,
where the caller's environment does not exist. It fell back to the hardcoded
default, putting a guessable shared token on a public tunnel.

**Fix.** Pass the token as an explicit run argument and raise when it is
absent.

**Lesson.** Any default credential is a real credential. A fallback that works
is worse than a failure, because it works insecurely and says nothing. Code
that reads an environment variable must be clear about which machine's
environment it will read.

## 21. A scripted edit did nothing and reported success

**Symptom.** Adding a protocol constant appeared to work. The build then failed
with `undefined: proto.OpDiag`.

**Cause.** The patch matched `OpTimerList  = "timer.list"` with two spaces.
`gofmt` had already realigned that const block to a different width, so the
replacement matched nothing. The script had no way to notice and printed its
success message anyway.

**Fix.** Assert the postcondition after patching. Prefer a pattern tolerant of
whitespace.

**Lesson.** An edit that cannot fail is an edit that can silently not happen.
Check that the intended change is present, not that the command exited zero.

## 22. Moving a workspace carried 400 MB of node_modules

**Symptom.** A cross-vendor move took 38 seconds. An empty workspace moves in
about a second.

**Cause.** A snapshot is the whole filesystem, and the agent's harness had been
installed into the workspace with `npm install`. Every move uploaded and
downloaded the entire dependency tree.

**Fix.** Create the workspace with `--exclude node_modules` and reinstall on
the far side, or accept the transfer knowingly.

**Lesson.** Snapshot portability has a size cost paid on every move. Exclude
anything reproducible from a lockfile.

---

## Patterns in the first build/debugging pass

Almost all of them fall into four groups.

**State that outlives a connection.** Mistakes 1, 3, 4, 6 and 15 were all cases
where something persisted past the socket that created it and code assumed
otherwise. The session log, the workspace on disk, the lease, and the control
plane's database all outlive connections. Code that ties their lifetime to a
connection is wrong.

**Ordering assumptions.** Mistakes 2, 5, 8 and 9 assumed one thing would finish
before another started. A stream beat its own open response. A claim beat its
own restore. Two offers beat each other. The fix each time was to make the
dependency explicit: a buffer, a state, a reservation, a renewal.

**Things that looked successful while doing nothing.** Mistakes 7, 12, 13, 14,
16 and 21 all returned success or silence for an operation that had no effect.
A cancelled append, a dead session with a valid id, a dropped flag, a queued
workspace with no eligible node, an install that never ran, a text edit that
matched nothing. Every one of these would have been caught faster by a check
that the intended effect had happened rather than that the call had returned.

**A failure that reports itself as someone else's problem.** Mistakes 18, 19
and 20 all produced a message pointing away from the cause. The platform said
it lacked capacity when our own single-container ceiling was the constraint. A
missing flag surfaced as a permissions error about an environment we never
meant to use. A wrong token surfaced as `unauthorized` rather than as the
silent fallback that produced it. The habit that works is to state the
hypothesis, then design the cheapest experiment that would falsify it, rather
than believing the loudest message.

The race detector found three of these. The simulation package found six.
Running against a real vendor found seven more that no simulation would have.

---

## 23. macOS killed the binary with signal 9 after an in-place copy

**Symptom.** Every invocation of the CLI exited immediately with status 137 and
printed nothing at all, including `remount version`. The server it talked to
was healthy and answering `/healthz`.

**Cause.** The binary had been updated with `cp` over an existing file. That
writes through the same inode, so macOS sees the pages of a code-signed
executable change underneath it and kills the process with SIGKILL. Status 137
is 128 plus 9.

**Fix.** Remove the file before copying, so the new binary gets a fresh inode.
`install` and `mv` are also safe, because both replace the directory entry.

```sh
rm -f ./remount && cp /path/to/remount .
```

**Lesson.** A process that dies with no output has usually been killed by
something outside it. The exit status names the signal, and 137 means SIGKILL
rather than a bug in the program.

**How it was found.** Running the command without a pipe, so the exit status
was visible rather than shadowed by the exit status of `head`.

## 24. A health check printed "healthy" while checking nothing

**Symptom.** `remount doctor` reported healthy on a deployment where the
node-consistency check could not run at all.

**Cause.** The deep node call needs an entitlement, and the client was not
attaching one, so every node returned "unauthorized". Doctor treated that as a
node worth skipping and moved on, then printed its summary with no errors.

**Fix.** Two changes. The client now attaches a grant for a workspace the
control plane says that node holds. Doctor emits `node.diag_unavailable` when
a check cannot run, and says explicitly that those workspaces were not
checked.

**Lesson.** The worst output a diagnostic tool can produce is a clean bill of
health it did not earn. A check that cannot run is not a passing check, and
"skipped" must never render as "fine".

**How it was found.** Reading the output of a live run and noticing that a
warning about an unreachable node sat directly above the word "healthy".

## 25. A read of guarded state one line after the unlock

**Symptom.** One simulation test failed under the race detector roughly one run
in four. The report named two lines that both appeared to be inside the same
critical section, which made it look like a false positive from the tool.

```
Write at 0x00c000220270 by goroutine 82:
  control.(*Control).wsReady()  control.go:853
Previous read at 0x00c000220270 by goroutine 77:
  control.(*Control).wsClaim()  control.go:814
```

**Cause.** Line 813 released the lock and line 814 read `ws.State` while
formatting the error message.

```go
if ws.State != proto.WSPending {
    c.mu.Unlock()
    return nil, proto.Err(proto.CodeConflict, "workspace %s is %s", id, ws.State)
}
```

The read is on the return line, so it reads like part of the guarded block. It
is not.

**Fix.** Hoist the value before unlocking, in `internal/control/control.go`.

```go
state := ws.State
c.mu.Unlock()
return nil, proto.Err(proto.CodeConflict, "workspace %s is %s", id, state)
```

**Lesson.** An error message is a read of shared state like any other. Two
useful habits follow: assume the race detector is right when it names two
lines that look safe, and read the line numbers rather than the shape of the
code.

**How it was found.** The race detector, on the fourth of eight repeated runs
of a single test. It had passed the full suite several times before this.

**What was done about the class.** `scripts/lint-locks.sh` now walks forward
from every unlock and flags a read of guarded state before the next return or
closing brace. It is wired into `make lint` and CI. Verified in both
directions: it catches the historical bug when reintroduced, and it passes on
the fixed tree. It found one further instance of the same class in `wsCreate`,
where the newly created workspace was published to the shared map and then
read after unlocking, which was fixed the same way. Two annotated exceptions
carry `lint:locks-ok` with the reason.

## 26. A partial stdin write consumed the retry sequence

**Symptom.** An injected writer accepted the first two bytes of `hello` and
then failed. Retrying the same input sequence could be treated as a duplicate,
so the missing suffix would never reach the process.

**Cause.** Session input associated the idempotency sequence with the call
rather than the completed byte-and-EOF effect. A single `Write` is allowed to
return fewer bytes than requested.

**Fix.** `internal/session/session.go` writes the full buffer and advances
`lastISeq` only after all bytes and any requested EOF action succeed.
`TestInputSequenceAdvancesOnlyAfterCompleteWrite` fails the first write and
proves an identical retry is still applied.

**Lesson.** Deduplication state is a commit record. Publish it after the whole
effect, never after an attempt.

## 27. A failed snapshot returned before its producer stopped

**Symptom.** A size-limited or failed artifact write could return from snapshot
creation while the backend's archive-producing goroutine was still walking the
workspace. The caller would then release the workspace tree lock.

**Cause.** Closing the consumer side of an `io.Pipe` requested the producer to
stop but did not prove it had stopped. The failure path did not join the
producer.

**Fix.** `node.snapshotRaw` closes the pipe with the terminal error and waits
on `producerDone` before returning on both success and failure. The tree
boundary therefore outlives every archive read.

**Lesson.** Cancellation is a request. Joining is evidence. A critical section
that delegates work to a goroutine lasts until that goroutine exits.

## 28. An authorized operation could wake after its workspace was gone

**Symptom.** A filesystem or session operation could validate a grant, queue
behind a release, quarantine, fence or checkpoint, and then touch a stale
handle after the lifecycle operation removed or closed it.

**Cause.** Authorization and resource use were separated by a blocking lock.
The authorization result described the workspace before the wait, not after
it.

**Fix.** `node.lockWorkspaceTree` acquires the tree boundary and then checks
that the same workspace handle and generation are still serviceable. Removal
takes the exclusive boundary to drain in-flight work. Focused tests queue work
before removal and prove it returns a conflict without touching the handle.

**Lesson.** Authorization has a time dimension. Revalidate at the point of use
after every wait during which authority or object identity can change.

## 29. A checkpoint had a write window between archive and commit

**Symptom.** An explicit authoritative checkpoint could finish producing its
archive, allow a filesystem mutation, and only afterward commit the old digest
as failover state. The acknowledged checkpoint did not necessarily represent
the workspace at its commit point.

**Cause.** The exclusive tree lock covered archive production but not the
generation-specific control-plane commit.

**Fix.** An authoritative checkpoint holds the tree boundary through the
control commit. `TestAuthoritativeCheckpointHoldsTreeUntilControlCommit`
blocks the commit deliberately and proves a writer cannot cross that interval.
Concurrent lifecycle removal also makes the checkpoint lose rather than revive
stale state.

**Lesson.** The consistency boundary ends at the durable authority commit, not
when serialization reaches EOF.

## 30. Failed quiescence could still look safe to snapshot or release

**Symptom.** Session termination failures could be ignored before a checkpoint,
release or quarantine. A failed release path could also restore the workspace
without restoring its local lease deadline, causing it to self-fence later.

**Cause.** Process termination was treated as best effort even though
`quiesced` is a correctness claim. Rollback restored the resource map but not
all of the ownership state removed during prepare.

**Fix.** `session.Manager.KillWorkspace` reports any process it cannot confirm
stopped. Lifecycle callers abort on that error. Failed prepare/snapshot paths
restore the source, serving record and a fresh local lease window without
destroying the handle.

**Lesson.** A failed precondition invalidates every claim built on it. Rollback
must restore the complete invariant, not merely the most visible pointer.

## 31. Workspace authority could cross the database's numeric boundary

**Symptom.** The wire model allowed a `uint64` generation while SQLite stores
signed integers. Advancing authority past `MaxInt64` could wrap, fail to
persist, or make protocol and recovery code disagree.

**Cause.** Each increment was locally reasonable, but no one owned the
cross-representation boundary.

**Fix.** `nextWorkspaceGeneration` and `canAdvanceWorkspaceAuthority`
reject exhaustion. Lease expiry at the boundary transitions the workspace to
durable `failed` without changing its generation. Exhaustive and property
tests cover the transition table and authority checks.

**Lesson.** Monotonic authority counters need an explicit terminal state.
Never wrap a fence token.

## 32. Bounded history could retain its bookkeeping forever

**Symptom.** Reference-aware assignment pruning could repeatedly inspect a
protected oldest prefix and never reach deletable history behind it. Separately,
keyed lifecycle, producer, mutation and fleet locks retained arbitrary keys
after the work completed.

**Cause.** The visible data had limits, but the selection algorithm and
synchronization indexes did not share the same lifecycle. Applying a deletion
limit before excluding protected rows caused starvation.

**Fix.** SQL excludes the current live-authority set before applying the
bounded deletion limit. Keyed mutex entries are reference counted and removed
after their last waiter leaves. Tests populate protected prefixes and churn
unique lock keys to prove both collections shrink.

**Lesson.** Boundedness includes metadata and algorithmic progress. A maximum
row count is not useful if collection can scan the same undeletable rows
forever.

## 33. An authoritative checkpoint could have nowhere authoritative to go

**Symptom.** A caller could request an authoritative uploaded checkpoint from a
node with no control-plane artifact endpoint. Local snapshot work could succeed
but the result could not become portable failover state.

**Cause.** The operation validated storage availability after adopting the
semantic promise of an authoritative checkpoint.

**Fix.** The node returns typed `unsupported` before snapshot work when the
artifact endpoint or commit callback is absent.
`TestAuthoritativeCheckpointRequiresControlPlaneArtifactStore` preserves the
boundary.

**Lesson.** Validate the capability needed to fulfill a promise before doing
the work. A local artifact ID is not an authoritative checkpoint.

## 34. Public output helpers hid an eviction gap

**Symptom.** A session whose oldest output had been evicted could be copied into
a buffer and presented as complete output. The low-level stream contained a
`gap` marker, but the convenient public helper did not turn it into failure.

**Cause.** The helper treated every non-stdout/stderr chunk as ignorable
metadata. That interpretation destroyed the log's completeness signal.

**Fix.** Public `Copy` and aggregate `Run` validate the gap payload, render
an elision marker where possible, and return the typed `evicted` error.
`TestOutputGapIsNeverReportedAsComplete` covers valid and malformed gaps.

**Lesson.** Missing data is data. Convenience APIs must preserve completeness
and uncertainty signals from lower layers.

## 35. A green fuzz step selected no fuzz targets

**Symptom.** A seeded-fuzz CI command completed successfully without executing
the registered fuzz functions it was meant to exercise.

**Cause.** The test-selection expression did not match the `Fuzz...` entry
points. Package discovery and a zero exit status looked like useful work.

**Fix.** CI runs `go test ./... -run='^Fuzz'` for every seed corpus, while
`make fuzz` explicitly names and actively fuzzes all eight targets.

**Lesson.** Validate test selection as well as test results. A green harness
that ran zero intended cases is a false diagnostic.

## 36. Encoding a frame changed the caller's object

**Symptom.** Serializing a frame with an omitted version wrote the default
version back into the caller-owned frame. Concurrent reuse could observe a
mutation caused by what looked like a read-only operation.

**Cause.** `EncodeFrame` filled defaults directly on its pointer argument for
convenience.

**Fix.** Encoding defaults a private copy.
`TestEncodeFrameDefaultsVersionWithoutMutatingCaller` checks both sides: the
wire frame has V1 and the input remains unchanged.

**Lesson.** Serialization should not acquire hidden ownership of caller state.
Copy before normalization at concurrency boundaries.

## 37. Session exit became visible before its capacity was reusable

**Symptom.** `Session.Wait` returned, but an immediate replacement session
could still receive `resource_exhausted`. Waiting or polling briefly made the
same open succeed.

**Cause.** Exit notification woke callers before an asynchronous manager
observer decremented active-session accounting.

**Fix.** Session finish performs manager accounting before closing the exited
signal. The quota test opens the replacement immediately after `Wait`, and a
concurrent-open test proves the limit cannot be overcommitted.

**Lesson.** Define the handoff event for a quota. If callers reasonably treat
completion as capacity release, accounting must happen before completion is
observable.

## 38. A fixed port made port forwarding look broken

**Symptom.** The simulator's port-forward test timed out on a hosted macOS
runner even though the forwarding implementation was sound.

**Cause.** The test launched a separate server on a fixed development port that
could already be occupied or reserved. The resulting dial failure surfaced far
from setup.

**Fix.** The test uses an in-process HTTP server on an OS-assigned loopback
port and passes the selected port into `OpenPort`.

**Lesson.** Tests should ask the operating system for scarce resources and keep
their ownership in process. A fixed port is shared global state.

## 39. The cloud smoke test proved the wrong Modal app

**Symptom.** `modal run` could report a successful smoke result even though
the named persistent deployment was unhealthy or absent. Earlier readiness
could also succeed when only the HTTP listener was up and the node had not
enrolled.

**Cause.** `modal run` creates an ephemeral source app. The smoke used that
execution context as if it were the deployed object. The deployment also
conflated import-time and container-time binary checks, guessed the JSON shape
of `nodes --json`, and explicitly committed a Volume during shutdown when the
provider lifecycle should own it.

**Fix.** Smoke resolves `modal.Function.from_name` and tests the named
deployment's URL. Startup waits for an authenticated node listing, decodes the
actual array/object shapes, validates dependencies in the container lifecycle,
and relies on the supported Volume lifecycle. The live exercise then stopped
the app and removed its unique volume and secret.

**Lesson.** Cloud tests must name the deployed object and prove the full
readiness chain. Provider source code, an ephemeral invocation, an HTTP socket
and a persistent deployment are four different things.

## 40. A reconnected client was told it had gone

**Symptom.** `TestReconnectMidStreamIsLossless` failed on macOS CI only: after
two connection cuts the client held 5,074 of 18,890 bytes and the session then
finished normally. Hundreds of Linux runs under `-race` never reproduced it.

**Cause.** A cut and the redial it triggers are one event seen from two relay
goroutines. The new hello could authenticate, register the client's subject
and report `PeerConnected` before the old connection's `Serve` noticed its peer
had closed. That stale teardown then called `PeerGone`, which deleted the
subject the new connection had just registered, and sent `peer.gone` to the
node, which cancelled any subscription the reconnected client had already
re-established. Both failures were silent: control calls returned
`unauthorized` until the SDK's retries ran out, or the stream simply stopped.
The relay's own tables were guarded against this (`r.peers[id] != peer`); the
controller and the correspondents were not.

**Fix.** `internal/relay/relay.go`. Connect and disconnect bookkeeping for one
peer id run under a per-id lock, from `Authenticate` through `PeerConnected`
and around the whole of `remove` including the `peer.gone` sends
(ADR 0059). A regression test holds the second hello open inside
`Authenticate` while the first connection dies and asserts the controller
never sees `gone` after the new `connected`.

**Lesson.** A replacement rule has to cover every observer of the thing being
replaced, not only the table that stores it. When two goroutines start from
the same event, name the order the rest of the system depends on and enforce
it with a lock, not a hope about scheduling. A platform-only failure is a
scheduling-order failure until proven otherwise.

## 41. Three waits with no bound, one cookie with too much reach

**Symptom.** The second review round found three places where the Agent
lifecycle waited on something it did not control. `harnessExit` called
`cmd.Wait()` with no deadline, so a harness that closed its stdout and lingered
(or whose children kept the group alive) pinned the run slot, the transcript
and the node's capacity for as long as the tree lived, and every later run for
that Agent got `conflict`. A policy sleep the node refused was retried on the
very next reconcile, and the completion path kicked reconcile again, so one
refusal became a hot loop against the same node. A soft `agent.run.cancel`
that arrived during a cold recipe install was not seen until the install
finished or hit its fifteen-minute timeout, with the binding environment live
the whole time. Separately, the JSON fallback of `GET /a/{id}` accepted the
preview session cookie, so same-origin preview content could read the Agent
record (task, recipe, inbox text, owner).

**Fix.** `internal/node/agentrun.go`: `harnessExit(cmd, grace)` waits in a
goroutine, kills the group after `grace`, and abandons the tree with
`harnessExitCode` after a second `grace`; the run gets a `softCtx` that
`requestCancel` ends, and the install session waits on it.
`internal/control/agents.go`: a failed policy sleep or wake calls `deferAgent`,
and the sleep and wake decisions check `agentRetry` like launch already did.
`internal/server/api.go`: the `/a/{id}` JSON form is header-only (ADR 0060);
the cookie is accepted by the preview proxy alone. Regression tests:
`TestHarnessExitIsBounded`, `TestAgentRunCancelInterruptsInstall`,
`TestAgentPolicySleepFailureBacksOff`, and the cookie-only route list in
`internal/sim/agent_http_test.go`.

**Lesson.** Every wait on a process, a peer or a retry needs a bound and an
observable outcome when the bound is hit, and a cancel has to reach every phase
of a run, including the ones before the thing being cancelled exists. A
browser credential's reach is the set of routes that accept it; add a route
that returns data and you have widened the credential, whether or not that was
the intent.

## 42. A 41x performance regression nobody could see, because the tool that would have seen it was broken

**Symptom.** Moving a 200 MB workspace between two local nodes took 33 s. The
checked-in benchmark evidence claimed 183 MB/s for a larger payload. Both were
honest measurements; they were taken at different commits, and nothing ever
compared them. `git bisect` over 23 commits put the whole regression inside
one:

    21ef995  test(sim): record fleet-scale failover evidence     0.85s  (235 MB/s)
    a3b5235  feat(platform): converge hosted runtime handoff     75.79s (2.6 MB/s)

`a3b5235` is 12,504 insertions across 66 files, and its own handoff document
had already written the warning: "Three subagents shared the integration
worktree, so the final commit groups a large convergence diff. Review by
subsystem and behavior, not merely by commit size." It was reviewed by
subsystem. It was not reviewed by behavior, and workspace movement — the
product's central operation — got roughly 90x slower without a single test
noticing.

**Why nothing caught it.** The move benchmark was the one instrument pointed at
this, and it could not run. `bench/move_matrix.py` refuses to put a credential
on argv, so it requires `REMOUNT_TOKEN` in the environment; the artifact
endpoint answered a *presented* bearer with 401 in standalone mode while
accepting a request with no credential at all. So the benchmark failed against
the simplest possible server, the evidence file went stale at a commit before
the regression, and the number in `docs/benchmarks.md` kept describing a build
nobody was running.

Two independent failures had to line up: a correctness bug in an auth path made
the instrument unusable, and a performance claim was carried forward as a
committed fact rather than re-earned. Neither alone would have hidden this.

**Fix.** `internal/server/server.go`: a standalone server with no shared token
and no authenticator now resolves the same subject whether or not a bearer is
presented, because it has nothing to check one against and every other surface
already admits any bearer in that configuration. Presenting a credential must
never reduce authority. Regression tests
`TestStandaloneArtifactsAcceptABearerTheSameAsNone` and
`TestConfiguredTokenStillRejectsAWrongBearer` — the second exists because the
first would also pass if the fix had opened every mode.

**Lesson.** A performance number is evidence about a commit, not about a
program. Once it is checked in it stops being a measurement and becomes a
claim, and a claim decays silently while the code moves underneath it. Re-earn
it on the candidate or delete it — the same rule this repository already
applies to correctness evidence, which is why `make plan-b` refuses to promote a
recorded outcome into a pass.

The second half is sharper: **when a measurement contradicts a committed
number, do not pick a side — find out why they differ.** The contradiction was
the finding. Believing the benchmark would have hidden a 41x regression;
believing my own measurement without bisecting would have sent someone hunting
an inherent cost that did not exist. Three hypotheses died on the way there
(gzip, fsync, the network), each cheap to test and each wrong, and testing them
is what made the bisect obviously worth doing.

And an instrument that cannot run is not a passing instrument. A benchmark that
fails to start looks exactly like a benchmark nobody scheduled.

## 44. Three ceilings that were never ceilings

**Symptom.** A load pass that measured growth across repeated cycles — rather
than checking a single absolute number — found three places where a bound was
assumed and never enforced.

`Control.tails` had no limit of any kind. One authenticated peer, on **one
connection**, opened **2,000 follow subscriptions with zero refusals**: 2,000
control-plane goroutines and 150 MiB, about 75 KiB each, growing linearly with
no rejection code and no metric. Each subscription also takes a slot that
`Log.fanOutLocked` walks on every append under the log mutex, so the cost is
per-event as well as per-subscriber. A remote peer sized the control plane's
memory and its per-event work.

`eventSubscription.enqueue` dropped a slow consumer by closing its channel. A
consumer that stopped reading during 4,000 events received 256 of them and then
saw a closed channel — which is exactly what it sees after cancelling itself.
The loss was both uncounted and indistinguishable from a clean shutdown.

`Control.poolIdle` was one control-wide map keyed by machine id, but
`enrichPoolInventory` pruned every entry not visible to *the pool it was
currently reconciling*. Since `reconcilePoolsAsync` starts every pool on the
same tick, reconciling pool A erased the idle clocks of pools B through Z. A
single pool drained in 870 ms; six pools left 4 of 6 machines running after
30 s, and an eight-pool run left machines up after 3 minutes 21 seconds with
every node online, assignment-free and correctly labelled. The wider the fleet,
the less likely any machine survived from "marked idle" to "old enough to
destroy". Idle provider inventory is billed by the hour, so this was a spend
bug wearing a tidiness bug's clothes.

**Fix.** `MaxEventTailsPerRequester` (default 64) refuses a new subscription
with `resource_exhausted` and `remount_event_tail_quota_rejections_total`;
replacing an existing subscription id is still always allowed, because that is
not the unbounded direction. `enqueue` now drops the event and **keeps the
subscription open**, setting `Lagged()` and moving
`remount_event_subscribers_dropped_total`; events carry a monotonic `Seq`, so
the consumer sees the discontinuity the same way a session sees a `gap` chunk.
`poolIdle` is keyed by `{pool, machine}` and each reconcile prunes only its own
pool's entries.

**Lesson.** A ceiling nobody measured is a hypothesis. All three of these were
believed to be bounded; none were, and the code read as though they were.

Measure a bound by **slope, not level**: run to steady state, run again, and
compare a later cycle to an earlier one. An absolute number tells you what a
workload costs; only the second cycle tells you whether anything was released.
That method is what separated the real leaks from ordinary cost here — 400
reconnecting cursors peak at 2,429 goroutines and settle to 29 every single
cycle, which looks alarming and is completely fine.

Two of these are the same mistake as #38 and #43 again, in a third costume: a
silent drop and a green gauge are both "I could not tell" rendered as good
news. **Every rejected or dropped unit needs a counter and an explicit result**
— if the only evidence of loss is that a number is smaller than expected, the
loss is invisible to everyone who was not counting.

And shared mutable state must be keyed by the scope that owns it. `poolIdle`
was correct for one pool and silently wrong for two, which is the kind of bug
that ships because the test fixture had one of everything.

---

The smaller fixes from the same hardening pass—error shadowing in persistence
helpers, short local file writes, connector object-count limits, owned-only
orphan cleanup, module-metadata drift and incomplete validation messages—are
indexed in the [implementation
closure](docs/engineering/implementation-closure-2026-09-03.md#additional-defects-found-during-implementation-review).
Their reusable implications are folded into the [hardening
playbook](docs/engineering/hardening-lessons.md).

## 43. Six ways to hold a boundary and one way to hand it away

**Symptom.** An adversarial pass over the newest subsystems and over the two
oldest security boundaries found bugs that share one shape: the code defended
the property it was written to defend and did not notice a second path to the
same place.

- `internal/e2ee`: `peer.gone` was handled before the required-policy plaintext
  check and without asking who sent it, so any peer could forge a plaintext
  fleet event and destroy another peer's keys. Only the control plane speaks
  for the fleet.
- `internal/e2ee`: a connection cached its binding identity forever. A node
  uplink outlives a dated credential, so once the binding expired the peer
  could never encrypt again — a permanent outage under a required policy, and a
  permanent silent downgrade under a preferred one.
- `internal/fsops`: containment failures were classified by matching
  `"path escapes from parent"` against an error string that embeds the caller's
  own path. A workspace could create a directory with that name and manufacture
  the exact audit signal an operator reads as an exfiltration attempt. The
  error mapper was matching on message text, which is the thing this protocol
  tells every other caller never to do.
- `internal/fsops`: a search whose root could not be walked returned "no
  matches" rather than an error, so a jail denial rendered as a clean empty
  result.
- `internal/broker`: the ambiguous-encoded-path guard ran after the path had
  already been decoded, so `%2f` and `%2e` — the two forms the specification
  names first — were exactly the two it could no longer see. `%25` and `%5c`
  still tripped it, which is why it looked like it worked.
- `internal/broker`: the workspace's broker capability, a bearer that
  authorizes that workspace's entire egress surface, was written into audit
  records by two pre-authentication paths and from there into the durable,
  exportable event log.
- `internal/control`: a retention gauge was set to zero when the check could not
  run, so the number an operator alerts on went green at the moment the check
  broke.

**Lesson.** Every one of these is a boundary that holds against the attack it
was designed for and leaks through an adjacent path: a second frame kind, a
second lifetime, a second encoding, a second error class, a second code path
that runs before authentication. When reviewing a boundary, do not re-verify
the case it already handles — enumerate the other ways in.

Two of these are also the same mistake as #38 wearing different clothes: a
signal that reports healthy when it is merely uninformed. An empty search
result, a zeroed gauge, and a skipped job are all indistinguishable from good
news unless the code makes "I could not tell" a distinct answer.

And a credential's blast radius is every surface that records it, not just
every surface that accepts it. The broker capability was never *checked* in an
audit record; it was only *written* there, and that was enough.

---

That playbook is also the cross-cutting pattern analysis for mistakes 23-41:
truth must survive asynchronous boundaries, authorization must be revalidated
at use, destructive work waits for durable commit, every retained structure is
bounded, and verification must distinguish success from work that never ran.
