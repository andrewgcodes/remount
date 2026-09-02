---
name: remount-dev
description: Use when working on the Remount codebase itself. Building, running the test suite, adding a protocol operation end to end, writing a simulation test with fault injection, or debugging the failure model. Not for using a running Remount deployment; that is remount-operate.
---

# Developing Remount

Remount is a Go monorepo with one binary and one protocol. Read `AGENTS.md`
first for layout and invariants. This skill is the working loop.

## The loop

```sh
make            # vet, unit and sim tests, static binary
make race       # required before any change is done
go test -count=1 -run 'TestName$' -v -timeout 60s ./internal/sim/
```

Always pass `-count=1`. Cached results hide flakes. Always pass a `-timeout`
shorter than the default when chasing a hang, because the timeout panic prints
every goroutine's stack and that stack is the diagnosis.

`make race` is not optional. Three bugs in this repo existed only under the race
detector: a live pointer escaping a mutex, a lease expiring during a restore, and
a renew interval longer than the lease.

## Adding an operation end to end

An operation is a request a client sends to a node or to the control plane.
Adding one touches five places, in this order.

1. `internal/proto/types.go`. Add the op name to the constant block and define
   the request and response structs with `cbor` tags. Every mutating request
   gets an `IdempotencyKey string` field tagged `idem`.
2. Dispatch. For a node op, add a case in `dispatch` in `internal/node/node.go`
   that decodes with `decode[T](f)`, calls `n.authorize(f.From, req.WS,
   req.Grant)`, and returns a body or a `proto.Err`. For a control op, the case
   goes in `internal/control/control.go` and node-only ops check `c.isNode`.
3. `internal/client/client.go`. Add a method. Node calls go through
   `c.nodeCall`, which attaches a grant and retries once on a stale generation.
   Control calls go through `c.call`.
4. `spec/PROTOCOL.md`. Add a row to the table in section 6 or 7.
5. `internal/sim/sim_test.go`. Add a test that exercises it through the client.

If the operation changes state, emit an event at the change. A state change with
no event is a bug.

## Writing a sim test

`internal/sim` runs the whole system in one process over in-memory pipes.

```go
func TestSomething(t *testing.T) {
	w := newWorld(t)                        // server, relay, artifact store
	n := w.node("n1", map[string]string{"zone": "a"})
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "..."}})
	// ...
	w.cut("c1")                             // sever every connection c1 holds
	// ... assert nothing was lost
}
```

`w.cut(name)` closes every pipe that peer opened. The peer's own reconnect logic
then runs against the real relay. Cut a client to test replay. Cut a node once to
test uplink flap. Cut a node in a loop to test lease expiry and failover.

The world's lease is two seconds so failover tests finish quickly. Give contexts
sixty seconds anyway; the race detector makes everything several times slower
and the `go test -timeout` is the real bound.

Assert on events, not internal state. `c.ReadEvents(ctx, 1, ws.ID)` returns the
workspace's history. If the event you expect is missing, the state change did
not emit it, which is itself the bug.

## Invariants to check your change against

- The workspace is trusted with nothing. Nothing that holds a secret or makes a
  policy decision moves closer to it.
- Seq 0 is the `info` chunk. The `exit` chunk is last. A gap is a `gap` chunk.
- Input is deduplicated by `iseq`.
- A grant is bound to a generation and refused after a move.
- The broker blocks a placeholder aimed at an unbound host.
- `ws.ready` gates `claimed`.
- A node renews at no more than one third of the lease, including while
  materializing.
- Anything returned from under `control.mu` is a copy.
- `.remount/env` is rewritten on materialize and excluded from snapshots.

## Debugging playbook

These are the techniques that actually found the bugs in `MISTAKES.md`.

**A request times out with no error.** Put a `fmt.Fprintf(os.Stderr, ...)` at
the top of `relay.route` printing kind, op, from, and to. Run one test with
`-v`. If you see the request go out and no response come back, the response was
consumed somewhere it should have been forwarded. If you see nothing, the
request never reached the relay.

**A test hangs.** Run it alone with `-timeout 30s -v`. The panic on timeout
dumps every goroutine. Look for the test goroutine and read what it is parked
on. `chan receive` inside `range s.Chunks()` means the exit chunk never arrived.

**Something passes without `-race` and fails with it.** Timing, not logic. Look
for a lease, a renew interval, or a state that is set before the work it
describes is finished.

**A scan for a secret says clean.** Prove the scan works. Plant a copy of the
secret in a file, run the same scan, and confirm it finds exactly one. Do not
pipe `grep` into `head` inside an `if`; `head` always exits 0.

**A scripted edit reports success and the build disagrees.** Assert the
postcondition. `gofmt` realigns const blocks, so a patch matching
`OpFoo  = "foo"` with two spaces silently matches nothing after a reformat.
Grep for the intended result after every scripted edit, or match on a pattern
tolerant of whitespace. Exit status zero is not evidence the edit happened.

**A workspace sits pending.** `remount ws get WS` and compare
`spec.requires` and `spec.placement` against `remount nodes`. Eligibility is
the intersection of every constraint.

**Logs disagree with the CLI.** Two processes think they are the same control
plane. Check that exactly one server is running behind the URL.

## Style

Comments say why. Exported identifiers have doc comments. Errors across the wire
use `proto.Err(code, ...)` with a stable code. No new dependencies without a
written reason; the binary is static and 13 MB.

## Before you commit

```sh
make lint      # vet, gofmt, and the lock-discipline check
make race      # the whole suite under the race detector
```

`scripts/lint-locks.sh` catches reads of mutex-guarded state after an unlock.
That class of bug appeared once in this codebase, failed roughly one run in
four under `-race`, and looked like a false positive because the unlock and
the read were adjacent lines. If the linter flags something you believe is
safe, annotate it with `lint:locks-ok` and a reason rather than loosening the
check.

## Debugging a deployment

```sh
scripts/collect.sh --deep > snap.json
scripts/explain.py snap.json
```

That is one command instead of a dozen, and the output leads with the verdict.
`explain.py` exits 1 when the deployment reports problems and 2 when the
collection itself was incomplete, so a partial answer is never mistaken for a
healthy one.
