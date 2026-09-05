---
name: remount-hardening-review
description: Use when reviewing, hardening, diagnosing, or implementing Remount lifecycle, concurrency, durability, isolation, egress, quota, retention, protocol, public SDK, deployment, or production-readiness changes. Not for routine operation of an already-running deployment.
---

# Remount hardening review

`docs/using-remount.md` is the canonical user and coding-agent entry point.
When a hardening change alters a command, recipe, provider, deployment,
security behavior, file-transfer rule, diagnostic, or lifecycle outcome,
update that guide and its linked detailed document in the same change. Run
`make docs` and verify `make lint`; never edit generated `llms.txt` or
`llms-full.txt` by hand.

Treat Remount as an authority-transfer system, not merely a remote shell. A
correct change must preserve the resource, its authoritative generation, its
durable record, and the evidence an operator uses to distinguish success from
unknown state.

## Establish the current truth

1. Read `AGENTS.md`.
2. Read `docs/engineering/README.md`, then the current closure/status document
   it identifies. For a comprehensive audit, read every engineering input in
   full. For a narrow change, also read the relevant ADR, protocol section,
   operations section, and entries in `MISTAKES.md`.
3. Read `docs/engineering/hardening-lessons.md` before designing a lifecycle,
   concurrency, security, capacity, public-API, or deployment change.
4. Fetch the remote and record `git status --short --branch`, `HEAD`, and
   `origin/main`. Treat edits you did not make as user-owned. Never erase or
   rewrite another agent's work to make the tree convenient.

## Map the boundary before editing

Write down, at least in working notes:

- the authoritative state and the component that owns it;
- the resource that can be leaked, duplicated, corrupted, or destroyed;
- the generation, operation ID, cursor, or other fence that rejects stale work;
- the irreversible action and the durable commit that must precede it;
- every goroutine, stream, lock, queue, staging file, and retry that crosses the
  boundary;
- the quota and retention rule for every new long-lived collection;
- the event, metric, diagnostic, or error that proves success, failure, and
  incomplete verification separately.

If any answer is unclear, inspect the implementation before proposing a patch.
Do not infer authority from a nearby cache, connection, or in-memory pointer.

## Implement the smallest coherent change

1. Reproduce the failure or encode the missing invariant in a focused test.
2. Centralize lifecycle transitions through the state machine. Validate actor,
   predecessor state, node, generation, and authorization revision.
3. Revalidate serviceability after acquiring the resource lock. Authorization
   before waiting on a lock is not authorization to use the resource later.
4. For destructive work, use prepare, quiesce, snapshot/verify, durable commit,
   and only then destroy. A timeout, cancelled context, lost acknowledgement,
   or failed persistence step retains and fences the source.
5. Join spawned work before releasing the lock or reporting completion.
   Cancellation requests work to stop; it does not prove that work stopped.
6. Make admission and accounting atomic. Define exactly which observable event
   hands capacity to the next caller.
7. Reject capabilities the selected backend cannot enforce. Keep the process
   and built-in Docker backends described as cooperative, not production
   isolation.
8. Emit the state-change event only for a committed transition. Represent an
   unavailable check as unavailable, never healthy.

## Prove the change

Start narrow, then widen in proportion to risk:

```sh
go test -count=1 -run 'TestName$' -v -timeout 60s ./internal/affected
go test -race -count=10 -run 'TestName$' -timeout 300s ./internal/affected
make lint
make test
make race
make conformance
make fuzz FUZZTIME=5s
go mod verify
go mod tidy -diff
make dist
```

- Lifecycle, reconnect, ordering, and failure-model changes require a simulation
  test with a cut, delay, replay, restart, or failed commit.
- Protocol changes require negotiation/compatibility tests, the golden wire
  fixture when encoding changes, and `spec/PROTOCOL.md`.
- Public SDK changes require `make public-api`; same-module tests cannot prove
  that exported code avoids Go `internal` imports.
- Archive, path, grant, broker-parser, lifecycle, ID, and cursor changes require
  the corresponding fuzz target.
- Capacity changes require concurrent overcommit tests, idempotent-at-capacity
  tests, restart reconstruction where durable, and observable rejection counts.
- Run static analysis and vulnerability checks when dependencies, parsing,
  unsafe boundaries, or release inputs change.

Do not substitute one green layer for another. A race run is not a provider
smoke test; a provider smoke test is not a failure-injection test.

## Validate live deployments safely

Use cloud credentials only when the task authorizes live provider work and the
change needs it. Never print, copy into arguments, write into a workspace,
commit, or quote a real secret.

- Build the candidate from the exact commit under review.
- Use unique test resource names and the intended provider environment.
- Prove readiness through an authenticated Remount operation and an enrolled
  node, not merely an open HTTP port.
- Exercise the named deployed service, not an ephemeral source copy.
- Inspect structured logs, events, metrics, `status`, `inspect`, and
  `doctor --deep` around the failure or transition being tested.
- Test restart/re-adoption when persistence or deployment code changes.
- Clean up sandboxes, apps, containers, volumes, and test-only secrets; then
  verify they are absent.

## Review and handoff

Inspect the complete diff for new races, stale authority, unbounded state,
silent truncation, ignored errors, false success, accidental secret material,
and documentation drift. Fetch again before integrating with a moving branch.

Report:

- the exact files and behavior changed;
- focused, broad, race, conformance, fuzz, public-API, distribution, and live
  checks actually run;
- the commit or artifact tested;
- cleanup evidence for external resources;
- residual architectural or external gates, without relabeling them as fixed.

Do not claim production readiness from the reference process or Docker backend,
a single SQLite writer, filesystem-only checkpoints, or point-in-time cloud
evidence. The current residual register is authoritative.
