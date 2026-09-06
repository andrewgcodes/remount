# Session wrap — 2026-09-04: the Windows and Linux merges, and what they exposed

Both host-verification branches are on `main`: PR #10 (Windows) as `6ac65d5`,
PR #11 (Linux, 72 files, +4427/−566) as `e7b15c3`. Six commits followed them.

The theme of this session is that **merging two branches that had each been
verified alone exposed defects neither could see**, and that **running the
product live exposed one that no test suite was going to catch.**

## The merge

PR #11 arrived with nine conflicts. Every one was resolved by keeping both
sides' intent rather than picking a winner:

| File | Resolution |
|---|---|
| `internal/session/log.go` | PR #11's `closeWithPublish(finalCommit, publish)` **and** main's `closeLocal`, which three Windows tests use to release a spill descriptor before the temp directory is removed |
| `internal/client/client.go` | PR #11's deadline-and-backoff convergence loop, bounded by main's attempt count — see below |
| `internal/node/node_test.go` | PR #11's three subscription tests **and** main's `closeNodeRuntimeForTest` (ten call sites) |
| `internal/sim/planb_scale_cursors_test.go` | main's graceful detach and failure count with PR #11's `closed` flag and two-phase connection release |
| `Makefile` | main's `-p 1` and skip list with PR #11's longer timeout |
| `scripts/gvisor-spike.sh` | main's netdev egress rules, which mirror the production `installNetdevDenyTable`, plus PR #11's `setsid` dependency and both UDP probes |

`internal/client/client.go` is the one worth remembering. Taking PR #11's loop
wholesale broke main's `TestNodeCallWaitsForAuthorizationPush`, because it
retried a `CodeConflict` only once and a node with an authorization push in
flight needs several. Applying PR #11's 15-second deadline to *every*
retryable code then stalled a genuinely dead node for 15 seconds on a path
every filesystem and session call takes. The merge is a per-code budget:
convergence gets the long one, a conflict or unreachable node the short one.

## Four defects the Windows lane found, and one it did not

The Windows lane had never run against both branches. It found:

- three tests from PR #11 using a POSIX shell script as a fake `docker` or
  `runsc` binary, which Windows cannot execute;
- the python wheel lane comparing an 8.3 short path (`C:\Users\RUNNER~1\...`)
  against the same directory's long name, and — once that was fixed — a
  carriage return left on a path because Windows Python ends lines with CRLF;
- the Helm golden comparison normalising line endings on the golden side only.
  `helm` on Windows writes CRLF too, and `TrimSpace` reaches only a document's
  edges, so a CRLF on an interior line made two identical documents compare
  unequal and printed a diff whose two sides looked character-for-character
  the same.

The one it did not find is in `internal/session`: **"already closed" has two
sentinels and `errors.Is` does not relate them.** A pipe reports
`os.ErrClosed`; a socket reports `net.ErrClosed`; `errors.Is(err,
os.ErrClosed)` on a closed `net.Conn` is **false**. The EOF paths tolerated
only the first, so an ordinary EOF became a `CodeClosed` failure once the peer
had gone. Nothing about that is Windows-specific — that lane simply happened
to run the port session whose peer went away. `alreadyClosed` now tests both,
and the regression test reproduces the exact Windows error message on macOS.

## What live testing found that CI could not

Running a real deployment on Modal, with real OpenAI traffic through the
broker, surfaced the most serious defect of the session.

**After a node restart, control's log went silent for everything the node
observed.** No `s.opened`, no `cred.used`, no `egress.*`. Workspaces claimed,
sessions ran, and credentials were brokered to a live provider with none of it
recorded — and `remount doctor` still reported `healthy`.

The node keeps its event store in memory, so its producer sequence began again
at 1 on every restart while control still held the watermark from that node
identity's previous life. Control is right to refuse a sequence that already
names a different event. The node was wrong to treat that refusal as transient
and retry the same batch forever: one permanent rejection at the head of the
queue stopped the entire audit stream, silently and indefinitely.

A standalone runs server and node in one process and never shows this. The
topology that hides the bug is the one used on a laptop; the topology that has
it is every real deployment. Full account in
`node-event-delivery-finding-2026-09-04.md`.

## Two findings from the PR #7 audit, confirmed and closed

`deep-bug-hunt-2026-09-03.md` (PR #7, still open) lists eight defects. Two are
now fixed, both High, and both were reached independently here:

- **RBH-002** is the audit-stream defect above. That report found it by
  reading the code; this session found it by watching a live deployment go
  quiet. Two independent routes to one defect is a reason to treat the rest of
  that report as live.
- **RBH-003**: a client picks its own `c_` peer id, and the relay replaces a
  peer that reconnects under an id it already holds. `Authenticate` rebound
  that id to whoever presented it last, so a second authenticated subject could
  claim another's id, evict the holder, and take delivery of frames addressed
  to it — across tenants. `subjectOf` resolves authority from the same map, so
  the claim moved that too. A node id was already pinned to its key; this is
  the same rule for clients.

**Still open from that report,** in the order I would take them: RBH-008
(signalling a Docker-backed session can leave in-container descendants
running), RBH-004 (relay blocks a source peer on a slow destination), RBH-001,
RBH-005, RBH-006 (partly addressed — `internal/fsops` now checks `IsRegular`
after opening), RBH-007.

## ix.dev replaces iximiuz Labs

`internal/provision/ix` targeted **iximiuz Labs** playgrounds through
`labctl`, which is a different company and a different product from **ix.dev**
(Indexable). The tree already disagreed with itself: `handoff-2026-09-03.md`
called it "the ix.dev provisioner" and the credential was `IX_DEV_API_KEY`,
while the package doc, ADR 0062, and the `Playground`/`Account` fields all
described iximiuz. iximiuz support is removed.

The credential and region are now the vendor's own `IX_TOKEN` and `IX_REGION`
rather than names Remount invented, and an unset region leaves ix.dev's own
default in force. Region is passed through; size stays unavailable.

The helper protocol remains, and the reason is recorded in
`ix-dev-native-driver-2026-09-04.md`: **ix.dev has no tags or labels on a VM**,
so there is nowhere to record the tenant and pool that pool reconciliation
filters on. That is a design decision, not a parsing problem. The response
schema itself is known — captured from a real VM after updating the CLI.

## Things worth knowing before you touch this

- `MISTAKES.md` #49 and #50 are from this session. #49: I spent two rounds
  tuning a retry in `nodeCall` before checking that the failing call does not
  use it — a shared error code is not a shared code path. #50: I moved a
  sequence space forward twice and stranded the cursors into it both times,
  and each time it presented as the fix not working rather than as a new bug.
- A loop that exits on an unexamined error exits silently.
  `if err != nil { return }` in a background goroutine is how you lose a
  subsystem with no evidence — which is what hid #50 and, before it, the
  original audit-stream defect.
- `TestPlanbPerfMoveIncompressible` is excluded from the ordinary and race
  lanes now. A shared runner measured 17.9 MB/s against its 20 MB/s floor on a
  commit that runs at 188 MB/s on a dedicated host; the Windows job already
  recorded it as known-failing at 7.6–8.3 MB/s. It measures runner I/O
  contention, not the move path, and still runs in `make test` and `make race`.
- A test that releases a fixture on the first arrival of N concurrent callers
  is timing the scheduler. `TestConcurrentProbesShareOneUncachedRead` did that
  and failed at 3 of 16; it now waits on a coalesce count that is the property
  it is actually asserting.
