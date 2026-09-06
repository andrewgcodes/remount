# Session wrap — 2026-09-04

94 commits, all pushed to `main`. Every gate green at the end: `go test ./...`
no failures, `make race` no races, gofmt, `go vet`, `scripts/lint-locks.sh` and
the `bench` Python suite clean.

**Every required Plan B acceptance row now has an owning proof.** Six optional
rows do not, listed by `go run ./cmd/evidence list --unowned`.

## The one thing to read if you read nothing else

`docs/engineering/gvisor-egress-finding-2026-09-04.md`.

Two lanes had been marked "needs a Linux box" and deferred. They needed a Linux
*daemon*, not a different machine. Docker Desktop cannot host them because its
daemon runs inside a locked-down LinuxKit VM; Colima gives a real Ubuntu VM with
its own daemon, and on Apple Silicon M3+ with macOS 15+,
`colima start --nested-virtualization` gives a real `/dev/kvm`.

Running those lanes for the first time found **eleven defects**. That is the
finding. A lane nobody can run accumulates defects nobody can see, and nothing
about them was exotic — five surfaced within minutes of a host existing.

## What was fixed

**Security.** The gVisor backend advertised `EgressMode: enforced_gateway`
unconditionally and did not enforce egress at all. gVisor with
`--network=sandbox` runs its own userspace netstack and writes frames to the
veth below netfilter's IP hooks, so the nftables `output` chain filtered traffic
the sandbox never generated. Every forbidden destination crossed the boundary;
containment came only from Docker's default `FORWARD DROP`, and with
`FORWARD ACCEPT` — ordinary on routers and many Kubernetes nodes — a sandboxed
process reached 8.8.8.8 and was answered. Now carried by a netdev egress chain.

**The E4 suite was passing for the wrong reason.** Six of its seven denial
checks asserted a command fails, and each failed for lack of a *reply*, not lack
of egress. They would have passed against a completely open network. The suite
now asserts what leaves the workspace, observed with AF_PACKET.

**Four defects in Firecracker, each of which made an entire operation
impossible:** a guest handshake that connected once and raced a 2.5 s boot (no
workspace could materialize); a compatibility fence that contained a Firecracker
log timestamp and so could never equal itself (no checkpoint could be taken); a
`/snapshot/load` with no `network_overrides` (no checkpoint could be restored);
and a teardown that reclaimed the network while the VMM still held its TAP (no
workspace could move).

**Also:** a `SIGSEGV` in `remount ws create --wait` on the error path; a
quarantined workspace that could never be claimed again by anyone; resource
leaks on every killed gVisor run; a Modal deployment that could not start; and a
conformance suite that could not judge a remote implementation because its own
grant expired mid-run.

## What is proven now that was not

The Firecracker lifecycle runs end to end on a real microVM:

```
create   -> claimed, backend=firecracker
exec     -> uname -r = 5.10.223 (host: 6.8.0-117-generic)
snapshot -> --authoritative: consistency=quiesced, 37 MB
restore  -> reaches claimed; the pre-checkpoint file reads back
move     -> node A -> node B, uptime 18s -> 23s, one microVM alive after, not two
```

Also: gVisor E4 and E5 pass on a real candidate; Modal is CONFORMANT over the
public internet at 53 required rows; conformance manifest 1.1.0 adds
`CONF-SESS-009`, which is the first row that requires a session's output to
actually arrive.

## What is left

1. **`.github/workflows/kvm.yml` fails its final step on purpose**, waiting for
   adapters that are wired in `cmd/remount/build_node.go` and have been since
   `7ce54c9`. The premise is stale. It needs someone who can run the workflow;
   this session could not.
2. **Six optional Plan B rows** without owning proof — `evidence list --unowned`.
3. **`internal/conformance`'s manifest has no required row asserting stderr
   delivery**, only stdout. Same class as the gap `CONF-SESS-009` closed.
4. Two performance items with measurements attached, in
   `performance-regressions-2026-09.md`: taking the session-log seal off the exec
   critical path (needs an ADR; ADR 0082 explains why it stays for now) and the
   `connector.Store.lookup` lock, which is a hypothesis rather than a finding.

## Things worth knowing before you touch this

- `MISTAKES.md` #47 and #48 are from this session. #47: a recognisable shape is
  not a diagnosis — I named a lock-across-I/O the cause of a regression it
  contributed 1.4% to, and nearly restructured a durable-commit boundary for it.
  #48: four defects were one mistake, a cleanup path releasing the resource it
  was thinking about and keeping the one underneath.
- The dev skill's debugging playbook gained two entries: read profiles with
  `-peek` rather than `-top`, and when an error names a file or device as busy,
  the name is not the subject — it is the fingerprint of an owner nobody
  released.
- `docs/benchmarks.md` is **generated**. It says so now; it did not, and a
  hand-edit broke a Python test that `go test ./...` does not run.
- `git add <path>` stages a concurrently running agent's edits to that file too.
  Avoiding `-a` does not protect you. This shipped a registry entry naming a
  package that was not in the commit.
