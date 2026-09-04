# Windows-host handoff — 2026-09-03

**Status:** open. This is the least-verified platform in the repository.

Read first: `AGENTS.md`, and the `2026-09 build plan disposition` section of
`docs/engineering/implementation-closure-2026-09-03.md`.

## The situation, stated plainly

Remount ships Windows binaries. `make dist` produces
`remount-windows-amd64.exe` and `remount-windows-arm64.exe`, and the release
workflow publishes both. Seven files carry Windows-specific implementations:

| File | What it implements for Windows |
|---|---|
| `cmd/remount/resize_windows.go` | terminal resize |
| `cmd/remount/autostart_windows.go` | autostart registration |
| `internal/session/process_windows.go` | process control and signalling |
| `internal/fsops/open_windows.go` | jailed file opening |
| `internal/artifact/open_windows.go` | artifact file opening |
| `internal/node/syncdir_windows.go` | directory fsync |
| `internal/node/disk_windows.go` | free-space reporting |

**No test has ever run on Windows.** `.github/workflows/ci.yml` matrixes
`ubuntu-latest` and `macos-latest` only; the sole Windows reference in CI is
cross-compilation inside the release workflow. So every one of those files is
compiled and shipped, and none is exercised.

Until this session the test suite did not even **compile** for Windows:
`GOOS=windows go vet ./...` failed on `internal/node/agentrun_test.go` using
`syscall.Kill`, which does not exist there. That is now fixed — the POSIX
process-group probe moved behind `procgroup_unix_test.go` /
`procgroup_other_test.go`, and `TestHarnessExitIsBounded` skips with an
explicit reason rather than blocking the platform. As of this commit:

```
$ GOOS=windows go vet ./...      # clean, tests included
$ GOOS=windows go build ./...    # clean
```

That is a compile guarantee, not a behavior guarantee. Everything below is
about the difference.

## The rule that governs your work

A check that cannot run is **`unavailable`**, never `passed`. There is no
`skipped-success` outcome. If a Windows test skips, it must say why, and that
reason must be a fact about Windows rather than a way to get green.

Equally: **do not weaken a test to make it pass on Windows.** If a POSIX
guarantee genuinely has no Windows equivalent, the honest result is a skip with
a named reason plus, where one exists, a Windows-specific test asserting the
Windows mechanism (a job object rather than a process group, for example).

---

## 1. First task: find out what actually happens

Nobody knows. Run this and record the result verbatim — a list of real failures
is the single most valuable artifact you can produce:

```powershell
go build ./...
go vet ./...
go test -count=1 -timeout 40m ./...
go test -race -count=1 -timeout 60m ./...
```

Expect failures. They are the deliverable, not a problem. Record them in
`docs/engineering/verification-2026-09.md` with the exact package, test and
error.

Then run the black-box conformance suite, which is the best single measure of
whether a Windows build is a correct Remount implementation:

```powershell
go run ./cmd/conformance --build .
```

It has 67 requirements across nine semantic categories, imports no Remount
implementation package, and reports `CONFORMANT` with 53/53 required rows on
darwin/arm64. **Any required row that fails on Windows is a genuine defect**,
because the manifest asserts protocol semantics, not platform behavior. See
`docs/engineering/plan-b-b4-conformance.md`.

---

## 2. Where I expect trouble, and why

These are predictions from reading the code, not observed failures. Verify each
rather than trusting it.

**Path jailing (`internal/fsops`).** The security boundary is a
descriptor-rooted jail built on `os.OpenRoot`. Windows path semantics differ in
ways that have historically broken exactly this kind of check: `\` and `/` both
separate, drive-relative paths (`C:foo`), device names (`CON`, `NUL`, `AUX`),
alternate data streams (`file.txt:stream`), 8.3 short names, trailing dots and
spaces being stripped, and case-insensitive comparison. The suite has hostile
path tests; make sure they run and add Windows-shaped hostile inputs. **A
traversal that escapes the jail is a security defect, not a portability
nuisance.**

**Symlinks and reparse points.** Snapshot and restore refuse to write through a
symlinked parent. Windows symlinks need privilege or Developer Mode, and
junctions and other reparse points behave differently. Confirm the refusal
still holds for junctions.

**Atomic rename over an open file.** `artifact.ApplyOverlay` lands every
regular file by `rename` into its final path so a reader never sees a partial
file. On Windows a rename over a file another handle has open can fail. If it
does, the atomicity property is at risk — treat that as a correctness finding.

**Process groups and termination (`internal/session/process_windows.go`).**
Session termination, `s.signal`, and the harness kill path assume POSIX process
groups. Windows uses job objects. Verify a killed session actually kills its
descendants; an orphaned child that keeps running is a containment failure.

**The process backend.** `internal/workspace`'s process backend runs sessions
as child processes in the node's filesystem. It is the default backend and the
one every sim test uses. Whether it works on Windows at all is unknown.

**File locking and SQLite.** The control plane is `modernc.org/sqlite`, pure Go,
so it should port, but Windows file locking and delete-while-open differ.
Exercise restart and recovery paths, not just the happy path.

**Line endings.** Deterministic snapshots hash file bytes. If anything in the
pipeline translates `\n` to `\r\n`, a snapshot taken on Windows will not match
the same tree snapshotted elsewhere, silently breaking dedup and byte-identity
claims. Check `.gitattributes` behavior too.

---

## 3. What is realistic to claim

**In scope for a Windows host:** the CLI and client, the control plane, the
relay, the artifact store, the process backend, the conformance suite, and the
SDKs.

**Out of scope, permanently:** gVisor and Firecracker are Linux kernel
mechanisms. `internal/workspace/firecracker/compat.go` already refuses any
non-Linux OS by name. Do not attempt them; see
`docs/engineering/handoff-linux-host-2026-09-03.md`.

**Docker on Windows** can host the docker backend, but a Linux-container daemon
runs in a VM, so it proves the same thing it proves on macOS: composition, not
isolation.

---

## 4. Specific items you can close

**E18-adjacent installation paths (Plan B B32).** The release workflow ships
Windows binaries that nobody has run. Verify `remount-windows-amd64.exe`
executes, that `remount version` reports the expected version, and that the
conformance smoke passes against it. **Do not publish anything** — Plan B §18
explicitly cuts public release actions and the user withheld release authority
on 2026-09-03.

**Windows CI.** Add `windows-latest` to the `ci.yml` matrix once the suite
passes, or add it with the known-failing packages explicitly excluded and a
comment naming each exclusion. An excluded package is visible debt; a platform
nobody tests is invisible debt.

**A Windows section in `docs/operations.md`.** It currently documents Unix
paths and assumptions throughout.

---

## 5. Useful facts about this codebase

- Go 1.27, `CGO_ENABLED=0` everywhere, so the binary is static and there is no
  toolchain problem to solve.
- The CLI env vars are `REMOUNT_SERVER` (not `REMOUNT_URL`) and `REMOUNT_TOKEN`.
- `remount standalone --listen 127.0.0.1:PORT --data DIR` runs a full control
  plane plus one node in one process — the fastest way to get something real
  running.
- Production mode additionally needs `REMOUNT_MASTER_KEY` (base64, 32 bytes).
- `make race` is not optional in this project: `AGENTS.md` states that three
  real bugs were only visible under the race detector. Run it on Windows too.
- `internal/sim` is the whole system in one process with fault injection. If
  the process backend works on Windows, most of the sim suite should run, and
  it is the highest-value signal available.

## What to do with your results

Add entries to `docs/engineering/verification-2026-09.md` — exact command,
observed result, marked verified / verified but bounded / unavailable. Record
defects with a failing test rather than only prose. If you close a Plan B
scenario, wire it into `internal/evidence/registry.go` by setting `Owner` and
`Argv`, and add its id to `wiredScenarios` in `registry_test.go`; that guard
exists so wiring is a deliberate act.

Do not edit historical findings to make them look closed.
