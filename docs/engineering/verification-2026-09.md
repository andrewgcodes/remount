# External and live verification ledger — 2026-09

This file records point-in-time external verification runs: the exact command,
the provider identifiers used, the observed result, and the teardown. It is a
ledger, not a status page. Every entry is one of **verified**, **verified but
bounded**, **unavailable (externally gated)**, or **not attempted**.

An entry that says "unavailable" means the proof could not run. Per
`docs/engineering/handoff-2026-09-03-codex-wrap.md` §6, a skipped secret-gated
job is unavailable, never healthy. Do not promote an entry here to a product
claim without the evidence the entry itself names.

Credentials come from the git-ignored root `.env`. No secret value is recorded
in this file, and no run below printed one.

---

## 2026-09-04 - native Windows host verification

**Status: required conformance verified; two performance gates and two
full-suite Docker OpenCode lanes remain known failing and are visible CI
debt.**

Host: Windows Server 2022 amd64. Go: 1.27.1. Race C toolchain: MinGW-w64
16.1.0. Final post-merge verification used `origin/main` at `51185dd`.

### Final command results

| Command | Result |
|---|---|
| `go build ./...` | passed |
| `go vet ./...` | passed |
| `go test -count=1 -timeout 40m ./...` | failed `remount.dev/remount/internal/sim.TestExecRoundTripCostOfTheDurableSessionTier` at 1.18x versus the 1.2x skipped-success guard and `TestPlanbPerfMoveIncompressible` at 9.0 MB/s versus 20 MB/s; earlier full-suite runs also exposed the two Docker OpenCode failures recorded below |
| `go test -race -count=1 -timeout 60m ./...` | failed `TestPlanBOpenCodeAgentTranscriptApprovalAndResume` with a 12,730-byte node transcript versus a 12,465-byte mirror, `TestPlanBOpenCodeDeterministicModelLane` when its session stream did not close in ten minutes, and `TestPlanbPerfMoveIncompressible` at 7.4 MB/s |
| `go test -p 1 -count=1 -timeout 40m -skip '^(TestExecRoundTripCostOfTheDurableSessionTier\|TestPlanbPerfMoveIncompressible\|TestPlanBOpenCodeAgentTranscriptApprovalAndResume\|TestPlanBOpenCodeDeterministicModelLane)$' ./...` | passed after the final upstream merge; package serialization isolated the sim timing lanes from cross-compilation and artifact-install load |
| `go test -race -p 1 -count=1 -timeout 60m -skip '^(TestPlanbPerfMoveIncompressible\|TestPlanBOpenCodeAgentTranscriptApprovalAndResume\|TestPlanBOpenCodeDeterministicModelLane)$' ./...` | passed after the final upstream merge; package serialization avoided the Windows asynchronous file-I/O exhaustion observed when race packages ran concurrently |
| `go run ./cmd/conformance --build .` | 60 passed, 0 failed, 8 unavailable; all 53 required rows passed; cleanup verified |
| `remount-windows-amd64.exe version` | `remount v0.0.0-20260904095415-3e577228074c` |
| `go run ./cmd/conformance --binary .\remount-windows-amd64.exe` | 60 passed, 0 failed, 8 unavailable; all 53 required rows passed; cleanup verified |
| `./scripts/lint-locks.sh .` | passed |

The raw ordinary-suite failure is retained exactly:

```text
--- FAIL: TestPlanbPerfMoveIncompressible (8.45s)
    planb_perf_move_test.go:114: incompressible: checkpoint phase 3.537s
    planb_perf_move_test.go:114: incompressible: restore phase 3.33s
    planb_perf_move_test.go:114: incompressible: chunks=0 uploaded_chunks=0 uploaded_bytes=0
    planb_perf_move_test.go:114: incompressible: 64 MiB moved in 6.939s (9.2 MB/s), format=tar
    planb_perf_move_test.go:116: incompressible move ran at 9.2 MB/s, want at least 20 MB/s
FAIL remount.dev/remount/internal/sim 468.240s
```

Classification: **confirmed Windows-host performance defect**, not a passed
benchmark and not a POSIX-only boundary. Windows CI runs every other test and
names this exact exclusion in the workflow and job summary.

After integrating `d938d7b`, three focused reruns still failed at 7.6, 8.0,
and 8.3 MB/s against the 20 MB/s gate.

The final ordinary suite also found a second Windows performance-lane boundary:

```text
--- FAIL: TestExecRoundTripCostOfTheDurableSessionTier (2.25s)
    exec_seal_cost_test.go:104: exec round trip p50: artifact tier off 31ms, on 37ms, ratio 1.18x (n=24 each)
    exec_seal_cost_test.go:115: the artifact tier cost nothing (1.18x: 31.0813ms -> 36.7439ms): the node is almost certainly not sealing session logs at all, so this lane is measuring two identical configurations and cannot detect the regression it exists for
```

Three focused ordinary runs measured 1.22x (pass), 1.15x (fail), and 1.14x
(fail). Three focused race runs measured 1.47x, 1.44x, and 1.32x and passed.
The portable behavior is covered elsewhere; this timing control cannot
reliably distinguish the seal cost from Windows process-start overhead in an
ordinary build. It remains a named ordinary Windows CI exclusion, not a pass;
the race lane still runs it.

The first post-integration `go run ./cmd/conformance --build .` also exposed
the same asynchronous audit race in `CONF-BIND-003` that an earlier fix had
closed for `CONF-BIND-002`:

```text
failed CONF-BIND-003 the broker refused the request
(403 remount broker: egress to denied.conformance.invalid:443 is not permitted
for this workspace) but recorded no egress.denied on the workspace stream
```

The denied request completes before the canonical event append is necessarily
visible. `CONF-BIND-003` now polls for the event using the same bounded,
context-aware rule as `CONF-BIND-002`; five consecutive built-binary
conformance runs passed all 60 available rows.

### Post-review CI findings

Pull-request CI exposed five additional defects. Each failure is retained here
rather than being folded into a passing summary.

**Linux gVisor setup failed before the sandbox started:**

```text
Error: Nexthop has invalid gateway.
```

The failing command installed the namespace default route while its veth
endpoint was still down. WSL reproduced the same kernel rejection. Bringing
only the guest endpoint up before adding the route succeeds while the host
endpoint remains down, so no traffic can cross before the deny-all policy and
sandbox are ready. `bash -n scripts/gvisor-spike.sh` passed after the ordering
fix.

The next CI run reached the denial probe and exposed that the spike still
installed only the ineffective inet output chain documented by the Linux
verification:

```text
PASS: broker reachable
PASS: direct IPv4 TCP denied
PASS: IPv6 denied
FAIL: UDP unexpectedly succeeded
```

The spike now mirrors the production boundary with a netdev egress chain on
the namespace veth. Its final counter-and-drop rule directly proves the
connectionless UDP frame reached the deny-first policy; unlike `nc -u` exit
status, that assertion does not confuse a locally accepted send with escaped
traffic. The corrected denial probe passed every E4 check, then exposed a
separate cleanup defect: runsc left its state-root `null-netns` mount attached,
so recursive removal failed with `Device or resource busy`. Cleanup now
unmounts that runsc-owned mount after deleting the sandbox and before removing
the temporary state directory.

**A MinIO precondition response poisoned the next write connection.** The
`s3-compatibility` lane repeatedly failed `TestMinIOIntegration` in
`internal/artifact/s3` with `EOF` or `http: server closed idle connection` on
the private conditional probe. Local repetitions reproduced both errors.
MinIO closes some connections after returning the expected HTTP 412 response;
depending on close timing, that connection can briefly remain selectable for
the next non-replayable PUT. The client does not retry an ambiguous write.
Conditional PUTs now opt out of connection reuse, so a precondition response
cannot poison the next write. A transport-level regression test pins that
behavior.

**The first compatibility follow-up exposed three Ubuntu test-environment
defects.** `TestB32TheStaticBinaryInstallsAndPassesTheBlackBoxSmoke` reached
the package-wide five-minute timeout while real builds and smoke tests were
competing across packages, so the ordinary suite now retains every assertion
with a ten-minute package budget. Helm 3.21.4 on the current runner
rendered the committed chart without two insignificant inter-document blank
lines; CI and release verification now pin Helm 3.20.0 and the reviewed golden
matches that output. `TestPlanBOpenCodeDeterministicModelLane` failed
`planBWriteCatalog` with `openat .config/opencode/.remount-...: permission
denied` because the earlier version probe had let container-root create
OpenCode's workspace config directory. The pin probe now uses a temporary
`HOME`; the actual model run still uses the workspace home and the same
catalog and isolation assertions.

**The rerun exposed additional failures while heavyweight packages competed
for the same host.** The ordinary install lane lost its
standalone node before destroy and reported `workspace source node ... is
unavailable; destroy remains uncommitted`. The race conformance target
reported a stale Bob grant in `TestE8PrincipalRevocationComposes`, 200
post-failover reattach deadlines in `TestHandoffScaleAndControlFailover`, and
`openat .local/state/opencode/locks/...: permission denied` while the Plan B
lane snapshotted OpenCode state. Focused race reruns of those three simulation
tests passed. The ordinary and race-conformance targets now serialize
packages with `-p 1`, retaining all tests and concurrency within each package
while removing competition between unrelated heavyweight packages.

**Package serialization did not resolve the OpenCode snapshot failure.** The
next Ubuntu conformance run failed at the same lock file. OpenCode stores
durable session data under `.local/share/opencode`, which the recipe already
declares as movable state, but derives process-coordination locks from
`XDG_STATE_HOME`. With workspace `$HOME`, those transient locks landed inside
the snapshot tree and could be replaced by container-root after the backend's
ownership handoff. The recipe now keeps XDG state in a workspace-specific
directory under `/tmp`; durable OpenCode state, the workspace tree, and the
leak-scan assertions remain unchanged. A focused Docker run passed with an
explicit assertion that `.local/state/opencode` never entered the workspace.
A five-run stress attempt reproduced the separately recorded session-stream
close failure before completing; it did not reproduce the lock-file failure.

**The following CI run exposed four independent regressions.** The exact jobs
were macOS `101053506318`, Ubuntu `101053506160`, Windows `101053506144`, and
race conformance `101053505886`.

- macOS and Ubuntu both failed `TestE8PrincipalRevocationComposes` when Bob's
  newer grant reached a node before the corresponding authorization-revision
  push. Treating every revision mismatch as stale returned
  `unauthorized: grant authorization revision or tenant is stale`. Nodes now
  distinguish an older grant (`unauthorized`) from a control-ahead grant
  (`conflict`); the client discards the grant and retries for a bounded
  interval while the node receives the push. The existing E8 revocation
  assertion remains unchanged, including node-side closure evidence. The
  focused client regression passed ten repetitions, node authorization tests
  passed ten ordinary and five race repetitions, and E8 passed fifty ordinary
  and ten race repetitions.
- Windows failed five B32 install lanes with `executable file not found in
  %PATH%`: the release artifact was `remount-windows-amd64.exe`, but the
  isolated install copied it to `remount`. Installed Windows binaries now
  retain the `.exe` suffix. The linked-npm control separately expanded its
  prefix beneath a literal `${APPDATA}` because the intentionally scrubbed
  environment also removed Windows package-manager roots; the clean
  environment now retains only the required Windows runtime and user-data
  variables. Focused executable-name, environment, static install, Go-module,
  npm, and Python-wheel regressions passed where their named prerequisites
  were available.
- Windows `TestHelmGoldenTemplateIsCurrent` displayed identical lines but
  failed at line 14 because the checkout held CRLF bytes and Helm emitted LF.
  Golden comparison now normalizes CRLF to LF before comparing content; the
  focused policy tests passed.
- Race conformance pruned 992 of 1,200 events, reported oldest sequence 993,
  then advanced the asynchronous retention watermark before the recovery
  request. Recovery now follows each explicit increasing `CodeEvicted`
  watermark until it reaches the retained window; it still rejects silent
  truncation. The same job spent fifteen minutes in sequential cursor
  teardown after one partitioned detach request never returned. Scale teardown
  now issues bounded detaches concurrently and then closes every client and
  connection, while the existing peer, goroutine, heap, and descriptor release
  assertions remain unchanged. Full-width ordinary and race cursor runs
  passed. The security-focused `make conformance` lane now excludes the
  separately exercised Plan B and handoff scale benchmarks so its twenty-minute
  budget measures conformance rather than benchmark teardown.

The same Windows job failed `TestHandoffScaleAndControlFailover`: after a
16-second control restart, at least one of 200 live sessions exceeded the
client's fixed 30-second reattach deadline. This remains a named Windows CI
exclusion and is not reported as passed. A focused run on the verification VM
passed in 86.5 seconds, confirming that the failure depends on hosted-runner
load rather than disproving the observed deadline miss.

The Windows follow-up passed every Go package, then the job itself failed
because its nested public-SDK module step ran `npm ci` in
`integration/publicsdk`, which contains a Go module and no `package.json`.
That step now runs the same `go test -count=1 ./...` contract as the Makefile;
the focused native-Windows run passed.

The next Linux race-conformance run reached all required packages but also ran
`TestPlanbPerfMoveIncompressible`, which measured 18.1 MB/s against its 20 MB/s
performance gate. The `make conformance` target now excludes all explicitly
named `TestPlanbPerf*`, durable-tier cost, Plan B scale, and handoff-scale
measurements. Those tests remain in ordinary and race package coverage; the
conformance lane retains the hostile-input and compromised-workspace tests it
was created to enforce.

The following ordinary Ubuntu package run, job `101088227997`, failed the same
hosted-runner boundary previously observed on Windows:
`TestHandoffScaleAndControlFailover` restarted control in 9.84 seconds, then
many of its 200 simultaneous post-restart session reattachments exceeded the
30-second deadline. Hosted macOS passed and a focused Windows run passed, but
that does not invalidate the Ubuntu result. The hosted ordinary and race lanes
now exclude that exact scale test and name the debt in the job summary; it
remains in `make test`, `make race`, and focused developer runs.

The next ordinary macOS package run, job `101101072814`, exposed the other
already identified hosted timing boundary:
`TestExecRoundTripCostOfTheDurableSessionTier` measured 12 ms with the artifact
tier disabled and 13 ms enabled (1.06x), so process-start overhead masked the
seal-cost signal. Hosted ordinary and race lanes now also exclude that exact
measurement and publish both exclusions in the job summary. The assertion and
test remain unchanged in developer package targets.

## Current-main compatibility recheck

After the Windows pull request merged, `origin/main` was
`51185ddeda5c4451c024a1fd7c660e4cc271a52b`. That history includes the Windows
work and four commits that landed concurrently: the Helm document comparator,
safe retry of an S3 request the transport never wrote, host-sized simulation
loads with container-user catalog writes, and the September 4 session handoff.

The merged Helm test retains both document-level semantic comparison and
CRLF/LF normalization. The S3 suite retains both the conditional-write
connection-closing regression and the rewindable unused-connection retry
regression. The OpenCode lane retains the assertion that transient state does
not enter the workspace while writing its model catalog as the container user.
Focused policy and S3 package tests passed, and the deterministic OpenCode model
lane passed in 46.128 seconds.

The first post-merge CI run exposed one integration defect in the composed S3
test: `staticcheck` reported `U1000` for an unused `bodies` field left on
`staleOnceTransport`. The field was test scaffolding with no behavior and was
removed; the focused S3 package and its static analysis were rerun afterward.

On the merged history, native Windows passed build, vet, the complete
serialized ordinary package lane with the five documented Windows exclusions,
the nested public-SDK module, and the complete serialized race package lane
with its four documented exclusions. Formatting, lock-discipline lint, and
diff checks also passed. Built-binary conformance remained:

```text
60 passed, 0 failed, 8 unavailable of 68 requirements
required         53 passed, 0 failed, 0 unavailable
capability-gated 7 passed, 0 failed, 7 unavailable
extension        0 passed, 0 failed, 1 unavailable
cleanup: verified
```

**A Windows-hosted Docker conformance run selected Windows commands for a
Linux workspace.** Command selection was compiled from the runner's OS, so
Docker sessions received `cmd.exe` even though they execute inside Linux. The
selector now follows the workspace backend: native Windows process workspaces
use Windows programs, while Docker, gVisor, and Firecracker use POSIX programs.
The eight session requirements passed against a Linux Docker workspace hosted
on this Windows VM.

**The development binary selected an invalid Docker image reference:**

```text
image "ghcr.io/andrewgcodes/remount-workspace:v0.0.0-20260904095559-f3fb13569f00+dirty"
docker: invalid reference format
```

Go pseudo-versions and dirty build versions are not published release image
tags, and `+dirty` is not valid in a Docker tag. Only numeric
`vMAJOR.MINOR.PATCH` versions now select a versioned workspace image; every
development, pseudo-version, or dirty build selects `latest`.

**macOS rejected a Windows-motivated cwd assertion.** `TestExecEnvAndCwd`
compared `/var/...` with the equivalent `/private/var/...` literally. Both
paths are now resolved through `filepath.EvalSymlinks` before comparison, while
the Windows CRLF normalization remains in place.

Review also identified a real Job Object registration window. Starting a
Windows process before assigning it to the Job Object allowed it to spawn an
uncontained descendant in between. Windows commands now start suspended, are
assigned to the kill-on-close Job Object, and only then have their primary
thread resumed. `TestWindowsProcessIsContainedBeforeItCanSpawn` proves the
command cannot create its immediate child before registration; ten ordinary
and three race repetitions passed together with the descendant-termination
test.

### Confirmed defects found and fixed

| Area | Exact regression or conformance signal | Pre-fix behavior |
|---|---|---|
| Conformance launcher | `internal/conformance.TestLocalExecutableNameUsesWindowsSuffix` | temporary binaries were started without `.exe` |
| Required session semantics | `CONF-SESS-004`, `CONF-SESS-006` | the runner attempted `/bin/cat` and `/bin/sh`, which do not exist on Windows |
| Verbatim stdout conformance | `CONF-SESS-009` and `internal/conformance.TestWindowsEchoProgramPreservesLiteral` | after the manifest grew to 1.1.0, `cmd.exe` echoed `"conf-sess-009: stdout must arrive verbatim."` with literal quotes because the command and payload were passed as separate arguments |
| Filesystem jail | `internal/fsops.TestWindowsHostilePathsAreRejectedBeforeFilesystemAccess` | drive, UNC, device, ADS, reserved-name, and trailing-dot/space inputs were not rejected as Windows path aliases before access |
| Junction containment | `internal/fsops.TestWindowsJunctionParentCannotEscapeRoot` | no native regression proved a reparse-point parent could not escape the workspace |
| Artifact namespace | `integration/storage.TestTenantArtifactPhysicalPath` and volume/encrypted-store tests | logical IDs containing `:` produced `The filename, directory name, or volume label syntax is incorrect.` |
| Key confidentiality | `internal/artifact/encrypted.TestFileMasterKeyRejectsWindowsEveryoneReadACL` | Windows key permission validation was a no-op and accepted an Everyone-readable DACL |
| Process containment | `internal/session.TestKillWorkspaceTerminatesWindowsDescendants` | native sessions had no Job Object descendant containment |
| Git byte identity | repository materialization tests | managed checkouts returned `hello\r\n`, `yes\r\n`, and `1\r\n` instead of the committed LF bytes |
| Volume retarget defense | `internal/volume.TestDetachRefusesRetargetedMountPathAndRetainsProof` | the generic non-Unix directory identity returned `(0, 0, nil)`, so a retarget was not detected |
| SQLite locking | `internal/control/replicate.TestSQLiteDatabaseMustCloseBeforeWindowsDelete` | no test covered Windows delete-while-open behavior |
| Broker errors | broker classification tests | Winsock refused/reset errors were not mapped to the stable broker error classes |
| Conformance audit visibility | `CONF-BIND-002` | an immediate event-tail read could race the canonical audit append; the denied request was observed before its `leak_blocked` event |
| Conformance unbound-audit visibility | `CONF-BIND-003` | an immediate event-tail read could race the canonical audit append; the denied request was observed before its `egress.denied` event |
| Race cleanup | `internal/session.TestE11FastProducerReplaysEverySequenceAcrossTiers` | `TempDir RemoveAll cleanup: unlinkat ...\session.log: The process cannot access the file because it is being used by another process.` |
| Race process registration | `internal/sim.TestHandoffScaleAndControlFailover/real-peers-and-durable-restart` | fast processes could exit while Job Object assignment returned `Access is denied`, surfacing as `contain process tree: Access is denied.` |
| Pre-containment process execution | `internal/session.TestWindowsProcessIsContainedBeforeItCanSpawn` | a process started running before Job Object assignment and could spawn descendants outside the containment boundary |
| Job Object descendant test synchronization | `internal/session.TestKillWorkspaceTerminatesWindowsDescendants` | the PID file could be observed after creation but before `WriteAllText` stored the PID, producing `strconv.Atoi: parsing "": invalid syntax` |
| Tiered session restart test synchronization | `internal/sim.TestE11TieredSessionRecordSurvivesNodeRestart` | collecting the terminal chunk did not prove the asynchronous `session.log.committed` completion event was durable before the simulated node death; 2 of 10 focused runs failed with `internal: session: incomplete archived record` |
| Docker OpenCode installation in the full suite | `internal/sim.TestPlanBOpenCodeAgentTranscriptApprovalAndResume` | the Docker workspace fenced at its local lease safety deadline while installing pinned OpenCode; the install session returned `context deadline exceeded` after fifteen minutes, while a focused ordinary run passed in 77 seconds |
| Docker session completion in the full suite | `internal/sim.TestPlanBOpenCodeDeterministicModelLane` | after a client cut, the Docker workspace fenced at its local lease safety deadline but the session chunk stream did not close; a full race command timed out after one hour with the test blocked for 56 minutes, and an ordinary full-suite rerun hit the bounded ten-minute context; focused ordinary and race runs passed in 58 and 44 seconds |
| Install artifact source scan | `integration/installs.TestB32TheSourceTreeScanCatchesAnUntrimmedBinary` | Go recorded source paths with `/`, so a detector searching only for the host's `\` form missed an untrimmed binary |
| Local Go module proxy | `integration/installs.TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer` and `TestB32AReplaceIntoTheCheckoutIsCaught` | Windows paths produced invalid `file://C:%5C...` proxy URLs |
| Linked npm control | `integration/installs.TestB32ALinkedNpmInstallIsCaught` | npm used a Windows junction, but the detector treated its installed path as an ordinary copied directory |
| Backend-aware conformance programs | eight `CONF-SESS-*` rows against a Windows-hosted Docker workspace | compile-time Windows selection sent `cmd.exe` and PowerShell programs into Linux containers |
| Development Docker image selection | `internal/workspace.TestDefaultImageTracksRelease` | Go pseudo-versions and `+dirty` versions were treated as published image tags; Docker rejected the resulting reference |
| All-package resource pressure | `integration/installs.TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer`, `integration/reproducible.TestB31BinariesAreAFunctionOfTheSourceAlone`, `internal/sim.TestHandoffScaleAndControlFailover`, `TestAgentEndToEndTurnsAndTranscript`, and `TestAgentSurvivesNodeLoss` | parallel packages produced Windows `The supplied user buffer is not valid for the requested operation` writes, scale reattach deadlines, and lease-fencing failures; focused reruns passed or reached their named prerequisite boundary, so Windows CI serializes packages with `-p 1` while retaining concurrency coverage within each package |

The concurrent `c47b4bb` integration also exposed
`integration/policy.TestInfrastructureDoesNotOwnRuntimeFacts` on every host:
a Terraform validation error named the forbidden command as operator guidance.
The message now describes the supported move command without looking like an
imperative runtime invocation to the ownership scanner.

The path-jail suite now covers both separators, `..`, drive-relative and
drive-absolute paths, UNC and device paths, `CON`/`NUL`/`AUX`, ADS syntax,
case aliases, trailing dots/spaces, junction escapes, symlink escapes, and 8.3
aliases when the host generates them. The 8.3 row reports a named skip when
short-name generation is disabled.

Artifact regressions cover rename-over-open behavior, read-only destinations,
byte-exact LF snapshots, Windows-safe physical names, deterministic slash-form
archive names, and parent symlink/reparse-point containment. No hostile input
escaped the disposable workspace.

### Unavailable native Windows boundaries

- Unix PTYs are unavailable because the process backend has no ConPTY
  implementation. `TestPTYAndStdin` skips with that named reason.
- Native process-workspace recipe and ACP launcher scripts requiring a POSIX
  shell are unavailable; Linux Docker workspaces still use `/bin/sh`.
- Firecracker and gVisor are Linux-kernel backends and were not attempted.
- Portable Go exposes no Windows equivalent to parent-directory fsync. The
  Windows directory-sync implementation is an explicit no-op, not a durability
  claim.
- Read-only mounted volumes are unavailable in the native process backend.
- Conformance reported eight unavailable rows:
  `CONF-AUTH-007`, `CONF-SESS-007`, `CONF-APP-003`, `CONF-APP-004`,
  `CONF-APP-005`, `CONF-AGT-005`, `CONF-EVT-009`, and `CONF-EVT-010`.

**Teardown, verified.** Both conformance runs reported `cleanup: verified`.
All standalone processes exited, Job Objects terminated descendants, and all
disposable workspace, volume, SQLite, artifact, and session-log handles were
closed before temporary-directory removal.

---

## 2026-09-03 — E1 live broker substitution and leak blocking (OpenAI)

**Status: verified.**

Host: Darwin 25.3.0 arm64. Binary: `b213017`, built with `make build`.
Backend: `process`. Server: `remount standalone`, disposable data directory
under the session scratchpad, destroyed at the end of the run.

Binding file (the secret is an `$ENV` reference; the literal never touches
disk):

```json
[
  {
    "id": "b_openai",
    "secret": "$OPENAI_API_KEY",
    "destinations": ["api.openai.com"],
    "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
    "ttl_sec": 900
  }
]
```

```sh
remount standalone --data <scratch>/data --bindings <scratch>/bindings.json \
  --allow api.openai.com
remount ws create --name live --binding b_openai \
  --env OPENAI_API_KEY=ref:b_openai \
  --env OPENAI_BASE_URL='${REMOUNT_BROKER}/d/api.openai.com/v1'
```

### What the workspace holds

```
KEY=sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY
BASE=http://127.0.0.1:62109/c/<per-workspace-token>/d/api.openai.com/v1
```

The workspace holds the placeholder and a per-workspace broker address. It
never holds the credential.

### The call still succeeds

`GET $OPENAI_BASE_URL/models` from inside the workspace, sending the
placeholder as its bearer, returned a real OpenAI model list
(`text-embedding-ada-002`, …). The broker substituted the real key at the
network edge.

Audit record:

```
cred.used      decision=substituted  binding=b_openai  host=api.openai.com:443
               method=GET path=/v1/models status=200
               reason="credential released to outbound transport"
egress.allowed decision=allowed      host=api.openai.com:443 status=200
```

### The credential is nowhere the workspace can reach

Scanned for the literal key; counts only, the value was never printed.

| Location | Files containing the real key |
|---|---:|
| workspace environment (`env`) | 0 |
| workspace root (recursive) | 0 |
| `/tmp` inside the workspace | 0 |
| host data directory (`<scratch>/data`, recursive) | 0 |
| server log | 0 |

Only the binding **name** appears in event payloads. No event payload carries a
secret value.

### Leak blocking, proven two ways

1. **Unbound host, no allow rule.** A synthetic canary
   (`sk-proj-SYNTHETICCANARY…`, generated for this run, same shape as a real
   key, not a real credential) was written into the workspace and sent to
   `https://example.com/`. The connection was refused (curl exit 56):

   ```
   egress.denied  decision=denied  host=example.com:443  method=CONNECT
                  reason="CONNECT requires an explicit allow or typed CONNECT rule"
   ```

2. **Allowed host that the binding is not bound to.** With `httpbin.org` added
   to `--allow`, the workspace sent its **placeholder** to
   `${REMOUNT_BROKER}/d/httpbin.org/post`. The broker returned **HTTP 403**:

   ```
   egress.denied  decision=leak_blocked  host=httpbin.org:443  binding=b_openai
                  reason="placeholder for b_openai sent to httpbin.org:443"
   ```

   This is the important case: the destination was permitted by the allow list,
   and the request was still refused because the placeholder for `b_openai` was
   leaving toward a host that binding is not destined for.

**Canary discipline.** The canary planted in this run was synthetic and
generated at run time. Per §6 of the wrap handoff, the real credential is never
planted as a canary again.

**Teardown.** Both workspaces destroyed, both servers stopped, scratch data
directories removed. Nothing was written into the repository.

---

## 2026-09-03 — E2B pool lifecycle

**Status: unavailable (externally gated). Credential verified.**

```sh
set -a && . ./.env && set +a
go test -tags integration -count=1 -v -run TestLiveProvisionerLifecycle ./internal/provision/e2b
```

```
live_test.go:16: SKIPPED unavailable: missing required environment names
  [REMOUNT_E2B_TEMPLATE REMOUNT_VENDOR_SERVER_URL REMOUNT_VENDOR_ENROLL_TOKEN
   REMOUNT_VENDOR_BINARY_URL REMOUNT_VENDOR_TENANT REMOUNT_VENDOR_POOL]
--- SKIP: TestLiveProvisionerLifecycle (0.00s)
```

The skip is correct behavior and is **not** evidence the driver works.

**Bounded credential probe** (read-only, no resources created, no cost):

| Request | Result |
|---|---|
| `GET https://api.e2b.app/sandboxes` | `200`, body `[]` |
| `GET https://api.e2b.app/templates` | `200`, **0 templates** |

So `E2B_API_KEY` in `.env` is live and authorized. The gate is real and not a
guessable variable: the account has **no** `remount-node` template, so
`REMOUNT_E2B_TEMPLATE` has no correct value to supply. Closing this entry
requires, at minimum:

- a built and published E2B template containing the remount node binary;
- a publicly reachable `REMOUNT_VENDOR_SERVER_URL` (a laptop `127.0.0.1`
  control plane cannot be dialed back by a vendor sandbox);
- a one-time `REMOUNT_VENDOR_ENROLL_TOKEN` and a downloadable
  `REMOUNT_VENDOR_BINARY_URL`; and
- a disposable `REMOUNT_VENDOR_TENANT` / `REMOUNT_VENDOR_POOL`.

Do not invent these identifiers. Record them here when a real run is performed.

---

## 2026-09-03 — Remount deployed to Modal, live, and judged conformant

**Status: verified.**

This entry supersedes nothing below it: the Modal *pool provisioner* lane
(`internal/provision/modal`) remains unavailable for the reasons in the next
section. What was verified here is the repository's own Modal deployment
(`deploy/modal_app.py`), which is a different thing — a control plane and node
running on Modal rather than Modal supplying nodes to a control plane.

The `modal` CLI (1.5.5) was installed into a throwaway virtualenv and
authenticated from the git-ignored root `.env`. No secret was printed.

```sh
make modal-binary                              # dist/remount-linux-amd64
modal secret create remount-control REMOUNT_TOKEN=<generated>
REMOUNT_MODAL_APP=remount-planb make modal-deploy
```

The deployment came up with a real public HTTPS endpoint:

```
https://action-dev--remount-planb-control.modal.run
GET /healthz → 200 {"ok":true,"peers":1,"security_mode":"standalone",
                    "security_ready":true,"serving":true}
```

One node attached and reported itself honestly:

```
linux/amd64  17 CPU  225104 MiB  process  labels map[vendor:modal zone:cloud]
```

A workspace was created and executed real work on that hardware:

```
running on: Linux 4.19.0-gvisor x86_64
cpus: 17
hello-from-modal
```

**The black-box conformance suite judged the live deployment over the public
internet**, in `--endpoint` mode with no source access to the target:

```
CONFORMANT: https://action-dev--remount-planb-control.modal.run
  53 passed, 0 failed, 14 unavailable of 67 requirements in 55.88s
    required         52 passed, 0 failed, 0 unavailable
    capability-gated  1 passed, 0 failed, 13 unavailable
```

This is the strongest form the suite supports: an already-running endpoint,
judged only through public protocol, HTTP and event surfaces.

**Honest limits.** The kernel string says `gvisor` because Modal runs its
containers under gVisor. That is *Modal's* isolation of the whole node, not
Remount's `enforced_gateway` backend, and it earns no capability: per Plan B
§7.2 a provider lane that cannot run the inner gVisor contract "must not claim
`isolated` or `multi_tenant`". The deployment ran in `standalone` security mode
with a shared token, which is a development posture, not production. One node
means placement and movement were not exercised.

**Teardown, verified.** Workspace destroyed; `modal app stop remount-planb`
(app shows `stopped`); the `remount-control` secret and the
`remount-planb-data` volume deleted; the public endpoint now answers `404`; the
local token file removed. The pre-existing `remount-openai` secret,
`remount-demo-data` volume and `action-evals` app were left untouched.

## 2026-09-03 — a workspace moved between Modal cloud and a laptop, live

**Status: verified.** This is the product's central claim exercised across two
providers, two operating systems and two CPU architectures.

A control plane was deployed to Modal with a public HTTPS endpoint, and **this
laptop was enrolled as a second node against it**, giving one fleet spanning
cloud and local hardware:

```
n_06g6m8csgjdecnd7zy5yzy7qa0  true  linux/amd64   map[vendor:modal zone:cloud]
n_06g6m8dfxch40da7s7jy62tbww  true  darwin/arm64  map[vendor:local zone:laptop]
```

A workspace was created on the cloud node, then moved to the laptop, then moved
back. Each step appended to one file, and each step read what the previous host
had written:

| Step | Host | `uname -srm` | Generation |
|---|---|---|---:|
| 1 | Modal cloud | `Linux 4.19.0-gvisor x86_64` | 1 |
| 2 | this laptop | `Darwin 25.3.0 arm64` | 2 |
| 3 | Modal cloud | `Linux 4.19.0-gvisor x86_64` | 3 |

Final contents of `journal.txt`, read on the cloud node after the round trip:

```
step-1-on-Linux
step-2-on-Darwin
step-3-back-on-Linux
```

Each move published a real checkpoint (`restored_from=art_sha256:89bd1d93de015…`
then `art_sha256:a3f8f9248c94c…`) and advanced the generation exactly once. The
filesystem crossed x86_64 → arm64 → x86_64 and Linux → Darwin → Linux intact.

**Honest limits.** The moves restart processes: these are filesystem
checkpoints, and `ws.moved` correctly reported `processes: restarted` rather
than claiming continuity it did not have. No AI harness ran on the cloud node,
because the deployed demo (`deploy/modal_app.py`) allows egress to
`api.openai.com` but configures no binding, so it has no credential to broker —
a real gap recorded below. Security mode was `standalone` with a shared token.

**Teardown, verified.** Workspaces destroyed, local node stopped, app stopped
and removed, `remount-control` secret and `remount-migrate-data` volume
deleted. `modal app list` shows no deployed remount app; `modal secret list`
shows no `remount-control`.

## 2026-09-03 — an AI agent on Modal, brokered, and what the migration cost

**Status: verified for the agent and the broker; the large-workspace migration
is an open performance finding.**

The gap recorded below — that `deploy/modal_app.py` allowed egress to
`api.openai.com` but configured no binding, so no harness could run there — is
now **closed**. The control function writes a `b_openai` binding whose secret is
the `$OPENAI_API_KEY` *name*, resolved by the node at lease time from an
optional Modal secret. The secret is genuinely optional: a deploy with it
absent prints why and proceeds with no binding, verified by deploying against a
deliberately nonexistent secret name.

**Brokering on Modal, verified.** A workspace on the Modal node holds only the
placeholder:

```
KEY=sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY
BASE=http://127.0.0.1:58118/c/<per-workspace-token>/d/api.openai.com/v1
```

A live `POST /chat/completions` through that broker returned the model's reply,
`brokered-from-modal`, and the real key appeared in **0** environment entries
and **0** files in the workspace.

**A real agent ran on Modal.** `remount run opencode --binding b_openai` in the
cloud installed the harness, reached OpenAI through the broker, and produced a
durable agent: `waiting_input`, 14 transcript records, an ACP session id, and
its own URL. It stopped on OpenCode's own sandbox permission prompt for the
workspace path, which is a harness-configuration issue rather than a Remount
one, and it stopped *durably* — the agent survived as a resumable record.

**The migration finding.** Moving that agent's workspace from Modal to a laptop
began correctly — a real checkpoint, generation 2, `claiming` on the laptop
node, and the agent record survived the move with its ACP session intact
(transcript 14 → 15). But the restore transferred roughly **113 KB/s**: 562 MB
of OpenCode's `node_modules` in about 25 minutes, still incomplete when the run
was abandoned. The same round trip with a small workspace completes in seconds
(see the entry above), so the cost is the artifact, not the mechanism.

Two things follow, and both are honest limits rather than defects:

- pulling a large legacy-tar checkpoint through Modal's web endpoint is
  impractically slow, so a cloud-to-laptop migration of a harness workspace
  needs either chunked artifacts on this path or an S3-compatible blob store
  both ends can reach directly; and
- `remount run` writes the harness into the workspace, so the workspace
  inherits `node_modules`. A base image carrying the harness would move a small
  delta instead of the whole tree.

**Teardown, verified.** Local node stopped, app stopped, `remount-control`
secret and `remount-broker-data` volume deleted, the throwaway
`remount-nobind` test app and its volume removed. `modal app list` shows no
deployed remount app and no `remount-control` secret.

### Gap found (closed 2026-09-03): the Modal demo cannot broker a credential

`deploy/modal_app.py` passes `--allow api.openai.com` to the node but no
`--bindings` to the server, so a workspace there can reach the provider and has
nothing to send. A harness therefore cannot run on the deployed demo at all.

A change adding a `b_openai` binding sourced from the existing `remount-openai`
Modal secret was written and deployed, and the container did not come back
healthy within ten minutes. Rather than push deployment code that had not been
proven, the change was **reverted**. Closing this properly means adding the
binding, confirming the container boots, and proving a brokered call from a
Modal workspace with the key absent from every workspace path — the same scan
the local E1 entry above performs. It is the prerequisite for running an agent
on Modal and moving it, which remains unproven.

## 2026-09-03 — Modal pool lifecycle

**Status: unavailable (externally gated). Credential verified.**

```sh
set -a && . ./.env && set +a
go test -tags integration -count=1 -v -run TestLiveProvisionerLifecycle ./internal/provision/modal
```

```
live_test.go:16: SKIPPED unavailable: missing required environment names
  [REMOUNT_MODAL_HELPER REMOUNT_MODAL_APP REMOUNT_MODAL_IMAGE
   REMOUNT_VENDOR_SERVER_URL REMOUNT_VENDOR_ENROLL_TOKEN
   REMOUNT_VENDOR_BINARY_URL REMOUNT_VENDOR_TENANT REMOUNT_VENDOR_POOL]
--- SKIP: TestLiveProvisionerLifecycle (0.00s)
```

**Bounded credential probe** (read-only, no resources created, no cost):

| Request | Result |
|---|---|
| `GET https://api.modal.com/v1/apps?environment_name=dev` | `200`, no apps |
| `which modal` | not installed |

`MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` are live and authorized against
environment `dev`. The driver shells out to a helper executable
(`internal/provision/modal/modal.go:53-71`), and that helper — the `modal` CLI —
is absent on this host, as is any deployed app or image. Closing this entry
requires the `modal` CLI installed, `modal deploy deploy/modal_app.py` run
against a disposable environment (`make modal-deploy`), and the same public
control-plane URL, enrollment token and binary URL as the E2B entry.

## 2026-09-04 — gVisor E4, E5 and B28 on Linux

**Status: verified.** The candidate ran on Ubuntu Linux x86_64, kernel
`5.15.200`, Docker Server `27.4.1`, and `runsc release-20260817.0`. Docker
reported the registered runtime name `runsc`. The rootfs and Docker workload
probe both came from the immutable image
`alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce`.

The disposable rootfs was built with:

```sh
sudo rm -rf /tmp/remount-gvisor-rootfs
mkdir -p /tmp/remount-gvisor-rootfs
container=$(docker create alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce)
docker export "$container" | sudo tar -C /tmp/remount-gvisor-rootfs -xf -
docker rm -f "$container"
CGO_ENABLED=0 GOOS=linux go build -o /tmp/remount-rawprobe ./internal/workspace/gvisor/testdata/rawprobe
sudo install -m 0755 /tmp/remount-rawprobe /tmp/remount-gvisor-rootfs/rawprobe
CGO_ENABLED=0 GOOS=linux go build -o /tmp/remount-udpprobe ./internal/workspace/gvisor/testdata/udpprobe
sudo install -m 0755 /tmp/remount-udpprobe /tmp/remount-gvisor-rootfs/udpprobe
```

The owning aggregate command was:

```sh
REMOUNT_GVISOR_INTEGRATION=1 \
REMOUNT_GVISOR_ROOTFS=/tmp/remount-gvisor-rootfs \
REMOUNT_CHAOS_IMAGE=alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  ./scripts/gvisor-conformance.sh
```

The host probe reported Docker and gVisor available against that exact image.
The mechanism spike then reported broker reachability, a positive UDP echo
control, denial of direct IPv4 TCP, IPv6, UDP data transfer, DNS to `8.8.8.8`,
ICMP, a raw socket, and CONNECT to an unlisted host, followed by denial after
network revoke.

`TestE4DenialConformance` passed all seven denial subtests in 15.88 seconds.
It also proved that a transfer was advancing before `RevokeNetwork`, that the
file stopped advancing when synchronous revoke returned, and that the broker
was no longer connectable. `TestE4FailedSetupCleanupConformance` rejected a
broker outside the generation link and observed that the namespace, host veth
and host ingress nftables table were all absent afterward.

`TestE5TenantIsolationConformance` materialized `ws_e5_a` for `tenant-a` and
`ws_e5_b` for `tenant-b` on one node and one gVisor backend. Each tenant was
unable to observe the other's workspace file. Each tenant's request toward the
other tenant's broker endpoint was denied by its own broker, and the node log
contained exactly one `egress.denied` event attributed to each source
workspace and tenant. This proves the `isolated` profile and sibling isolation;
it does **not** advertise gVisor as a `multi_tenant` backend.

**Teardown, verified.** The aggregate script removed its compiled tests and
spike directory. Both tests destroyed their runsc sandboxes, synchronously
revoked veths, removed namespaces and host nftables tables, closed brokers, and
unmounted the disposable `null-netns` mounts. The following postcondition
command produced empty sections for every resource class:

```sh
printf '%s\n' namespaces:
sudo ip netns list | rg '^(rm-|rmspike-)' || true
printf '%s\n' links:
ip -o link show | rg 'rm[gh]-|rmh-|rmg-' || true
printf '%s\n' nftables:
sudo nft list ruleset | rg 'remount_|rmspike' || true
printf '%s\n' listeners:
ps -ef | rg 'socat (TCP4|UDP4)-LISTEN:1744[34],bind=169.254.251.1' | rg -v rg || true
printf '%s\n' mounts:
findmnt | rg 'remount-gvisor|runsc' || true
```

### Post-rebase host-veth proof

The final candidate incorporated the stronger AF_PACKET assertion from
`origin/main`. A clean-host rerun first showed that the host-side netdev
ingress rule stopped forwarding but did not stop forbidden frames from
reaching the host veth:

```text
packets reached rmh1536 for [1.1.1.1 169.254.84.218]
```

The boundary now installs a `clsact` flower policy on the guest veth egress
before runsc starts: ARP and the exact broker IPv4/TCP tuple pass, and a lower
priority all-protocol rule drops everything else. This uses the traffic-control
egress hook because Linux 5.15 does not support nftables' `netdev` egress hook.
The host nftables ingress rule remains as a second boundary. The AF_PACKET
observer was also made direction-aware so broker replies transmitted from the
host are not misclassified as workspace egress.

The exact final command used a rootfs exported from the immutable Alpine image
above, with the two static probe binaries installed:

```sh
REMOUNT_GVISOR_INTEGRATION=1 \
REMOUNT_GVISOR_ROOTFS=/home/ubuntu/firecracker-artifacts/gvisor-rootfs-alpine-3.22 \
REMOUNT_CHAOS_IMAGE=alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  ./scripts/gvisor-conformance.sh
```

The spike passed both positive controls, all seven denial checks and
post-revoke denial. `TestE4DenialConformance` passed in 22.31 seconds with no
forbidden destination observed on the host-side veth; the in-flight transfer
stopped when synchronous revoke returned.
`TestE4FailedSetupCleanupConformance` passed in 0.20 seconds.
`TestE5SiblingTenantsCannotReachEachOther` passed in 9.69 seconds after proving
each workspace could reach its own broker and could not reach the sibling's
broker, guest address or DNS path. `TestE5TenantIsolationConformance` passed in
0.54 seconds. A post-run audit found zero `rmh*` links, zero `remount_rmh*`
nftables tables, zero `/run/remount/netns/rm-*` mounts and zero runsc
sandbox/gofer processes.
**Status: verified on the exact final candidate.**

The startup crash-reclamation boundary was exercised separately:

```sh
sudo env PATH="$PATH" HOME="$HOME" GOCACHE="$HOME/.cache/go-build" \
  go test -count=10 \
  -run '^TestReclaimOrphansRemovesOnlyUninhabitedNamespaces$' \
  -v ./internal/netns
```

All ten repetitions removed the uninhabited namespace and retained the one
whose holder had demonstrably entered it. The adoption regression also proved
that retained generation and mount metadata construct a new deny-first network
boundary after startup reclamation instead of requiring the deleted veth.

---

## 2026-09-04 — Firecracker 2.3 and B29 on Linux/KVM

**Status: verified on the exact candidate.** Candidate
`8d27d6d036e246e837d685bc6e4e74c007e96908` ran on Linux x86_64, kernel
`5.15.200`, with KVM and nested virtualization available. The static
Firecracker and jailer were both v1.16.1. Their SHA-256 digests were
`2fd0171309af7e24cf8dafc8a6f921c1434c49b5f9349bb996b7ed0a4deb8aa7`
and `1f3a0c1fe86212d0001819bfe0819071c01208b3ccc939c8b3bc1b84cf21edd`.

The kernel `/mnt/f/vmlinux-6.1.155` had SHA-256
`e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2`.
The rootfs `/mnt/f/remount-rootfs.ext4` had SHA-256
`2aa1b97ed499d804e4113a1300c99d7495e61a916d9a9409f092643e00c9fb80`.
The manifest `/mnt/f/guest-manifest.json` had SHA-256
`25cc5440a8683c943775ae345bca637de48531b3d81c8a53ef909ab653658961`
and identified guest protocol 1, vsock port 10789, workspace `/workspace`,
and guest binary SHA-256
`f9ad7fd58b39d4b66213c910e6b36459b34adf79aac205624ccedd9d92a1ac56`.

The owning aggregate command was:

```sh
REMOUNT_FIRECRACKER_DATA_ROOT=/mnt/f \
REMOUNT_FIRECRACKER_BINARY=/home/ubuntu/firecracker-v1.16.1/release-v1.16.1-x86_64/firecracker-v1.16.1-x86_64 \
REMOUNT_FIRECRACKER_JAILER=/home/ubuntu/firecracker-v1.16.1/release-v1.16.1-x86_64/jailer-v1.16.1-x86_64 \
REMOUNT_FIRECRACKER_CGROUP_PARENT=remount \
REMOUNT_FIRECRACKER_KERNEL=/mnt/f/vmlinux-6.1.155 \
REMOUNT_FIRECRACKER_ROOTFS=/mnt/f/remount-rootfs.ext4 \
REMOUNT_FIRECRACKER_GUEST_MANIFEST=/mnt/f/guest-manifest.json \
  ./scripts/firecracker-conformance.sh
```

The real jailer and vsock bridge booted a guest and passed filesystem write
and read, exec, PTY size/input/output, guest port, broker access and direct
public-egress denial. An in-flight broker stream advanced before
`RevokeNetwork`; synchronous revoke killed and joined the VMM, the guest
request exited nonzero, and the upstream byte count stopped advancing.

A running guest counter was checkpointed under a generation fence. Abort
resumed it exactly once. A second prepare committed without resuming the source;
the source was destroyed, and the destination restored disk, VMM state and VMM
memory. The PID was unchanged and the observed counter sequence was exactly
1..N with no repeat or gap. Equal and older restore generations were rejected
before network preparation or VMM launch. `ws.moved` remained
`restore_pending`; only the successfully restored destination's `ws.ready`
settled `restore_processes:"preserved"` on `ws.claimed`. A filesystem restore
claiming preservation was rejected.

The same command ran package proofs for incompatible CPU fingerprint and
Firecracker version, corrupt/truncated bundles, manifest hashes, prepare
abort/commit, failed destruction retention and capability honesty. A
privileged 64 KiB tmpfs forced restore staging to return `ENOSPC`; the staging
directory and image admission were released, and the same workspace id could
be created afterward. The descriptor used by node status and doctor exposed
`microvm`, `fs+mem`, and `enforced_gateway` only after the exact backend probes
succeeded; an unverified backend remains `none`, `fs`, and `open`.

**Teardown, verified.** The aggregate command found no Firecracker or jailer
process, `rm-*` cgroup, Remount netns, Remount veth/TAP, or Remount nftables
table after the success, stale-generation, corrupt-bundle and disk-full paths.
The runbook output is archived at
`docs/engineering/evidence/firecracker-kvm-2026-09-04.log`; the optional KVM
workflow uploads the same exact-host log under the candidate commit SHA.

---

## Performance attribution sweep, 2026-09-03 (late)

Host: Darwin 25.3.0 arm64, 18 cores, shared with other suites. Backend:
`process`. All commands run from the repository root.

| Command | Result | Status |
|---|---|---|
| `go test ./internal/sim -run '^TestHandoffScaleAndControlFailover$' -count=1 -mutexprofile=... -blockprofile=...` | PASS in 23.68 s; claim p50 425 ms / p99 668 ms, exec_round_trip p50 2.78 s, move p50 4.84 s, reattach p50 4.74 s | verified |
| `go tool pprof -peek` on each suspect symbol | dispatch 26.5%, artifact publish 9.7%, forkExec 9.4%, sessionLogCommit 1.4% | verified — corrects the earlier claim of 47% for the session-log path |
| `go test ./internal/sim -run '^TestExecRoundTripCostOfTheDurableSessionTier$'` | tier off 11 ms, on 29-30 ms, ratio 2.47-2.66x over four runs | verified |
| Same lane with the tier disabled in *both* arms (deliberate fault injection) | FAILS with "the artifact tier cost nothing (1.02x)" | verified — the lane detects its own irrelevance |
| Phase instrumentation of `sealSpillLocked` and `artifact.Store.put` | 2 puts per seal, one per store, each `tmp.Sync()` + `syncDir()`; spill sync 3.4 ms, put 14.5 ms, head 0.12 ms, commit 0.6 ms | verified; instrumentation removed afterwards |
| 200 concurrent `sh -c printf` from one Go process | 436 ms wall, p50 247 ms, against 5 ms unloaded | verified — `ForkLock` floor, a simulation artifact |
| `go test ./... -count=1 -timeout=40m` | 0 failures | verified |
| `gofmt -l`, `go vet ./...`, `staticcheck ./...`, `scripts/lint-locks.sh` | all clean | verified |

The one code change from this sweep is the removal of the redundant spill fsync
in `sealSpillLocked`. What remains open, and why each was deliberately not
changed, is in `docs/engineering/performance-regressions-2026-09.md` under
"Regression 3, re-diagnosed".

---

## Plan B B32 — clean installs of every shipped artifact, 2026-09-03 (late)

**Status: verified, with two named unavailable sub-lanes.**

Host: Darwin 25.3.0 arm64; Docker Desktop 29.4.1 serving `linux/aarch64`;
Python 3.14.5 (`pip` is not on PATH, `python3 -m pip` and `python3 -m venv`
are); npm 11.12.1 with node 25.9.0; Go 1.27.1; no `syft`. Candidate
`c89e413` with this suite as the working-tree change. Every artifact comes
from `make dist`; nothing was published, tagged, pushed or signed.

| Command | Result | Status |
|---|---|---|
| `go test -count=1 -timeout 30m ./integration/installs/...` | ok, 52.8 s, one skip (`syft`) | verified |
| `TestB32TheStaticBinaryInstallsAndPassesTheBlackBoxSmoke` — `dist/remount-darwin-arm64` copied to a temp prefix, started there with an allow-listed environment, judged by `go run ./cmd/conformance --endpoint URL --external` | `CONFORMANT`, required 52 passed / 0 failed / 0 unavailable, cleanup verified | verified |
| `TestB32TheOCIArchivesInstallAndPassTheBlackBoxSmoke` — both release images built from a context holding only the linux binary and the two Dockerfiles, `docker save`, `docker rmi`, `docker load`, run on a private network, judged over `--endpoint … --external --token` | `CONFORMANT`, required 52 passed / 0 failed / 0 unavailable; containers, network and images removed and their removal re-checked | verified |
| `TestB32ThePythonWheelInstallsIntoAFreshVenvAndDrivesTheInstalledServer` — `python3 -m pip wheel` then install into a second fresh venv | `remount.__file__` under the venv; workspace created, `/bin/echo` run, workspace destroyed | verified |
| `TestB32TheNpmTarballInstallsIntoAnEmptyDirAndDrivesTheInstalledServer` — `npm ci`, `npm run build`, `npm pack`, `npm install <tarball> --omit=dev` into an empty directory | `@remount/sdk` resolves inside the install directory; same workspace lifecycle | verified |
| `TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer` — module zip served from a `file://` proxy, consumer module outside the repository, no `replace` | `go list -m` resolves to the consumer's own module cache; the compiled consumer contains no byte of the checkout path | verified |
| `TestB32AChecksumManifestTravelsWithTheArtifactsAndCatchesDamage` | six artifacts verify against the manifest that travelled with them; a byte flip is rejected | verified |
| `TestB32TheInstalledBinaryCarriesItsOwnComponentInventory` — `go version -m` on the installed file | main module `remount.dev/remount`, 15 modules, every direct `go.mod` requirement present at the pinned version | verified but bounded — module components only, not a full SPDX document |
| `TestB32TheLinuxArtifactsAreStaticallyLinked` | neither linux ELF names a dynamic loader or a shared library | verified |
| `TestB32TheSPDXSBOMLaneNeedsSyft` | `unavailable: syft is not on PATH` | unavailable (host) |
| Homebrew formula install | the formula's URLs and digests come from a published release manifest (`packaging/homebrew/README.md`); installing one locally would need the publish action Plan B §18 cuts | unavailable (release-gated) |
| `install.sh` against real release assets | same gate; its decision logic is covered hermetically by `scripts/release/test_install.sh` | unavailable (release-gated) |
| `dist/remount-linux-amd64`, both windows artifacts | this host can execute neither; the linux/arm64 artifact is the one the image lane runs | unavailable (host) |
| `gofmt -l`, `go vet ./...`, `staticcheck ./...` | clean | verified |

What "clean" means here, and how it is enforced: every lane installs into a
`t.TempDir()`, runs from a directory that is not the checkout, and gets an
environment built from an allow list (`PATH`, `HOME`, `TMPDIR`, `LANG`) rather
than a scrub list, so `GOPATH`, `GOFLAGS`, `PYTHONPATH`, `NODE_PATH`,
`VIRTUAL_ENV`, `REMOUNT_SERVER` and the rest cannot survive. The Go consumer
gets its own `GOMODCACHE`, `GOCACHE` and an empty `GOENV`.

**Failure injection.** Each detector has a permanent control, and the main Go
lane was additionally broken by hand:

| Injected defect | What caught it |
|---|---|
| `replace remount.dev/remount => <checkout>` added to the consumer built by the passing lane | `the consumer acquired a replace directive`, then, with that check removed, ``remount.dev/remount was replaced by &{Path:/Users/…/remount Dir:/Users/…/remount}; the build did not use the artifact`` |
| the same replace, as the permanent control `TestB32AReplaceIntoTheCheckoutIsCaught` | asserts `go list -m` reports the replacement, that it resolves to the checkout, and that the compiled binary then does contain the checkout path |
| a binary built without `-trimpath` (`TestB32TheSourceTreeScanCatchesAnUntrimmedBinary`) | the same byte scan that passes on the dist artifact finds the checkout path |
| `pip install -e sdk/python` (`TestB32AnEditablePythonInstallIsCaught`) | the package location check reports a path inside the checkout |
| `npm install <checkout>/sdk/typescript` (`TestB32ALinkedNpmInstallIsCaught`) | npm links rather than copies, and the realpath check reports the checkout |
| a byte flipped in one installed artifact | the checksum verifier rejects it |
| `PYTHONPATH`, `GOFLAGS`, `NODE_PATH` set in the parent process (`TestCleanEnvDropsInheritedPaths`) | `cleanEnv` drops all three |

**A packaging fact this lane pins.** The image lane loads two images, not one.
`packaging/container/Dockerfile` is `FROM scratch`, which is right for the
control plane and the CLI and impossible for a process-backend node: judged
alone it fails `CONF-SESS-004` and `CONF-SESS-006` with `/bin/cat` and
`/bin/sh` missing, observed here as `NOT CONFORMANT … required 50 passed, 2
failed`. `deploy/compose/node.Dockerfile` exists for exactly that asymmetry.
Paired, the stack is `CONFORMANT` on all 52 required rows.

---

## Conformance manifest 1.1.0, 2026-09-03 (late)

The B32 install lanes recorded above were judged against manifest **1.0.0**, so
their "52 required passed" lines are correct as observations and are left as
written. The manifest has since moved to **1.1.0** with the addition of
`CONF-SESS-009`, and the same lanes now report 53. The counts differ because the
suite grew, not because anything regressed.

| Command | Result | Status |
|---|---|---|
| `go run ./cmd/conformance --build .` | `CONFORMANT`, manifest 1.1.0, required **53 passed, 0 failed, 0 unavailable**; `CONF-SESS-009` passed | verified |
| `go test ./internal/conformance/...` incl. `TestB21BrokenImplementationFailsEachSemanticCategory/stdout` | ok — the new `stdout` shim defect fails `CONF-SESS-009` while `CONF-SESS-001/002/003/005/008` and every other category keep passing | verified |

Why the row was added is in `docs/engineering/plan-b-b4-conformance.md` under
"Manifest 1.1.0". In short: an OCI image built `FROM scratch`, where `/bin/echo`
does not exist, still scored 50 of 52 required rows, because every session
requirement asserted the shape of the log and a shape assertion holds vacuously
for a log with no output in it.

---

## Plan B phase B5 — OpenTofu and Helm validation, 2026-09-03 (late)

**Status: verified for the static gates; the live-cluster and live-cloud halves
are unavailable and named below.**

Host: Darwin 25.3.0 arm64. `tofu` OpenTofu v1.12.6 (darwin_arm64), `helm`
v4.2.4. Nothing was applied, installed, pushed or published: Plan B §18 cuts
public release actions and no cloud or cluster credential was used or present in
the validators' environment.

This closes work items 2, 3 and 7 of §13. Items 1, 4, 5 and 6 are separate:
item 1 (the Compose stack) was already proven and was re-proven here because
this change pinned one of its images; items 4, 5 and 6 remain open.

### What was added

| Path | What it is |
|---|---|
| `deploy/tofu/modules/{network,control,artifact-store,node-pool}` | infrastructure only: network inputs, the single-writer control service, the artifact bucket, node capacity and capability labels |
| `deploy/tofu/examples/reference` | the root module the gates validate |
| `deploy/helm/remount-node` | a DaemonSet chart that schedules nodes and advertises capability labels; no CRD, no controller, no RBAC |
| `deploy/helm/remount-node/golden/default.yaml` | the committed render, diffed by a test so chart drift is visible in review |
| `integration/policy` | the §13.7 policy tests, each with a control that has been watched failing |

Runtime workspaces, sessions, claims, moves and checkpoints are outside
Terraform state and outside the chart, and that is asserted rather than
asserted-in-prose: see the rejection tables below.

### Gates

| Command | Observed | Verdict |
|---|---|---|
| `tofu fmt -check -recursive deploy/tofu` | exit 0, no output | verified |
| `tofu init -backend=false` then `tofu validate -no-color` in `deploy/tofu/examples/reference` | `Success! The configuration is valid.`, exit 0 | verified |
| the same, with an allow-list environment (`PATH`, `HOME`, `TMPDIR`, `CHECKPOINT_DISABLE`, `TF_IN_AUTOMATION`, `TF_INPUT`) and no cloud variables | `Success! The configuration is valid.` | verified |
| `helm lint deploy/helm/remount-node -f deploy/helm/remount-node/golden/values.yaml` | `1 chart(s) linted, 0 chart(s) failed` (one INFO: no icon) | verified |
| `helm template remount-nodes deploy/helm/remount-node --namespace remount -f .../golden/values.yaml \| diff -u .../golden/default.yaml -` | no diff, exit 0 | verified |
| `go test -count=1 ./integration/policy/` | ok, 12 tests, 14 rejection sub-cases | verified |
| `gofmt -l .` | no output | verified |
| `go vet ./...` | clean | verified |
| `staticcheck ./...` | exit 0, no findings | verified |
| `docker compose -f deploy/compose/compose.yaml config` | valid after the MinIO digest pin | verified |
| `./deploy/compose/smoke.sh` after rebuilding both images | `2 nodes online`, workspace created, `filesystem reads back composed`, moved off `n_06g6p1szz2j499z088b0w61cqw`, `filesystem survived the move`, `OK` | verified |

The Compose smoke was re-run because this change pinned MinIO by digest, and a
proven stack whose image reference changed is no longer a proven stack until it
is re-proven. `make dist`, both image builds, `up -d`, `smoke.sh`, then
`docker compose down -v --remove-orphans`; `docker ps -a` and `docker volume ls`
filtered on `remount-reference` both came back empty, so cleanup is verified.

**Validation needs no account, by construction.** No module declares
`required_providers`; the provider seam is `terraform_data`, a builtin. So
`tofu init -backend=false` contacts no registry and `tofu validate` needs no
cloud identity. `integration/policy.TestTheModulesDeclareNoProvider` fails if a
module ever grows one, because that is the change that would quietly make this
gate un-runnable in CI.

### Every image reference is now pinned by digest

`integration/policy.TestEveryImageReferenceIsPinnedByDigest` scans `deploy/**`,
`packaging/container/Dockerfile` and `images/workspace/Dockerfile`. Four
references were floating and were pinned; all four digests resolve to
multi-architecture indexes, so the pins are not architecture-specific:

| Reference | Digest |
|---|---|
| `minio/minio:RELEASE.2025-04-22T22-12-26Z` (compose) | `sha256:a1ea29fa2835…b015e` |
| `debian:bookworm-slim` (`deploy/compose/node.Dockerfile`) | `sha256:88200866dfff…a4171` |
| `node:22-bookworm-slim` (`images/workspace/Dockerfile`) | `sha256:83f487e0a634…a7e5` |
| `ghcr.io/astral-sh/uv:0.9.5` (`images/workspace/Dockerfile`) | `sha256:f459f6f73a8c…89b7` |

Two exemptions, both narrow and both tested in each direction by
`TestALocalBuildIsNotAFloatingTag`: `scratch` has nothing to pin, and
`remount:local` / `remount-node:local` are built by this tree, so a digest
written into the file would pin the reference to somebody else's build.

### The §13.7 rejections, and the proof each one can fail

Each rejection is a root module or a set of Helm values that `tofu validate` or
`helm template` must refuse. Each was then re-run with its rule deleted, and the
test was watched failing. A rule nobody has seen reject something is not yet a
rule.

| Rejected configuration | Refused by | With the rule deleted |
|---|---|---|
| `node_assignments = { ws_01HZQ = "n_a1" }` — Terraform pinning a live workspace to a node | `tofu validate`: "Terraform does not place, move, or pin live workspaces." | `tofu validate accepted moves-a-live-workspace` |
| `capability_labels = { vendor = "sk-live-…" }` | `tofu validate`: "looks like a reusable credential" | `tofu validate accepted credential-in-node-labels` |
| `admin_token_env = "sk-live-…"` — a token where the module wants a variable name | `tofu validate`: "admin_token_env is the NAME of an environment variable" | `tofu validate accepted credential-as-token-value` |
| `image = "ghcr.io/example/remount-node:latest"` | `tofu validate`: "image must be pinned by digest" | `tofu validate accepted floating-node-image` |
| `replicas = 2` on the control service | `tofu validate`: "The control plane is a single writer; replicas must be 1." | `tofu validate accepted two-control-replicas` |
| `capability_labels = { workspace = "ws_01HZQ" }` | `tofu validate`: "Labels describe what a node can do, not what is running on it." | `tofu validate accepted workspace-in-node-labels` |
| `--set capabilityLabels.workspace=ws_01HZQ` | `helm template` fails | `helm template accepted [capabilityLabels.workspace=…]` |
| `--set capabilityLabels.vendor=sk-live-…` | `helm template` fails | (same rule as above) |
| `--set capabilityLabels.openai_api_key=…` | `helm template` fails: "must not name a credential" | (same rule) |
| `--set node.extraArgs[0]='ws move ws_01HZQ'` | `helm template` fails: "runtime workspace operation" | (same rule) |
| `--set image.digest=` | `helm template` fails: "must be pinned by digest" | n/a, `required` |
| `--set image.digest=latest` | `helm template` fails: "must be sha256:" | n/a |
| `--set image.repository=ghcr.io/andrewgcodes/remount` | `helm template` fails: the control-plane image is FROM scratch | `helm template accepted [image.repository=…/remount]` |
| `--set control.endpoint=` | `helm template` fails: "control.endpoint is required" | n/a, `required` |

The static scanners were injected against as well, on the real tree rather than
on a fixture, and each was watched catching its injection:

| Injected defect | What caught it |
|---|---|
| `node_assignments` added to `deploy/tofu/examples/reference/main.tf` | `two owners for one fact: …main.tf:75: workspace-assignment` |
| `vendor: sk-live-…` added to the chart's `capabilityLabels` | `a credential has two owners: …values.yaml:53: credential-literal` |
| `--token=an-actual-bearer-value` substituted in `compose.yaml` | `…compose.yaml:48: credential-field-holds-a-literal` |
| the MinIO digest removed, leaving the tag | `floating image reference: …compose.yaml:106: manifest-image` |
| `maxUnavailable` changed in the chart without regenerating the golden | `the chart no longer renders …golden/default.yaml` |
| the ownership canary manifest emptied | `rule "kubernetes-workspace-object" found nothing in the canary` |
| the floating-tag canary Dockerfile emptied | `the digest scan found no floating FROM in the canary Dockerfile` |

The last two are the control on the controls. Every scanner is additionally
pointed at `integration/policy/testdata/canary`, where each violation is planted
deliberately; if a scanner comes back clean there, the test fails. This is the
`grep … | head` lesson from AGENTS.md applied to a scan whose passing result
would otherwise be indistinguishable from a scan that had stopped working.

### A real defect found on the way

The repository's `.gitignore` carried an unanchored `remount-node/`, meant for
the data directory `remount up` creates in the working tree. It also matched
`deploy/helm/remount-node/`, so the entire Helm chart was invisible to git and
would have been silently omitted from the commit — `git status` simply did not
list it. The patterns are now anchored (`/remount-node/`, `/remount-data/`).

### Unavailable, with reasons

| Check | Why it could not run here |
|---|---|
| `helm install` / `helm upgrade` against a cluster | no Kubernetes cluster on this host, and Plan B §18 forbids provisioning one for this phase |
| the DaemonSet actually scheduling nodes that join a fleet | same: needs a cluster |
| `tofu plan` or `tofu apply` against any provider | forbidden by the phase's own rules; validation must not require an account, and no cloud credential was present |
| the modules against a real provider (`aws_instance`, a managed instance group, …) | the provider seam is `terraform_data` by design; substituting a real resource is a per-target edit that this repository has not operated |
| §13 work items 4 (private-network examples), 5 (secret-manager adapters) and 6 (Prometheus scrape and event-export examples) | not attempted in this pass |

The chart and the modules have therefore been **validated, not operated**. This
repository does not claim that any AWS, Azure, GCP or Kubernetes deployment of
Remount has been run.

---

## Modal, judged over the public internet, 2026-09-04

The earlier attempt at this was abandoned at a 303 and recorded as not
verified. Chasing it found two real defects, both now fixed, and the deployment
is now **CONFORMANT** across the public internet.

| Command | Result | Status |
|---|---|---|
| `make modal-deploy` then `GET /healthz` | 303 after 150 s, twice | the finding, not a flake |
| `modal app logs remount-demo` | `ExecutionError: Function has 3 dependencies but container got 4 object ids` | root cause |
| `GET /healthz` after the fix | **200 in 1.4 s**, then 0.47 s warm | verified |
| `go run ./cmd/conformance --endpoint https://action-dev--remount-demo-control.modal.run --external --token …` | **CONFORMANT**, required **53 passed, 0 failed, 0 unavailable**, 1 m 38 s | verified |
| Same suite against a local build | `CONFORMANT`, 53/0/0 — unchanged | verified |

**Defect 1: the Modal deployment could not start at all.**
`model_secrets = _optional_model_secret() if modal.is_local() else []` attaches
one secret at deploy time and declares none inside the container, and Modal
refuses to run a function whose dependency count disagrees with what it was
given. The `is_local()` guard reads like an optimisation and is a correctness
bug; whatever that expression does, it has to do it identically on both sides.
This was a regression introduced earlier in the same session while making the
model secret optional — the deploy succeeded, so it looked fine, and only the
container logs showed otherwise.

**Defect 2: the conformance suite could not judge a remote implementation.**
`Session.Fixture` creates one shared workspace and grant behind a `sync.Once`
and never refreshes it. A grant lives about twenty seconds; the suite takes
about ninety-five over a WAN. So the last checks presented an expired grant and
`CONF-SNAP-004` failed with `unauthorized: grant expired` — the suite reporting
its own bookkeeping as a defect in the implementation under test. It reproduced
every run against Modal and never once locally, which is the shape of a bug that
only appears in the case the tool exists for: judging somebody else's deployment
over a network.

The fix refreshes the shared grant when less than ten seconds of it remain. The
first version of that fix did nothing at all, because `ExpiresAt` is in
milliseconds and it was compared against `time.Now().Unix()` in seconds, which
makes every grant appear to expire fifty thousand years from now. That is a bug
that hides itself — the refresh silently never runs and the symptom is
unchanged — and it was found only by printing the two numbers side by side.

---

## Still not attempted

These remain open with no evidence in this file. Listing them here is
deliberate: absence of an entry is not a pass.

| Item | Why |
|---|---|
| Fly / ix / SSH pool E17 | no credentials present in `.env` |
| Real WorkOS OIDC, group mapping, revocation | no WorkOS tenant or credentials |
| Real GitHub App (E14) | no app registered |
| Non-AWS S3 conditional writes and multipart | no non-AWS S3 endpoint configured |
| Temporal Cloud worker restart | no Temporal Cloud credentials |
| Real Slack signature and delivery | no Slack signing secret or webhook URL |
| Docker / gVisor scale benchmarks on named hosts | no named benchmark hosts |
| E18 signed public release | irreversible public action; requires explicit release authority |

---

## Rotation note

`docs/engineering/handoff-2026-09-03-codex-wrap.md` §6 records an earlier
incident in which a real OpenAI key was briefly written into a disposable
workspace as a canary and then deleted without being printed. That key should
still be rotated as a precaution. The 2026-09-03 run recorded above did **not**
repeat that pattern: its canary was synthetic and generated at run time, and
the scans above confirm the real key appeared in no workspace path, no host
data directory, and no log.

---

## 2026-09-04 — B30, B31, B32 and the isolation scale matrix

Host: `devin-box`, Linux x86_64, kernel 5.15.200, 8 CPUs, 31 GiB RAM and
no swap. The checkout was based on `origin/main`
`0f77dad628811737ab2aa1e5c9cd91bfdd41e150`. Nothing was published, pushed to
an image registry, tagged or signed.

### B30 resource ceilings

Command:

```sh
./scripts/planb-resource-ceilings.sh
```

The script used its default `REMOUNT_SCALE_CURSORS=10000` and ran the
measurement without race instrumentation; `make race` remains a separate
gate. `TestPlanBScale*` passed in 81.884 seconds after rebasing onto the
candidate above.

| Proof | Observed result | Status |
|---|---|---|
| event retention | 1,200 posts under a 200-event ceiling; all 1,000 pruned events were counted before the retained range was read | verified |
| slow subscriber and bounded tails | dropped/rejected work was explicit; no silent sequence gap | verified |
| export backpressure | 1,200 events in batches of at most 64; one rejected 64-event batch left the cursor at 193, and retry exported the complete range without loss | verified |
| reconnecting cursors | three cycles of 10,000 cursors; 7,500 connections severed per cycle; peak 60,028 goroutines and 922.8 MiB heap-in-use; every cycle settled to 28 goroutines, 17 descriptors and two relay peers | verified |
| cursor loss accounting | dropped-frame deltas `+250122`, `+264531`, `+250584`; gap delta zero in every cycle | verified |
| provider burst/drain | four burst cycles returned to zero provider machines; a six-machine wide burst drained in 1.969 seconds; zero provision failures | verified |
| quota rejection/recovery | 24 workspace rejections in each of two cycles were counted and capacity recovered; session quota rejection also recovered | verified |
| session-log spill | four sessions emitted about 1.25 MB; 13,441 chunks were evicted explicitly; four spill files totaled 303.0 KiB and replay reported a gap rather than silent loss | verified |
| snapshot deduplication | four unchanged 20 MiB snapshots: 80 MiB logical, 1.3 MiB uploaded, deduplication ratio 0.9843 | verified |

Two defects were found while earning this result. Cursor teardown had used
10,000 serialized detach RPCs and could stall indefinitely; teardown now cuts
the clients and connections directly, including connections racing with
shutdown, and a 400-cursor control returned to two peers in all three cycles.
Event retention had accepted the first asynchronous prune before reading the
range; it now waits for the complete `posts - ceiling` count. Ten focused race
repetitions of each corrected boundary passed.

### B31 reproducible clean-container builds

Command:

```sh
./scripts/reproducible-container-builds.sh
```

The digest-pinned Go image
`golang@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`
built every supported OS/architecture pair twice in clean containers with
cold caches and different `TMPDIR` values. The two checksum manifests were
identical:

```text
verified: two clean containers produced byte-identical static binaries
```

All containers and output directories were removed. **Status: verified.**

### B32 clean installs and SBOM

Command:

```sh
PATH=/home/ubuntu/.local/bin:$PATH \
  go test -count=1 -v -timeout 30m ./integration/installs
```

The full lane passed in 56.490 seconds. It built and installed the static
binary, both OCI candidates, Python wheel, npm tarball and external Go module
consumer in disposable environments; both black-box image checks reported
manifest 1.1.0 `CONFORMANT`, required 53 passed, 0 failed, 0 unavailable, with
cleanup verified. Checksums, static-link checks and the installed component
inventory passed. The SPDX lane used Syft 1.32.0 obtained from
`anchore/syft@sha256:b6a6da626d98f5cb92e28934176709003cce6cdcf674816959c7d84845d94045`
and passed. No artifact was published. **Status: verified.**

The aggregate evidence runner executes direct `go test` proofs verbosely and
treats a selected proof that exits zero with a Go `--- SKIP` as `unavailable`
when the test names its prerequisite, and as a failure when the skip has no
`unavailable` reason. B32 therefore cannot become green merely because Syft is
absent.

### Docker 200-node / 2,000-workspace scale lane

Command:

```sh
REMOUNT_HANDOFF_SCALE_BACKEND=docker \
REMOUNT_HANDOFF_SCALE_IMAGE=alpine@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 \
  go test -count=1 -run '^TestHandoffScaleAndControlFailover$' \
  -v -timeout 30m ./internal/sim
```

Candidate: Docker Server 27.4.1 and the exact Alpine digest above. The host
reached 1,023 running workspaces and 200 partially created containers before
Docker failed additional bridge endpoint creation:

```text
failed to add the host (veth...) <=> sandbox (...) pair interfaces:
exchange full
```

The run was terminated rather than misreported as a pass. It produced no
complete claim, exec, move or reattach distribution, so no Docker benchmark
number was added to `docs/benchmarks.md`. This named host cannot supply the
required bridge/veth capacity. **Status: unavailable on `devin-box`.**

The failure also exposed a real cleanup defect: `docker run` can leave a
container in `Created` after endpoint setup fails, while `Docker.Create`
removed only the host workspace root. The failure path now inspects the
container mount, removes only a container proven to own that exact root under
an independent bounded cleanup context, and reports cleanup failure. Ten race
repetitions of the regression test passed. After the attempted scale run,
1,223 containers selected by the `remount.workspace` label were removed;
zero labeled or `remount-ws-*` containers remained, and the temporary scale
tree was removed.

### gVisor 200-node / 2,000-workspace scale lane

Command:

```sh
sudo env \
  REMOUNT_HANDOFF_SCALE_BACKEND=gvisor \
  REMOUNT_GVISOR_ROOTFS=/home/ubuntu/firecracker-artifacts/rootfs-tree \
  REMOUNT_RUNSC=/usr/bin/runsc \
  PATH="$PATH" \
  go test -count=1 -run '^TestHandoffScaleAndControlFailover$' \
  -v -timeout 30m ./internal/sim
```

Candidate: `runsc release-20260817.0` and the root filesystem tree previously
used by the successful gVisor cleanup proof. The host used about 1.5 GiB before
the lane. At 813 live sandboxes it used 14 GiB with 15 GiB available; while
the test process was being stopped it reached 1,060 sandboxes and 18 GiB used
with 11 GiB available. The measured slope cannot reach 2,000 sandboxes within
31 GiB and no swap without risking an OOM kill, so the lane was terminated
before host exhaustion. It produced no complete claim, exec, move or reattach
distribution, and no gVisor benchmark number was added. **Status: unavailable
on `devin-box` because the named host lacks RAM for 2,000 runsc sandboxes.**

Cleanup deleted all 1,060 runsc containers, verified no sandbox or gofer
process remained, unmounted 197 backend `null-netns` mountpoints, verified no
mount below the test root remained, and removed the temporary scale tree.

A later preflight before the final E4 rerun corrected that teardown claim:
1,145 `rmh*` links, 1,061 `remount_rmh*` netdev tables and matching
`/run/remount/netns/rm-*` mounts from terminated scale attempts still existed.
They were removed by exact Remount-owned prefixes before rerunning any provider
proof; the subsequent audit reported zero for every class. The 2,000-workspace
scale result remains unavailable. The integrated startup reaper now destroys
containers recorded in the backend's runsc state root and removes only network
namespaces whose inode is not inhabited by any process, together with their
veths; the live-namespace regression ensures restart cleanup does not cut the
network out from under a running workspace.

### B32 reconnect race and final repository gates

Repeated B32 external Go consumer runs exposed an intermittent loss of the
installed node uplink while the consumer replaced a session cursor. The same
failure reproduced from an exact detached `origin/main`
`0f77dad628811737ab2aa1e5c9cd91bfdd41e150` worktree, so the lane was not
reported green merely because the defect predated this branch. Replacing a
cursor cancelled the old subscriber while it could be inside a WebSocket
write; that cancellation could close the shared peer carrying the node
uplink. Subscriber cancellation now prevents the next write but does not
cancel a write already active on the shared transport.

Commands:

```sh
go test -race -count=10 \
  -run '^TestReplacingSessionCursorDoesNotCancelSharedPeerWrite$' \
  -timeout 300s ./internal/node

go test -count=10 \
  -run '^TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer$' \
  -timeout 15m ./integration/installs

go test -race -count=5 \
  -run '^TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer$' \
  -timeout 20m ./integration/installs
```

All repetitions passed. The deterministic node regression models a
cancellation-sensitive transport write and proves that replacing one cursor
does not close the shared peer. The ten clean-install repetitions passed in
66.148 seconds, and the five race-instrumented repetitions passed in 45.410
seconds. **Status: verified.**

The complete final race gate then passed:

```sh
make race
```

The B32 installation package passed under race in 62.340 seconds; the node
package passed in 45.702 seconds; the simulation package passed in 365.470
seconds; every package completed successfully. **Status: verified.**

The remaining final-tree gates also passed:

```sh
make
make lint
make test
make conformance
go run ./cmd/conformance --build .
go mod verify
go mod tidy -diff
make dist
go run ./cmd/protogen --check
make public-api
scripts/lint-locks.sh
make fuzz FUZZTIME=5s
```

The built-binary conformance run reported 60 passed, 0 failed and 8
unavailable of 68 requirements in 6.793 seconds. All 53 required requirements
passed; the unavailable capability-gated and extension checks retained their
named prerequisites. Cleanup was verified. The fuzz gate completed every
registered target without a failure. No artifact was published.

### Post-rebase aggregate and canonical-event observation, 2026-09-04

The Linux branch was rebased onto `origin/main`
`0f19959fef6403d0e4f75b09758c4babe6019f7e`, preserving the upstream Modal
startup fix, conformance grant refresh, gVisor crash reclamation and E5 proof.
The first aggregate run on candidate
`e2668ffcfa919f4db099c29f3e1ae63b67449c9b` completed in 1,864,017 ms:

```text
39 passed, 0 failed, 28 unavailable
required: 39 of 55 passed, 0 failed, 16 unavailable
external resources: 1 created, 1 cleanup verified, 0 cleanup failed
```

The exact command supplied the pinned gVisor and Firecracker candidates and
ran `go run ./cmd/evidence run`. B28, B29, B30, B31 and B32 passed. Every
unavailable row retained its named missing prerequisite or explicit release
decision. The baseline gates passed because their subprocess environment now
removes scenario-only host switches rather than accidentally activating
privileged integration tests inside ordinary `make test` and `make race`.
Post-run inspection found zero `rmh*` links, Remount nftables tables,
Remount namespace mounts, runsc sandboxes or gofers. **Status: verified.**

The subsequent exact built-binary command exposed a separate intermittent
conformance-runner race:

```sh
go run ./cmd/conformance --build .
```

The denied broker request returned 403, but `CONF-BIND-002` immediately read
the control-plane event stream before the node's durable event outbox had
forwarded its `egress.denied` audit. One of five focused repetitions reproduced
the false failure; the other four observed the same canonical event and
passed. The checker now polls the canonical log for at most five seconds,
within the requirement's existing context, and still fails with the original
diagnostic if the event never arrives. `CONF-BIND-003` uses the same boundary,
and `CONF-BIND-005` waits until both refused requests are observable before
asserting that neither audit contains the secret.

Verification:

```text
go test -count=20 ./internal/conformance
10 focused built-binary CONF-BIND-002 runs
go run ./cmd/conformance --build .
```

All focused repetitions passed. The complete built-binary run reported 60
passed, 0 failed and 8 unavailable of 68 requirements in 6.838 seconds; all 53
required requirements passed and cleanup was verified. **Status: verified.**

### Durable session completion and replacement-attach fencing, 2026-09-04

The repeated E11 restart proof exposed a completion-ordering defect. A session
published its exit chunk, closed `s.exited`, and let `Session.Wait` and readers
finish before the node's observer committed the complete durable session-log
record. A node stop in that interval could close the control connection and
leave the retained record partial even though the client had observed complete
success.

The completion callback now runs synchronously in `session.finish`, after the
terminal spill has been sealed and before active-session accounting, `s.exited`,
terminal visibility, EOF, or observer callbacks. The final spill reference is
committed directly by the complete-record callback, avoiding a redundant
partial commit for the same final segment; earlier partial segment commits are
unchanged. The terminal chunk remains retained but invisible through
`Log.Next`, `Log.Read`, and `Cursor.Next` until the complete commit returns.
Commit failures still invoke `OnRecordError`, retain the lower tiers, and do not
claim a complete durable record.

The deterministic regression blocks `CompleteSessionLogRecord` and proves that
`Session.Wait`, the terminal chunk, and cursor EOF all remain blocked. Releasing
the commit publishes the terminal chunk and EOF in order. The E11 node-restart
proof then passed 50 focused repetitions.

The first complete non-race suite after this fix repeatedly timed out in
`TestConsoleE15SimulatedOperatorFlow` while a replacement terminal attachment
waited for exit. The old WebSocket's delayed `s.close` could arrive after the
new `s.attach`; because detach named only `(client, session)`, it cancelled the
replacement cursor. `s.attach` and `s.close` now carry an optional unique
subscription fence. A matching close detaches only its own cursor; omitted
fields retain the v0 unconditional behavior. Generated Python, TypeScript and
JSON schema surfaces were regenerated. Unit, wire-compatibility and simulation
regressions prove that an old decoder ignores the additive field, a new decoder
accepts legacy bodies, and a delayed old detach cannot remove the replacement
attachment.

Focused commands:

```sh
go test -count=50 \
  -run '^(TestWaitBlocksUntilDurableCompletionRecordCommits|TestE11TieredSessionRecordSurvivesNodeRestart)$' \
  -timeout 300s ./internal/session ./internal/sim

go test -count=50 \
  -run '^(TestSessionSubscriptionFieldIsWireCompatible|TestStaleSessionDetachDoesNotCancelReplacementCursor|TestReplacingSessionCursorDoesNotCancelSharedPeerWrite)$' \
  -timeout 180s ./internal/proto ./internal/node

go test -count=50 \
  -run '^(TestStaleDetachDoesNotCancelReplacementAttach|TestConsoleE15SimulatedOperatorFlow)$' \
  -timeout 600s ./internal/sim

go test -race -count=20 \
  -run '^(TestSessionSubscriptionFieldIsWireCompatible|TestWaitBlocksUntilDurableCompletionRecordCommits|TestStaleSessionDetachDoesNotCancelReplacementCursor|TestReplacingSessionCursorDoesNotCancelSharedPeerWrite|TestStaleDetachDoesNotCancelReplacementAttach|TestE11TieredSessionRecordSurvivesNodeRestart|TestExecRoundTripCostOfTheDurableSessionTier|TestConsoleE15SimulatedOperatorFlow)$' \
  -timeout 900s ./internal/proto ./internal/session ./internal/node ./internal/sim
```

Every focused command passed. The final tree also passed:

```sh
make
make lint
make test
make race
make conformance
make fuzz FUZZTIME=5s
go mod verify
go mod tidy -diff
make dist
go run ./cmd/protogen --check
make public-api
scripts/lint-locks.sh
```

The complete race run passed every package; `internal/sim` completed in
383.713 seconds. The conformance race subset passed with `internal/sim` in
380.879 seconds. Every fuzz target completed without failure. Distribution
artifacts were built locally only and were not published. No external resource
was created by these regressions. **Status: verified.**

### Aggregate-gate race in restored session-log authority, 2026-09-04

The first clean aggregate run on candidate
`d865ec62e80bb6ada8a8676e0a0465c75821829a` truthfully failed:

```text
Rows: 38 passed, 1 failed, 28 unavailable
Required rows: 38 of 55 passed, 1 failed, 16 unavailable
B0.conformance: failed
External resources created: 1; cleanup verified 1, failed 0
```

`make conformance` detected a data race in
`TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob`.
`restoreSessionLog` copied the complete live node workspace after
`authorizeClaims` released `Node.mu`, while the renew loop updated
`Workspace.LeaseUntil` under that mutex. Session-log artifact authorization
needs only the immutable workspace id, tenant and generation, so restoration
now projects exactly those fields instead of copying mutable lease and
authorization state.

The focused regression mutates `LeaseUntil` concurrently with 10,000 authority
projections; it would race if the projection read mutable workspace fields.
The failing E11 composition test was also repeated under the race detector:

```sh
go test -race -count=100 \
  -run '^TestSessionLogAuthorityDoesNotReadMutableLeaseFields$' \
  -timeout 180s ./internal/node

go test -race -count=20 \
  -run '^TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob$' \
  -timeout 600s ./internal/sim

make lint
make test
make conformance
make race
```

All commands passed. The final `make conformance` simulation package completed
in 392.930 seconds; the complete final `make race` simulation package
completed in 400.301 seconds. The failed aggregate run still verified cleanup
of its one external resource and retained all unavailable rows as unavailable.
The repaired implementation candidate was committed as
`7957c40456ef06fdce4ef441dcf8424f875c324a`.

The clean committed-candidate aggregate was then rerun with the gVisor and
Firecracker host prerequisites named earlier in this ledger:

```sh
go run ./cmd/evidence run
```

It exited zero and reported:

```text
Candidate: 7957c40456ef06fdce4ef441dcf8424f875c324a
Rows: 39 passed, 0 failed, 28 unavailable
Required rows: 39 of 55 passed, 0 failed, 16 unavailable
Registry: 57 scenarios, 6 with no owning proof
External resources created: 1; cleanup verified 1, failed 0
```

The command remains truthfully incomplete rather than a completion gate:
sixteen required rows have no executable aggregate proof and remain named
unavailable. The host-backed B28 and B29 rows passed on this exact candidate.

Built-binary conformance on the same candidate:

```sh
go run ./cmd/conformance --build .
```

reported:

```text
CONFORMANT: remount standalone (remount)
60 passed, 0 failed, 8 unavailable of 68 requirements in 6.845s
required: 53 passed, 0 failed, 0 unavailable
capability-gated: 7 passed, 0 failed, 7 unavailable
extension: 0 passed, 0 failed, 1 unavailable
cleanup: verified
```

The unavailable requirements named absent node-fault, retention-bound,
approve-mode egress, transcript-eviction and notification harness
prerequisites; none was upgraded to passed. Distribution and evidence
artifacts stayed local and no release, tag, image, package or signed artifact
was published. **Status: verified on the committed candidate; aggregate
remains incomplete because its 16 required unavailable rows are unwired.**

### Durable completion failure and CI transport/performance repairs, 2026-09-04

Automated review found that the synchronous session-log seal still discarded a
failed final `CompleteSessionLogRecord`. The log now retains the completion
error, keeps the terminal chunk hidden, and returns `ErrIncompleteLog` instead
of EOF. `session.finish` adds the persistence failure to the terminal
`ExitInfo` before `Session.Wait` can return, while preserving partial segment
records and `OnRecordError`. Node subscriptions translate the incomplete-log
boundary into the failed exit chunk so an attached client does not hang.
Deterministic regressions verify the failed wait/exit result, callback delivery,
preserved blob segments, rejected incomplete restart replay, and subscriber
failure delivery.

The wired-shell evidence runner now treats exit 77 as unavailable only when the
output contains a non-empty `UNAVAILABLE:` or `unavailable:` reason. Missing
`socat` and `setsid` cases remain unavailable; an unexplained exit 77 remains a
failure, and the existing unexplained Go-skip rejection is unchanged.

Focused verification:

```sh
go test -count=1 \
  -run 'TestWaitBlocksUntilDurableCompletionRecordCommits|TestDurableCompletionFailureIsObservableAndReplayStaysIncomplete' \
  -v -timeout 60s ./internal/session
go test -count=20 \
  -run 'TestSubscriptionReportsDurableCompletionFailure|TestReplacingSessionCursorDoesNotCancelSharedPeerWrite|TestStaleSessionDetachDoesNotCancelReplacementCursor' \
  -timeout 120s ./internal/node
go test -race -count=20 \
  -run 'TestWaitBlocksUntilDurableCompletionRecordCommits|TestDurableCompletionFailureIsObservableAndReplayStaysIncomplete' \
  -timeout 300s ./internal/session
go test -race -count=20 \
  -run 'TestSubscriptionReportsDurableCompletionFailure' \
  -timeout 300s ./internal/node
go test -count=1 -timeout 180s ./internal/session ./internal/node
go test -race -count=1 -timeout 300s ./internal/session ./internal/node
```

Every focused command passed.

The S3 compatibility job then reproduced `http: server closed idle connection`
inside the pinned MinIO lane. The same image and test configuration reproduced
the failure locally with the prior 25-second idle timeout on a fresh store,
proving that expiry shorter than a conventional load-balancer timeout did not
cover endpoints that close a reusable connection immediately. Retrying a
conditional write after losing its response would be ambiguous, so the
store-owned default transport now uses a fresh connection instead; a
caller-supplied HTTP client remains untouched.

Verification against the pinned local MinIO:

```sh
go test -race -count=1 -run '^TestMinIOIntegration$' \
  -v -timeout 180s ./internal/artifact/s3
go test -race -count=20 -run '^TestMinIOControlFailoverE10Core$' \
  -v -timeout 600s ./integration/failover
```

The integration passed once and the exact failover core passed 20
repetitions. The disposable MinIO container was removed after verification.

The conformance job also measured the unchanged incompressible-move floor at
18.5 MB/s while all conformance packages competed under the race detector.
Three isolated race repetitions on the same implementation measured 45.0 to
52.1 MB/s. The 20 MB/s lower bound remains unchanged; `make conformance` now
runs its packages serially so the physical throughput assertion measures the
move rather than concurrent package load. The complete conformance command
passed with `internal/sim` in 399.083 seconds, and the complete race command
passed with `internal/sim` in 376.935 seconds.

While the final verification was running, `origin/main` advanced from
`7d1bc8ddbc055be9f57e157bac1915c0cadbe876` to
`4b05a46834a4a84fd5a812296c280031fc603802`. The new commit changed only the
Linux handoff documentation and added a session-wrap document. It was merged
without conflict as `712f4e2a0494c301126bb6232250cf7baf43ea51`.

Post-merge compatibility verification:

```sh
make docs
make race
make conformance
```

`make docs` regenerated both LLM indexes without changing either file.
`make race` passed with `internal/sim` in 370.804 seconds. The serial
conformance target passed with `internal/sim` in 368.098 seconds. The complete
non-race `make` gate had passed immediately before the documentation-only
upstream change with the same implementation candidate.

The final pre-stage fetch then found two more upstream fixes at
`66db37c6b3e8da81bdf200b54ab908fd2d5127e5`: Helm 3/4 document comparison,
host-sized scale simulation, and container-user model-catalog creation. The
branch merged them without conflict. The exact affected tests passed under the
race detector:

```sh
go test -race -count=1 -p=1 \
  -run 'TestHelmLint|TestHelmGoldenTemplateIsCurrent|TestHelmRefusesATwoOwnerConfiguration|TestTheChartOwnsNoWorkspaceLifecycle|TestPlanBOpenCodeDeterministicModelLane|TestHandoffScaleAndControlFailover' \
  -timeout 1200s ./integration/policy ./internal/sim
```

The policy package passed in 1.284 seconds and the simulation package in
88.646 seconds.

No public output was published. **Status: implementation and post-upstream
race/conformance compatibility verification complete; committed-candidate
aggregate and built-binary conformance remain to be rerun after the final
commit.**

### Consolidated September 4 remediation candidate, 2026-09-05

PR #24 combined the independently reproduced RMR-001 through RMR-012
remediations on `origin/main` `7542a236e9cefef180237efd0df58381f104fdfd`.
The first exact provider candidate was
`5ac47ed07a70be53c1222f50c55477bf1af1c232`; a Fly API compatibility fix
produced `b092578`, secret-version and lifecycle fixes produced `10c6198`, and
explicit Fly process-secret selection produced final candidate `b30699a`.

**Modal — verified but bounded on `5ac47ed`.** The named deployment
`remount-pr24-5ac47ed` reported an online node running `5ac47ed`. A disposable
Git repository was uploaded to a workspace, OpenCode used brokered OpenAI
access, and a relative-path retry created `RESULT.txt` with the expected
content. Pulling the workspace recovered the file locally. The event stream
contained `cred.used` and `egress.allowed`; `doctor --deep --json` reported
`ok: true` with tenant-residency explicitly unavailable because that reference
deployment has no tenant authority. The agent was destroyed.

**E2B — verified but bounded on `5ac47ed`.** A custom 2 GiB sandbox ran the
exact binary, accepted a process-backed workspace, executed a command, and
returned inspect and deep-diagnostic output. Workspace and sandbox destruction
completed; the provider sandbox was no longer connectable and the node became
offline after lease expiry. The default 512 MiB E2B sandbox remains
insufficient for installing and running OpenCode.

**ix.dev — unavailable before Remount readiness on `5ac47ed`.** Two VM boots
failed in provider infrastructure before a usable VM existed. The observed
errors included VMM worker startup, virtio-blk root-device, CAS-fold, and page
writeback failures. Cleanup verification showed zero remaining ix VMs. This is
not a Remount pass.

**Fly — authenticated exact-candidate readiness verified on `b30699a`.**
The first live run exposed that the driver sent `timeout=120` to Fly's Machine
wait endpoint, whose documented and observed maximum is 60 seconds; Fly
returned HTTP 400. `b092578` sets and enforces a one-minute maximum, and the
exact integration lifecycle then created, listed, destroyed, and verified
absence of a Machine in 17 seconds.

The first authenticated-readiness runs then exposed three distinct boundaries:
launch was not fenced to the version returned by Fly's app-secrets API; the
driver removed the enrollment secret after provider `started`, before the
guest process consumed it; and a unique process-secret reference was accepted
but not injected without explicit app-secret selection. `723b4e1` added
`min_secrets_version`, `10c6198` retained the unique secret until Machine
destruction, and `b30699a` set `ignore_app_secrets: true` while mapping only
that unique secret to `REMOUNT_ENROLL_TOKEN`.

The repository integration test ran the exact `b30699a` binary from the named
Modal binary endpoint against Fly app `remount-pr24-9f4daf1`:

```sh
go test -tags integration -count=1 -v \
  -run '^TestLiveProvisionerEnrollmentReadiness$' \
  -timeout 240s ./internal/provision/fly
```

It passed in 26.24 seconds after observing a new authenticated, online Fly node
with version `b30699a` and a successful node diagnostic. The control-plane
event stream recorded `node.enrolled`, `node.online`, then `node.offline`;
provider logs recorded the node starting and establishing its uplink.
`doctor --deep --json` reported `ok: true`. Deferred teardown removed the
Machine and its unique app secret; subsequent provider inventory contained
zero Machines and the app-secret list was empty.

The generic provider `started` result still is not the readiness authority:
the pool/controller layer must continue to require authenticated Remount
enrollment before treating a provisioned node as usable.

**Final local candidate gates — verified on `b30699a`.** `make lint`,
`make test`, `make race`, `make conformance`, `make public-api`,
`make fuzz FUZZTIME=5s`, `go mod verify`, `go mod tidy -diff`, `make dist`,
and `git diff --check` passed. The ordinary simulation package completed in
290.272 seconds, the full race simulation package in 394.367 seconds, and the
serialized conformance simulation package in 244.382 seconds. Distribution
produced Linux, macOS, and Windows binaries for amd64 and arm64.

PR checks reported seventeen failures, but attempts to fetch representative
job logs returned Azure `BlobNotFound`; the repository owner reported that the
organization had exhausted its GitHub Actions minutes. Those CI results remain
unavailable evidence rather than local or live-test failures, and they are not
reported as passing.

No credential values were written to this ledger, repositories, workspaces, or
provider logs by the test harnesses.

## 2026-09-04 — local Claude/Codex conversations handed off to a Modal VM

**Verified, with an explicit runtime qualification.** Candidate
`99d3b0f-handoff-r2` is the uncommitted handoff correction on base `99d3b0f`.
The exact frozen binaries were uploaded and their version and SHA-256 verified:

| Binary | SHA-256 |
|---|---|
| macOS arm64 | `de414799fdd9d3ffcede45dede9ddae49b432b086d987d6e89148fe7cf342d9f` |
| Linux amd64 | `bf6c939ad1621028f337d0ac8aa66cd3de36a768f5553608e2dd8757b69a42ef` |

`scripts/live-handoff.py` created synthetic conversations with the locally
installed Claude Code `2.1.260` and Codex `0.152.1`, in isolated homes and
checkouts. No personal conversation history or Keychain login was read or
transferred. Seeds used Anthropic/OpenAI API authentication, and remote model
requests used Remount bindings and placeholders. Claude used
`claude-haiku-4-5-20251001`; Codex used `gpt-5-mini`.

The remote host was a real Modal VM with Docker, 4 GiB memory, one requested
physical CPU and a two-CPU limit, and a 30-minute TTL. A dedicated Docker wrapper
applied `seccomp=unconfined` only to labeled handoff containers using the test
image, after explicit operator approval. AppArmor was not relaxed, privileged
containers were not used, and Codex's own sandbox remained enabled. A hermetic
probe proved an in-workspace write succeeded while a write to `/etc` remained
denied. **This is not evidence that stock Docker supports Codex's nested
sandbox, and no Remount production default was weakened.**

| Proof | Claude | Codex |
|---|---|---|
| Local conversation UUID preserved | `6853709a-b946-4184-80ba-e968062c2008` | `01a06f64-2d8c-7590-9199-18e393b4aebb` |
| Conversation-only nonce recalled and written remotely | passed | passed |
| Linux execution at canonical macOS checkout path | passed | passed |
| Live client interrupted, reconnected, identical replay | passed | passed |
| Original transcript extended | 4,571 → 23,917 bytes | 36,314 → 66,294 bytes |
| Broker audit | 2 `cred.used`, 2 `egress.allowed` | 1 `cred.used`, 1 `egress.allowed` |
| Real credential matches in workspace/environment and pulled result | zero | zero |
| Pull recovered the correct result | passed | passed |

Deep doctor and metrics collection passed. The final workspaces were
`ws_06g6z3x60ke3maptwvg3nez7g4` and `ws_06g6z3ysd3efav44ejxzj3js18`.
Both were destroyed, workspace inventory was verified empty, sandbox
`sb-sKaQqYRpMUR6hmO9prkUtm` disappeared from inventory, its tunnel stopped
serving, app `ap-LtfbJMeVKn6h7EBqxmXaA0` was stopped, and the generated local
Remount token was removed. There are no retained cloud resources from the runs.

Earlier attempts are not relabeled as passes: the first driver used an invalid
`node ls` command and failed readiness; a corrected run passed Claude but found
Codex's provider-filtered `--last` lookup and nested namespace denial; a later
hermetic probe initially omitted its synthetic `CODEX_HOME`. Each earlier VM
was cleaned. Explicit UUID continuation fixed the product lookup defect, and
the approved test-only runtime profile resolved the separate namespace
prerequisite. No additional model calls were needed to recreate local seeds.

Reproduction uses the opt-in `seed`, `prepare`, `test`, `summarize`, and `cleanup`
modes of `scripts/live-handoff.py`, with explicit frozen binary paths, candidate
identifier, and `--nested-sandbox-test-profile` on both preparation and test.
Dependencies are Modal Python SDK and python-dotenv in a separate environment.
The driver consumes only allowlisted values from the explicitly named repository
`.env`, redacts evidence, and rejects implicit VM/test retries. `--keep-on-failure`
retains a failed test VM only until its fixed TTL and requires explicit cleanup.
The offline safety suite is `python scripts/test_live_handoff.py`.

Local detailed evidence remains under ignored
`remount-data/handoff-live-20260905-r4/`, including binary manifests, provider
state, per-step command results, security-profile checks and `summary.json`.
The public record above deliberately contains no credential values or model
conversation text. Model usage was observed but this test does not claim a
hard total dollar cap, production multi-tenant isolation, or E2B/ix.dev coverage.

Local verification of the final candidate passed the full race suite with
`GOFLAGS=-p=1 make race`, the public SDK test, twelve offline driver safety
tests, focused repeated handoff regressions, Linux/Windows cross-vet of affected
packages, module verification/tidy, and the handoff metadata fuzz target. The
binary conformance runner reported 60 passed, zero failed and eight explicitly
unavailable requirements; all 53 required requirements passed and cleanup was
verified. `make lint` passed using a temporary index containing regenerated
`llms` documents; the real staging index was unchanged, and a second generation
produced identical document hashes.

The initial parallel `make race` run failed the existing gVisor inherited-output
timing assertion at 1.335 seconds against its one-second bound. Five isolated
race repetitions and the complete serialized race rerun passed without changing
that test or its implementation. The serialized simulation package completed in
401.060 seconds. This failure is retained as timing-sensitive evidence, not
silently omitted from the record.

## 2026-09-05 — Claude Code local and Modal ACP permission validation

**Verified on an uncommitted candidate based on `68df7f1`.** A disposable local
repository was exercised through both Claude Code transports. PTY mode created
`CLAUDE_LOCAL.txt`; ACP initially reached Anthropic through `b_anthropic` but
could not write because the adapter remained in its default permission mode.
After the recipe and node applied `acceptEdits` through ACP
`session/set_mode`, the same workspace-write launch created
`CLAUDE_ACP_LOCAL.txt`. Both files were read through `remount fs read`, not
inferred from model output. Local events recorded successful credential
substitution and allowed egress to `api.anthropic.com`.

The exact Linux candidate was then deployed as the uniquely named Modal app
`remount-claude-0c59ef2b` in the `dev` environment with a unique volume,
control secret, and Anthropic secret. Authenticated `nodes --json` showed one
online Modal process node running `68df7f1-dirty`. Claude ACP created
`CLAUDE_ACP_MODAL.txt` containing `claude-acp-modal-ok`; `remount fs read`
confirmed the bytes. The Agent reported a structured ACP session and one
completed turn. Durable events recorded four successful `cred.used` decisions
for `b_anthropic` to `api.anthropic.com:443/v1/messages` with HTTP 200, plus
the accompanying `egress.allowed` events.

This run also verified the deployment change that accepts an optional,
separately named Anthropic Modal secret and constructs `b_anthropic`; the
previous reference deployment could broker only OpenAI. The disposable Agent
and owned workspace reached `destroyed`. The Modal app was stopped and its
volume and test-only secrets were deleted; provider inventory verified that
all uniquely named resources were absent.

No credential value was placed in a command argument, workspace, repository,
or this ledger. The Modal reference still uses Remount's process backend and
cooperative proxy egress; this point-in-time test is not production
multi-tenant isolation evidence.

The first full local verification run also exposed a test-isolation defect:
`TestRunValidatesBeforeDialing` discovered the active local `b_anthropic`
binding, selected API-key authentication for its Claude case, launched a real
Agent, and reached the package timeout instead of exercising the expected
pre-dial error. The test now uses an empty temporary `REMOUNT_DATA` directory.
The formerly hanging case then passed in 0.008 seconds while the live local
binding still existed. With an empty verification data directory, all 17
portable `make verify` gates then passed, including the full suite, race,
conformance, bounded fuzzing, static analysis, vulnerability scan, generated
documentation check, and cross-platform vetting. Four pending Agents created by
the diagnostic reruns were explicitly destroyed, their owned workspaces were
absent from the active workspace list, and the disposable standalone process
was stopped.

### Follow-up: omitted Agent sandbox normalization

Automated review found that the CLI's `workspace-write` default did not cover
direct `AgentCreateReq` clients, because `AgentSpec.Sandbox` is optional on the
wire. The control plane now rejects unknown sandbox values and normalizes an
omitted value to `workspace-write` after parent inheritance and before the
Agent is persisted or dispatched.

The regression was first observed against the unfixed code: the focused
control test returned an empty persisted sandbox and the end-to-end simulation
returned the same empty value before the harness turn. The fixed simulation
uses the public Go client to omit `sandbox`, crosses the control plane and node,
and observes `acceptEdits` in the fake ACP session before the first prompt.
Existing child-Agent coverage confirms an omitted child sandbox still inherits
the parent's explicit `read-only` value.

The review verification also exposed a second source of nondeterminism in
`TestRunValidatesBeforeDialing`: an intentionally provisioned provider key lets
the CLI synthesize a default binding even when `REMOUNT_DATA` points at an
empty directory. The test now clears every preset provider-key environment
variable as well as isolating local data, so its pre-dial validation cases do
not depend on the developer's credentials. Ten focused ordinary repetitions
and five race-detector repetitions passed with the Anthropic key still
provisioned outside the test.

After that correction, an isolated full `make verify` run passed all 17
portable local gates. The ordinary suite completed in 423 seconds, the race
lane in 679 seconds, serialized conformance in 352 seconds, and bounded
fuzzing in 87 seconds. Native macOS and Windows runtime lanes remain CI-only;
their cross-platform vet gates passed locally.

## 2026-09-05 — PR #30 live functional regression verification

**Verified within the boundaries below.** The candidate is PR #30 commit
`47b379fb36e9d1dac6ece6e0d9a4121cd1328f8f`; merge commit `0cab8a1` has the
same tree. Tests used the repository `.env` through allowlisted parsing,
without sourcing shell commands, displaying credential values, or copying the
repository or personal conversations into a workspace.

| Frozen binary | SHA-256 |
|---|---|
| macOS arm64 | `4b44780f7d2d166ef6cf8c8ab21353a0b123fa37b7a2e812162536eedbaf892f` |
| Linux amd64 | `8d52a88667fdd406d76b3ad914e4495e4afcc48c942523cd3f645531678aa16e` |

### Real Claude conversation and tools on Modal

The checked-in `scripts/live-handoff.py` driver ran `seed`, `prepare`, and
`test --recipes claude`, with `--execute`, explicit frozen binary paths, and
`--candidate 47b379f`. Run directory:
`remount-data/pr30-live-20260905-claude/`. This was a new ephemeral Modal VM,
not a redeployment or test of the named reference service. It used stock Docker
security options; the nested-sandbox test relaxation was not enabled.

Claude Code `2.1.260`, using `claude-haiku-4-5-20251001`, remembered a nonce
from a synthetic local conversation and wrote it into `RESULT.txt` remotely.
The selected conversation UUID survived, its transcript grew, execution was
Linux at the canonical checkout path, and `pull` recovered the exact result.
A live attachment was interrupted and a fresh attachment replayed identical
output. Audit evidence contained two `cred.used`, two `egress.allowed`, and
one each of `run.started` and `run.finished`.

The remote workspace scan examined 26 files (52,326 bytes) and found zero real
credential matches; the workspace environment and retrieved tree were also
clean. A synthetic canary verified the scanner. Deep artifact checks found
zero damaged objects in both server and node stores. **Tenant residency was
explicitly unavailable:** this standalone test had no tenant-policy authority.
The doctor's zero exit and top-level `ok` do not turn that finding into a
verified residency check.

Workspace `ws_06g77apen1k6zb71qfagrzd5q0` was destroyed and authenticated
workspace inventory was verified empty. Sandbox `sb-ndsPMU6o727ItQN0maPknS`
was terminated, absent from inventory, and its tunnel stopped serving. App
`ap-6j5RlgZwxhKVzVUM7x5kyE` was stopped; the generated local control token was
removed. No persistent provider volume or named secret was created.

### Real OpenCode workflows on local Docker

The real-provider Go lanes used `OPENAI_API_KEY` from `.env` only as the
subprocess environment variable `REMOUNT_INTEGRATION_OPENAI_KEY`, with
`go test -race -count=1 -v -timeout=25m ./internal/sim -run
'^TestRunOpenCode(DockerIntegration|HandoffAcrossNodesDockerIntegration|QueueDockerIntegration)$'`.
The host daemon was Docker `29.4.1`. The `node:22-bookworm-slim` image resolved
to digest `sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5`;
the stock recipe installed OpenCode `1.18.29` and used `openai/gpt-4o-mini`.
The recipe's npm install is not version-pinned, so this is point-in-time
harness evidence rather than a reproducible dependency-resolution guarantee.

The two-node conversation-continuity lane passed in 127.10 seconds: two
model turns preceded the move, and a third turn on generation 2 recalled
both facts. Three run-start/run-finish pairs and at least three credential-use
events were verified. The two-task queue lane passed in 99.49 seconds,
checking both file contents, cursor completion, and checkpoint/queue events.
That live lane checkpoints without sleeping; the separate repeated simulator
test covers the queue's sleep-and-move path.

The file-writing/broker lane passed in 316.50 seconds, including real
`GREETING.txt` creation, placeholder configuration, workspace/snapshot/event/
diagnostic secret scans, and planted synthetic-canary positive controls. All
three live lanes passed under the race detector in 544.695 seconds. The
redacting wrapper detected no credential in command output. Final Docker
inventory contained no Remount workspace containers, matching the empty
pre-run inventory; the test process exited. The extensive snapshot scans are
not a benchmark of workspace movement latency.

### SDK and lifecycle regression checks

An isolated authenticated local server and process node, both running the
frozen binary, exercised the actual Python and built TypeScript SDKs:

- nonempty digest-verified artifact roundtrips (1,054,200 bytes in Python;
  1,048,613 bytes in TypeScript), plus over-limit download rejection;
- stdin plus EOF, fresh-client replay, and ten fast-output execs per SDK;
- Python nonzero exit propagation, authoritative snapshot, sleep/wake, and
  file persistence;
- a control-plane process restart during Python session output, with exact
  `beforeafter` output, preserved files, and same-generation node re-adoption;
- a real TypeScript WebSocket cut during output, with exact `beforeafter`
  output and a successful terminal exit.

Both workspaces were destroyed, inventory was empty, and the owned server/node
processes exited. Local deep doctor found no damaged artifacts. Private
reproduction scripts and diagnostics are under
`/private/tmp/remount-live-pr30.3ERIxs/`; they are evidence, not new public API.

Python's 28 tests, TypeScript's 31 tests, and the strict-profile cross-language
protocol gate all passed. Three race-detector repetitions passed the focused
session-authority, reused-workspace security, partial-input, lossless reconnect,
sleep/wake, and queue-across-sleep/move regressions. The handoff driver's twelve
offline safety tests also passed.
The complete client, session, and launch packages passed one further race
run. `make docs`, `make lint`, and `git diff --check` passed for this ledger
update; generated user-documentation bundles did not change.

These are functional checks, not throughput benchmarks or a production
isolation certification. Process has no host isolation and Docker egress is
cooperative. No E2B, ix.dev, native Windows, KVM, or gVisor live lane was run
in this pass. The full portable verification matrix was exercised during PR
preparation; this follow-up adds targeted race and real-provider evidence,
not another full-matrix run. GitHub-hosted checks remained externally blocked
by the account billing/spending-limit condition reported on PR #30.

## 2026-09-05 — Current-documentation freshness pass

Reviewed current user guides, examples, architecture/status claims, build and
release guidance, repository skills and generated documentation against source.
The checkout began at `47b379f`; a final fetch found documentation-only PR #31,
and the checkout was fast-forwarded to `4e3066b` before reapplying the audit
changes. The new desktop/VNC guidance was retained. There is no Go runtime or
dependency change: the only Go edit is the ix.dev constructor's explanatory
comment. The documentation generator now uses the actual repository URL and
has offline regression tests.

The new `current-status.md` distinguishes implemented features, incomplete
stock integrations and exact-host verification requirements. Old ADRs and
dated findings were preserved; the engineering index and living review
playbook now point to current source/evidence instead of treating a historical
closure as the current feature inventory.

Verification on macOS arm64 with Go 1.27.1:

- `make test` passed all ordinary packages and the separate public-SDK module;
  `internal/sim` completed in 325.053 seconds. The suite began before the
  documentation-only fast-forward; `git diff 47b379f origin/main -- '*.go'
  go.mod go.sum` confirmed no Go or module changes in that update.
- `make docs` regenerated both bundles. Four
  `python3 scripts/test_gen_llms.py` tests passed: default repository/section
  targets, base-URL override and determinism, empty override fallback, and
  checked-in output freshness.
- `make lint` passed after integration. A temporary Git index held the expected
  generated outputs for its `git diff --quiet` gate; the real staging index
  was left unchanged. This checks regenerated content without committing it.
- The relative-link scan checked 147 file links across 137 Markdown inputs,
  including linked heading fragments, with no missing targets.
- Both edited Claude skills passed the skill-creator frontmatter validator.
  Its PyYAML dependency was installed only in a temporary validation environment.
- Focused `go test ./scripts/security-profiles ./internal/provision/ix` and
  `git diff --check` passed. Authenticated GitHub inspection still reported a
  private repository and an empty release inventory.

This was not a new comprehensive security audit, a dependency upgrade, or a
production qualification. No `.env` credentials were loaded, cloud resources
deployed, browser/host-isolation lanes rerun, or new race/full-verify result
claimed. Prior live and race evidence remains dated in its original entries.

## Historical PR #13 evidence integrated on 2026-09-05

The following entries retain the original Linux candidate results. Their
integration does not claim a new Linux/runsc host pass on current main.

### 2026-09-04 — release-matrix node inventory propagation

The Ubuntu test job failed `TestReleaseMatrixUnderLocalProfile` because the
test queried control-plane node inventory immediately after both node-local
`Online` signals. One node had completed its local handshake but had not yet
become visible to the subsequent client list request. The test now waits,
within its existing 30-second context, until both exact node identities are
listed before asserting their negotiated protocols. The protocol and placement
assertions are unchanged.

Verification:

```sh
go test -race -count=20 \
  -run '^TestReleaseMatrixUnderLocalProfile$' \
  -timeout 300s ./internal/sim
go test -count=1 -timeout 1200s ./internal/sim
staticcheck ./internal/sim
go vet ./internal/sim
```

All commands passed. The repeated race proof completed in 19.219 seconds and
the complete simulation package completed in 251.767 seconds.

### 2026-09-04 — Windows-main integration after the Ubuntu CI fix

The Linux branch merged `origin/main` at
`51185ddeda5c4451c024a1fd7c660e4cc271a52b`, retaining the upstream
cross-platform split files, serialized heavy test gates, bounded concurrent
cursor detach, and local workspace cleanup. The merge also retained the Linux
branch's durable session completion ordering, exact stale-grant handling,
durable event polling, filesystem-access preparation, and deny-first gVisor
proof.

Verification:

```sh
make lint
make test
make race
make conformance
go test -race -count=20 \
  -run '^TestReleaseMatrixUnderLocalProfile$' \
  -timeout 300s ./internal/sim
staticcheck ./internal/sim
go vet ./internal/sim
go test -count=1 -timeout 1200s ./internal/sim
go run ./cmd/conformance --build .
sudo -n env \
  REMOUNT_GVISOR_ROOTFS=/home/ubuntu/firecracker-artifacts/gvisor-rootfs-alpine-3.22 \
  scripts/gvisor-spike.sh
```

All commands passed. The repeated release-matrix race proof completed in
19.063 seconds and the complete simulation package completed in 254.963
seconds. The complete race suite and serialized conformance suite passed. The
built-binary conformance run reported 60 passed, 0 failed, and 8 unavailable;
all 53 required requirements passed and cleanup was verified.

Linux 5.15 rejected the upstream netdev egress hook with `Operation not
supported`; the spike installed its deny-first host-ingress fallback and
passed broker reachability, IPv4 TCP, IPv6, UDP, DNS, ICMP, raw-socket, unlisted
CONNECT, and post-revoke denial checks. Its exit trap deleted the runsc
sandbox, veth, nftables table, network namespace, bind mount, and temporary
directory. **Status: verified.**

### 2026-09-04 — B32 aggregate correction and merged-main follow-up

The first aggregate run on committed candidate
`a48d046132ba01976fb799424607654bb33c32b9` correctly failed:

```text
evidence: verdict failed: 1 required rows failed, 20 unavailable, 0 leaks
B32: owning proof skipped without an unavailable reason
```

The B32 registry command ran the entire `integration/installs` package even
though its owner is `integration/installs.TestB32*`. The package also contains
the unrelated `TestCleanEnvKeepsWindowsPackageManagerRoots`, whose
Windows-contract skip is not a B32 proof. The evidence parser remains strict;
the registry command was narrowed to its owning tests and a regression fixes
that exact command:

```sh
go test -count=1 -timeout=20m -run '^TestB32' ./integration/installs/
```

On committed candidate `ed84f259c17043b81391ae758df2048faa950014`,
the command passed in 45.813 seconds. The clean committed-candidate aggregate
then completed:

```sh
go run ./cmd/evidence run
```

```text
Rows: 35 passed, 0 failed, 32 unavailable
Required rows: 35 of 55 passed, 0 failed, 20 unavailable
External resources created: 1; cleanup verified 1, failed 0
```

This result is deliberately **incomplete**, not passed: all 20 required rows
that did not execute remain unavailable with named missing prerequisites or
unwired owning proofs. B32 passed and no required row failed.

After `main` advanced to `069a17d3acf63bcd6050acf4bbadd4ef3d90ad61`
with four Windows-lane test corrections, that commit was merged without
overwriting the follow-up changes. On committed candidate
`ff23e7e29c3c96dd676de4e516b9d317da93f380`, the direct B32 proof passed again
in 49.368 seconds, the affected workspace packages passed under the race
detector, and the aggregate repeated the same 35 passed, 0 failed, 32
unavailable result with verified cleanup.

Built-binary conformance on the same candidate:

```sh
go run ./cmd/conformance --build .
```

```text
60 passed, 0 failed, 8 unavailable of 68 requirements
required: 53 passed, 0 failed, 0 unavailable
cleanup: verified
```

After the original pull request merged, the unmerged work was moved to a
follow-up branch based on current `main`. The exact Linux host proof was
re-run:

```sh
sudo -n env \
  REMOUNT_GVISOR_ROOTFS=/home/ubuntu/firecracker-artifacts/gvisor-rootfs-alpine-3.22 \
  scripts/gvisor-spike.sh
```

Linux 5.15 again rejected the guest netdev egress hook with `Operation not
supported`. The host-veth ingress fallback installed; the positive control,
all denial probes, and post-revoke denial passed; the script reported
`gVisor E4 spike passed`. Its exit trap removed the runsc sandbox, nftables
tables, namespace, veth pair, bind mount, bundle, and state root. B32 removed
its temporary install and build directories. **Status: verified.**

## Reviewed PR integration and real-provider check — 2026-09-05

Candidate `0da858a539d6ace30784a01cf6aea70ce50216db` integrates PRs #1–#4,
#7, #12, and #13 onto `3620140691c2f5b70652da7c10d1b1af097e4c0d`.
The control, node, workspace, client, CLI, and public SDK runtime code is
unchanged from that base. Historical host results above remain evidence for
their named candidates, not fresh host qualification of this integration.

Focused checks passed: the evidence package under race, 20 race-enabled
release-matrix repetitions, and 10 race-enabled repetitions of the new
gVisor script contract. The script contract fails against the previous
script, proving that its fallback regression is observable. It stubs
privileged commands and does not qualify Linux kernel enforcement.
Actionlint 1.7.12 passed with ShellCheck and Pyflakes disabled; shell syntax,
`make lint`, generated-document tests, and `make docs` also passed.

The exact candidate also passed a real OpenCode/OpenAI integration on local
Docker, using only the authorized OpenAI key loaded in memory from `.env`:

```sh
go test -race -count=1 -v -timeout=12m ./internal/sim \
  -run '^TestRunOpenCodeDockerIntegration$'
```

The test passed in 292.31 seconds (package: 293.858 seconds), using
`openai/gpt-4o-mini` and
`node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5`.
The real agent wrote `GREETING.txt` containing `hello`, also independently
read from its container. Assertions covered run lifecycle and broker
credential-use events, workspace and snapshot leak scans with planted
canaries, event and deep-diagnostic checks, and workspace/container cleanup.
After completion, Docker inventory for the exact workspace label was empty.
The key was not placed in the workspace, command arguments, or this ledger.

This exercises candidate Go components with in-process simulation transport
and a real Docker harness/provider, not a deployed cloud service or a frozen
standalone CLI binary. Docker remains cooperative isolation; unavailable
read-only mounts are not an enforced-profile pass. No cloud resources were
created. GitHub jobs on the pre-integration main were refused before startup
by account billing/spending limits. Native Windows, Linux/runsc, KVM, cloud
deployment, and release-signing qualification remain unavailable here.

`PATH=/Users/andrewgao/go/bin:$PATH make verify` completed with all 17
portable local gates passing: formatting; host/Linux/Windows/Darwin vet;
lock discipline; the main suite (556 seconds) and external public SDK suite;
distribution builds; module verification/tidy; staticcheck; govulncheck;
seeded fuzz corpus; the full race suite (763 seconds); conformance (379
seconds); and nine bounded fuzz targets (110 seconds total). This is the
complete local gate set, not a hosted CI or native multi-platform pass.
Only this ledger entry was added after the tested code commit; documentation
generation, lint, generated-document tests, and diff checks were rechecked.

---

## 2026-09-05 - durable workspace holds and idle policy, live standalone

**Status: verified.** Every claim below was observed against a real
`remount standalone` process on loopback, not in simulation. One defect was
found and fixed during the run; it is recorded at the end.

Host: macOS 26.3 (build 25D2125), arm64. Go 1.27.1 darwin/arm64. Tree:
branch `worktree-agent-a40d1a9490b4e2dd8` at `d3ddc7a` (base
`claude/gap-brief-2026-09-06` at `fe59175` plus the integrator's helper rename),
plus the working-tree changes this entry describes. No credential of any kind
was used or needed; the standalone server has no token and the run reached no
network but loopback.

Setup, run twice — once with the binary built from `d3ddc7a`, and again after
the node fix below:

```sh
go build -o "$SCRATCH/remount" ./cmd/remount
"$SCRATCH/remount" standalone --listen 127.0.0.1:7455 --data "$SCRATCH/live/data"
export REMOUNT_SERVER=http://127.0.0.1:7455
```

`/healthz` reported `{"ok":true,"peers":1,"security_mode":"standalone",
"security_ready":true,"serving":true}`. Node `n_06g78r36j6rr8szdf61szaqdxw`,
process backend, local and unisolated by design.

### 1. A hold outlives the process that took it

```sh
remount ws create --name live-lease-1 --wait          # ws_06g78xsntjh37chere1wk0g0h8
remount exec ws_06g78x… -- sh -c 'echo background job started; sleep 300' &
remount ws lease ws_06g78x… --max 30s --reason background_job
kill -9 <the exec client pid>
```

`ws lease` returned `wl_06g78xtyevmnxkxrv27vg6hhkr` with
`max_alive_until=2026-09-06T02:06:45Z` and exited. The streaming client was
SIGKILLed; no Remount client process from this run remained. A **fresh**
`remount ws lease get` then reported the hold and
`lifecycle deadline: sleep at 2026-09-06T02:06:45Z (in 25s, source=lease)`,
proving the deadline is readable from a process that never armed it.

At 02:06:45.552Z, with no client alive, the control plane acted:

| seq | event | payload |
|---|---|---|
| 78 | `ws.lifecycle.expired` | `action=sleep source=lease at=1788660405158 timer=t_lc_ws_06g78x…` |
| 79 | `ws.lease.expired` | `lease=wl_06g78xt… reason=deadline` |
| 82 | `s.exited` | `code=143 signal=terminated reason=lifecycle_deadline_expired` |
| 83 | `ws.snapshot` | `authoritative=true consistency=quiesced` |
| 86 | `ws.paused` | `reason=lifecycle_deadline_expired` |

`remount ws get` reported `"state": "paused"`. Exit code 143 is SIGTERM, so the
graceful stop ran before the kill rather than after it.

### 2. The deadline survives a control-plane restart

```sh
remount ws create --name live-lease-2 --wait          # ws_06g78y0stj4zkthb0m1g0y4bf0
remount ws lease ws_06g78y0… --max 90s --reason survives_control_plane_restart
kill -TERM <standalone pid>                            # 02:07:14Z, mid-deadline
"$SCRATCH/remount" standalone --listen 127.0.0.1:7455 --data "$SCRATCH/live/data"
```

The hold was granted at 02:07:06.750Z with a deadline of 02:08:36.750Z. The
server process was stopped 8 seconds later and a new one started at 02:07:17Z,
which re-claimed the workspace (`ws.state_changed node.disconnect` at
02:07:10, `ws.claimed` at 02:07:17.283). `remount ws lease get` against the new
process reported the same lease and
`lifecycle deadline: sleep at 2026-09-06T02:08:36Z (in 1m11s, source=lease)`.

At 02:08:37.271Z — 0.5s after the deadline, and across a process boundary the
original grant never saw — `ws.lifecycle.expired`, `ws.lease.expired
{reason: deadline}` and `ws.paused {reason: lifecycle_deadline_expired}`
committed. That is the durability claim: nothing in memory carried the
schedule.

### 3. Idle policy plus mark-idle sleeps the workspace

```sh
remount ws create --name live-idle-3 --wait           # ws_06g78yfffke0ggkv5bv6ctadpc
remount ws idle-policy ws_06g78yf… --sleep-after 15s
remount ws mark-idle ws_06g78yf… --reason turn_settled
```

`mark-idle` printed
`lifecycle deadline: sleep at 2026-09-06T02:09:24Z (in 15s, source=idle)`.
Events: `ws.idle.policy_set {sleep_after_sec: 15}` at 02:09:06.857,
`ws.idle.marked {idle: true, idle_since: 1788660549876}` at 02:09:09.876, and
`ws.lifecycle.expired {action: sleep, source: idle}` at 02:09:25.270 followed
by `ws.paused`. Note the source is `idle`, not `lease`: the workspace had no
hold.

### 4. mark-active before the deadline prevents it

```sh
remount ws create --name live-active-4 --wait         # ws_06g78ym078h3pyvpa2fx43kjs8
remount ws idle-policy ws_06g78ym0… --sleep-after 20s
remount ws mark-idle ws_06g78ym0… --reason turn_settled     # deadline 02:10:04Z
remount ws mark-active ws_06g78ym0… --reason new_turn       # 02:09:56Z
```

`mark-active` printed no deadline line, because there was no longer one to
print. At 02:10:39Z — 35 seconds past the original deadline — `ws get` still
reported `"state": "claimed"` with `last_activity_at` set and no
`lifecycle_deadline`, and the workspace's event stream ended at
`ws.idle.marked {idle: false, idle_since: 0, reason: new_turn}`. No
`ws.lifecycle.expired` was ever emitted for it.

### Defect found and fixed: the exit chunk never reached a live subscriber

Running `examples/long-running-autosleep` against this server exposed a real
bug that none of the simulation tests covered. The workspace reached `paused`
and the node emitted `s.exited {reason: lifecycle_deadline_expired}`, but the
example hung indefinitely in `client.Copy` and had to be killed after seven
minutes. Its own output stopped at "deadline fired".

`Node.releasePrepare` cancelled every output subscription for the workspace in
the same critical section that removed the workspace from the serving map, and
only then terminated the sessions. The exit chunk the graceful stop produced
therefore had no subscriber left to receive it. The reason still reached the
durable log — which is why the existing sim test, which reattaches and replays,
passed — but a client that never went away saw its stream go silent and never
close. That is worse than an exit and worse than an explicit gap: it is
indistinguishable from work still in progress.

The fix separates the fence from the cut. Removing the workspace from the
serving map still happens first, so a queued session starter fails its
post-lock serviceability check. The subscriptions are now drained after
`stopWorkspaceSessionsGraceful` has joined every session and closed every log,
bounded by `subscriberDrainTimeout` (5s), and cancelled after that. A failed
quiesce still cancels immediately.

`internal/sim/lifecycle_exit_delivery_test.go`
(`TestLiveSessionReceivesLifecycleExitChunk`) was written first and observed
failing on the unfixed tree — "the attached session stream never ended after
the lifecycle deadline fired", after the full 60-second bound — and passes in
3.7 seconds after the fix.

With the fix in place the example completed in about 35 seconds against the
same live server:

```
held ws_06g792n… until 2026-09-06T02:27:39Z (lease wl_06g792n…, on_expiry sleep)
background job started
renewed wl_06g792n… until 2026-09-06T02:27:34Z (renewals 1)
pending deadline: sleep at 2026-09-06T02:27:34Z (source lease)
deadline fired: workspace ws_06g792n… is paused, checkpoint art_sha256:2737d5e1…
session s_06g792n… exit code 143 reason "lifecycle_deadline_expired"
event ws.lifecycle.expired action=sleep source=lease
event ws.lease.expired lease=wl_06g792n… reason=deadline
woken: node n_06g78r3… gen 2
filesystem survived: written before the hold expired
processes after wake: processes-gone
workspace ws_06g792n… destroyed
```

The last two lines are the non-guarantee stated out loud: a hold checkpoints
the filesystem, not memory.

### Bounds

This is a single-host, single-node, process-backend standalone deployment on
loopback. It proves the control-plane deadline is durable across client death
and a control-plane restart, and that the CLI, the events and the exit reason
say what the documentation says they say. It does **not** exercise a
multi-node failover during an expiry, an isolated or enforced-egress backend, a
cloud deployment, Windows (where the graceful stop degrades to immediate
termination), or a control plane behind a real load balancer. Those remain
covered only by `internal/sim` or not at all.

### Cleanup

Six workspaces were created. The successful example run destroyed its own; the
other five were destroyed by hand with `remount ws destroy`, including the one
left behind by the aborted first example run — which is itself worth recording,
because a client killed mid-run leaves the workspace for its owner to reclaim
rather than tidying up on the way out. `remount ws ls` afterwards was empty.
The standalone process was stopped and the scratch data directory removed.
Nothing was created outside the session scratchpad and no cloud or provider
resource was involved.
