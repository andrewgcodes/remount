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
16.1.0. Final verification integrated `origin/main` at `7d1bc8d`.

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
| Firecracker KVM end-to-end | macOS cannot provide `/dev/kvm`; needs a Linux KVM host |
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
