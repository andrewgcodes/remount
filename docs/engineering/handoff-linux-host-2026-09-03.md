# Linux-host handoff — 2026-09-03

**Status:** open, with a named next proof for each item

Everything here was blocked on this session's host (Darwin 25.3.0 arm64) by a
missing kernel mechanism or a missing external resource, not by unfinished
code. Each item names the exact prerequisite and the exact command, so a Linux
agent can run it without re-deriving anything.

Read first: `AGENTS.md`; `docs/engineering/plan-b-repository-executable-2026-09-03.md`;
the `2026-09 build plan disposition` section of
`docs/engineering/implementation-closure-2026-09-03.md`; and
`docs/engineering/verification-2026-09.md`, which is the ledger you must add to.

## The rule that governs every item below

A check that cannot run is **`unavailable`**, never `passed`. Plan B §6.2 is
explicit that there is no `skipped-success` outcome, and §15.3 says unit and
simulation tests **may not** upgrade a host-gated status. Do not convert a
skipped job into a pass; record the reason instead. `go run ./cmd/evidence run`
already produces exactly that record — use it rather than inventing a format.

---

## 1. gVisor: E4, E5, B28 — the isolation lane

**Why it is blocked here:** Docker Desktop on macOS runs its daemon inside a
VM, so `runsc` cannot be registered as a runtime. Verified:

```
$ sh integration/chaos/backend-gates.sh
docker status=available server_version=29.4.1
gvisor status=unavailable reason=runsc is not registered with Docker
```

**What you need:** a Linux host with Docker and `runsc` registered as a
runtime.

**What to run:**

```sh
sh integration/chaos/backend-gates.sh --probe   # REMOUNT_CHAOS_IMAGE must be set
go test -count=1 ./internal/workspace/...        # gVisor unit + real-backend
```

**What must be proven** (ADR 0061 and the wrap handoff): the seven-check denial
conformance suite at connect time — IPv4 TCP, IPv6, UDP, DNS to 8.8.8.8, ICMP,
raw socket, and CONNECT to an unlisted host. The backend may advertise
`enforced_gateway` **only** after that suite passes on the exact candidate.
Also prove synchronous `RevokeNetwork` kills an in-flight transfer, and that
cleanup runs after every failure path.

E5 additionally needs two mutually untrusting tenants on one gVisor node, with
every denial appearing as an event.

**Do not** claim `isolated` or `multi_tenant` from a passing unit test. The
capability is earned by the probe, not by the code existing.

---

## 2. Firecracker: 2.3, B29 — deferred by decision, still needs a KVM host

The user decided on 2026-09-03 to defer this to a Linux session. It is blocked
here by hardware: macOS cannot provide `/dev/kvm`.

```
$ sh integration/firecracker/host-gate.sh
firecracker status=unavailable reason=Firecracker requires Linux
```

**What you need:** Linux x86_64 or arm64 with KVM, a matching static
`firecracker` and `jailer`, a trusted guest kernel and rootfs, and a cgroup
parent.

**The full required-proof list is in `handoff-2026-09-03-codex-wrap.md` §5**
("P1 — finish the Firecracker production proof"). In summary: build the guest
agent and verify its manifest hash; boot through the real jailer and vsock
bridge; prove file, exec, PTY and port operations; checkpoint a running guest,
move it, restore disk/state/memory and prove the process continues **exactly
once**; exercise prepare-abort, prepare-commit, stale generation, incompatible
CPU or Firecracker version, corrupt bundle and disk-full staging; prove
TAP/netns deny-first behavior and cleanup after every failure.

**One thing changed here that you must know about.** `ws.moved` used to report
`processes="preserved"` purely from the artifact format, at move-request time,
before a destination was chosen or any guest restored — a false audit claim
that ADR 0063 forbids. It now reports `restore_pending`, pinned by
`TestMoveNeverClaimsPreservedProcessesBeforeRestore`. **Completing the
positive claim is your work:** a destination that actually restored a guest
should be what promotes `restore_pending` to `preserved`, and that needs a
protocol field carrying the restore outcome (`WSReadyReq` carries none today).
Do not simply revert to claiming it from the format.

`.github/workflows/kvm.yml` deliberately fails its final integrated-host
adapter step because the real KVM E2E invocation is not wired. Wire it; do not
delete the failure.

---

## 3. Live E2B pool lanes: B11, B12, B13, B19, and E17's real-vendor half

**Why it is blocked here:** not credentials. I verified the key works:

| Request | Result |
|---|---|
| `GET https://api.e2b.app/v2/sandboxes` | `200` |
| `GET https://api.e2b.app/templates` | `200`, **0 templates** |
| `POST /sandboxes` with public template `base` | `201` |
| `DELETE /sandboxes/{id}` | `204`, account verified back to 0 |

The blockers are (a) no `remount-node` template exists in the account, and (b)
a vendor sandbox must dial the control plane back, which a laptop
`127.0.0.1` cannot offer.

**What you need:** a publicly reachable control-plane URL, a built E2B
template containing the exact candidate binary, and a one-time enrollment
token. Plan B §10 requires the test harness to **create all of these itself**:
"the lane must not depend on a manually maintained template, binary URL,
enrollment token, tenant, or pool variable."

**What already exists so you do not rebuild it:** `internal/provision/e2b` is
fully tested against `internal/provision/e2b/e2bfake`, a fixture-driven fake
whose shapes were captured from the real API on 2026-09-03. B8, B9 and B10 are
proven keyless. The env-gated live test is
`internal/provision/e2b/live_test.go` (`-tags integration`) and it names its
missing variables explicitly:

```
SKIPPED unavailable: missing required environment names
  [REMOUNT_E2B_TEMPLATE REMOUNT_VENDOR_SERVER_URL REMOUNT_VENDOR_ENROLL_TOKEN
   REMOUNT_VENDOR_BINARY_URL REMOUNT_VENDOR_TENANT REMOUNT_VENDOR_POOL]
```

**A correction you should carry forward:** the real `POST /sandboxes` returns
**no** `metadata`, `startedAt` or `state` — only the list endpoint carries
those. The fake was originally more generous, which is exactly how a driver
grows a dependency production never satisfies. See
`internal/provision/e2b/e2bfake/contract/`. If E2B's shapes drift, update those
fixtures; they are the single visible contract.

**Cleanup is part of the proof.** B13 requires every sandbox and template to be
absent afterwards, verified rather than assumed.

---

## 4. Docker and gVisor scale matrix: item 6.4's remaining half

The 200-node/2,000-workspace scenario is proven on the **process** backend and
reproduces in 37.9 s:

```sh
go test ./internal/sim -run '^TestHandoffScaleAndControlFailover$' -count=1 -timeout=15m
```

Item 6.4 asks for the same p50/p99 for claim, exec round trip, move and
reattach on the **docker and gVisor** backends. Docker works on this host and
the docker backend is proven functional, but the scale matrix was not re-run
under it. Do that, and gVisor's, on a named host, and record the host in
`docs/benchmarks.md` alongside the numbers.

Do not quote the process-backend numbers as isolation-backend performance.

---

## 5. Plan B B5: Terraform and Helm validation

`tofu` and `helm` are not installed here, so those items can be authored but
not validated, and must report `unavailable`.

**Already done and proven on this host:** the Docker Compose reference
deployment in `deploy/compose/` — control plane, two nodes and MinIO, with
`smoke.sh` proving both nodes join, a workspace runs real work, moves between
nodes, and keeps its filesystem. `docker compose config` validates.

**Yours:** OpenTofu/Terraform modules (`tofu fmt -check`, `tofu validate`
without cloud credentials) and a Helm chart or Kustomize reference
(`helm lint`, golden `helm template` output), plus the policy tests §13.7 asks
for — configurations with two owners for the same fact must be rejected, such
as Terraform attempting to move a live workspace.

**A packaging finding you will hit immediately:** `packaging/container/Dockerfile`
is `FROM scratch`. That is correct for the control plane and the CLI, and
**impossible for a process-backend node** — its workspaces have no userland, so
a session dies with `"sh": executable file not found in $PATH`. See
`deploy/compose/node.Dockerfile` for the node image and the comment explaining
the asymmetry with docker-backend nodes.

---

## 6. Plan B B31 and B32: reproducible artifacts and clean installs

Not blocked by this host in principle, but they want clean containers and
disposable environments, which a Linux CI host does properly.

- **B31**: build the static binaries **twice in clean containers** and compare
  checksums. `make dist` produces all six platforms and is green here.
- **B32**: install every artifact — OCI archive, local Python wheel, local npm
  tarball, Go external module, SBOM, checksums — into a clean disposable
  environment and run the black-box conformance smoke:
  `go run ./cmd/conformance --endpoint URL --external`.

The conformance runner exists and reports **CONFORMANT** against a freshly
built binary here: 52/52 required rows, 0 failed. It imports no Remount
implementation package, so it can judge an install without a source tree.

**Nothing is published.** Plan B §18 explicitly cuts "a public release, tag,
image push, package publish, or signing-identity action", and the user
separately withheld release authority on 2026-09-03. Build and install
locally; do not publish.

---

## 7. Other externally gated rows

| Item | Missing prerequisite |
|---|---|
| E12 published SDKs | a real PyPI/npm publication; a local install is not publication |
| E13 two real MCP harnesses | two harness keys; absence stays an explicit skip |
| E14 real GitHub App | a registered app; today it runs against a local fake |
| Real WorkOS OIDC | a WorkOS tenant; simulated OIDC is not WorkOS evidence |
| Non-AWS S3 conditional writes and multipart | a non-AWS S3 endpoint |
| Temporal Cloud worker restart | Temporal Cloud credentials |
| Real Slack signature and delivery | a signing secret and webhook URL |
| Clean-machine README flow under five minutes | a disposable VM |

---

## What to do with your results

Add an entry to `docs/engineering/verification-2026-09.md` for every run:
the exact command, the provider identifiers used, the observed result, and the
teardown. Mark each **verified**, **verified but bounded**, or **unavailable**.
Then update the disposition section of
`docs/engineering/implementation-closure-2026-09-03.md`, and wire any scenario
you close into `internal/evidence/registry.go` — set `Owner` and `Argv`, and
add its id to `wiredScenarios` in `registry_test.go`, which exists so wiring is
a deliberate act rather than an accident.

Do not edit historical findings to make them look closed. The ledger is where
status changes belong.
