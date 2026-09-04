# Linux-host handoff — 2026-09-03

**Status:** mostly closed on 2026-09-04. Read this header before the sections.

This document was written on the belief that everything in it was blocked on a
Darwin host by a missing kernel mechanism. **That belief was wrong**, and the
two largest items — gVisor (§1) and Firecracker (§2) — have since been run to
completion on this same Mac. Docker Desktop cannot host them because its daemon
is a locked-down LinuxKit VM, but Colima provides a real Linux VM with its own
daemon, and `--nested-virtualization` provides a real `/dev/kvm`. The recipe is
in each section.

Running them found eleven defects, one of them a security defect in gVisor
egress enforcement. All are recorded in
`docs/engineering/gvisor-egress-finding-2026-09-04.md`, and the ones that were
fixed are fixed. **Nothing here should be deferred to "a Linux box" again
without first trying the recipe.**

The remaining sections (§3 onward) are still genuinely externally gated: they
need credentials, a Kubernetes cluster, or a published release, not a kernel.

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

**This is no longer blocked on macOS, and the lane has now been run.** Docker
Desktop cannot host `runsc` because its daemon lives in a locked-down LinuxKit
VM, but that is a property of Docker Desktop, not of the machine. Colima
provides a real Ubuntu VM with its own daemon, and gVisor's `systrap` platform
needs no KVM:

```sh
brew install colima && colima start --arch aarch64 --cpu 4 --memory 6 --disk 20
colima ssh -- sudo sh -c 'cd /tmp && \
  curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/latest/$(uname -m)/runsc && \
  chmod 755 runsc && mv runsc /usr/local/bin/ && runsc install && systemctl restart docker'
sh integration/chaos/backend-gates.sh --probe   # REMOUNT_CHAOS_IMAGE=alpine:3.20
```

Running it found four defects, all recorded with reproduction steps in
`docs/engineering/gvisor-egress-finding-2026-09-04.md`. **Two are fixed and
`TestE4DenialConformance` now passes**, three consecutive runs, leaving no
resource behind:

- the deny-first egress policy was not enforced at all — gVisor injects frames
  below netfilter's IP hooks, so the output chain never saw them and every
  forbidden destination crossed the veth. Containment came only from Docker's
  default `FORWARD policy drop`; with `FORWARD ACCEPT` a sandbox reached 8.8.8.8
  and was answered. Now carried by a netdev egress chain on the workspace veth.
- an `nsfs` mount leaked per workspace on the successful path. Now released in
  `execRuntime.destroy`.

- a killed run leaked its netns, veth and runsc sandboxes, and because the
  slot is derived from the workspace rather than the PID, a restarted node
  collided with its own leftovers deterministically. Now reaped at startup,
  sandboxes first so the namespaces read as uninhabited.
- the docker backend did not verify its daemon could see the node's data
  directory, so a remote daemon produced workspaces that started, accepted
  writes and silently held none of them. Now probed with a nonce at startup.

**E5 is done too.** `TestE5SiblingTenantsCannotReachEachOther` puts two
workspaces on one backend, proves each reaches its own broker so the negative
results mean something, and proves neither reaches the other's broker, guest
address or DNS. Confirmed on the wire rather than by exit status, because exit
status is exactly what fooled everyone about E4: capturing during a run shows 7
frames inside each tenant's own /30 and zero crossing. That is what earns
`SiblingIsolation`. B28 is recorded passed on the same evidence.

**What remains yours:** B29 (Firecracker, needs `/dev/kvm`) and the rest of
§15's scale and release-candidate work.

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

**A Mac can now be that host, which changes who should do this.** Apple Silicon
M3 and later with macOS 15+ support nested virtualization, and Colima exposes it
with one flag. Verified on an M5 Pro / macOS 26.3:

```sh
colima delete --force
colima start --nested-virtualization --arch aarch64 --cpu 4 --memory 6 --disk 20
colima ssh -- ls -la /dev/kvm        # crw-rw---- 1 root kvm 10, 232
```

With real firecracker + jailer v1.16.1, the Firecracker CI kernel
(`vmlinux-5.10.223`, aarch64) and a 256 MiB ext4 rootfs built from `alpine:3.20`,
`integration/firecracker/host-gate.sh` reports **AVAILABLE** rather than
"Firecracker requires Linux", and `go test -race -count=10
./internal/workspace/firecracker` is green.

**The lane now works end to end.** On that host the whole lifecycle was driven
by hand:

```
create   -> claimed, backend=firecracker
exec     -> uname -r = 5.10.223 (host is 6.8.0-117-generic)
snapshot -> --authoritative: consistency=quiesced, 37 MB
restore  -> ws create --restore-from <artifact> reaches claimed,
            and the file written before the checkpoint reads back
```

Getting there fixed three defects in this backend, all recorded in
`gvisor-egress-finding-2026-09-04.md`: a guest handshake that raced the boot and
failed on the first attempt every time; a compatibility fence that could never
be satisfied because it contained a Firecracker log timestamp, so no checkpoint
could ever be taken; and a snapshot restore that never remapped the network
device, so no checkpoint could ever be restored.

**What is still not exercised**, and what B29 is bounded on: the §5 hardening
list — prepare-abort, stale generation, incompatible CPU or Firecracker version,
corrupt bundle, disk-full staging, and proving a restored process continues
*exactly once*. Also `.github/workflows/kvm.yml` still fails its final step on
purpose; that step's premise is stale, since the adapters it waits for are wired
in `cmd/remount/build_node.go`, but updating it needs someone who can run the
workflow.

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

**Updated 2026-09-03 (late): the static half is done.** `tofu` (OpenTofu
v1.12.6) and `helm` (v4.2.4) were installed on the Darwin host, so §13 work
items 2, 3 and 7 were authored and validated there. See
`docs/engineering/verification-2026-09.md`, entry "Plan B phase B5 — OpenTofu
and Helm validation", for the exact commands, the rejection table, and the
failure injections.

What now exists and is validated:

- `deploy/tofu/` — four modules (network, control, artifact-store, node-pool)
  and a reference root module. `tofu fmt -check -recursive` and
  `tofu validate` pass with no cloud credentials and no registry access,
  because no module declares `required_providers`: the provider seam is
  `terraform_data`, a builtin.
- `deploy/helm/remount-node/` — a DaemonSet chart with a committed golden
  render. `helm lint` and a golden `helm template` diff pass.
- `integration/policy/` — the §13.7 policy tests. Fourteen bad configurations
  are refused by `tofu validate` or `helm template`, and four static scanners
  cover `deploy/**`, `packaging/container/Dockerfile` and
  `images/workspace/Dockerfile`. Every rule has been watched failing.
- Every image reference in those surfaces is pinned by digest.

**Already done and proven on this host:** the Docker Compose reference
deployment in `deploy/compose/` — control plane, two nodes and MinIO, with
`smoke.sh` proving both nodes join, a workspace runs real work, moves between
nodes, and keeps its filesystem. Re-proven after the MinIO digest pin, with
cleanup verified.

### What is still yours

1. **Install the chart against a real cluster.** There is no Kubernetes cluster
   on the Darwin host, so nothing here has been applied. On a Linux host with
   kind, k3s or a real cluster:

   ```sh
   kubectl create secret generic remount-control-token --from-literal=token=...
   helm install remount-nodes deploy/helm/remount-node \
     --namespace remount --create-namespace \
     --set image.digest=sha256:<the digest your build produced> \
     --set control.endpoint=http://remount-control.remount.svc.cluster.local:7443
   ```

   The postcondition to assert is the one `deploy/compose/smoke.sh` asserts:
   every scheduled node appears in `remount nodes` with `ONLINE true`, a
   workspace runs real work, and it survives a move. Then delete the release and
   confirm the namespace is empty. Record it as `host-ci`.

   The node image must be the one `deploy/compose/node.Dockerfile` builds. The
   chart refuses the control-plane image outright, because that image is
   `FROM scratch` and a process-backend workspace in it dies with
   `"sh": executable file not found in $PATH`.

2. **Substitute a real provider at a module seam.** Each module's
   `terraform_data` resource is the seam. Replacing one with, say,
   `aws_instance` is a local edit inside that module; nothing outside it moves,
   because consumers read the module's outputs. Doing that needs a cloud
   account, so it is out of scope for a validation lane and belongs in a
   separate, credentialed ledger entry. Do not weaken
   `TestTheModulesDeclareNoProvider` to land it in the main tree: a module with
   `required_providers` makes the credential-free gate un-runnable.

3. **§13 work items 4, 5 and 6 are untouched.** Private-network examples
   (VPC-only, relay, Tailscale sidecar), secret-manager adapters through the
   existing secret-source interface, and Prometheus scrape plus event-export
   examples. Items 4 and 5 have static-validation halves that could be done on
   any host; item 6 wants the local receivers the Compose stack does not yet
   run.

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
built binary here: 53/53 required rows, 0 failed. It imports no Remount
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
