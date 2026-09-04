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
