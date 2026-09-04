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
