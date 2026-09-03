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
