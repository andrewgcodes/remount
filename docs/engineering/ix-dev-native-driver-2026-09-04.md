# ix.dev: what is done, and what a native driver still needs

`internal/provision/ix` targets **ix.dev** (Indexable). It previously described
**iximiuz Labs** playgrounds driven by `labctl`, which is a different company
and a different product; that support is removed. The confusion was already
visible in the tree: `handoff-2026-09-03.md` called this "the ix.dev
provisioner" and the credential was `IX_DEV_API_KEY`, while the package doc,
ADR 0062, and the `Playground`/`Account` config fields all described iximiuz.

## What is done

- Every iximiuz and `labctl` reference is gone from code, tests, ADR 0062, and
  `.github/workflows/vendors.yml`.
- `Playground` and `Account` are removed from the driver config and from
  `provisionerEntry`; nothing else used them.
- The credential is the vendor's own `IX_TOKEN`, and region is the vendor's own
  `IX_REGION`. Remount does not invent names for either. An unset region is
  left unset so ix.dev's own default applies rather than Remount pinning one.
- Region is now passed through rather than refused. Size stays `unavailable`:
  it is not part of ix.dev's documented machine surface.
- The operator-facing env var name remains per-deployment configurable through
  `token_env` in the provisioners file; the driver never hardcodes a read.

## Why the helper protocol is still there

`ix` covers the whole lifecycle a pool reconciler needs, and does it in a way
that satisfies Remount's secret rule without any wrapper:

| need | command |
|---|---|
| create | `ix new ix/base:latest --name N --region R --no-shell` |
| list | `ix ls --output json` |
| destroy | `ix rm <id>` |
| enrollment token off argv | `ix secret set <name>` (value from **stdin**) then `ix new --secret-env` |

`ix secret set` documents that the value "is read from a hidden prompt, stdin,
or `--value-file`; it is never taken on the command line" — the same
secret-blind provider handoff that the Fly driver implements through Fly's
encrypted app-secrets API.

So a native driver is both possible and preferable: it removes a
`remount-ix-helper` binary that is not shipped anywhere in this repo, which
means the driver as it stands cannot actually provision anything.

The schema is now known. Verified live on 2026-09-04 after updating the CLI
(`ix 2026-06-28` was rejected by the server with `unsupported method:
vm.build_commit_from_oci`; `curl https://ix.dev/install.sh | sh` brought it to
`ix 2026-08-20 (a4ef7254bb)`, which works). One VM was created, observed, and
destroyed:

```json
{
  "id": "01a06e40-a390-7631-a0a9-0015cc458545",
  "name": "remount-probe-1",
  "owner_id": "01a02248-076a-7881-86ee-f8bcfb22d14e",
  "owner": "andrewgao22-01a02248",
  "region": "us-west-1",
  "status": "running",
  "image": "ix/base@blake3:b9842c21...",
  "ipv6": "2604:2dc0:500:8400:72:372:ae1a:578a",
  "ipv4": null,
  "failure_reason": null, "failure_kind": null, "failure_retryable": null,
  "node": "hil-compute-6",
  "created_at": 1788556125071,
  "disk_bytes_used": 0
}
```

Mapping onto `provision.Machine`:

| `provision.Machine` | ix field | note |
|---|---|---|
| `ID` | `id` | UUID |
| `Name` | `name` | what `--name` set |
| `Region` | `region` | `us-west-1` was the default |
| `State` | `status` | `running` |
| `CreatedAt` | `created_at` | Unix **milliseconds**, not seconds |
| `Tenant`, `Labels` | — | **no field exists** |

**The open design question is tenancy, not parsing.** ix.dev has no tags or
labels on a VM, so there is nowhere to record the tenant and pool that
`provision.ListOptions` filters on and that `Create` refuses to cross. Every
other driver leans on provider-side metadata for this. The options are to
encode tenant and pool into the VM `name` and parse them back in `List`, or to
keep that mapping in Remount's own state. The name is the only provider-side
string under Remount's control, so encoding it there is the likely answer, but
it needs a delimiter that cannot collide with an operator-chosen node name and
it makes the tenant visible to anyone who can list the account.

Until that is settled the helper protocol keeps the mapping behind a versioned
contract, which is what ADR 0062 asks for.

Two operational details worth knowing, both learned the hard way here:

- `ix rm` refuses to act without a TTY unless given `--force`
  ("refusing to prompt without a TTY"). Any automation must pass it.
- `ix new` prints a boot trace and opens a shell unless given `--no-shell`.

## Note for whoever runs the live lane

`handoff-2026-09-03.md` records that a 2026-09-02 session hit provider-side CAS
errors in `us-west-1`, which is ix.dev's default region, and that these VMs get
no public IPv4 — so the node must dial out, and the binary upload path must not
depend on `ix shell` stdin. If the default region misbehaves, set
`REMOUNT_IX_REGION=us-east-1`; the driver passes it through as `IX_REGION`.

## Live Agent validation, 2026-09-05

A disposable ix.dev VM in `us-east-1` successfully ran a candidate Remount
process node, dialed out to the Modal control plane, and claimed an existing
workspace. Repeated Codex ACP launches then failed before `initialize`; debug
node logs showed the generated launcher exiting under `set -u` because
`OPENAI_API_KEY` was unset. The workspace had binding `b_openai`, but a direct
`ws create --binding` had no `remount.bindings` launch label, so the node could
not reconstruct the recipe environment even though `.remount/env` correctly
contained `REMOUNT_REF_OPENAI`.

The protocol now carries non-secret binding declarations on `AgentSpec`.
`agent create --ws ... --binding ID:PRESET` preserves custom IDs and
parameterized presets, the control plane verifies each ID is already attached
to the workspace, child Agents inherit only declarations their parent held,
and nodes retain the old workspace-label fallback.

The rebuilt candidate was deployed to both the Modal control plane and the
ix.dev node. A new Codex ACP Agent then initialized, completed one turn, wrote
`IX_AGENT_PROOF.md`, read it itself, and stopped with `end_turn`. An independent
`remount fs read` returned exactly:

```text
ix-agent-real
binding-propagation-fixed
```

The Agent response retained `binding_specs: ["b_openai:openai"]`; the workspace
still had binding `b_openai` and no launch labels. Durable events recorded the
run, session, OpenAI egress, turn, destruction, and session exit. Destroying
the Agent joined its ACP process, and a direct process check on the ix.dev VM
found no remaining Codex adapter.

The ix.dev base image had Codex but not its ACP adapter. Its first adapter fetch
through the workspace proxy stalled at the npm registry, while direct VM
network access worked, so the adapter was preinstalled into the disposable
workspace before the successful Agent run. This validates a real remote
Codex/API-key Agent and the binding-propagation fix; it does not validate
on-demand npm bootstrap on ix.dev, other recipes, or production isolation. The
node used the cooperative process backend.
