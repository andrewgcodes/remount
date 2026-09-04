# ADR 0062: vendors provision whole Remount nodes

## Status

Accepted.

## Context

`workspace.Backend` is a local execution contract. Its handle exposes a
host-side jailed filesystem, rewrites local session specifications, freezes
managed execution for checkpoints, and may synchronously revoke a workspace's
network capability. A remote vendor sandbox cannot implement that contract
without a second node-like protocol and a second source of lifecycle truth.

Provider credentials also have a different authority boundary from workspace
credentials. They create machines for the control plane; they must never be
sent to a node or workspace. A newly created node needs only one short-lived,
single-use enrollment capability and then proves possession of its persistent
node key on every connection.

## Decision

Infrastructure vendors implement `internal/provision.Driver`. A driver creates
and destroys a whole machine that runs `remount up`; it is not a workspace
backend. The backend selected inside that node advertises the capabilities it
actually enforces. Vendor firewall or sandbox settings are defense in depth and
do not upgrade those backend capabilities.

A vendor-pool machine belongs to exactly one tenant for its lifetime. The
`provision.Request` carries that tenant and a short-lived enrollment token only
during `Create`. Drivers place the token in the provider's protected process
environment, never argv, labels, returned `Machine` state, reusable images,
events, or logs. Returned provider inventory is non-secret and copied before it
crosses a goroutine or reconciliation boundary.

## Consequences

- One node per vendor machine and one tenant per machine is the tenant boundary.
- The same provisioner works with process, Docker, gVisor, or future
  Firecracker backends; only the backend's descriptor determines placement.
- Pool reconciliation can be provider-neutral and test against a fake driver.
- Provider APIs and eventual consistency remain external failure modes. Create,
  destroy, and list must be idempotent by provider identity and expose partial
  or unavailable state rather than reporting success from request acceptance.
- The control plane must mint, consume once, expire, and audit node enrollment
  tokens before pool-created nodes can be wired into production.

## Verified provider contracts (2026-09-03)

The implementations use only the following documented surfaces. A provider
feature that is absent from these surfaces returns `provision.ErrUnavailable`;
the driver does not infer success or invent an undocumented request.

- **Fly Machines:** Bearer-authenticated `POST` and `GET
  /v1/apps/{app}/machines`, `GET .../{id}/wait`, and forced `DELETE .../{id}`
  follow the [Machines resource](https://fly.io/docs/machines/api/machines-resource/).
  Region and the documented `shared-cpu-Nx` / `performance-Nx` guest shapes
  are native Machine fields. The enrollment value is staged from stdin with
  [`fly secrets import --stage`](https://fly.io/docs/flyctl/secrets-import/),
  referenced by name in the Machine process, then removed with the documented
  staged [`fly secrets unset`](https://fly.io/docs/flyctl/secrets-unset/).
- **E2B:** `POST /sandboxes`, paginated `GET /v2/sandboxes`, and idempotent
  `DELETE /sandboxes/{id}` use E2B's checked-in [OpenAPI
  contract](https://github.com/e2b-dev/e2b/blob/main/spec/openapi.yml). The API
  authenticates with `X-API-Key`; bootstrap values use `NewSandbox.envVars`
  and tenant/pool identity uses `metadata`. The public create schema has no
  explicit region or size fields, so requests containing either are
  unavailable. Inventory follows `X-Next-Token` with a finite page bound.
- **Modal:** the supported [Sandbox SDK
  contract](https://modal.com/docs/guide/sandboxes) provides create, tags,
  list, terminate, inline secrets, resources, and region placement. Modal does
  not publish a stable Sandbox REST contract, and its CLI does not expose the
  full reconciliation lifecycle. Remount therefore requires an independently
  versioned `remount-modal-provisioner` helper built against a supported Modal
  SDK instead of guessing private HTTP. Missing helper or Modal credentials is
  unavailable.
- **ix.dev:** publishes a CLI and per-language SDKs
  ([docs](https://ix.dev/docs)) but no REST contract. The CLI does cover the
  lifecycle a pool needs — `ix new --name --region --no-shell`, `ix ls
  --output json`, `ix rm`, and `ix secret set`, which reads a value from stdin
  and documents that it "is never taken on the command line". That is enough
  to drive natively, the way `fly` is driven, and a native driver is the
  intended end state.

  Remount ships the helper protocol for it today because ix.dev has no tags or
  labels on a VM, and so nowhere to record the tenant and pool that pool
  reconciliation filters on. Encoding them into the VM name is the likely
  answer — the name is the only provider-side string Remount controls — but it
  needs a delimiter that cannot collide with an operator-chosen node name, and
  it publishes the tenant to anyone who can list the account. Until that is
  settled the helper keeps the mapping behind a versioned contract. The
  response schema itself is known and recorded in
  `docs/engineering/ix-dev-native-driver-2026-09-04.md`. The driver pins to a
  tenant and pool. Region is placement the vendor
  supports and is passed through; an unset region leaves the vendor's own
  default in force. Size is not part of that surface and is unavailable.
  Missing helper or API credential is unavailable.
- **BYO SSH:** OpenSSH documents that remote arguments are joined into one
  command string before transmission ([`ssh(1)`](https://man.openbsd.org/ssh.1)),
  and [RFC 4254 section 6.5](https://www.rfc-editor.org/rfc/rfc4254#section-6.5)
  defines that remote `exec` string. Consequently only a fixed absolute helper
  path and fixed `create|list|destroy` words enter that string; the JSON request
  and enrollment token travel on stdin. `BatchMode`,
  `StrictHostKeyChecking=yes`, a dedicated `UserKnownHostsFile`, no global
  known-host fallback, and a pinned identity follow
  [`ssh_config(5)`](https://man.openbsd.org/ssh_config.5). An unreachable host
  is unavailable/error state, never empty healthy inventory.

External helper adapters use the same bounded JSON protocol: invoke
`HELPER create|list|destroy`, provide exactly one JSON request on stdin, accept
bounded JSON stdout (1 MiB by default, 16 MiB hard maximum), discard stderr,
and never include helper output in an error. `create` returns one non-secret
`provision.Machine`; `list` returns
`{"machines":[...]}` (SSH returns `{"present":bool,"machine":...}`); and
`destroy` returns no body and treats an absent provider object as success. The
Modal helper receives credentials only through `MODAL_TOKEN_ID` and
`MODAL_TOKEN_SECRET`; the ix helper receives `IX_TOKEN`, the vendor's own
variable. Neither helper
may copy those credentials or the enrollment value into argv, tags, stdout,
stderr, a reusable image, or provider inventory.

## Rejected alternatives

- A remote `workspace.Backend` or `remount shim`: it duplicates the node
  protocol and splits filesystem/process authority.
- Long-lived node tokens baked into images: compromise scales to every future
  node made from that image.
- Treating a vendor network allow-list as `enforced_gateway`: it cannot prove
  the per-workspace broker and revocation contract.
