# ADR 0040: Security semantics are named capabilities, not additive fields

## Status

Accepted.

## Context

ADR 0019 fixed the v1 wire contract: peers negotiate exact capability strings
at hello, and within v1 every change is an additive field that an older peer
ignores. That rule is right for anything an older peer can ignore *safely*: a
new label, a new optional response field, a new operation that answers
`unsupported`.

It is wrong for the changes the durable runtime and governance layers are
about to make. Pushed revocation epochs, controller epoch fencing,
principal-bound session capabilities, chunked artifact verification, held
approvals, and artifact encryption each close a hole. If any of them arrived
as an additive field, an older node would ignore the field and keep the hole
open, while the control plane, having sent the field, would believe the hole
closed. That is a silent downgrade: the deployment's `isolated` or
`multi_tenant` promise would be false on exactly the nodes least likely to be
noticed. Before P0.10 the only identifier `proto.NegotiateCapabilities` knew
was `v1`, so there was no way to express "this peer implements the fix".

## Decision

1. **Two classes of protocol change.** A change an older peer could ignore
   without weakening a security property stays an additive field. A change an
   older peer ignoring it would weaken is a **named capability**: an exact
   identifier offered at hello and echoed only when both sides implement it.
   `spec/PROTOCOL.md` §3.1 keeps the table; the identifiers are defined now
   so the table, the profile requirements and the golden fixture are stable
   before the semantics land: `authz-push` (P0.11), `controller-epoch`
   (P4.4), `session-cap` (P3.4), `chunked-artifacts` (P4.1), `approvals`
   (P3.1), `encrypted-artifacts` (P2.5).

2. **Negotiation is the ordered intersection with what this release
   implements.** `proto.NegotiateCapabilities` still refuses a peer without
   `v1`, then returns, in canonical order, the offered identifiers this
   release implements. Unknown identifiers are dropped, never echoed, so a
   peer can only rely on what both sides understand. `proto.PeerCapabilities`
   is the single list a node or client offers; a capability joins it in the
   same commit that lands its semantics on both sides, never earlier.

3. **Security profiles require capabilities.** `proto.SecurityCapabilities`
   maps `local` to nothing and `isolated`/`multi_tenant` to every named
   capability this release implements. The control plane enforces this at
   three points and the node at one:
   - **Hello.** A deployment with a security floor (`--mode production-*`)
     refuses any peer, node or client, whose negotiated set lacks a required
     capability. The error is `unsupported` and names the missing identifiers
     and the profile, so an operator sees "node lacks authz-push required by
     isolated" rather than a workspace that never leaves `pending`.
   - **Placement.** Without a floor, `eligibleBackendLocked` treats a node
     that negotiated fewer capabilities than a workspace's profile requires
     as ineligible, the same way it treats a missing connector or backend
     descriptor. An old node keeps serving `local` workspaces.
   - **Observability.** `NodeStatus.protocol` records what each node
     negotiated at its last hello, so `node ls` shows why a workspace is
     waiting.
   - **Node.** `node.materialize` refuses a workspace whose profile requires
     a capability the control plane never echoed, before any bytes land, and
     the ordinary materialize-failure path releases the claim with the reason.
     This is the fail-closed answer to an *older control plane* driving a
     newer node: the node cannot know what the old control plane will not do,
     so it declines to serve profiles that depend on it.

4. **Old peers keep working under `local`.** Nothing here changes the
   standalone experience. A node built before this ADR negotiates `["v1"]`,
   is enrolled, and serves local workspaces exactly as before.

## Consequences

- A security fix that changes what a peer must *do* now has a place to be
  named, and the profile table is where its enforcement is stated. Adding a
  capability is: define the identifier here and in §3.1, implement both
  sides, add it to `implementedCapabilities` and to the isolated and
  multi-tenant rows of `profileCapabilities` in the same commit, extend the
  golden hello fixture.
- Refusal is loud and early. A deployment floor rejects an old node at hello;
  a mixed fleet without a floor holds isolated workspaces pending and reports
  the node's negotiated set. Neither path serves the workspace with the
  property missing.
- The v1 golden fixture now has a companion: a hello offering every named
  capability, pinning that capabilities travel as a plain ordered array of
  strings so an old peer sees unfamiliar strings and simply never echoes
  them.
- Tests: `internal/proto` covers negotiation order, unknown-identifier
  dropping and the invariant that every profile-required capability is one
  this release offers (or the release would refuse itself);
  `internal/control` covers hello refusal under each floor and per-workspace
  eligibility for an old node; `internal/node` covers the node-side refusal
  under an old control plane; `internal/sim` runs the release matrix under
  `local` with an old node (hello rewritten and re-signed on the wire) and an
  old control plane (hello response rewritten), asserting both directions
  keep working and that an isolated workspace pinned to the old node stays
  pending.
- ADRs 0020–0046 are reserved by the build plan for the items it names; the
  Phase 0 decisions that predate this ADR were renumbered to 0047–0050 so the
  plan's references stay exact.
