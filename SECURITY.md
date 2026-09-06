# Security policy

Remount is a self-hosted runtime. There is no Remount-operated service, so
every deployment is owned and operated by the people who run it; this policy
covers the code in this repository.

## Reporting a vulnerability

Report privately through GitHub's vulnerability reporting for this repository
("Security" tab, "Report a vulnerability"), which opens a private advisory
with the maintainers. Do not open a public issue for a suspected
vulnerability.

Include the commit or version, the backend and runtime profile involved
(`process`, `docker`, `gvisor`, `firecracker`; `dev`,
`trusted-single-tenant`, `multi-tenant-isolated`, `microvm`), a reproduction,
and what an attacker gains. Never include a real credential; a synthetic
canary is enough to show a leak.

You will get an acknowledgement within a few days. Fixes land on `main` with
a regression test and, where the boundary allows, an entry in the
verification ledger under `docs/engineering/`.

## Supported versions

No release has been tagged yet. Until the first tagged release, `main` is the
only supported line and every fix ships there.

## What Remount promises, and where the edges are

The security model is documented rather than implied:

- `docs/security-profiles.md` states, per backend, what isolation, egress
  enforcement and multi-tenant guarantees are advertised, and what is not.
- `docs/adr/0089-runtime-profiles-and-drift.md` defines the named runtime
  profiles and the fail-closed node gate.
- `spec/PROTOCOL.md` §9 defines secret-blind execution and the broker's
  refusal contract.
- `AGENTS.md` lists the invariants every change is checked against.

The `process` backend has no host isolation, and `docker` is not a boundary
between mutually untrusted tenants; both are refused by the production
profiles. A report that one of them allows a workspace to reach the host is
expected behavior, not a vulnerability. A report that `gvisor` or
`firecracker` allows it is.

Brokered credentials never enter a workspace; harness-native logins do. A
compromised node can read the upstream credentials its broker substitutes,
and revoking a binding in Remount does not revoke the key at the provider.
