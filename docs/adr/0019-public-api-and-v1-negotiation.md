# 19. Public clients use resource types and protocol v1 negotiates exactly

**Status:** accepted

## Context

The original Go client lived under `internal/`, making it impossible for an
external module to import. At the same time, frame comments implied permissive
version skew even though no compatibility contract or fixture proved it. That
combination made both source and wire stability aspirational.

## Decision

`remount.dev/remount/api` exposes resource types, stable error codes and event
payload decoding. `remount.dev/remount/client` exposes URL-based construction,
workspace/session/filesystem/event/fleet/diagnostic methods, explicit
idempotency options and bounded aggregate helpers. Grants, frames, transport and
relay mechanics remain internal. CI compiles and tests the packages from a
separate module with Go's internal visibility rules enforced.

Wire version 1 is exact. Hello must carry frame version 1 and negotiate the
`v1` capability before application requests flow. Unknown additive
operations receive `unsupported`; unknown fields may be ignored. A checked-in
hex fixture fixes deterministic v1 request encoding, while malformed and
reordered frames are fuzzed.

Release CI pins third-party actions to immutable commits, validates formatting,
vet, lock discipline, unit/integration/race/conformance/fuzz/static/vulnerability
gates, cross-builds six platforms, and produces checksums, SPDX SBOM, build
provenance and a keyless checksum signature.

## Consequences

External harnesses no longer depend on implementation packages, and an
incompatible peer fails at handshake rather than halfway through an operation.
Changing an existing field's semantics requires a new frame version and new
fixtures; additive operations retain v1 and use `unsupported` discovery.

The public packages are a compatibility commitment, not a claim of a stable
1.0 release. Event tailing currently reports terminal stream errors through
connection/client state rather than a separate public error channel; callers
that require an exact historical range should use `ReadEvents` and handle
`api.CodeEvicted` explicitly.
