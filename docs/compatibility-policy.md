# Compatibility policy

Remount is a self-hosted, open-source runtime. This document says which
surfaces carry a compatibility promise, what a version number means for each,
and how a change that breaks one is announced. There is no hosted Remount
service, so nothing here promises anything about one.

Nothing has been tagged or published yet; the mechanics are prepared and
release is deferred by owner decision (see [releases](releases.md)). Read this
as the contract the first tagged release enters into.

## What is public

| Surface | Identifier | Current |
|---|---|---|
| Go SDK | `remount.dev/remount/api` and `remount.dev/remount/client` | untagged |
| Python package | `remount` (`sdk/python`) | 0.1.0 |
| TypeScript package | `@remount/sdk` (`sdk/typescript`) | 0.1.0 |
| Wire protocol | frame version `1` plus negotiated capabilities (`spec/PROTOCOL.md`) | v1 |
| CLI | documented flags and `--json` shapes of `cmd/remount` | untagged |

Everything under `internal/` is private, including `internal/proto`. The Go
type aliases in `api` are the supported names for those bodies; `make
public-api` compiles `integration/publicsdk` from outside the module to prove
a consumer never has to reach into `internal/`. A program that imports an
internal package is not covered by any promise here.

The `deploy/`, `bench/`, `evidence/`, `examples/` and `integration/` trees are
demonstrations and proofs, not API. They may change in any release.

## What a version number means

Remount uses [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)
per surface. Below 1.0.0 the minor number carries the breaking change, as
SemVer §4 allows; the announcement rules below apply either way.

- **Go SDK.** MAJOR for removing or changing the meaning of an exported
  identifier in `api` or `client`, or for a change that stops existing code
  compiling. MINOR for new methods, new fields on request/response structs, and
  new constants. PATCH for fixes that keep every signature. The Go module path
  gains a `/v2` suffix at major 2, as the Go toolchain requires. Error `Code`
  values are stable across patch releases; `Msg` is for humans and may change
  in any release, which is why callers match on `Code` and never on message
  text.
- **Python and TypeScript packages.** The same rules applied to the generated
  declarations and the client classes. Their types are generated from
  `internal/proto` by `cmd/protogen` (`make protogen-check`), so a protocol
  change that is additive on the wire is additive in both packages.
- **Wire protocol.** Versioned by the frame's `v` field, independently of any
  package version. See the next section.
- **CLI.** MAJOR for removing a subcommand or flag, changing a flag's meaning,
  or removing or retyping a field in a `--json` document. MINOR for new
  subcommands, new flags with a backward-compatible default, and new `--json`
  fields. A `--json` consumer must ignore fields it does not know.

## The wire format is additive within a version

`spec/PROTOCOL.md` §2 and §13 are normative; this is the summary.

- A receiver rejects a frame whose `v` is not a version it negotiated. Within a
  negotiated version, unknown fields are ignored, unknown frame kinds are
  ignored, and an unknown `op` gets a response with code `unsupported`.
- New optional operations and new optional fields are therefore additive inside
  v1 and need no version bump. A peer never assumes a field it did not send
  will come back.
- A change whose omission by an older peer would weaken a security property is
  **not** an additive field. It is a named, exact, case-sensitive capability
  negotiated in `hello` (§3.1), so a peer that cannot honor it is detected
  rather than silently trusted.
- A semantic change, a removal, or an incompatible change to an existing
  baseline field requires a new frame version and an explicit dual-version
  migration window in which both are served.
- Deterministic v1 golden fixtures under `internal/proto/testdata` fail the
  build on accidental wire drift.

## Deprecation

A deprecation is announced before it takes effect, never at the moment of
removal.

1. The replacement ships first, in a MINOR release, so both paths work.
2. The old path is marked `Deprecated:` in its Go doc comment, in the generated
   Python and TypeScript declarations, and in the reference documentation, each
   naming the replacement.
3. The `CHANGELOG.md` `Deprecated` section records it in the release that marks
   it, and the `Removed` section records it in the release that removes it.
4. Removal happens no earlier than the next MAJOR release and no sooner than
   two MINOR releases after the mark, whichever is later.
5. A deprecated wire operation keeps answering for the whole window. It never
   starts returning `unsupported` before the version that removes it.

A security fix may shorten this. When it does, the release notes say so
explicitly and name what changed and why.

## Support window

- The current MAJOR line receives fixes. The previous MAJOR line receives
  security fixes only, for six months after the next MAJOR's first release.
- Wire protocol v1 is supported for at least twelve months after a v2 exists,
  so a fleet can upgrade nodes and clients separately.
- Go: the two most recent Go releases, and at minimum the version in `go.mod`
  (currently 1.27.1). Python: `requires-python` in `sdk/python/pyproject.toml`
  (currently `>=3.11`). Node: `engines.node` in
  `sdk/typescript/package.json` (currently `>=20`). Raising any of these three
  floors is a MINOR change announced in the changelog, not a silent one.
- Which backend supports which feature, and which is fit for which trust level,
  is a separate matter of fact rather than of policy; the generated support
  matrix in [security profiles](security-profiles.md) lists operating systems,
  per-backend features and profile suitability, and `remount conformance` and
  `remount doctor --profile` settle it against a live deployment.

Because Remount is self-hosted, an operator chooses when to upgrade. Nothing in
this policy is a promise that someone else will keep a service running.
