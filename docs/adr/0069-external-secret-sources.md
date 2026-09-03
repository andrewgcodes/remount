# ADR 0069: Binding values resolve from external secret sources at lease time

## Status

Accepted.

## Context

Bindings currently accept a literal credential in server configuration. That is
useful for the single-process standalone profile, but it makes a production
controller the durable configuration source for reusable credentials. Rotation
also requires replacing that configuration rather than allowing the system of
record to issue the current value.

The workspace must remain trusted with nothing. Moving source resolution into a
workspace, persisting the resolved value, or returning it from diagnostics would
move the trust boundary in the wrong direction.

## Decision

Production bindings name a source rather than carrying a literal:

- `env://NAME` reads the controller environment;
- `file:///absolute/path` reads a bounded regular file;
- `vault://address/mount/path#key` reads Vault KV v2 by default, using a token or
  an AppRole login over the Vault REST API;
- `awssm://arn:...:secretsmanager:...:secret:...` calls AWS Secrets Manager's
  JSON REST API with hand-written SigV4;
- `gcpsm://projects/PROJECT/secrets/SECRET/versions/VERSION` calls GCP Secret
  Manager with an OAuth bearer token and verifies `dataCrc32c` when supplied;
  and
- `github-app://APP_ID/INSTALLATION_ID` signs a short-lived RS256 app JWT and
  mints an installation token through GitHub's REST API.

`internal/secretsource.Resolver` is the integration boundary. `Resolve` returns
the current value only to lease issuance; `Probe` performs an authenticated,
uncached read for diagnostics and never returns the value. Provider response
bodies, credentials and resolved values are excluded from errors and logs.
Redirects are not followed, plaintext provider HTTP is rejected outside an
explicit test configuration, responses and files are byte-bounded, and all
requests have deadlines.

Successful resolutions use a count- and byte-bounded TTL/LRU cache. Work for one
source is serialized so concurrent lease requests share one provider call.
Provider expiry, such as a GitHub installation token expiry, shortens the local
TTL. Expired and evicted byte buffers are zeroed before removal. Rotation becomes
observable after at most the configured cache TTL.

Source references may be persisted as binding configuration; resolved values
must never be written to SQLite, event payloads, diagnostics, or workspaces. A
production-mode configuration rejects any binding value that does not pass
`ValidateSource`. The root integration retains literal `Secret` only for the
explicit standalone compatibility profile.

## Consequences

The control plane and node broker remain the only components trusted with a
leased value. A lease still contains the real credential because the node needs
it for edge substitution, but the durable binding record contains only its
source reference.

Provider reachability is a readiness dependency only when configured as such.
An unavailable or unauthorized `Probe` is reported as unavailable, never as a
healthy check. Cloud-provider implementations are tested against in-process REST
fixtures; release evidence must separately exercise the chosen real provider.

The default cache is 1,024 entries, 1 MiB total value bytes, and five minutes.
Operators must size these limits and source-provider quotas for their binding
count and rotation requirements. Cache entries are process memory, not durable
state, so a restart resolves sources again.
