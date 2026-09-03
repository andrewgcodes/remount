# ADR 0065: Artifact identity is plaintext-derived and storage is tenant-encrypted

## Status

Accepted.

## Context

ADR 0006 makes a filesystem snapshot a portable, deterministic file artifact,
and the wire protocol names it `art_sha256:<sha256 of plaintext bytes>`. That
identifier is embedded in workspace state, move/checkpoint requests, restore
references, and diagnostics. Changing it to a ciphertext digest would make the
same snapshot acquire a new logical identity on every encryption and key
rotation.

Storing those artifacts as plaintext is not acceptable for a multi-tenant
control plane. Global content deduplication is also unsafe: whether another
tenant already stored a digest is an existence oracle. Encryption must preserve
the existing logical contract, prevent one tenant from selecting another
tenant's object, survive planned key rotation, and make damage explicit.

## Decision

### Identity and physical namespace

The logical artifact id remains `art_sha256:<sha256 of plaintext>`. The digest
is recomputed after decryption before a read can complete successfully.

The physical object key is:

```text
tenants/<tenant>/<tenant-key-version>/<logical-artifact-id>
```

Tenant and version segments use a restricted, canonical ASCII alphabet. They
are not caller-controlled filesystem paths. Every store operation takes an
authenticated tenant explicitly, or uses a `TenantStore` permanently bound to
one tenant.

Deduplication is limited to the exact tenant and active key-version path. The
same plaintext in two tenants produces two independently encrypted objects.
The same plaintext written after rotation produces a new object under the new
version. No global digest lookup or cross-tenant existence response is allowed.

### Key hierarchy

Every artifact receives a fresh random 256-bit data-encryption key. The data
key is wrapped with the tenant's active 256-bit key using AES-256-GCM. Tenant
keys are independently random per tenant and version and are stored only after
being wrapped by a master `MasterKey` provider.

The built-in local master provider accepts a 32-byte base64 or hexadecimal key
from `REMOUNT_MASTER_KEY` or a mode-0600 file. The key remains on the trusted
control-plane side. AWS KMS and Google Cloud KMS providers use their JSON REST
encrypt/decrypt APIs over an authenticated `http.Client`; they do not add cloud
SDK dependencies. Provider response bodies and key material are never included
in errors.

The directory key repository stores wrapped tenant-key records and a small
active-version pointer. Old tenant-key versions remain readable. A multi-master
provider can read records under old master versions while new records use the
current master, and tenant-key records can be rewrapped without changing the
tenant key or any artifact ciphertext.

### Encrypted object format

Version 1 is a binary envelope:

- fixed magic, format version, zero flags, chunk size, and plaintext size;
- canonical tenant, tenant-key version, and plaintext digest;
- an eight-byte random nonce prefix for data chunks;
- a random GCM nonce and wrapped 32-byte artifact data key; and
- AES-256-GCM ciphertext records, each covering at most the configured chunk
  size and carrying a 16-byte authentication tag.

The nonce prefix plus a monotonically increasing 32-bit chunk number forms
each unique 96-bit data nonce. Construction refuses a plaintext that would
need more than `2^32-1` chunks. Associated data authenticates the format
version, chunk size, plaintext size, tenant, logical id, nonce prefix, chunk
number, and chunk plaintext length. Data-key wrapping additionally
authenticates the tenant-key version. Changing metadata, moving ciphertext to
another tenant/version/id path, truncating or extending the body, changing a
chunk, or substituting a wrapped key therefore fails closed.

Plaintext is hashed into a bounded private staging file before encryption so
the logical id and authenticated metadata are known before physical
publication. Encryption then streams in bounded chunks; it does not buffer an
artifact in memory. The staging file is mode 0600, removed on every return
path, and owned crash remnants are removed during startup inventory.

The physical backend's `Create` is a conditional, atomic publication: a short
or long body, cancellation, reader error, or existing key never exposes a
partial replacement. The local file implementation reserves the declared
ciphertext bytes and object count before staging, publishes with a no-replace
hard link, accounts publication before it becomes observable to capacity
waiters, pins open readers against deletion, and inventories retained use on
restart.

### Reads, verification, and rotation

`Open` validates the path-bound header and unwraps the data key before returning
plaintext. Authentication is checked per chunk. Reaching EOF validates that
there are no trailing bytes and that the recomputed plaintext SHA-256 equals
the logical id. `Close` drains an unread suffix and returns the same verification
failure, so a caller cannot treat a partial read plus successful close as a
verified artifact. `Verify` always consumes the complete stream. Malformed
metadata, missing key versions, failed KMS lookup, failed authentication, and
digest mismatch are corruption/unavailable errors, never not-found success.

Rotation activates a newly wrapped tenant key but retains every older version
for reads. A bounded `RewrapRetired` pass creates a new header that wraps the
same per-artifact data key with the active tenant key and copies the already
authenticated data ciphertext unchanged. It decrypts and re-hashes the new
physical object before deleting any old version. Cancellation, failed create,
failed verification, or ambiguous acknowledgement retains the old readable
object. A retired tenant-key record can be pruned only after its complete
physical namespace is empty; the active version is never prunable.

### Bounds and observability

The encrypted layer has finite defaults and configurable limits for one
plaintext artifact, aggregate plaintext staging bytes, concurrent writers,
tenant count, and tenant-key versions per tenant. The local keyed store also
bounds retained-plus-reserved ciphertext bytes and objects. Exhaustion and
integrity/key failures have stable sentinel errors. Integration increments the
artifact rejection/corruption metrics at the node/control boundary where the
tenant, operation, and event authority are known.

`doctor --deep` must enumerate per-tenant physical objects, decrypt and re-hash
each logical id, fail as unavailable when a required key lookup cannot run, and
report every object that remains on a retired tenant-key or master-key version.
It must not print ciphertext, wrapped keys, plaintext, environment values, or
provider response bodies.

### Integration and capability activation

`encrypted.KeyedStore` is the narrow raw-object adapter boundary. The S3
adapter maps conditional creation to `PutObject` with `If-None-Match: *` and
maps HTTP 412 to `encrypted.ErrObjectExists`. The local deployment uses
`encrypted.FileStore`. Existing logical artifact call sites use a tenant-bound
`encrypted.TenantStore`; shared server/control/node interfaces must propagate
authenticated tenant selection rather than accept a tenant from an artifact
URL or request body.

The `encrypted-artifacts` named capability from ADR 0040 is advertised only
after upload, download, checkpoint, restore, move, garbage collection, and deep
diagnostics all use this layer. Merely compiling the leaf package is not enough
to claim the capability.

ADR 0007 remains unchanged: `.remount/env`, binding placeholders, and all
workspace-local runtime configuration are excluded before snapshot creation.
Encryption is defense in depth for legitimate snapshot contents, not permission
to put a credential in a workspace or archive.

## Consequences

- Artifact references and restore semantics do not change during encryption or
  rotation.
- Cross-tenant storage deduplication is deliberately lost. Per-tenant,
  per-version deduplication remains.
- Random nonces, GCM tags, wrapped keys, and headers add bounded storage
  overhead. Rewrapping a tenant data key does not rewrite bulk ciphertext.
- A missing retired key makes its remaining artifacts unavailable by design.
  Operators migrate and verify objects before pruning keys.
- Key metadata and ciphertext may be backed up independently, but both are
  required for recovery. Backups must retain the master-key versions needed to
  unwrap their tenant-key records.
- AWS and GCP clients still need their normal authenticated transports and live
  provider verification. Fake REST tests prove request/response shape and
  failure sanitization, not cloud IAM, availability, or rotation behavior.
- The filesystem staging and key repositories assume the deployment's single
  control-plane writer. Multi-writer failover requires the Phase 4 fenced
  object/authority protocol; encryption does not create a second authority.

## Verification

Focused tests cover plaintext-id preservation, ciphertext-at-rest, empty and
multi-chunk reads, tenant isolation, same-tenant idempotency, atomic concurrent
creation, metadata/wrapped-key/ciphertext corruption, missing keys, verification
on close, bounded staging and ciphertext reservations, crash cleanup, tenant
and master rotation, bounded background rewrap, post-verification deletion,
key pruning, local file persistence, and fake AWS/GCP REST providers. The
encrypted-object parser also has a seeded fuzz target and the package is run
under the race detector.

End-to-end integration must additionally fuzz the node's snapshot exclusion
list with `.remount/env` and binding-placeholder canaries, because that
invariant is owned by snapshot production rather than the encryption format.
