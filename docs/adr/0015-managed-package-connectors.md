# 15. Managed package retrieval and scope-private cache references

**Status:** accepted

## Context

A registry hostname is not a package-installation capability. Generic HTTP
access to a repository can expose uploads, WebDAV methods, mutable namespaces,
and service-specific collaboration surfaces. A globally shared URL cache also
creates an observation channel: one workspace can probe whether another
workspace fetched a URL, even if the cached bytes themselves are immutable.

ADR 14 introduced typed egress rules and immutable-read classification. This
decision defines the narrower connector and cache semantics required for
package retrieval.

## Decision

An egress rule with `connector: "package"` is usable only through
`$REMOUNT_PACKAGE_CONNECTOR/<registry>/<path>`. It cannot authorize `/d/`, the
forward proxy, or CONNECT. Package rules require HTTPS and
`shared_state: "immutable_read"`; only `GET` and `HEAD` are accepted. Request
bodies, range requests, mutable shared-state declarations, WebDAV methods, and
redirects to non-HTTPS targets fail closed. Each HTTPS redirect is rewritten
through the connector endpoint, where host, port, method, path, request count,
and generation policy are evaluated again.

The package connector implements the common `Connector` interface with
side-effect-free `Authorize` and authorization-repeating `Execute` methods.
The broker supplies the authoritative tenant, workspace, principal,
generation, and normalized rule. A connector therefore receives no ambient
host permission.

Nodes advertise managed connector names separately from backend and display
capabilities. Placement rejects a package-scoped workspace when `package` is
absent. In production, the advertised list is replaced by the operator-approved
`NodeInfo`, so a node cannot self-assert the capability. This fail-closed gate
prevents an older node from ignoring the additive `connector` field and
reinterpreting the rule as generic HTTPS access.

Responses are completely staged under an opaque, HMAC-derived workspace scope
before being released. Successful objects are SHA-256 hashed and hard-linked
into a node-private, read-only content-addressed blob directory. Mutable
cache metadata and blob references live only inside the opaque workspace
scope. Neither scope identifiers nor filesystem paths appear in responses or
audit payloads.

A client may send `X-Remount-Expected-Digest: sha256:<hex>`. Cache reuse occurs
only when that same tenant/workspace scope already has a reference created by
its own prior successful fetch. Merely knowing a digest, or another workspace
having populated the physical blob, does not create a cache hit. This retains
physical deduplication without exposing sibling cache occupancy on a first
request. The private expected-digest header is never forwarded upstream. A
successful response reports content provenance in
`X-Remount-Content-Digest`; the audit event records connector, workspace,
generation, rule, result, digest, status, and byte count.

The store reserves the configured maximum before downloading. Node-total,
per-workspace logical, per-object byte, and physical/logical object-count
ceilings bound concurrent staging, disk use, and inode use. Cache metadata is
written atomically and blobs are never overwritten in place.

## Consequences

An approved package registry is no longer interchangeable with generic access
to that host. Package provenance and denied mutations are observable, while
binding substitution remains available for private HTTPS registries.

Full-response staging adds latency and temporary disk use, but it guarantees
that a digest mismatch or oversized object is rejected before unverified bytes
reach the workspace. Range downloads are intentionally unsupported in this
version.

The cache controls eliminate the direct shared-URL/cache-hit channel; they do
not claim to eliminate every physical timing or capacity side channel on a
shared node. Multi-tenant execution still requires an independently reviewed
backend that prevents a workspace from accessing node storage and enforces its
network boundary. The built-in process and Docker backends do not make that
claim.
