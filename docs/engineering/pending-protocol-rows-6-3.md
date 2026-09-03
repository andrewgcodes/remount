# Pending integration for Phase 6.3 — protocol rows and two wiring hooks

This file exists because the Phase 6.3 work was done under a file-ownership
split: `spec/PROTOCOL.md`, `internal/control/control.go` and `internal/client/`
were owned by other agents at the time. Everything below is written and tested
in the files that were in scope; these are the pieces that must be merged by
whoever owns those three paths.

Nothing here is optional. Until item 2 lands, `audit.export` and `audit.key`
are reachable only by calling `Control.AuditExport` / `Control.AuditPublicKey`
directly — which is how `internal/control/audit_test.go` and
`internal/sim/compliance_6_3_test.go` exercise them today — and
`remount audit export` compiles but returns `unsupported` from the control
plane.

## 1. `spec/PROTOCOL.md` §6, control-plane operations table

Append these two rows to the client (`C`) section of the table:

```markdown
| `audit.export` | C | `AuditExportReq{tenant?, from, to}` → `AuditExportRes{manifest, signature, bundle}`; the range is INCLUSIVE of both endpoints and `from` must be at least 1. The tenant is resolved from the authenticated subject; a caller whose tenant is not `*` may not name another, `*` is never a valid export subject, and administrative authority is required. A range the log can no longer serve completely is `evicted` carrying the oldest retained sequence, never a shorter bundle. One bundle is bounded at 16 MiB and one range at 1,048,576 sequences (§11.1) |
| `audit.key` | C | `AuditKeyReq{}` → `AuditKeyRes{key_id, algorithm, public_key}`; publishes only the verification half of the control plane's durable Ed25519 audit signing key, so a bundle can be verified without reaching the control plane |
```

## 2. `spec/PROTOCOL.md` §11, canonical event types

Add these five types to the canonical list:

```markdown
`audit.exported`, `audit.export_denied`, `retention.enforced`,
`retention.violation`, `residency.denied`
```

## 3. `spec/PROTOCOL.md` §11, a new subsection on retention semantics

Suggested text, to sit with the event-log section:

```markdown
### 11.1 Retention and compliance export

Retention deletes only a contiguous oldest prefix of the sequence, so a range
that is missing events is always reported as an explicit gap and never as a
shorter answer. Per-tenant retention is therefore two mechanisms rather than
one: the prefix is deleted at the oldest instant any tenant still requires,
and between a tenant's own cutoff and that floor the tenant's event payload is
replaced in place with the constant marker
`{"remount_redacted": "tenant_retention"}`. Sequence, time, type and tenant are
never changed, so contiguity holds; the redaction travels into an export as
ordinary content, so a bundle covering a redacted range is explicit about it.

A compliance bundle is JSON Lines: one canonical event object per line, then
one line holding `{"remount_audit_manifest": {...}, "signature": "..."}`. The
manifest's `payload_sha256` covers exactly the event bytes preceding that line.
Two exports of one range produce byte-identical event lines and the same hash;
only `created_at` differs. See ADR 0080.
```

## 4. `internal/control/control.go` — dispatch cases

Add to the client switch in `func (c *Control) dispatch`, next to the other
tenant-scoped administrative operations:

```go
	case proto.OpAuditExport:
		req, err := decode[proto.AuditExportReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.AuditExport(ctx, subject, req)
	case proto.OpAuditKey:
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.AuditPublicKey(ctx, subject)
```

Both handlers already exist in `internal/control/audit.go`. They are exported
precisely so this case is the only line of `control.go` the feature needs, and
they do their own tenant resolution and authorization, so no additional check
belongs in the dispatch case.

## 5. `internal/client/` — the SDK method (optional)

The CLI does not need it: `cmd/remount/cmd_audit.go` calls
`(*client.Client).Connect` and then `(*transport.Peer).Call`, because an audit
export is an administrative one-shot with no reconnect semantics of its own.
Add the typed method only if an SDK consumer wants it:

```go
// ExportAudit returns one signed, tenant-scoped audit bundle. The range is
// inclusive of both endpoints.
func (c *Client) ExportAudit(ctx context.Context, tenant string, from, to uint64) (*proto.AuditExportRes, error) {
	var result proto.AuditExportRes
	err := c.call(ctx, proto.PeerControl, proto.OpAuditExport, proto.AuditExportReq{Tenant: tenant, From: from, To: to}, &result)
	return &result, err
}
```

Adding it is a public API change, so it needs `make public-api`.

## 6. After merging

```sh
go run ./cmd/protogen && go run ./cmd/protogen --check   # already regenerated
go test -count=1 ./internal/control ./internal/sim ./internal/mcp ./cmd/remount
```

Then replace the direct `Control.AuditExport` calls in
`internal/sim/compliance_6_3_test.go` with a real frame through the relay, so
the dispatch case itself is covered. The tests already resolve each caller's
`control.Subject` from a real bearer through `identity.Manager.Authenticate`,
which is the same subject `subjectOf` would hand the dispatch case; the frame
decode is the one hop they cannot reach today.
