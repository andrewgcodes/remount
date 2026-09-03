package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"

	"remount.dev/remount/internal/compliance"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// auditSigningKeyName is the single row of audit_keys. One control plane signs
// with one key; rotation replaces the row and old bundles keep verifying
// against the key id their manifest names.
const auditSigningKeyName = "audit"

// auditKeySchema is applied on use rather than at open. Audit export is a rare
// administrative call, the statement is idempotent, and keeping the table with
// the code that owns it means a deployment that never exports never carries a
// signing key it did not ask for.
const auditKeySchema = `CREATE TABLE IF NOT EXISTS audit_keys (
	name TEXT PRIMARY KEY,
	key_id TEXT NOT NULL,
	private_key BLOB NOT NULL,
	created_at INTEGER NOT NULL
)`

// auditBundleBuffer collects one bundle in memory under an explicit cap.
//
// It is the BundleSink for a protocol response: the bytes go back to the
// caller instead of to a file, so there is no destination to leave untouched,
// but the all-or-nothing rule still holds. A bundle that outgrows the cap
// fails the whole export rather than publishing a prefix, because a truncated
// event stream whose manifest still validated would be a silently incomplete
// audit record.
type auditBundleBuffer struct {
	max   int64
	bytes []byte
}

func (b *auditBundleBuffer) Commit(_ context.Context, produce func(io.Writer) error) error {
	var staged bytes.Buffer
	if err := produce(&boundedBuffer{buffer: &staged, remaining: b.max}); err != nil {
		return err
	}
	b.bytes = staged.Bytes()
	return nil
}

// boundedBuffer refuses the write that would cross the cap instead of letting
// the buffer grow first and checking afterwards.
type boundedBuffer struct {
	buffer    *bytes.Buffer
	remaining int64
}

func (w *boundedBuffer) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, compliance.ErrLimit
	}
	w.remaining -= int64(len(p))
	return w.buffer.Write(p)
}

// auditSigningKey returns the durable Ed25519 key this control plane signs
// audit manifests with, creating it on first use.
//
// The private half lives only in the control database. It is never returned to
// a client, never placed in an event or a diagnostic, and never derived from
// anything a caller supplies: a reusable secret handed to the requester would
// let that requester forge a bundle that verifies. Concurrent first use is
// resolved the same way the identity token key resolves it, by an atomic
// insert followed by a re-read of whichever writer won.
func (c *Control) auditSigningKey(ctx context.Context) (string, ed25519.PrivateKey, error) {
	if c.db == nil {
		return "", nil, proto.Err(proto.CodeUnsupported, "this control plane has no durable store for an audit signing key")
	}
	if _, err := c.db.ExecContext(ctx, auditKeySchema); err != nil {
		return "", nil, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		var keyID string
		var raw []byte
		err := c.db.QueryRowContext(ctx, `SELECT key_id, private_key FROM audit_keys WHERE name=?`, auditSigningKeyName).Scan(&keyID, &raw)
		if err == nil {
			if len(raw) != ed25519.PrivateKeySize {
				return "", nil, errors.New("control: stored audit signing key has an invalid size")
			}
			return keyID, ed25519.PrivateKey(append([]byte(nil), raw...)), nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", nil, err
		}
		public, private, genErr := ed25519.GenerateKey(rand.Reader)
		if genErr != nil {
			return "", nil, genErr
		}
		if _, err := c.db.ExecContext(ctx, `INSERT OR IGNORE INTO audit_keys(name, key_id, private_key, created_at) VALUES(?,?,?,?)`,
			auditSigningKeyName, auditKeyID(public), []byte(private), c.now().UnixMilli()); err != nil {
			return "", nil, err
		}
	}
	return "", nil, errors.New("control: audit signing key could not be established")
}

// auditKeyID names a key by its public half so two control planes never claim
// the same id for different keys and a manifest identifies exactly what must
// verify it.
func auditKeyID(public ed25519.PublicKey) string {
	sum := sha256.Sum256(public)
	return "audit-" + hex.EncodeToString(sum[:8])
}

// AuditPublicKey publishes the verification half of the audit signing key so a
// bundle can be checked by an auditor who cannot reach this control plane.
func (c *Control) AuditPublicKey(ctx context.Context, actor Subject) (*proto.AuditKeyRes, error) {
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "audit", ID: auditSigningKeyName, Tenant: actor.Tenant}); err != nil {
		return nil, err
	}
	keyID, private, err := c.auditSigningKey(ctx)
	if err != nil {
		return nil, auditStoreError(err)
	}
	public, ok := private.Public().(ed25519.PublicKey)
	if !ok {
		return nil, proto.Err(proto.CodeInternal, "audit signing key is not an ed25519 key")
	}
	return &proto.AuditKeyRes{
		KeyID: keyID, Algorithm: "ed25519", PublicKey: base64.RawURLEncoding.EncodeToString(public),
	}, nil
}

// AuditExport produces one signed, tenant-scoped compliance bundle.
//
// The range is inclusive on both ends. Tenant isolation is decided here, on
// the authenticated subject, before any event is read: a caller bound to one
// tenant cannot name another, and no sequence number or cursor value supplied
// by the caller can widen the set of events the exporter will emit, because
// the exporter filters on the tenant this function resolved rather than on
// anything in the request body.
//
// Two exports of the same range produce byte-identical event lines and the
// same PayloadSHA256. Only the manifest's CreatedAt differs, because it
// records when the bundle was produced.
func (c *Control) AuditExport(ctx context.Context, actor Subject, req *proto.AuditExportReq) (*proto.AuditExportRes, error) {
	if c.opts.Log == nil {
		return nil, proto.Err(proto.CodeUnsupported, "this control plane has no canonical event log to export")
	}
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		c.recordAuditDenial(actor, req.Tenant, req, err)
		return nil, err
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "audit", ID: tenantID, Tenant: tenantID}); err != nil {
		c.recordAuditDenial(actor, tenantID, req, err)
		return nil, err
	}
	if c.opts.Tenants != nil {
		if _, err := c.opts.Tenants.Get(ctx, tenantID); err != nil {
			return nil, mapTenantError(err)
		}
	}
	if req.From == 0 || req.To < req.From {
		metrics.AuditExportDenied.Inc()
		return nil, proto.Err(proto.CodeBadRequest, "audit export needs an inclusive range with from >= 1 and to >= from")
	}
	if req.To-req.From >= proto.MaxAuditRangeEvents {
		metrics.AuditExportDenied.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "audit export range spans more than %d sequences", proto.MaxAuditRangeEvents)
	}
	keyID, private, err := c.auditSigningKey(ctx)
	if err != nil {
		return nil, auditStoreError(err)
	}
	exporter, err := compliance.NewExporter(c.opts.Log, keyID, private, compliance.Options{
		Now: c.now, MaxRangeEvents: proto.MaxAuditRangeEvents, MaxPayloadBytes: proto.MaxAuditBundleBytes,
	})
	if err != nil {
		return nil, proto.Err(proto.CodeInternal, "audit exporter is unavailable: %v", err)
	}
	sink := &auditBundleBuffer{max: proto.MaxAuditBundleBytes}
	signed, err := exporter.Export(ctx, compliance.Request{Tenant: tenantID, From: req.From, To: req.To}, sink)
	if err != nil {
		return nil, auditExportError(err)
	}
	result := &proto.AuditExportRes{
		Manifest:  auditManifestToProto(signed.Manifest),
		Signature: signed.Signature,
		Bundle:    sink.bytes,
	}
	event := c.newEvent(proto.EvAuditExported, "tenant:"+tenantID, actor.ID, "", map[string]any{
		"from": req.From, "to": req.To, "events": signed.Manifest.EventCount,
		"payload_sha256": signed.Manifest.PayloadSHA256, "key_id": signed.Manifest.KeyID,
	})
	event.Tenant = tenantID
	if err := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); err != nil {
		return nil, err
	}
	metrics.AuditExports.Inc()
	return result, nil
}

// recordAuditDenial pairs the counter with an attributable event. An
// authorization refusal that only moved a counter cannot be audited later, and
// this is exactly the refusal an auditor most wants to see. A malformed range
// is not recorded this way: it is a client bug, not an access attempt, and
// recording it would let any authenticated caller grow the log at will.
func (c *Control) recordAuditDenial(actor Subject, requested string, req *proto.AuditExportReq, reason error) {
	metrics.AuditExportDenied.Inc()
	stream := actor.Tenant
	if stream == "" {
		stream = requested
	}
	event := c.newEvent(proto.EvAuditExportDenied, "tenant:"+stream, actor.ID, "", map[string]any{
		"requested_tenant": requested, "from": req.From, "to": req.To, "reason": codeOfError(reason),
	})
	event.Tenant = actor.Tenant
	// The denial is the record; a failure to store it must not become a
	// different error for the caller, who is being refused either way.
	if err := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); err != nil {
		c.logger.Warn("audit export denial was not recorded", "err", err, "principal", actor.ID)
	}
}

// auditExportError turns a compliance failure into a stable protocol code. A
// range that is no longer complete is CodeEvicted carrying the oldest retained
// sequence, so a caller learns what it may still ask for instead of receiving
// a bundle that quietly omits the missing events.
func auditExportError(err error) error {
	var gap *compliance.GapError
	if errors.As(err, &gap) {
		metrics.AuditExportGaps.Inc()
		return &proto.Error{
			Code: proto.CodeEvicted,
			Msg:  "audit range is no longer complete: " + gap.Error(),
			// Oldest is the first sequence still exportable, when the log
			// could name one; a mid-range hole reports the sequence the
			// exporter expected next.
			Oldest: auditOldest(gap),
		}
	}
	if errors.Is(err, compliance.ErrLimit) {
		return proto.Err(proto.CodeResourceExhausted, "audit bundle exceeds the %d byte response limit; export a narrower range", proto.MaxAuditBundleBytes)
	}
	var protoErr *proto.Error
	if errors.As(err, &protoErr) {
		return protoErr
	}
	return proto.Err(proto.CodeInternal, "audit export failed: %v", err)
}

func auditOldest(gap *compliance.GapError) uint64 {
	if gap.Oldest != 0 {
		return gap.Oldest
	}
	return gap.Expected
}

func auditStoreError(err error) error {
	var protoErr *proto.Error
	if errors.As(err, &protoErr) {
		return protoErr
	}
	return proto.Err(proto.CodeInternal, "audit signing key is unavailable: %v", err)
}

func auditManifestToProto(manifest compliance.Manifest) proto.AuditManifest {
	return proto.AuditManifest{
		Schema: manifest.Schema, Tenant: manifest.Tenant,
		RangeFrom: manifest.RangeFrom, RangeTo: manifest.RangeTo, CreatedAt: manifest.CreatedAt,
		EventCount: manifest.EventCount, FirstSeq: manifest.FirstSeq, LastSeq: manifest.LastSeq,
		PayloadBytes: manifest.PayloadBytes, PayloadSHA256: manifest.PayloadSHA256,
		HashAlgorithm: manifest.HashAlgorithm, SignatureAlgorithm: manifest.SignatureAlgorithm,
		KeyID: manifest.KeyID,
	}
}

// codeOfError names why a request was refused without repeating a message that
// could carry another tenant's identifiers into this tenant's log.
func codeOfError(err error) string {
	var protoErr *proto.Error
	if errors.As(err, &protoErr) {
		return protoErr.Code
	}
	return proto.CodeInternal
}
