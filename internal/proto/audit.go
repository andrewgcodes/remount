package proto

// Compliance export and retention operations. `audit.export` produces one
// signed, tenant-scoped bundle of the canonical event log; `audit.key`
// publishes the verification key so a bundle can be checked by someone who
// cannot reach the control plane.
const (
	OpAuditExport = "audit.export" // AuditExportReq -> AuditExportRes
	OpAuditKey    = "audit.key"    // AuditKeyReq -> AuditKeyRes
)

// Retention, residency and compliance-export event types. Every one of these
// is paired with a counter in internal/metrics: a refusal that emits only a
// metric cannot be audited, and one that emits only an event cannot be
// alerted on.
const (
	EvAuditExported      = "audit.exported"
	EvAuditExportDenied  = "audit.export_denied"
	EvRetentionEnforced  = "retention.enforced"
	EvRetentionViolation = "retention.violation"
	EvResidencyDenied    = "residency.denied"
)

// MaxAuditBundleBytes bounds one audit export response. A range whose bundle
// would exceed it is refused with CodeResourceExhausted rather than truncated,
// because a short bundle whose manifest still validated would be a silently
// incomplete audit record. Split the range and export it in pieces.
const MaxAuditBundleBytes = 16 << 20

// MaxAuditRangeEvents bounds how many sequences one export may span. The bound
// is on the requested range, not on the tenant's share of it, because the
// exporter must read every intervening sequence to prove the range has no gap.
const MaxAuditRangeEvents = 1 << 20

// AuditExportReq asks the control plane for a signed audit bundle.
//
// The range is INCLUSIVE on both ends: an export of 10..20 contains sequence
// 10 and sequence 20. From must be at least 1 because sequence numbering
// starts at 1; From == 0 is rejected rather than silently meaning "oldest",
// so a caller can never widen a range by omitting a field.
//
// Tenant is optional and defaults to the caller's own tenant. A caller whose
// tenant is not "*" may not name a different one, and "*" is never a valid
// export subject: a bundle mixing tenants would defeat the per-tenant
// isolation the signature attests to.
type AuditExportReq struct {
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	From   uint64 `cbor:"from" json:"from"`
	To     uint64 `cbor:"to" json:"to"`
}

// AuditManifest is the signed description of one bundle. PayloadSHA256 covers
// the exact JSONL event bytes in Bundle up to but not including the trailing
// manifest line, so a verifier compares bytes rather than two interpretations
// of an event.
type AuditManifest struct {
	Schema             string `cbor:"schema" json:"schema"`
	Tenant             string `cbor:"tenant" json:"tenant"`
	RangeFrom          uint64 `cbor:"range_from" json:"range_from"`
	RangeTo            uint64 `cbor:"range_to" json:"range_to"`
	CreatedAt          int64  `cbor:"created_at" json:"created_at"`
	EventCount         uint64 `cbor:"event_count" json:"event_count"`
	FirstSeq           uint64 `cbor:"first_seq,omitempty" json:"first_seq,omitempty"`
	LastSeq            uint64 `cbor:"last_seq,omitempty" json:"last_seq,omitempty"`
	PayloadBytes       uint64 `cbor:"payload_bytes" json:"payload_bytes"`
	PayloadSHA256      string `cbor:"payload_sha256" json:"payload_sha256"`
	HashAlgorithm      string `cbor:"hash_algorithm" json:"hash_algorithm"`
	SignatureAlgorithm string `cbor:"signature_algorithm" json:"signature_algorithm"`
	KeyID              string `cbor:"key_id" json:"key_id"`
}

// AuditExportRes carries the whole bundle. Bundle is the verbatim file: one
// JSON object per event, then one line holding the signed manifest. Writing
// those bytes to a file and verifying them must reproduce Manifest exactly, so
// callers must not re-encode them.
type AuditExportRes struct {
	Manifest  AuditManifest `cbor:"manifest" json:"manifest"`
	Signature string        `cbor:"signature" json:"signature"`
	Bundle    []byte        `cbor:"bundle" json:"bundle"`
}

// AuditKeyReq asks for the public half of the audit signing key.
type AuditKeyReq struct{}

// AuditKeyRes publishes the verification key. The private half is generated
// once, stored only by the control plane, and never leaves it; publishing the
// public half is what lets an auditor verify a bundle offline.
type AuditKeyRes struct {
	KeyID     string `cbor:"key_id" json:"key_id"`
	Algorithm string `cbor:"algorithm" json:"algorithm"`
	PublicKey string `cbor:"public_key" json:"public_key"`
}
