package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/compliance"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

func auditKeyring(t *testing.T, f *controlFixture) *compliance.Keyring {
	t.Helper()
	published, err := f.c.AuditPublicKey(context.Background(), Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(published.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := compliance.NewKeyring(map[string]ed25519.PublicKey{published.KeyID: raw})
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

// A bundle is only evidence if the key that signed it is durable and the
// requester never gets a reusable secret back.
func TestAuditSigningKeyIsDurableAndPrivate(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	firstID, first, err := f.c.auditSigningKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondID, second, err := f.c.auditSigningKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != secondID || !bytes.Equal(first, second) {
		t.Fatal("the audit signing key is not stable across calls")
	}
	published, err := f.c.AuditPublicKey(ctx, Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}})
	if err != nil {
		t.Fatal(err)
	}
	if published.KeyID != firstID || published.Algorithm != "ed25519" {
		t.Fatalf("published key=%+v", published)
	}
	raw, err := base64.RawURLEncoding.DecodeString(published.PublicKey)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		t.Fatalf("published public key=%q err=%v", published.PublicKey, err)
	}
	// The response must carry the verification half and nothing that could
	// sign a forged bundle.
	if bytes.Contains([]byte(published.PublicKey), first.Seed()) {
		t.Fatal("the private seed reached the client")
	}
}

// Tenant isolation is decided on the authenticated subject, so guessing a
// tenant id or a sequence range cannot reach another tenant's events.
func TestAuditExportCannotCrossTenants(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}, "tenant-b": {}})
	now := time.Now()
	appendTenantEvent(t, f, "tenant-a", "ws.created", now, map[string]any{"secret": "a-private"})
	appendTenantEvent(t, f, "tenant-b", "ws.created", now, map[string]any{"secret": "b-private"})
	last, err := f.log.Last(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bob := Subject{ID: "bob", Tenant: "tenant-b", Roles: []string{"operator", "admin"}}

	before := metrics.AuditExportDenied.Value()
	if _, err := f.c.AuditExport(ctx, bob, &proto.AuditExportReq{Tenant: "tenant-a", From: 1, To: last}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant export=%v", err)
	}
	if _, err := f.c.AuditExport(ctx, bob, &proto.AuditExportReq{Tenant: "*", From: 1, To: last}); err == nil {
		t.Fatal("a wildcard tenant produced a bundle")
	}
	if delta := metrics.AuditExportDenied.Value() - before; delta != 2 {
		t.Fatalf("denial counter delta=%d", delta)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	denials := 0
	for _, event := range events {
		if event.Type == proto.EvAuditExportDenied {
			denials++
		}
	}
	if denials != 2 {
		t.Fatalf("recorded denials=%d", denials)
	}

	// Bob's own export covers the whole sequence range, which spans tenant-a's
	// events, and still contains none of them.
	result, err := f.c.AuditExport(ctx, bob, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(result.Bundle, []byte("a-private")) {
		t.Fatal("a tenant-b bundle carried tenant-a payload")
	}
	if !bytes.Contains(result.Bundle, []byte("b-private")) {
		t.Fatal("the bundle omitted its own tenant's event")
	}
	if result.Manifest.Tenant != "tenant-b" {
		t.Fatalf("manifest tenant=%q", result.Manifest.Tenant)
	}
	// An agent has no administrative authority over the audit record even
	// inside its own tenant.
	agent := Subject{ID: "agent:bob", Tenant: "tenant-b", Roles: []string{"agent"}}
	if _, err := f.c.AuditExport(ctx, agent, &proto.AuditExportReq{From: 1, To: last}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("agent export=%v", err)
	}
}

// The manifest is only worth signing if it commits to the exact bytes, and a
// second export of the same range must produce those same bytes.
func TestAuditExportIsDeterministicAndVerifies(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}})
	now := time.Now()
	for i := 0; i < 5; i++ {
		appendTenantEvent(t, f, "tenant-a", "ws.created", now.Add(time.Duration(i)*time.Second), map[string]any{"index": i})
	}
	last, err := f.log.Last(ctx)
	if err != nil {
		t.Fatal(err)
	}
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"operator", "admin"}}
	first, err := f.c.AuditExport(ctx, alice, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.c.AuditExport(ctx, alice, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	// Only the manifest's CreatedAt may differ, so the event lines and the
	// hash that covers them must be identical.
	payload := func(bundle []byte) []byte {
		index := bytes.LastIndex(bundle[:len(bundle)-1], []byte("\n"))
		return bundle[:index+1]
	}
	if !bytes.Equal(payload(first.Bundle), payload(second.Bundle)) {
		t.Fatal("two exports of one range produced different event bytes")
	}
	if first.Manifest.PayloadSHA256 != second.Manifest.PayloadSHA256 {
		t.Fatalf("payload hash %s != %s", first.Manifest.PayloadSHA256, second.Manifest.PayloadSHA256)
	}
	stored, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var owned uint64
	for _, event := range stored {
		// Only the range that was exported: the exports themselves appended
		// audit.exported events after `last`.
		if event.Tenant == "tenant-a" && event.Seq <= last {
			owned++
		}
	}
	if first.Manifest.EventCount != owned || first.Manifest.RangeFrom != 1 || first.Manifest.RangeTo != last {
		t.Fatalf("manifest=%+v tenant events=%d", first.Manifest, owned)
	}
	keyring := auditKeyring(t, f)
	verified, err := compliance.Verify(ctx, bytes.NewReader(first.Bundle), keyring, compliance.VerifyOptions{ExpectedTenant: "tenant-a"})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.PayloadSHA256 != first.Manifest.PayloadSHA256 {
		t.Fatal("the verified manifest does not cover the exported payload")
	}
	// One flipped byte inside the event lines must fail, which is what proves
	// the signature covers the content and not only the summary.
	tampered := append([]byte(nil), first.Bundle...)
	index := bytes.Index(tampered, []byte(`"index":0`))
	if index < 0 {
		t.Fatalf("bundle does not contain the expected payload: %s", tampered)
	}
	// Still valid JSON, still canonically encoded, one different value: the
	// only thing that can catch it is the hash the manifest signs.
	tampered[index+8] = '9'
	if _, err := compliance.Verify(ctx, bytes.NewReader(tampered), keyring, compliance.VerifyOptions{}); err == nil {
		t.Fatal("a tampered bundle verified")
	}
}

// A range that retention has already removed must be refused with the oldest
// sequence still available, never returned as a complete bundle that quietly
// omits the missing events.
func TestAuditExportRefusesAnIncompleteRange(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}})
	now := time.Now()
	for i := 0; i < 6; i++ {
		appendTenantEvent(t, f, "tenant-a", "ws.created", now.Add(time.Duration(i)*time.Second), map[string]any{"index": i})
	}
	last, err := f.log.Last(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.log.PruneSize(ctx, 3, 100); err != nil {
		t.Fatal(err)
	}
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"operator", "admin"}}
	before := metrics.AuditExportGaps.Value()
	_, err = f.c.AuditExport(ctx, alice, &proto.AuditExportReq{From: 1, To: last})
	if codeOf(err) != proto.CodeEvicted {
		t.Fatalf("export across an evicted prefix=%v", err)
	}
	if delta := metrics.AuditExportGaps.Value() - before; delta != 1 {
		t.Fatalf("gap counter delta=%d", delta)
	}
	if !strings.Contains(err.Error(), "no longer complete") {
		t.Fatalf("gap message=%v", err)
	}
}

// Range validation is explicit so a caller can never widen a range by omitting
// a field.
func TestAuditExportRangeValidation(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}})
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"operator", "admin"}}
	for _, request := range []proto.AuditExportReq{
		{From: 0, To: 10},
		{From: 10, To: 9},
		{From: 1, To: proto.MaxAuditRangeEvents + 1},
	} {
		if _, err := f.c.AuditExport(ctx, alice, &request); err == nil {
			t.Fatalf("range %+v was accepted", request)
		}
	}
}

// A denial is the record an auditor most wants, and it is written for every
// refused export. That makes the request body a lever on the durable log: any
// authenticated caller can name a tenant it does not own, as often as it
// likes. What reaches the log must therefore be bounded by this control plane,
// not by the size of the caller's request — the same reason a malformed range
// is deliberately not recorded at all.
func TestAuditDenialRecordDoesNotLetTheCallerSizeTheLog(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}})
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"operator", "admin"}}

	oversized := strings.Repeat("z", 64<<10)
	if _, err := f.c.AuditExport(ctx, alice, &proto.AuditExportReq{Tenant: oversized, From: 1, To: 2}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("an export naming another tenant=%v", err)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	denials := 0
	for _, event := range events {
		if event.Type != proto.EvAuditExportDenied {
			continue
		}
		denials++
		if len(event.Payload) > 1024 {
			t.Fatalf("one refused request appended %d durable bytes; a caller must not size the audit log", len(event.Payload))
		}
		if len(event.Stream) > 1024 {
			t.Fatalf("the denial stream name is %d bytes", len(event.Stream))
		}
		// The refusal stays attributable: the record still says a tenant
		// selector was named and how large it was.
		if !bytes.Contains(event.Payload, []byte("65536")) {
			t.Fatalf("the denial no longer describes what was requested: %s", event.Payload)
		}
	}
	if denials != 1 {
		t.Fatalf("recorded denials=%d", denials)
	}

	// A tenant id of ordinary length is still recorded verbatim, because that
	// is the fact an auditor is looking for.
	if _, err := f.c.AuditExport(ctx, alice, &proto.AuditExportReq{Tenant: "tenant-b", From: 1, To: 2}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant export=%v", err)
	}
	events, err = f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	named := false
	for _, event := range events {
		if event.Type == proto.EvAuditExportDenied && bytes.Contains(event.Payload, []byte("tenant-b")) {
			named = true
		}
	}
	if !named {
		t.Fatal("an ordinary cross-tenant denial no longer names the tenant that was requested")
	}
}
