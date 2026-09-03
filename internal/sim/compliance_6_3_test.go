package sim

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/compliance"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/tenant"
	"remount.dev/remount/internal/workspace"
)

// complianceWorld is a production multi-tenant deployment with the background
// collectors disabled, so each retention pass in these tests is one explicit,
// observable call rather than a race with a ticker.
func complianceWorld(t *testing.T) *world {
	t.Helper()
	return newWorldWith(t, func(options *server.Options) {
		options.Token = ""
		options.Mode = server.ModeProductionMultiTenant
		options.TenantArtifacts = identityEncryptedResolver(t)
		options.EventRetention = 24 * time.Hour
		options.ArtifactGracePeriod = 2 * time.Hour
		options.EventGCInterval = -1
		options.ArtifactGCInterval = -1
		options.RecordGCInterval = -1
	})
}

// complianceSubject resolves a bearer credential exactly as the relay hello
// does, so an export request carries the same server-authoritative identity a
// wire caller would have.
func complianceSubject(t *testing.T, w *world, token string) control.Subject {
	t.Helper()
	subject, err := w.srv.Identity.Authenticate(context.Background(), control.Credential{Token: token, Role: proto.RoleClient})
	if err != nil {
		t.Fatal(err)
	}
	return subject
}

func complianceKeyring(t *testing.T, w *world, actor control.Subject) *compliance.Keyring {
	t.Helper()
	published, err := w.srv.Control.AuditPublicKey(context.Background(), actor)
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

// TestE63AuditExportIsolatesTenantsAndIsDeterministic drives the real
// deployment: two tenants, real signed credentials, real workspaces created
// over the protocol, and the control plane's own export handler reached with
// the subject the identity manager derived from each bearer.
//
// Tenant B holds an operator credential and asks for tenant A's range by id
// and by sequence. Neither reaches tenant A's events.
func TestE63AuditExportIsolatesTenantsAndIsDeterministic(t *testing.T) {
	w := complianceWorld(t)
	ctx := ctxT(t, 60*time.Second)
	quotas := tenant.Quotas{MaxWorkspaces: 4, MaxActiveSessions: 4}
	for _, id := range []string{"tenant-a", "tenant-b"} {
		if _, err := w.srv.Tenants.Create(ctx, id, tenant.Policy{Quotas: quotas}, tenant.Mutation{
			OperationID: "e63-create-" + id, Actor: "bootstrap",
		}); err != nil {
			t.Fatal(err)
		}
	}
	aliceToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:alice", "tenant-a", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	bobToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:bob", "tenant-b", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	alice := w.clientWithToken("e63-alice", aliceToken)
	bob := w.clientWithToken("e63-bob", bobToken)
	aliceWS, err := alice.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "alice-private-workspace"}, client.WithIdempotencyKey("e63-ws-a"))
	if err != nil {
		t.Fatal(err)
	}
	bobWS, err := bob.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "bob-private-workspace"}, client.WithIdempotencyKey("e63-ws-b"))
	if err != nil {
		t.Fatal(err)
	}
	last, err := w.srv.Log.Last(ctx)
	if err != nil {
		t.Fatal(err)
	}

	aliceSubject := complianceSubject(t, w, aliceToken)
	bobSubject := complianceSubject(t, w, bobToken)
	if aliceSubject.Tenant != "tenant-a" || bobSubject.Tenant != "tenant-b" {
		t.Fatalf("subjects=%+v %+v", aliceSubject, bobSubject)
	}

	// Naming another tenant, and naming every tenant, are both refused.
	if _, err := w.srv.Control.AuditExport(ctx, bobSubject, &proto.AuditExportReq{Tenant: "tenant-a", From: 1, To: last}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant export=%v", err)
	}
	if _, err := w.srv.Control.AuditExport(ctx, bobSubject, &proto.AuditExportReq{Tenant: "*", From: 1, To: last}); err == nil {
		t.Fatal("a wildcard tenant produced a bundle")
	}
	// Guessing the sequence range is not a way around it either: the range is
	// the same for both tenants and the filter is the authenticated subject.
	bobBundle, err := w.srv.Control.AuditExport(ctx, bobSubject, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bobBundle.Bundle, []byte(aliceWS.ID)) || bytes.Contains(bobBundle.Bundle, []byte("alice-private-workspace")) {
		t.Fatal("tenant-b's bundle carried tenant-a workspace identifiers")
	}
	if !bytes.Contains(bobBundle.Bundle, []byte(bobWS.ID)) {
		t.Fatal("tenant-b's bundle omitted its own workspace")
	}

	// Repeated export of one range is byte-identical apart from the manifest's
	// own creation time, and the signature covers the exact payload.
	first, err := w.srv.Control.AuditExport(ctx, aliceSubject, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.srv.Control.AuditExport(ctx, aliceSubject, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bundlePayload(first.Bundle), bundlePayload(second.Bundle)) {
		t.Fatal("two exports of one range produced different event bytes")
	}
	if first.Manifest.PayloadSHA256 != second.Manifest.PayloadSHA256 {
		t.Fatalf("payload hash %s != %s", first.Manifest.PayloadSHA256, second.Manifest.PayloadSHA256)
	}
	keyring := complianceKeyring(t, w, aliceSubject)
	verified, err := compliance.Verify(ctx, bytes.NewReader(first.Bundle), keyring, compliance.VerifyOptions{ExpectedTenant: "tenant-a"})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.PayloadSHA256 != first.Manifest.PayloadSHA256 || verified.EventCount != first.Manifest.EventCount {
		t.Fatalf("verified manifest %+v does not describe the exported bundle %+v", verified, first.Manifest)
	}
	// The same bundle must not verify as tenant-b's, so a bundle cannot be
	// re-presented as another tenant's record.
	if _, err := compliance.Verify(ctx, bytes.NewReader(first.Bundle), keyring, compliance.VerifyOptions{ExpectedTenant: "tenant-b"}); err == nil {
		t.Fatal("a tenant-a bundle verified as tenant-b")
	}
}

// bundlePayload is everything before the trailing signed-manifest line: the
// exact bytes PayloadSHA256 covers.
func bundlePayload(bundle []byte) []byte {
	index := bytes.LastIndex(bundle[:len(bundle)-1], []byte("\n"))
	return bundle[:index+1]
}

// TestE63RetentionRemovesOneTenantsContentAndKeepsTheRangeHonest proves the
// two-mechanism retention design end to end: tenant A's own retention destroys
// tenant A's event content on tenant A's schedule, tenant B is untouched, the
// sequence stays contiguous, and an export of the affected range reports the
// redaction rather than presenting a quietly shorter history.
func TestE63RetentionRemovesOneTenantsContentAndKeepsTheRangeHonest(t *testing.T) {
	w := complianceWorld(t)
	ctx := ctxT(t, 60*time.Second)
	quotas := tenant.Quotas{MaxWorkspaces: 4}
	// One hour is the shortest policy the tenant store accepts. The pass below
	// is dated two hours ahead, which is how a test expresses "tenant A's
	// window has closed and tenant B's has not" without sleeping through one.
	if _, err := w.srv.Tenants.Create(ctx, "tenant-a", tenant.Policy{
		Quotas: quotas, Retention: tenant.Retention{Events: time.Hour},
	}, tenant.Mutation{OperationID: "e63r-create-a", Actor: "bootstrap"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.srv.Tenants.Create(ctx, "tenant-b", tenant.Policy{
		Quotas: quotas, Retention: tenant.Retention{Events: 100 * time.Hour},
	}, tenant.Mutation{OperationID: "e63r-create-b", Actor: "bootstrap"}); err != nil {
		t.Fatal(err)
	}
	aliceToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:alice", "tenant-a", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	bobToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:bob", "tenant-b", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	alice := w.clientWithToken("e63r-alice", aliceToken)
	bob := w.clientWithToken("e63r-bob", bobToken)
	if _, err := alice.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "alice-private-workspace"}, client.WithIdempotencyKey("e63r-ws-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := bob.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "bob-private-workspace"}, client.WithIdempotencyKey("e63r-ws-b")); err != nil {
		t.Fatal(err)
	}
	before, err := w.srv.Log.Read(ctx, 0, "", 2000)
	if err != nil {
		t.Fatal(err)
	}
	last, err := w.srv.Log.Last(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.srv.PruneEvents(ctx, time.Now().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	after, err := w.srv.Log.Read(ctx, 0, "", 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) < len(before) {
		t.Fatalf("retention removed rows from inside the prefix: %d -> %d", len(before), len(after))
	}
	marker := string(eventlog.RedactedPayload())
	originals := make(map[uint64]proto.Event, len(before))
	for _, event := range before {
		originals[event.Seq] = event
	}
	redacted, kept := 0, 0
	for _, event := range after {
		original, known := originals[event.Seq]
		if !known {
			continue
		}
		switch event.Tenant {
		case "tenant-a":
			if len(original.Payload) == 0 {
				continue
			}
			if string(event.Payload) != marker {
				t.Fatalf("tenant-a payload survived its own retention at seq %d: %q", event.Seq, event.Payload)
			}
			if event.Type != original.Type || event.At != original.At || event.Tenant != original.Tenant {
				t.Fatalf("redaction altered the envelope at seq %d", event.Seq)
			}
			redacted++
		default:
			if string(event.Payload) != string(original.Payload) {
				t.Fatalf("tenant-a retention altered a %q event at seq %d", event.Tenant, event.Seq)
			}
			kept++
		}
	}
	if redacted == 0 {
		t.Fatal("tenant-a retention removed nothing")
	}
	if kept == 0 {
		t.Fatal("the test observed no other tenant's rows, so isolation was not exercised")
	}
	if bytes.Contains([]byte(eventPayloads(after, "tenant-a")), []byte("alice-private-workspace")) {
		t.Fatal("expired tenant-a content is still readable")
	}
	if !strings.Contains(eventPayloads(after, "tenant-b"), "bob-private-workspace") {
		t.Fatal("tenant-b content was destroyed by tenant-a's policy")
	}
	for i := 1; i < len(after); i++ {
		if after[i].Seq != after[i-1].Seq+1 {
			t.Fatalf("retention punched a hole between %d and %d", after[i-1].Seq, after[i].Seq)
		}
	}

	// The export of the affected range still covers every sequence and says
	// what happened: the redaction marker is in the signed payload, so the
	// bundle is never a quietly complete-looking history.
	aliceSubject := complianceSubject(t, w, aliceToken)
	bundle, err := w.srv.Control.AuditExport(ctx, aliceSubject, &proto.AuditExportReq{From: 1, To: last})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(bundle.Bundle, []byte(eventlog.RedactedPayloadKey)) {
		t.Fatal("the bundle hid the redaction instead of recording it")
	}
	if bytes.Contains(bundle.Bundle, []byte("alice-private-workspace")) {
		t.Fatal("the bundle re-published content retention had removed")
	}
	keyring := complianceKeyring(t, w, aliceSubject)
	if _, err := compliance.Verify(ctx, bytes.NewReader(bundle.Bundle), keyring, compliance.VerifyOptions{ExpectedTenant: "tenant-a"}); err != nil {
		t.Fatalf("a redacted bundle must still verify: %v", err)
	}
	// Both signals, for both outcomes.
	found := false
	for _, event := range after {
		if event.Type == proto.EvRetentionEnforced && event.Tenant == "tenant-a" {
			found = true
		}
	}
	if !found {
		if latest, err := w.srv.Log.Read(ctx, 0, "", 2000); err == nil {
			for _, event := range latest {
				if event.Type == proto.EvRetentionEnforced && event.Tenant == "tenant-a" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("a retention pass removed content without recording an event")
	}
}

// eventPayloads concatenates one tenant's payload bytes so a test can assert
// that a phrase is present or absent across the whole history.
func eventPayloads(events []proto.Event, tenantID string) string {
	var out strings.Builder
	for _, event := range events {
		if event.Tenant == tenantID {
			out.Write(event.Payload)
		}
	}
	return out.String()
}

// TestE63ArtifactRetentionPreservesALiveClosure proves that an aggressive
// per-tenant artifact retention cannot delete what a live workspace still
// depends on. Reachability outranks age: a retention policy may only collect
// sooner, never break a closure the control plane still names.
func TestE63ArtifactRetentionPreservesALiveClosure(t *testing.T) {
	w := complianceWorld(t)
	ctx := ctxT(t, 90*time.Second)
	if _, err := w.srv.Tenants.Create(ctx, "tenant-a", tenant.Policy{
		Quotas: tenant.Quotas{MaxWorkspaces: 4, MaxActiveSessions: 4},
		// Stricter than the two-hour shared grace window, so the per-tenant
		// cutoff is what decides collection for this tenant.
		Retention: tenant.Retention{Artifacts: time.Hour},
	}, tenant.Mutation{OperationID: "e63a-create-a", Actor: "bootstrap"}); err != nil {
		t.Fatal(err)
	}
	operatorToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:alice", "tenant-a", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := w.srv.Identity.IssueEnrollment(ctx, "compliance-e63", "tenant-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.nodeWith("e63-n1", func(options *node.Options) {
		process, processErr := workspace.NewProcess(filepath.Join(options.DataDir, "e63-process"))
		if processErr != nil {
			t.Fatal(processErr)
		}
		options.Token = enrollment
		options.Backends = workspace.NewRegistry(&identityTestBackend{process: process})
	})
	operator := w.clientWithToken("e63a-operator", operatorToken)
	ws := mustWS(t, operator, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "identity-test"}})
	if err := operator.WriteFile(ctx, ws.ID, "keep.txt", []byte("closure"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshot, err := operator.Snapshot(ctx, ws.ID, true, client.WithIdempotencyKey("e63-snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Artifact == "" {
		t.Fatal("an authoritative snapshot produced no artifact")
	}
	// Pin it as a base. A base is one of the named durable roots, so its whole
	// closure is what age-based retention must never be able to reach.
	base, err := operator.CreateBase(ctx, proto.BaseCreateReq{
		Name: "e63-golden", Artifact: snapshot.Artifact, Workspace: ws.ID,
	}, client.WithIdempotencyKey("e63-pin"))
	if err != nil {
		t.Fatal(err)
	}
	if base.Artifact != snapshot.Artifact {
		t.Fatalf("base=%+v", base)
	}

	// Two passes: the first records every orphan sighting, the second is dated
	// three hours later so tenant A's one-hour artifact retention has expired
	// and collection is free to delete. A referenced artifact must survive it.
	before := metrics.TenantRetentionEnforced.Value()
	for _, at := range []time.Time{time.Now(), time.Now().Add(3 * time.Hour)} {
		result, err := w.srv.CollectArtifacts(at)
		if err != nil {
			t.Fatal(err)
		}
		if result.Removed > 0 {
			t.Fatalf("retention removed %d objects from a live closure", result.Removed)
		}
		if result.Referenced == 0 {
			t.Fatal("the closure was not in the reference set, so this pass proved nothing")
		}
	}
	// The tenant's own cutoff really was applied; otherwise the pass above
	// would have proved only that the shared grace window is slow.
	if delta := metrics.TenantRetentionEnforced.Value() - before; delta < 2 {
		t.Fatalf("per-tenant artifact cutoffs were not applied: delta=%d", delta)
	}
	// The whole closure is still fetchable through the same authenticated path
	// a failover restore would use.
	body, err := operator.DownloadSnapshot(ctx, snapshot.Artifact, snapshot.Format)
	if err != nil {
		t.Fatalf("artifact retention broke a live closure at %s: %v", snapshot.Artifact, err)
	}
	restored, err := io.ReadAll(body)
	body.Close()
	if err != nil || len(restored) == 0 {
		t.Fatalf("restored snapshot bytes=%d err=%v", len(restored), err)
	}
	// The workspace is still usable, which is the operator-visible form of the
	// same claim.
	if body, err := operator.ReadFile(ctx, ws.ID, "keep.txt"); err != nil || string(body) != "closure" {
		t.Fatalf("workspace after retention: %q err=%v", body, err)
	}
}
