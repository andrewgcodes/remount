package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
)

func TestPrincipalDirectoryRolesTokensAndAuditSurviveRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000_000_000, 0)
	sqlite, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sqlite)
	defer log.Close()
	store, err := NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	manager, err := New(Options{PrivateKey: key, Store: store, Audience: "test", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := manager.CreatePrincipal(ctx, "tenant-a", "agent:alice", []string{RoleAgent, RoleViewer}, "operator:bob")
	if err != nil || principal.Tenant != "tenant-a" {
		t.Fatalf("principal=%+v err=%v", principal, err)
	}
	if _, err := manager.CreatePrincipal(ctx, "tenant-a", "agent:alice", []string{RoleAgent}, "operator:bob"); err == nil {
		t.Fatal("duplicate principal accepted")
	}
	values, err := manager.ListPrincipals(ctx, "tenant-a")
	if err != nil || len(values) != 1 || values[0].ID != "agent:alice" {
		t.Fatalf("values=%+v err=%v", values, err)
	}
	token, expires, err := manager.IssueAccessToken(ctx, "tenant-a", "agent:alice", RoleAgent, time.Minute)
	if err != nil || expires.Unix() != now.Add(time.Minute).Unix() {
		t.Fatalf("expires=%v err=%v", expires, err)
	}
	if _, _, err := manager.IssueAccessToken(ctx, "tenant-a", "agent:alice", RoleOperator, time.Minute); err == nil {
		t.Fatal("unassigned operator role issued")
	}
	if _, _, err := manager.IssueAccessToken(ctx, "tenant-b", "agent:alice", RoleAgent, time.Minute); err == nil {
		t.Fatal("cross-tenant token issued")
	}
	subject, err := manager.Authenticate(ctx, control.Credential{Token: token, Role: "client"})
	if err != nil || subject.ID != "agent:alice" || subject.Tenant != "tenant-a" {
		t.Fatalf("subject=%+v err=%v", subject, err)
	}
	updated, err := manager.SyncOIDCPrincipal(ctx, "tenant-a", "agent:alice", []string{RoleViewer})
	if err != nil || updated.Revision != 1 {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := manager.Authenticate(ctx, control.Credential{Token: token, Role: "client"}); err == nil {
		t.Fatal("old role token survived role change")
	}
	events, err := log.Read(ctx, 0, "identity", 100)
	if err != nil {
		t.Fatal(err)
	}
	var created, changed bool
	for _, event := range events {
		created = created || (event.Type == "identity.principal_created" && event.Actor == "operator:bob")
		changed = changed || (event.Type == "identity.roles_changed" && event.Actor == "oidc:agent:alice")
	}
	if !created || !changed {
		t.Fatalf("role audit missing: %+v", events)
	}
	if _, err := NewSQLiteStore(sqlite.DB(), log); err != nil {
		t.Fatal(err)
	}
}

func TestPrincipalCreationRejectsNodeAndInvalidTTL(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	manager, _ := New(Options{PrivateKey: key, Store: NewMemoryStore()})
	if _, err := manager.CreatePrincipal(context.Background(), "tenant-a", "node", []string{RoleNode}, "operator"); err == nil {
		t.Fatal("node role created through principal API")
	}
	if _, _, err := manager.IssueAccessToken(context.Background(), "tenant-a", "missing", RoleAgent, 0); err == nil || errors.Is(err, ErrPrincipalNotFound) {
		t.Fatalf("invalid ttl error=%v", err)
	}
}

// TestIssueAccessTokenRefusesATTLTheVerifierWouldReject pins the agreement
// between the issuer and the verifier. verify bounds an access token's lifetime
// by accessTTL, so minting a longer one produced a bearer that was dead on
// arrival: it authenticated as a bare unauthorized with nothing to explain it,
// which is exactly how a --bootstrap-ttl above the ceiling used to fail.
func TestIssueAccessTokenRefusesATTLTheVerifierWouldReject(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	// testManager configures AccessTTL: time.Minute.
	manager, _ := testManager(t, &now)
	ctx := context.Background()
	if _, err := manager.CreatePrincipal(ctx, "tenant-a", "agent:alice", []string{RoleAgent}, "operator:bob"); err != nil {
		t.Fatal(err)
	}

	// At the ceiling the token must both mint and authenticate.
	token, _, err := manager.IssueAccessToken(ctx, "tenant-a", "agent:alice", RoleAgent, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Authenticate(ctx, control.Credential{Token: token}); err != nil {
		t.Fatalf("a token issued at the ceiling did not authenticate: %v", err)
	}

	// Above it, issuance must refuse rather than hand back a dud.
	over, _, err := manager.IssueAccessToken(ctx, "tenant-a", "agent:alice", RoleAgent, time.Hour)
	if err == nil {
		if _, authErr := manager.Authenticate(ctx, control.Credential{Token: over}); authErr != nil {
			t.Fatalf("issued a bearer the verifier rejects (%v); issuance must refuse instead", authErr)
		}
		t.Fatal("issued an access token whose lifetime exceeds the configured maximum")
	}
	if !strings.Contains(err.Error(), "exceeds the configured maximum") {
		t.Fatalf("refusal does not name the ceiling: %v", err)
	}
}
