package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
)

func testManager(t *testing.T, now *time.Time) (*Manager, *MemoryStore) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	manager, err := New(Options{PrivateKey: key, Store: store, Now: func() time.Time { return *now }, AccessTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return manager, store
}

func TestPrincipalTokensExpireRefreshAndRevoke(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	manager, store := testManager(t, &now)
	ctx := context.Background()
	access, refresh, err := manager.IssueTokens(ctx, "alice", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	subject, err := manager.Authenticate(ctx, control.Credential{Token: access})
	if err != nil || subject.ID != "alice" || subject.Tenant != "tenant-a" || len(subject.Roles) != 1 {
		t.Fatalf("authenticate = %+v, %v", subject, err)
	}
	if _, err := manager.Authenticate(ctx, control.Credential{Token: access + "x"}); err == nil {
		t.Fatal("tampered token authenticated")
	}
	newAccess, err := manager.Refresh(ctx, refresh)
	if err != nil || newAccess == access {
		t.Fatalf("refresh = %q, %v", newAccess, err)
	}
	if err := manager.Revoke(ctx, access); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Authenticate(ctx, control.Credential{Token: access}); err == nil {
		t.Fatal("revoked token authenticated")
	}
	events := store.Events()
	if len(events) != 1 || events[0].Type != "identity.revoked" || events[0].Subject != "alice" {
		t.Fatalf("durable events = %+v", events)
	}
	now = now.Add(2 * time.Minute)
	if _, err := manager.Authenticate(ctx, control.Credential{Token: newAccess}); err == nil {
		t.Fatal("expired token authenticated")
	}
}

func TestEnrollmentTokenIsConsumedExactlyOnce(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	manager, store := testManager(t, &now)
	ctx := context.Background()
	token, err := manager.Issue(ctx, "fly-iad", "tenant-a", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(i int, key ed25519.PrivateKey) {
			defer wg.Done()
			identity, err := manager.AuthenticateNode(ctx, fmt.Sprintf("n_%d", i), token, key.Public().(ed25519.PublicKey))
			if err == nil && identity.Pool == "fly-iad" && identity.Tenant == "tenant-a" && identity.Fresh {
				successes.Add(1)
			}
		}(i, key)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful consumes = %d, want 1", successes.Load())
	}
	events := store.Events()
	if len(events) != 2 || events[0].Type != "identity.enrollment_issued" || events[1].Type != "node.enrolled" {
		t.Fatalf("enrollment events = %+v", events)
	}
}

func TestEnrolledNodeReconnectRequiresBoundKey(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	manager, _ := testManager(t, &now)
	token, err := manager.Issue(context.Background(), "pool-a", "tenant-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	pub := key.Public().(ed25519.PublicKey)
	first, err := manager.AuthenticateNode(context.Background(), "n_one", token, pub)
	if err != nil || !first.Fresh {
		t.Fatalf("first enrollment = %+v, %v", first, err)
	}
	reconnected, err := manager.AuthenticateNode(context.Background(), "n_one", "", pub)
	if err != nil || reconnected.Fresh || reconnected.Tenant != "tenant-a" {
		t.Fatalf("reconnect = %+v, %v", reconnected, err)
	}
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := manager.AuthenticateNode(context.Background(), "n_one", "", other.Public().(ed25519.PublicKey)); err == nil {
		t.Fatal("different key reconnected as bound node")
	}
}

func TestTokenIssuerIsVerified(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	manager, store := testManager(t, &now)
	access, _, err := manager.IssueTokens(context.Background(), "alice", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(Options{PrivateKey: manager.key, Store: store, Issuer: "another-issuer", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Authenticate(context.Background(), control.Credential{Token: access}); err == nil {
		t.Fatal("token from a different issuer authenticated")
	}
}

func TestSQLiteEnrollmentBindsKeyAndEventAtomically(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	sqlite, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sqlite)
	t.Cleanup(func() { _ = log.Close() })
	store, err := NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.LoadOrCreateSigningKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := New(Options{PrivateKey: key, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.Issue(context.Background(), "fly-iad", "tenant-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	identity, err := manager.AuthenticateNode(context.Background(), "n_one", token, nodeKey.Public().(ed25519.PublicKey))
	if err != nil || !identity.Fresh || identity.Tenant != "tenant-a" {
		t.Fatalf("enroll = %+v, %v", identity, err)
	}
	events, err := log.Read(context.Background(), 0, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "identity.enrollment_issued" || events[1].Type != "node.enrolled" || events[1].Node != "n_one" {
		t.Fatalf("events = %+v", events)
	}

	// Reopening the identity facade over the same tables must preserve the key
	// binding and must not append another enrollment event.
	restarted, err := NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	manager, err = New(Options{PrivateKey: key, Store: restarted, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	identity, err = manager.AuthenticateNode(context.Background(), "n_one", "", nodeKey.Public().(ed25519.PublicKey))
	if err != nil || identity.Fresh {
		t.Fatalf("restart reconnect = %+v, %v", identity, err)
	}
	events, _ = log.Read(context.Background(), 0, "", 10)
	if len(events) != 2 {
		t.Fatalf("reconnect appended state event: %+v", events)
	}
}

func TestSQLiteEnrollmentConcurrentDifferentNodesHasOneWinner(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	sqlite, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sqlite)
	t.Cleanup(func() { _ = log.Close() })
	store, err := NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	_, signingKey, _ := ed25519.GenerateKey(rand.Reader)
	manager, err := New(Options{PrivateKey: signingKey, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.Issue(context.Background(), "pool", "tenant", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		_, key, _ := ed25519.GenerateKey(rand.Reader)
		wg.Add(1)
		go func(i int, key ed25519.PrivateKey) {
			defer wg.Done()
			if _, err := manager.AuthenticateNode(context.Background(), fmt.Sprintf("n_%d", i), token, key.Public().(ed25519.PublicKey)); err == nil {
				successes.Add(1)
			}
		}(i, key)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful enrollments = %d, want 1", successes.Load())
	}
}

func TestAuthorizerEnforcesTenantAndRoles(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	manager, _ := testManager(t, &now)
	resource := control.Resource{Kind: "workspace", ID: "ws_a", Tenant: "tenant-a", Owner: "alice"}
	if err := manager.Check(context.Background(), control.Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{RoleAgent}}, control.ActionWrite, resource); err != nil {
		t.Fatalf("owner write denied: %v", err)
	}
	if err := manager.Check(context.Background(), control.Subject{ID: "mallory", Tenant: "tenant-b", Roles: []string{RoleOperator}}, control.ActionRead, resource); err == nil {
		t.Fatal("cross-tenant operator read allowed")
	}
	if err := manager.Check(context.Background(), control.Subject{ID: "root", Tenant: "*", Roles: []string{RoleOperator}}, control.ActionAdmin, resource); err != nil {
		t.Fatalf("global operator denied: %v", err)
	}
	if err := manager.Check(context.Background(), control.Subject{ID: "viewer", Tenant: "tenant-a", Roles: []string{RoleViewer}}, control.ActionWrite, resource); err == nil {
		t.Fatal("viewer write allowed")
	}
}
