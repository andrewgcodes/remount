package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			if enrollment, err := manager.ConsumeEnrollment(ctx, token); err == nil && enrollment.Pool == "fly-iad" && enrollment.Tenant == "tenant-a" {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful consumes = %d, want 1", successes.Load())
	}
	events := store.Events()
	if len(events) != 2 || events[0].Type != "identity.enrollment_issued" || events[1].Type != "identity.enrollment_consumed" {
		t.Fatalf("enrollment events = %+v", events)
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
