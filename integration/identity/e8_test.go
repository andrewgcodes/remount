package identity_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/identity"
)

func TestE8PrincipalRevocationCoreSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_900_000_000, 0)
	sqlite, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sqlite)
	t.Cleanup(func() { _ = log.Close() })
	store, err := identity.NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.LoadOrCreateSigningKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := identity.New(identity.Options{PrivateKey: key, Store: store, Audience: "relay", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	aliceAccess, _, err := manager.IssueTokens(ctx, "agent:alice", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	bobAccess, _, err := manager.IssueTokens(ctx, "agent:bob", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	aliceCap, err := manager.IssueSessionCapability(ctx, "agent:alice", "tenant-a", []string{identity.RoleAgent}, "ws_shared", 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bobCap, err := manager.IssueSessionCapability(ctx, "agent:bob", "tenant-a", []string{identity.RoleAgent}, "ws_shared", 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RevokePrincipal(ctx, "tenant-a", "agent:alice"); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := identity.NewSQLiteStore(sqlite.DB(), log)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := identity.New(identity.Options{PrivateKey: key, Store: restartedStore, Audience: "relay", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Authenticate(ctx, control.Credential{Token: aliceAccess}); err == nil {
		t.Fatal("Alice access survived durable revocation")
	}
	if _, err := restarted.VerifySessionCapability(ctx, aliceCap); err == nil {
		t.Fatal("Alice broker capability survived durable revocation")
	}
	if subject, err := restarted.Authenticate(ctx, control.Credential{Token: bobAccess}); err != nil || subject.ID != "agent:bob" {
		t.Fatalf("Bob access was collateral damage: %+v %v", subject, err)
	}
	if capability, err := restarted.VerifySessionCapability(ctx, bobCap); err != nil || capability.Principal != "agent:bob" {
		t.Fatalf("Bob capability was collateral damage: %+v %v", capability, err)
	}
	events, err := log.Read(ctx, 0, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type == "identity.principal_revoked" && event.Principal == "agent:alice" {
			found = true
		}
	}
	if !found {
		t.Fatal("durable identity.principal_revoked event missing")
	}
}
