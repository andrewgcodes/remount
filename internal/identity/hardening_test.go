package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

func TestPrincipalRevocationInvalidatesOnlyMatchingPrincipal(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	store := NewMemoryStore()
	manager, err := New(Options{PrivateKey: key, Store: store, Audience: "relay", Now: func() time.Time { return now }, ClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	aliceAccess, aliceRefresh, err := manager.IssueTokens(context.Background(), "agent:alice", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	bobAccess, _, err := manager.IssueTokens(context.Background(), "agent:bob", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	aliceCap, err := manager.IssueSessionCapability(context.Background(), "agent:alice", "tenant-a", []string{RoleAgent}, "ws_one", 7, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bobCap, err := manager.IssueSessionCapability(context.Background(), "agent:bob", "tenant-a", []string{RoleAgent}, "ws_one", 7, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := manager.RevokePrincipal(context.Background(), "tenant-a", "agent:alice")
	if err != nil || revision != 1 {
		t.Fatalf("revoke=(%d,%v)", revision, err)
	}
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: aliceAccess}); err == nil {
		t.Fatal("revoked principal access token accepted")
	}
	if _, err := manager.Refresh(context.Background(), aliceRefresh); err == nil {
		t.Fatal("revoked principal refresh token accepted")
	}
	if _, err := manager.VerifySessionCapability(context.Background(), aliceCap); err == nil {
		t.Fatal("revoked principal session capability accepted")
	}
	if _, err := manager.VerifySessionCapabilityFor(context.Background(), bobCap, "tenant-a", "ws_one", 8); err == nil {
		t.Fatal("stale generation capability accepted")
	}
	if subject, err := manager.Authenticate(context.Background(), control.Credential{Token: bobAccess}); err != nil || subject.ID != "agent:bob" {
		t.Fatalf("unrelated principal stopped: %+v %v", subject, err)
	}
	if cap, err := manager.VerifySessionCapability(context.Background(), bobCap); err != nil || cap.Principal != "agent:bob" || cap.Generation != 7 || cap.SPIFFEID != "spiffe://tenant-a/ws/ws_one/gen/7/principal/agent:bob" {
		t.Fatalf("unrelated capability=%+v err=%v", cap, err)
	}
	events := store.Events()
	if len(events) != 3 || events[2].Type != "identity.principal_revoked" || events[2].Revision != 1 {
		t.Fatalf("events=%+v", events)
	}
}

func TestTokensEnforceAudienceLifetimeRolesAndBounds(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	store := NewMemoryStore()
	manager, err := New(Options{PrivateKey: key, Store: store, Audience: "relay-a", AccessTTL: time.Minute, ClockSkew: time.Second, MaxTokenBytes: 2048, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	access, _, err := manager.IssueTokens(context.Background(), "alice", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.IssueTokens(context.Background(), "fake-node", "tenant-a", []string{RoleNode}); err == nil {
		t.Fatal("general token issuance granted node role")
	}
	nodeToken, _ := manager.sign(Claims{ID: "tok_node", Subject: "n_one", Tenant: "tenant-a", Roles: []string{RoleNode}, Kind: kindAccess, Audience: "relay-a", IssuedAt: now.Unix(), Expires: now.Add(time.Minute).Unix()})
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: nodeToken, Role: proto.RoleClient}); err == nil {
		t.Fatal("node credential authenticated a client peer")
	}
	other, _ := New(Options{PrivateKey: key, Store: store, Audience: "relay-b", AccessTTL: time.Minute, ClockSkew: time.Second, Now: func() time.Time { return now }})
	if _, err := other.Authenticate(context.Background(), control.Credential{Token: access}); err == nil {
		t.Fatal("wrong audience accepted")
	}
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: strings.Repeat("x", 2049)}); err == nil {
		t.Fatal("oversized token accepted")
	}
	badRole, _ := manager.sign(Claims{ID: "tok_bad", Subject: "alice", Tenant: "tenant-a", Roles: []string{"root"}, Kind: kindAccess, Audience: "relay-a", IssuedAt: now.Unix(), Expires: now.Add(time.Minute).Unix()})
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: badRole}); err == nil {
		t.Fatal("unknown signed role accepted")
	}
	tooLong, _ := manager.sign(Claims{ID: "tok_long", Subject: "alice", Tenant: "tenant-a", Roles: []string{RoleAgent}, Kind: kindAccess, Audience: "relay-a", IssuedAt: now.Unix(), Expires: now.Add(2 * time.Minute).Unix()})
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: tooLong}); err == nil {
		t.Fatal("overlong signed lifetime accepted")
	}
	now = now.Add(time.Minute + 2*time.Second)
	if _, err := manager.Authenticate(context.Background(), control.Credential{Token: access}); err == nil {
		t.Fatal("expired token accepted past skew")
	}
}

func TestRefreshRotationHasOneConcurrentWinner(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	manager, _ := New(Options{PrivateKey: key, Store: NewMemoryStore(), Now: func() time.Time { return now }})
	_, refresh, err := manager.IssueTokens(context.Background(), "alice", "tenant-a", []string{RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 24 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			access, rotated, err := manager.RotateRefresh(context.Background(), refresh)
			if err == nil && access != "" && rotated != "" {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatalf("refresh winners=%d, want 1", winners.Load())
	}
}

func TestEnrollmentAdmissionIsBoundedAndExpiredAuthorityCollected(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	store := NewMemoryStore()
	manager, _ := New(Options{PrivateKey: key, Store: store, MaxEnrollments: 1, Now: func() time.Time { return now }})
	if _, err := manager.IssueEnrollment(context.Background(), "pool", "tenant-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueEnrollment(context.Background(), "pool", "tenant-a", time.Minute); !errors.Is(err, ErrEnrollmentCapacity) {
		t.Fatalf("second issue=%v", err)
	}
	now = now.Add(time.Minute)
	if _, err := manager.IssueEnrollment(context.Background(), "pool", "tenant-a", time.Minute); err != nil {
		t.Fatalf("issue after expiry=%v", err)
	}
	found := false
	for _, event := range store.Events() {
		if event.Type == "identity.enrollment_expired" {
			found = true
		}
	}
	if !found {
		t.Fatal("expired enrollment collection was not observable")
	}
}

func FuzzIdentityTokenParser(f *testing.F) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	_, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	manager, _ := New(Options{PrivateKey: key, Store: NewMemoryStore(), Now: func() time.Time { return time.Unix(1_900_000_000, 0) }})
	f.Add("")
	f.Add("a.b.c")
	f.Add(strings.Repeat("x", defaultMaxTokenBytes+1))
	f.Fuzz(func(t *testing.T, token string) {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("parser panicked: %v", recovered)
			}
		}()
		_, err := manager.Authenticate(context.Background(), control.Credential{Token: token})
		if len(token) > defaultMaxTokenBytes && err == nil {
			t.Fatal("oversized token accepted")
		}
		_, _ = manager.AuthenticateNode(context.Background(), "n_fuzz", token, nodeKey.Public().(ed25519.PublicKey))
	})
}
