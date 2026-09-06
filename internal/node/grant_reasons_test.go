package node

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// TestAuthorizeClaimsCarriesStableReasons pins the sub-classification an SDK
// maps to a typed error. The codes are unchanged; what is new is that a caller
// can tell a moved workspace from a revoked principal from an expired grant
// without matching on message text.
func TestAuthorizeClaimsCarriesStableReasons(t *testing.T) {
	n := newTestNode(t, nil)
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &ws{Workspace: proto.Workspace{
		ID: "ws_grant_reasons", Node: n.id, Generation: 7, AuthzRevision: 3, Tenant: "team",
	}}
	n.mu.Lock()
	n.ctrlPub = public
	n.workspaces[w.ID] = w
	n.mu.Unlock()
	grant := func(generation, revision uint64, expires time.Time) *proto.Grant {
		claims := proto.GrantClaims{
			Client: "c_reasons", WS: w.ID, Node: n.id, Principal: "agent:bob", Tenant: w.Tenant,
			AuthzRevision: revision, ExpiresAt: expires.UnixMilli(), Gen: generation,
		}
		return &proto.Grant{Claims: claims, Signature: ed25519.Sign(private, proto.MustMarshal(claims)), Node: n.id}
	}
	live := time.Now().Add(time.Minute)

	cases := []struct {
		name   string
		grant  *proto.Grant
		code   string
		reason string
	}{
		{"moved workspace", grant(6, 3, live), proto.CodeConflict, proto.ReasonGenerationMismatch},
		{"revoked principal", grant(7, 2, live), proto.CodeUnauthorized, proto.ReasonRevoked},
		{"expired grant", grant(7, 3, time.Now().Add(-time.Minute)), proto.CodeUnauthorized, proto.ReasonGrantExpired},
	}
	for _, tc := range cases {
		_, _, err := n.authorizeClaims("c_reasons", w.ID, tc.grant)
		var protocolErr *proto.Error
		if !errors.As(err, &protocolErr) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if protocolErr.Code != tc.code || protocolErr.Reason != tc.reason {
			t.Fatalf("%s: code=%q reason=%q, want %q/%q", tc.name, protocolErr.Code, protocolErr.Reason, tc.code, tc.reason)
		}
	}
	// The healthy grant is unaffected by any of it.
	if _, claims, err := n.authorizeClaims("c_reasons", w.ID, grant(7, 3, live)); err != nil || claims.Gen != 7 {
		t.Fatalf("current grant claims=%+v err=%v", claims, err)
	}
}
