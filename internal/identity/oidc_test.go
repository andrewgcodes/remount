package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
)

type staticIDTokenVerifier struct {
	claims OIDCClaims
	err    error
}

func (v staticIDTokenVerifier) VerifyIDToken(context.Context, string, string, string) (OIDCClaims, error) {
	return v.claims, v.err
}

func TestOIDCDeviceFlowAndGroupExchange(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	var polls atomic.Int32
	var issuer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "device_authorization_endpoint": issuer + "/device", "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/keys", "authorization_endpoint": issuer + "/authorize"})
		case "/device":
			if err := r.ParseForm(); err != nil || r.Form.Get("client_id") != "client" || !strings.Contains(r.Form.Get("scope"), "openid") {
				t.Errorf("device form=%v err=%v", r.Form, err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"device_code": "secret-device-code", "user_code": "ABCD-EFGH", "verification_uri": issuer + "/verify", "expires_in": 600, "interval": 1, "extension": "allowed"})
		case "/token":
			_ = r.ParseForm()
			if r.Form.Get("device_code") != "secret-device-code" {
				t.Errorf("device code missing")
			}
			if polls.Add(1) == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "authorization_pending", "error_description": "not yet"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id_token": "provider.id.token", "access_token": "provider-access", "token_type": "Bearer", "scope": "openid profile"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	issuer = server.URL
	client, err := NewOIDCClient(OIDCOptions{Issuer: issuer, ClientID: "client", Audience: "audience", Scopes: []string{"openid", "profile"}, GroupRoles: map[string][]string{"remount-agents": {RoleAgent}, "remount-viewers": {RoleViewer}}, HTTPClient: server.Client(), Now: func() time.Time { return now }, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	discovery, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	device, err := client.StartDeviceAuthorization(context.Background(), discovery)
	if err != nil || device.UserCode != "ABCD-EFGH" {
		t.Fatalf("device=%+v err=%v", device, err)
	}
	if _, err := client.PollDeviceToken(context.Background(), discovery, device.DeviceCode); !errors.Is(err, ErrAuthorizationPending) {
		t.Fatalf("first poll=%v", err)
	}
	providerTokens, err := client.PollDeviceToken(context.Background(), discovery, device.DeviceCode)
	if err != nil {
		t.Fatal(err)
	}
	_, signingKey, _ := ed25519.GenerateKey(rand.Reader)
	manager, _ := New(Options{PrivateKey: signingKey, Store: NewMemoryStore(), Audience: "remount-api", Now: func() time.Time { return now }})
	verifier := staticIDTokenVerifier{claims: OIDCClaims{Issuer: issuer, Audience: []string{"audience"}, Subject: "agent:alice", Tenant: "tenant-a", Groups: []string{"remount-agents"}, Nonce: "nonce", IssuedAt: now, ExpiresAt: now.Add(time.Hour)}}
	access, refresh, err := client.Exchange(context.Background(), manager, verifier, providerTokens.IDToken, "nonce")
	if err != nil || access == "" || refresh == "" {
		t.Fatalf("exchange=(%q,%q,%v)", access, refresh, err)
	}
	subject, err := manager.Authenticate(context.Background(), controlCredential(access))
	if err != nil || subject.ID != "agent:alice" || subject.Roles[0] != RoleAgent {
		t.Fatalf("subject=%+v err=%v", subject, err)
	}
}

func TestOIDCRejectsIssuerNonceAndOversizedResponses(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 2048))) }))
	defer server.Close()
	client, err := NewOIDCClient(OIDCOptions{Issuer: server.URL, ClientID: "client", GroupRoles: map[string][]string{"g": {RoleAgent}}, HTTPClient: server.Client(), Now: func() time.Time { return now }, MaxResponseBytes: 1024, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Discover(context.Background()); err == nil {
		t.Fatal("oversized discovery accepted")
	}
	_, signingKey, _ := ed25519.GenerateKey(rand.Reader)
	manager, _ := New(Options{PrivateKey: signingKey, Store: NewMemoryStore(), Now: func() time.Time { return now }})
	claims := OIDCClaims{Issuer: server.URL, Audience: []string{"client"}, Subject: "alice", Tenant: "tenant-a", Groups: []string{"g"}, Nonce: "wrong", IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	if _, _, err := client.Exchange(context.Background(), manager, staticIDTokenVerifier{claims: claims}, "token", "expected"); err == nil {
		t.Fatal("nonce mismatch accepted")
	}
	claims.Nonce = "expected"
	claims.Issuer = server.URL + "/other"
	if _, _, err := client.Exchange(context.Background(), manager, staticIDTokenVerifier{claims: claims}, "token", "expected"); err == nil {
		t.Fatal("issuer mismatch accepted")
	}
}

func controlCredential(token string) control.Credential { return control.Credential{Token: token} }
