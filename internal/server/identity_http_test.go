package server

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/tenant"
)

func TestOIDCExchangeMapsTenantRolesAndRotatesRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var issuer string
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "device_authorization_endpoint": issuer + "/device", "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/keys"})
		case "/keys":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "one", "use": "sig", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer provider.Close()
	issuer = provider.URL
	previousTransport := http.DefaultTransport
	http.DefaultTransport = provider.Client().Transport
	defer func() { http.DefaultTransport = previousTransport }()

	s, err := New(Options{DataDir: t.TempDir(), Mode: ModeProductionMultiTenant, TenantArtifacts: testEncryptedResolver(t, nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.Tenants.Create(context.Background(), "tenant-a", tenant.Policy{OIDC: tenant.OIDC{
		Issuer: issuer, ClientID: "client", Audience: "client", Scopes: []string{"openid"}, GroupRoles: map[string][]string{"agents": {"agent"}},
	}}, tenant.Mutation{OperationID: "create", Actor: "bootstrap"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	idToken := signServerOIDCToken(t, key, map[string]any{"iss": issuer, "aud": "client", "sub": "alice", "tenant": "tenant-a", "groups": []string{"agents"}, "nonce": "nonce", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	body, _ := json.Marshal(map[string]string{"tenant": "tenant-a", "id_token": idToken, "nonce": "nonce"})
	recorder := httptest.NewRecorder()
	s.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/identity/oidc/exchange", bytes.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("exchange status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var pair tokenPairResponse
	if json.Unmarshal(recorder.Body.Bytes(), &pair) != nil || pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatalf("pair=%+v", pair)
	}
	subject, err := s.Identity.Authenticate(context.Background(), control.Credential{Token: pair.AccessToken, Role: "client"})
	if err != nil || subject.Tenant != "tenant-a" || len(subject.Roles) != 1 || subject.Roles[0] != "agent" {
		t.Fatalf("subject=%+v err=%v", subject, err)
	}
	refreshBody, _ := json.Marshal(map[string]string{"refresh_token": pair.RefreshToken})
	rotated := httptest.NewRecorder()
	s.Handler().ServeHTTP(rotated, httptest.NewRequest(http.MethodPost, "/v1/identity/refresh", bytes.NewReader(refreshBody)))
	if rotated.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", rotated.Code, rotated.Body.String())
	}
	replayed := httptest.NewRecorder()
	s.Handler().ServeHTTP(replayed, httptest.NewRequest(http.MethodPost, "/v1/identity/refresh", bytes.NewReader(refreshBody)))
	if replayed.Code != http.StatusUnauthorized {
		t.Fatalf("refresh replay status=%d", replayed.Code)
	}

	wrongTenant := signServerOIDCToken(t, key, map[string]any{"iss": issuer, "aud": "client", "sub": "mallory", "tenant": "tenant-b", "groups": []string{"agents"}, "nonce": "nonce", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	body, _ = json.Marshal(map[string]string{"tenant": "tenant-a", "id_token": wrongTenant, "nonce": "nonce"})
	denied := httptest.NewRecorder()
	s.Handler().ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/v1/identity/oidc/exchange", bytes.NewReader(body)))
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("cross-tenant exchange status=%d body=%s", denied.Code, denied.Body.String())
	}
}

func signServerOIDCToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "one", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
