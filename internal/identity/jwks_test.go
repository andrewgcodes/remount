package identity

import (
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
	"sync/atomic"
	"testing"
	"time"
)

func TestJWKSVerifierRS256AndSemanticBinding(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	exponent := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
	modulus := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "key-1", "use": "sig", "alg": "RS256", "n": modulus, "e": exponent}}, "extension": "allowed"})
	}))
	defer server.Close()
	now := time.Unix(1_900_000_000, 0)
	verifier, err := NewJWKSVerifier(JWKSOptions{JWKSURI: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now }, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"iss": "https://issuer.example", "aud": []string{"client", "api"}, "azp": "client", "sub": "alice", "tenant": "tenant-a", "groups": []string{"agents"}, "nonce": "nonce", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	token := signRS256TestToken(t, key, "key-1", claims)
	verified, err := verifier.VerifyIDToken(context.Background(), token, "https://issuer.example", "client")
	if err != nil || verified.Subject != "alice" || verified.Tenant != "tenant-a" || verified.AuthorizedParty != "client" {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
	if _, err := verifier.VerifyIDToken(context.Background(), token+"x", "https://issuer.example", "client"); err == nil {
		t.Fatal("tampered ID token accepted")
	}
	if _, err := verifier.VerifyIDToken(context.Background(), token, "https://other.example", "client"); err == nil {
		t.Fatal("wrong issuer accepted")
	}
	if _, err := verifier.VerifyIDToken(context.Background(), token, "https://issuer.example", "other"); err == nil {
		t.Fatal("wrong audience accepted")
	}
	unknown := signRS256TestToken(t, key, "unknown", claims)
	for range 2 {
		if _, err := verifier.VerifyIDToken(context.Background(), unknown, "https://issuer.example", "client"); err == nil {
			t.Fatal("unknown key id accepted")
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("unknown kid caused JWKS refetch amplification: requests=%d", requests.Load())
	}
}

func signRS256TestToken(t *testing.T, key *rsa.PrivateKey, keyID string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
