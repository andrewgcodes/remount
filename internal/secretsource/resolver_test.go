package secretsource

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnvAndFileSourcesRejectLiterals(t *testing.T) {
	t.Setenv("REMOUNT_SECRET_SOURCE_TEST", "env-secret-value")
	resolver := newTestResolver(t, Config{})
	value, err := resolver.Resolve(context.Background(), "env://REMOUNT_SECRET_SOURCE_TEST")
	if err != nil || value != "env-secret-value" {
		t.Fatalf("env Resolve = %q, %v", value, err)
	}
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte("file-secret-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	fileURLPath := filepath.ToSlash(file)
	if runtime.GOOS == "windows" {
		fileURLPath = "/" + fileURLPath
	}
	fileSource := (&url.URL{Scheme: "file", Path: fileURLPath}).String()
	value, err = resolver.Resolve(context.Background(), fileSource)
	if err != nil || value != "file-secret-value" {
		t.Fatalf("file Resolve = %q, %v", value, err)
	}
	if err := resolver.Probe(context.Background(), fileSource); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"literal-secret", "file://relative", "env://", "https://example.test/secret"} {
		if err := ValidateSource(source); err == nil {
			t.Fatalf("ValidateSource(%q) accepted", source)
		}
	}
}

func TestCacheExpiryBoundAndConcurrentSingleflight(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		_, _ = io.WriteString(w, `{"payload":{"data":"c2hhcmVkLXNlY3JldA=="}}`)
	}))
	defer server.Close()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	resolver := newTestResolver(t, Config{
		CacheTTL: time.Minute, MaxEntries: 1, MaxBytes: 64, Now: func() time.Time { return now },
		GCPEndpoint: server.URL, GCPAccessToken: func(context.Context) (string, error) { return "oauth-token", nil },
	})
	source := "gcpsm://projects/p/secrets/s/versions/latest"
	const workers = 24
	results := make(chan error, workers)
	for range workers {
		go func() {
			value, err := resolver.Resolve(context.Background(), source)
			if err == nil && value != "shared-secret" {
				err = errors.New("wrong value")
			}
			results <- err
		}()
	}
	<-started
	close(release)
	for range workers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
	stats := resolver.Stats()
	if stats.Entries != 1 || stats.Bytes != int64(len("shared-secret")) || stats.Inflight != 0 {
		t.Fatalf("Stats = %+v", stats)
	}
	now = now.Add(2 * time.Minute)
	value, err := resolver.Resolve(context.Background(), source)
	if err != nil || value != "shared-secret" || calls.Load() != 2 {
		t.Fatalf("expired Resolve = %q, %v; calls=%d", value, err, calls.Load())
	}
	t.Setenv("REMOUNT_SECRET_SOURCE_EVICT", "replacement")
	if _, err := resolver.Resolve(context.Background(), "env://REMOUNT_SECRET_SOURCE_EVICT"); err != nil {
		t.Fatal(err)
	}
	if stats := resolver.Stats(); stats.Entries != 1 {
		t.Fatalf("bounded cache Stats = %+v", stats)
	}
	if _, err := resolver.Resolve(context.Background(), source); err != nil || calls.Load() != 3 {
		t.Fatalf("evicted source was not resolved again: %v calls=%d", err, calls.Load())
	}
	resolver.Purge()
	if stats := resolver.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("Stats after Purge = %+v", stats)
	}
}

func TestConcurrentProbesShareOneUncachedRead(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		_, _ = io.WriteString(w, `{"payload":{"data":"c2VjcmV0"}}`)
	}))
	defer server.Close()
	resolver := newTestResolver(t, Config{
		GCPEndpoint: server.URL,
		GCPAccessToken: func(context.Context) (string, error) {
			return "token", nil
		},
	})
	source := "gcpsm://projects/p/secrets/s/versions/latest"
	const workers = 16
	results := make(chan error, workers)
	for range workers {
		go func() { results <- resolver.Probe(context.Background(), source) }()
	}
	<-started
	close(release)
	for range workers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider probe calls = %d, want 1", got)
	}
	if stats := resolver.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("Probe populated cache: %+v", stats)
	}
}

func TestVaultTokenAndAppRole(t *testing.T) {
	var appRole bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/auth/approle/login":
			appRole = true
			_, _ = io.WriteString(w, `{"auth":{"client_token":"short-vault-token"}}`)
		case "/v1/secret/data/app":
			if request.Header.Get("X-Vault-Token") == "" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = io.WriteString(w, `{"data":{"data":{"api_key":"vault-secret"}},"lease_duration":60}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	source := "vault://" + address + "/secret/app#api_key"
	resolver := newTestResolver(t, Config{VaultScheme: "http", VaultCredentials: func(context.Context) (VaultCredentials, error) {
		return VaultCredentials{Token: "vault-token"}, nil
	}})
	value, err := resolver.Resolve(context.Background(), source)
	if err != nil || value != "vault-secret" {
		t.Fatalf("Vault token Resolve = %q, %v", value, err)
	}
	resolver = newTestResolver(t, Config{VaultScheme: "http", VaultCredentials: func(context.Context) (VaultCredentials, error) {
		return VaultCredentials{RoleID: "role", SecretID: "role-secret"}, nil
	}})
	value, err = resolver.Resolve(context.Background(), source)
	if err != nil || value != "vault-secret" || !appRole {
		t.Fatalf("Vault AppRole Resolve = %q, %v, login=%v", value, err, appRole)
	}
}

func TestAWSGCPAndGitHubFixtures(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	now := time.Now().UTC().Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Header.Get("X-Amz-Target") == "secretsmanager.GetSecretValue":
			if !strings.Contains(request.Header.Get("Authorization"), "Credential=access/") || strings.Contains(request.Header.Get("Authorization"), "aws-secret") {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = io.WriteString(w, `{"SecretString":"aws-value"}`)
		case strings.HasSuffix(request.URL.Path, ":access"):
			if request.Header.Get("Authorization") != "Bearer gcp-token" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = io.WriteString(w, `{"payload":{"data":"Z2NwLXZhbHVl"}}`)
		case request.URL.Path == "/app/installations/456/access_tokens":
			jwt := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
			if err := verifyJWT(jwt, &privateKey.PublicKey); err != nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "github-value", "expires_at": now.Add(time.Hour)})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	resolver := newTestResolver(t, Config{
		Now: func() time.Time { return now }, AWSEndpoint: server.URL,
		AWSCredentials: func(context.Context) (AWSCredentials, error) {
			return AWSCredentials{AccessKeyID: "access", SecretAccessKey: "aws-secret", SessionToken: "aws-session"}, nil
		},
		GCPEndpoint: server.URL, GCPAccessToken: func(context.Context) (string, error) { return "gcp-token", nil },
		GitHubEndpoint: server.URL, GitHubPrivateKey: func(context.Context, string) ([]byte, error) { return keyPEM, nil },
	})
	cases := map[string]string{
		"awssm://arn:aws:secretsmanager:us-east-1:123456789012:secret:name": "aws-value",
		"gcpsm://projects/p/secrets/s/versions/latest":                      "gcp-value",
		"github-app://123/456": "github-value",
	}
	for source, want := range cases {
		got, err := resolver.Resolve(context.Background(), source)
		if err != nil || got != want {
			t.Errorf("Resolve source scheme = %q, %v; want %q", got, err, want)
		}
		if err := resolver.Probe(context.Background(), source); err != nil {
			t.Errorf("Probe source scheme: %v", err)
		}
	}
}

func TestErrorsNeverContainSecretCanaries(t *testing.T) {
	const canary = "secret-canary-must-not-escape-ABC123"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprintf(w, `{"message":%q,"SecretString":%q}`, canary, canary)
	}))
	defer server.Close()
	resolver := newTestResolver(t, Config{
		VaultScheme: "http", VaultCredentials: func(context.Context) (VaultCredentials, error) { return VaultCredentials{Token: canary}, nil },
		AWSEndpoint: server.URL, AWSCredentials: func(context.Context) (AWSCredentials, error) {
			return AWSCredentials{AccessKeyID: "access", SecretAccessKey: canary}, nil
		},
		GCPEndpoint: server.URL, GCPAccessToken: func(context.Context) (string, error) { return canary, nil },
		GitHubEndpoint: server.URL, GitHubPrivateKey: func(context.Context, string) ([]byte, error) { return nil, errors.New(canary) },
	})
	sources := []string{
		"vault://" + strings.TrimPrefix(server.URL, "http://") + "/secret/path#key",
		"awssm://arn:aws:secretsmanager:us-east-1:123456789012:secret:name",
		"gcpsm://projects/p/secrets/s/versions/latest",
		"github-app://123/456",
	}
	for _, source := range sources {
		_, err := resolver.Resolve(context.Background(), source)
		if err == nil || strings.Contains(err.Error(), canary) {
			t.Errorf("unsafe error for source scheme: %v", err)
		}
	}
	file := filepath.Join(t.TempDir(), "oversize")
	if err := os.WriteFile(file, []byte(canary), 0o600); err != nil {
		t.Fatal(err)
	}
	tiny := newTestResolver(t, Config{MaxBytes: 4})
	_, err := tiny.Resolve(context.Background(), (&url.URL{Scheme: "file", Path: file}).String())
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("unsafe file error: %v", err)
	}
}

func TestRedirectDoesNotForwardProviderAuthorization(t *testing.T) {
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	resolver := newTestResolver(t, Config{GCPEndpoint: redirect.URL, GCPAccessToken: func(context.Context) (string, error) { return "bearer-secret", nil }})
	_, err := resolver.Resolve(context.Background(), "gcpsm://projects/p/secrets/s/versions/latest")
	if err == nil {
		t.Fatal("redirect was accepted as a provider response")
	}
	if leaked.Load() {
		t.Fatal("redirect target received provider authorization")
	}
}

func TestGCPChecksumMismatchAndPlaintextDefaultFailClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, `{"payload":{"data":"Z2NwLXZhbHVl","dataCrc32c":"1"}}`)
	}))
	defer server.Close()
	resolver := newTestResolver(t, Config{GCPEndpoint: server.URL, GCPAccessToken: func(context.Context) (string, error) { return "token", nil }})
	if _, err := resolver.Resolve(context.Background(), "gcpsm://projects/p/secrets/s/versions/latest"); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum mismatch = %v", err)
	}
	if _, err := New(Config{VaultScheme: "http"}); err == nil {
		t.Fatal("production resolver accepted plaintext Vault")
	}
}

func newTestResolver(t *testing.T, cfg Config) *CachedResolver {
	t.Helper()
	cfg.AllowInsecureHTTP = true
	resolver, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func verifyJWT(token string, publicKey *rsa.PublicKey) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return errors.New("bad token")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, digest[:], signature)
}
