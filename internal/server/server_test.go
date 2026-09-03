package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/secretsource"
)

type allowAuthorizer struct{}

func (allowAuthorizer) Check(context.Context, control.Subject, string, control.Resource) error {
	return nil
}

type allowNodeAuthenticator struct{}

func (allowNodeAuthenticator) AuthenticateNode(context.Context, string, string, []byte) (control.NodeIdentity, error) {
	return control.NodeIdentity{Tenant: "tenant"}, nil
}

func testDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return artifact.ID(sum[:])
}

func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func TestNewRejectsNegativeEventCapacity(t *testing.T) {
	if _, err := New(Options{MaxEvents: -1}); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("New error = %v", err)
	}
}

func TestArtifactPutMismatchCannotDeleteExistingBlob(t *testing.T) {
	s := newTestServer(t, Options{MaxArtifactBytes: 1024})
	body := "pre-existing valid body"
	bodyID, _, err := s.Store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	wanted := testDigest("wanted")
	req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+wanted, strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !s.Store.Has(bodyID) {
		t.Fatal("digest mismatch deleted the body's pre-existing valid artifact")
	}
	if s.Store.Has(wanted) {
		t.Fatal("digest mismatch published the requested artifact")
	}
}

func TestArtifactPutEnforcesDeclaredAndStreamingLimits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		contentLength int64
	}{
		{name: "declared", contentLength: 5},
		{name: "streaming", contentLength: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, Options{MaxArtifactBytes: 4})
			body := "12345"
			req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+testDigest(body), strings.NewReader(body))
			req.ContentLength = tc.contentLength
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			ids, err := s.Store.List()
			if err != nil || len(ids) != 0 {
				t.Fatalf("oversized upload was published: %v, %v", ids, err)
			}
		})
	}
}

func TestArtifactPutEnforcesStoreCapacityAndAllowsDigestRetry(t *testing.T) {
	s := newTestServer(t, Options{
		MaxArtifactBytes: 1024, MaxArtifactStoreBytes: 4, MaxArtifactObjects: 1,
		ArtifactGCInterval: -1,
	})
	put := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+testDigest(body), strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := put("1234"); rec.Code != http.StatusOK {
		t.Fatalf("initial PUT = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := put("1234"); rec.Code != http.StatusOK {
		t.Fatalf("idempotent PUT at capacity = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := put("x"); rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-capacity PUT = %d: %s", rec.Code, rec.Body.String())
	}
	if s.Store.Has(testDigest("x")) {
		t.Fatal("over-capacity object was published")
	}
}

func TestEventRetentionPrunesPrefixAndExposesOldestSequence(t *testing.T) {
	s := newTestServer(t, Options{
		EventRetention: time.Hour, EventGCInterval: -1, ArtifactGCInterval: -1,
	})
	now := time.Now()
	for _, at := range []time.Time{now.Add(-2 * time.Hour), now.Add(-90 * time.Minute), now} {
		if err := s.Log.Append(context.Background(), &proto.Event{Type: "retention", At: at.UnixMilli()}); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := s.PruneEvents(context.Background(), now)
	if err != nil || removed != 2 {
		t.Fatalf("PruneEvents = (%d, %v)", removed, err)
	}
	first, err := s.Log.First(context.Background())
	if err != nil || first != 3 {
		t.Fatalf("First = (%d, %v)", first, err)
	}
	if _, err := s.Log.Read(context.Background(), 1, "", 10); !errors.Is(err, &proto.Error{Code: proto.CodeEvicted}) {
		t.Fatalf("old event read = %v", err)
	}
	events, err := s.Log.Read(context.Background(), 0, "", 10)
	if err != nil || len(events) != 1 || events[0].Seq != 3 {
		t.Fatalf("retained events = %+v, %v", events, err)
	}
}

func TestEventRetentionAlsoBoundsRecentRows(t *testing.T) {
	s := newTestServer(t, Options{
		EventRetention: 24 * time.Hour, MaxEvents: 2,
		EventGCInterval: -1, ArtifactGCInterval: -1,
	})
	now := time.Now()
	for index := 0; index < 5; index++ {
		if err := s.Log.Append(context.Background(), &proto.Event{Type: "recent", At: now.UnixMilli()}); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := s.PruneEvents(context.Background(), now)
	if err != nil || removed != 3 {
		t.Fatalf("PruneEvents = (%d, %v)", removed, err)
	}
	events, err := s.Log.Read(context.Background(), 0, "", 10)
	if err != nil || len(events) != 2 || events[0].Seq != 4 || events[1].Seq != 5 {
		t.Fatalf("retained events = %+v, %v", events, err)
	}
}

func TestServeWaitReadyHealthAndIdempotentClose(t *testing.T) {
	s := newTestServer(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ctx, "127.0.0.1:0") }()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	addr, err := s.WaitReady(waitCtx)
	if err != nil || addr == "" || addr != s.Addr() {
		t.Fatalf("WaitReady = (%q, %v), Addr = %q", addr, err, s.Addr())
	}
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Fatalf("ready response = %d %s", resp.StatusCode, body)
	}
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop after context cancellation")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseBeforeServeWakesWaitReadyAndRemovesTemporaryStore(t *testing.T) {
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	tempDir := s.tempArtDir
	if tempDir == "" {
		t.Fatal("test server did not allocate temporary artifact directory")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.WaitReady(ctx); err == nil {
		t.Fatal("WaitReady succeeded after Close-before-Serve")
	}
	if _, err := os.Stat(tempDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact directory remains: %v", err)
	}
	if err := s.Serve(context.Background(), "127.0.0.1:0"); err == nil {
		t.Fatal("Serve succeeded after Close")
	}
}

func TestConcurrentHealthAddrAndClose(t *testing.T) {
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ctx, "127.0.0.1:0") }()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if _, err := s.WaitReady(waitCtx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.Addr()
				rec := httptest.NewRecorder()
				s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
	}
	wg.Wait()
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestProductionModeRequiresIdentityAndRefusesSharedTokens(t *testing.T) {
	base := Options{
		Mode:          ModeProductionSingleTenant,
		Authenticator: control.StaticAuthenticator{"client-token": {ID: "user", Tenant: "tenant"}},
		Authorizer:    allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{},
	}
	if profile, err := validateSecurityMode(base); err != nil || profile != proto.SecurityIsolated {
		t.Fatalf("production identity profile=%q err=%v", profile, err)
	}
	base.Token = "shared"
	if _, err := validateSecurityMode(base); err == nil || !strings.Contains(err.Error(), "refuses shared tokens") {
		t.Fatalf("shared production token error=%v", err)
	}
	base.Token, base.NodeAuthenticator = "", nil
	if _, err := validateSecurityMode(base); err == nil || !strings.Contains(err.Error(), "principal and node identity") {
		t.Fatalf("incomplete production identity error=%v", err)
	}
}

func TestArtifactAuthorizationAcceptsConfiguredNodeOrClientCredential(t *testing.T) {
	s := newTestServer(t, Options{
		Token:         "node-token",
		Authenticator: control.StaticAuthenticator{"client-token": {ID: "user", Tenant: "tenant"}},
	})
	id := testDigest("missing")
	status := func(header string) int {
		req := httptest.NewRequest(http.MethodHead, "/v1/artifacts/"+id, nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		recorder := httptest.NewRecorder()
		s.Handler().ServeHTTP(recorder, req)
		return recorder.Code
	}
	if got := status("Bearer node-token"); got != http.StatusNotFound {
		t.Fatalf("configured node credential status = %d, want authenticated not-found", got)
	}
	if got := status("Bearer client-token"); got != http.StatusNotFound {
		t.Fatalf("client principal credential status = %d, want authenticated not-found", got)
	}
	for _, header := range []string{"", "node-token", "Bearer wrong-token"} {
		if got := status(header); got != http.StatusUnauthorized {
			t.Fatalf("unauthorized header %q status = %d", header, got)
		}
	}
}

func TestProductionModeBuildsDurableIdentityByDefault(t *testing.T) {
	s, err := New(Options{DataDir: t.TempDir(), Mode: ModeProductionMultiTenant})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Identity == nil || s.opts.Authenticator == nil || s.opts.Authorizer == nil || s.opts.NodeAuthenticator == nil {
		t.Fatal("production server did not construct its durable identity authority")
	}
	body := "tenant artifact"
	unauthenticated := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+testDigest(body), strings.NewReader(body))
	s.Handler().ServeHTTP(unauthenticated, req)
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated production artifact status = %d", unauthenticated.Code)
	}
	access, _, err := s.Identity.IssueTokens(context.Background(), "operator", "tenant-a", []string{"operator"})
	if err != nil {
		t.Fatal(err)
	}
	authenticated := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+testDigest(body), strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+access)
	s.Handler().ServeHTTP(authenticated, req)
	if authenticated.Code != http.StatusOK {
		t.Fatalf("authenticated production artifact status = %d body=%s", authenticated.Code, authenticated.Body.String())
	}
}

func TestProductionModeRejectsLiteralBindingSecrets(t *testing.T) {
	resolver, err := secretsource.New(secretsource.Config{})
	if err != nil {
		t.Fatal(err)
	}
	base := Options{
		Mode:          ModeProductionSingleTenant,
		Authenticator: control.StaticAuthenticator{"client-token": {ID: "user", Tenant: "tenant"}},
		Authorizer:    allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{}, SecretResolver: resolver,
	}
	base.Bindings = []control.Binding{{ID: "b_one", Secret: "must-not-persist", Destinations: []string{"api.example"}}}
	if _, err := validateSecurityMode(base); err == nil {
		t.Fatal("production mode accepted a literal binding secret")
	}
	base.Bindings[0].Secret = ""
	base.Bindings[0].Source = "env://API_TOKEN"
	if _, err := validateSecurityMode(base); err != nil {
		t.Fatalf("production mode rejected external source: %v", err)
	}
	base.SecretResolver = nil
	if _, err := validateSecurityMode(base); err == nil {
		t.Fatal("production mode accepted an external source without a resolver")
	}
}
