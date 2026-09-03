package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/encrypted"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/identity"
)

func testEncryptedResolver(t *testing.T, ready encrypted.ReadinessCheck) *encrypted.Resolver {
	t.Helper()
	root := t.TempDir()
	masterBytes := sha256.Sum256([]byte("server tenant artifact test master"))
	master, err := encrypted.NewAESMasterKey("master-v1", masterBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	keys, err := encrypted.NewDirectoryKeyProvider(root+"/keys", master, encrypted.DirectoryKeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := encrypted.NewFileStore(root+"/objects", encrypted.FileStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := encrypted.NewStore(objects, keys, root+"/stage", encrypted.Options{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := encrypted.NewResolver(store, keys, encrypted.ResolverOptions{Ready: ready})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func artifactRequest(t *testing.T, s *Server, method, token, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/v1/artifacts/"+id, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	s.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestProductionArtifactHTTPIsTenantScoped(t *testing.T) {
	resolver := testEncryptedResolver(t, nil)
	authenticator := control.StaticAuthenticator{
		"alice": {ID: "agent:alice", Tenant: "tenant-a", Roles: []string{identity.RoleAgent}},
		"bob":   {ID: "agent:bob", Tenant: "tenant-b", Roles: []string{identity.RoleAgent}},
	}
	s := newTestServer(t, Options{
		DataDir: t.TempDir(), Mode: ModeProductionMultiTenant,
		Authenticator: authenticator, Authorizer: allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{},
		TenantArtifacts: resolver, ArtifactGCInterval: -1,
	})
	body := "same plaintext belongs to two tenants"
	id := testDigest(body)
	unknown := testDigest("unknown")

	if response := artifactRequest(t, s, http.MethodPut, "alice", id, body); response.Code != http.StatusOK {
		t.Fatalf("alice PUT = %d: %s", response.Code, response.Body.String())
	}
	for _, candidate := range []string{id, unknown} {
		if response := artifactRequest(t, s, http.MethodHead, "bob", candidate, ""); response.Code != http.StatusNotFound {
			t.Fatalf("bob HEAD %s = %d, want indistinguishable 404", candidate, response.Code)
		}
	}
	if response := artifactRequest(t, s, http.MethodGet, "alice", id, ""); response.Code != http.StatusOK || response.Body.String() != body {
		t.Fatalf("alice GET = %d %q", response.Code, response.Body.String())
	}
	if response := artifactRequest(t, s, http.MethodPut, "bob", id, body); response.Code != http.StatusOK {
		t.Fatalf("bob PUT = %d: %s", response.Code, response.Body.String())
	}
	if response := artifactRequest(t, s, http.MethodGet, "bob", id, ""); response.Code != http.StatusOK || response.Body.String() != body {
		t.Fatalf("bob GET = %d %q", response.Code, response.Body.String())
	}

	a, err := resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolver.ResolveTenant("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := a.List(); err != nil || len(got) != 1 || got[0] != id {
		t.Fatalf("tenant-a inventory = %v, %v", got, err)
	}
	if got, err := b.List(); err != nil || len(got) != 1 || got[0] != id {
		t.Fatalf("tenant-b inventory = %v, %v", got, err)
	}
}

func TestProductionArtifactStorageFailsClosed(t *testing.T) {
	base := Options{
		DataDir: t.TempDir(), Mode: ModeProductionMultiTenant,
		Authenticator: control.StaticAuthenticator{"client": {ID: "client", Tenant: "tenant", Roles: []string{identity.RoleAgent}}},
		Authorizer:    allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{},
	}
	if _, err := New(base); err == nil || !strings.Contains(err.Error(), "requires encrypted tenant artifact storage") {
		t.Fatalf("missing encrypted resolver error = %v", err)
	}
	plain, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base.TenantArtifacts, err = artifact.NewSharedTenantResolver(plain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(base); err == nil || !strings.Contains(err.Error(), "requires encrypted tenant artifact storage") {
		t.Fatalf("plaintext resolver error = %v", err)
	}

	base.TenantArtifacts = testEncryptedResolver(t, func(context.Context) error { return errors.New("storage offline") })
	if _, err := New(base); err == nil || !strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("failed readiness error = %v", err)
	}
}

type recordingNodeArtifactAuthorizer struct {
	node, tenant, workspace, method, artifact, proof string
	generation                                       uint64
}

func (a *recordingNodeArtifactAuthorizer) AuthorizeNodeArtifact(_ context.Context, node, tenant, workspace string, generation uint64, method, artifact, proof string) error {
	a.node, a.tenant, a.workspace, a.generation, a.method, a.artifact, a.proof = node, tenant, workspace, generation, method, artifact, proof
	if workspace != "ws_one" || generation != 7 || proof != "signed-proof" {
		return errors.New("bad assignment proof")
	}
	return nil
}

func TestProductionNodeArtifactRequiresAssignmentProof(t *testing.T) {
	resolver := testEncryptedResolver(t, nil)
	authenticator := control.StaticAuthenticator{
		"node": {ID: "n_one", Tenant: "tenant-a", Roles: []string{identity.RoleNode}},
	}
	s := newTestServer(t, Options{
		DataDir: t.TempDir(), Mode: ModeProductionMultiTenant,
		Authenticator: authenticator, Authorizer: allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{},
		TenantArtifacts: resolver, ArtifactGCInterval: -1,
	})
	body := "node snapshot"
	id := testDigest(body)
	if response := artifactRequest(t, s, http.MethodPut, "node", id, body); response.Code != http.StatusNotFound {
		t.Fatalf("node PUT without assignment authorizer = %d", response.Code)
	}

	authorizer := &recordingNodeArtifactAuthorizer{}
	s.opts.NodeArtifactAuthorizer = authorizer
	request := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+id, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer node")
	request.Header.Set(ArtifactWorkspaceHeader, "ws_one")
	request.Header.Set(ArtifactGenerationHeader, "7")
	request.Header.Set(ArtifactProofHeader, "signed-proof")
	recorder := httptest.NewRecorder()
	s.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("proved node PUT = %d: %s", recorder.Code, recorder.Body.String())
	}
	if authorizer.node != "n_one" || authorizer.tenant != "tenant-a" || authorizer.artifact != id || authorizer.method != http.MethodPut {
		t.Fatalf("assignment access = node=%s tenant=%s artifact=%s method=%s", authorizer.node, authorizer.tenant, authorizer.artifact, authorizer.method)
	}
}

func TestTenantArtifactHTTPRejectsOversizeBeforePublication(t *testing.T) {
	resolver := testEncryptedResolver(t, nil)
	s := newTestServer(t, Options{
		DataDir: t.TempDir(), Mode: ModeProductionMultiTenant, MaxArtifactBytes: 4,
		Authenticator: control.StaticAuthenticator{"alice": {ID: "alice", Tenant: "tenant-a", Roles: []string{identity.RoleAgent}}},
		Authorizer:    allowAuthorizer{}, NodeAuthenticator: allowNodeAuthenticator{}, TenantArtifacts: resolver,
	})
	body := "12345"
	response := artifactRequest(t, s, http.MethodPut, "alice", testDigest(body), body)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize PUT = %d: %s", response.Code, response.Body.String())
	}
	store, err := resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if ids, err := store.List(); err != nil || len(ids) != 0 {
		t.Fatalf("oversize upload published: %v, %v", ids, err)
	}
}

func TestTenantArtifactGetStreamsVerifiedPlaintext(t *testing.T) {
	resolver := testEncryptedResolver(t, nil)
	store, err := resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := store.Put(strings.NewReader("verified"))
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(reader); err != nil || string(body) != "verified" {
		t.Fatalf("read = %q, %v", body, err)
	}
	if err := reader.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestArtifactReadErrorsDoNotDisguiseCorruptionAsAbsence(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{err: os.ErrNotExist, want: http.StatusNotFound},
		{err: encrypted.ErrKeyUnavailable, want: http.StatusServiceUnavailable},
		{err: encrypted.ErrIntegrity, want: http.StatusInternalServerError},
		{err: encrypted.ErrMalformed, want: http.StatusInternalServerError},
	} {
		recorder := httptest.NewRecorder()
		writeArtifactReadError(recorder, test.err)
		if recorder.Code != test.want {
			t.Fatalf("error %v status = %d, want %d", test.err, recorder.Code, test.want)
		}
	}
}
