package encrypted

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func testMaster(t *testing.T, id string) *AESMasterKey {
	t.Helper()
	key := bytes.Repeat([]byte{byte(len(id) + 17)}, 32)
	master, err := NewAESMasterKey(id, key)
	if err != nil {
		t.Fatal(err)
	}
	return master
}

func TestDirectoryKeysAreWrappedVersionedAndPersistent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	provider, err := NewDirectoryKeyProvider(dir, testMaster(t, "master-v1"), DirectoryKeyOptions{MaxVersionsPerTenant: 3})
	if err != nil {
		t.Fatal(err)
	}
	v1, key1, err := provider.Current(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if v1 == "" || key1 == [32]byte{} {
		t.Fatal("provider returned empty tenant key")
	}
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(body, key1[:]) || bytes.Contains(body, []byte(hex.EncodeToString(key1[:]))) || bytes.Contains(body, []byte(base64.StdEncoding.EncodeToString(key1[:]))) {
			t.Fatalf("tenant key is present in plaintext at %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	v2, err := provider.Rotate(ctx, "tenant-a")
	if err != nil || v2 == v1 {
		t.Fatalf("Rotate = (%q, %v)", v2, err)
	}
	got1, err := provider.Get(ctx, "tenant-a", v1)
	if err != nil || got1 != key1 {
		t.Fatal("retired tenant key stopped being readable")
	}
	current, _, err := provider.Current(ctx, "tenant-a")
	if err != nil || current != v2 {
		t.Fatalf("Current = (%q, %v)", current, err)
	}
	versions, err := provider.Versions(ctx, "tenant-a")
	if err != nil || len(versions) != 2 || versions[0].Version != v2 || versions[0].Retired || versions[1].Version != v1 || !versions[1].Retired {
		t.Fatalf("Versions = %+v, %v", versions, err)
	}
	if _, err := provider.Rotate(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Rotate(ctx, "tenant-a"); err == nil {
		t.Fatal("key-version retention bound was not enforced")
	}

	reopened, err := NewDirectoryKeyProvider(dir, testMaster(t, "master-v1"), DirectoryKeyOptions{MaxVersionsPerTenant: 3})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(ctx, "tenant-a", v1)
	if err != nil || got != key1 {
		t.Fatal("wrapped tenant key did not survive reopen")
	}
}

func TestDirectoryKeyMetadataCorruptionFailsClosed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	provider, err := NewDirectoryKeyProvider(dir, testMaster(t, "master-v1"), DirectoryKeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	version, _, err := provider.Current(ctx, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, "tenants", "tenant", "keys", version+".json")
	if err := os.WriteFile(record, []byte(`{"format":1,"tenant":"tenant","version":"`+version+`","master_key":"master-v1","ciphertext":"AAAA","created_at":"2026-09-03T00:00:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Get(ctx, "tenant", version); err == nil {
		t.Fatal("corrupt wrapped tenant key was accepted")
	}
	if err := os.Remove(filepath.Join(dir, "tenants", "tenant", "current")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := provider.Current(ctx, "tenant"); !errors.Is(err, ErrMalformed) {
		t.Fatalf("missing current with retained keys = %v", err)
	}
}

func TestEnvAndFileMasterKeys(t *testing.T) {
	key := bytes.Repeat([]byte{42}, 32)
	t.Setenv("REMOUNT_TEST_MASTER", base64.StdEncoding.EncodeToString(key))
	if _, err := NewEnvMasterKey("REMOUNT_TEST_MASTER", "env-v1"); err != nil {
		t.Fatal(err)
	}
	secretLooking := "do-not-echo-this-invalid-secret"
	t.Setenv("REMOUNT_TEST_BAD_MASTER", secretLooking)
	if _, err := NewEnvMasterKey("REMOUNT_TEST_BAD_MASTER", "env-v1"); err == nil || strings.Contains(err.Error(), secretLooking) {
		t.Fatalf("invalid env handling leaked or accepted secret: %v", err)
	}
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, []byte("hex:"+hex.EncodeToString(key)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileMasterKey(path, "file-v1"); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		t.Log("unavailable: Go file modes do not expose Windows ACL confidentiality")
		return
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileMasterKey(path, "file-v1"); err == nil {
		t.Fatal("world-readable master key file was accepted")
	}
}

func TestMultiMasterKeyReadsOldVersions(t *testing.T) {
	ctx := context.Background()
	v1 := testMaster(t, "master-v1")
	v2 := testMaster(t, "master-v2")
	wrapped, err := v1.Wrap(ctx, []byte("tenant-key"), []byte("aad"))
	if err != nil {
		t.Fatal(err)
	}
	multi := MultiMasterKey{Current: v2, ByID: map[string]MasterKey{"master-v1": v1, "master-v2": v2}}
	got, err := multi.Unwrap(ctx, wrapped, []byte("aad"))
	if err != nil || string(got) != "tenant-key" {
		t.Fatalf("old master unwrap = %q, %v", got, err)
	}
	newWrapped, err := multi.Wrap(ctx, []byte("tenant-key"), []byte("aad"))
	if err != nil || newWrapped.KeyID != "master-v2" {
		t.Fatalf("new master wrap = %+v, %v", newWrapped, err)
	}
}

func TestMasterRewrapAndRetiredVersionPruning(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	v1 := testMaster(t, "master-v1")
	v2 := testMaster(t, "master-v2")
	multi := MultiMasterKey{Current: v1, ByID: map[string]MasterKey{"master-v1": v1, "master-v2": v2}}
	provider, err := NewDirectoryKeyProvider(dir, multi, DirectoryKeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldVersion, oldKey, err := provider.Current(ctx, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	multi.Current = v2
	provider.master = multi
	if err := provider.RewrapMaster(ctx, "tenant", oldVersion); err != nil {
		t.Fatal(err)
	}
	versions, err := provider.Versions(ctx, "tenant")
	if err != nil || versions[0].MasterKeyID != "master-v2" {
		t.Fatalf("master metadata = %+v, %v", versions, err)
	}
	got, err := provider.Get(ctx, "tenant", oldVersion)
	if err != nil || got != oldKey {
		t.Fatal("master rewrap changed tenant key")
	}
	if _, err := provider.Rotate(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	objects := newMemoryObjects()
	store := newTestStore(t, objects, provider, Options{})
	if err := store.PruneKeyVersion(ctx, "tenant", oldVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Get(ctx, "tenant", oldVersion); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("pruned key lookup = %v", err)
	}
	current, _, err := provider.Current(ctx, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PruneKeyVersion(ctx, "tenant", current); err == nil {
		t.Fatal("current key version was pruned")
	}
}

func TestAWSKMSRESTProviderAgainstFake(t *testing.T) {
	ctx := context.Background()
	var sawContext bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		contextMap, _ := request["EncryptionContext"].(map[string]any)
		sawContext = contextMap["remount_aad"] != ""
		switch r.Header.Get("X-Amz-Target") {
		case "TrentService.Encrypt":
			plaintext, _ := request["Plaintext"].(string)
			_ = json.NewEncoder(w).Encode(map[string]string{"CiphertextBlob": plaintext, "KeyId": "kms-key-version"})
		case "TrentService.Decrypt":
			ciphertext, _ := request["CiphertextBlob"].(string)
			_ = json.NewEncoder(w).Encode(map[string]string{"Plaintext": ciphertext})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	provider := &AWSKMSMasterKey{Endpoint: server.URL, KeyID: "alias/remount", Client: server.Client()}
	wrapped, err := provider.Wrap(ctx, []byte("tenant-key"), []byte("tenant-aad"))
	if err != nil || !sawContext {
		t.Fatalf("Wrap = %+v, %v, context=%v", wrapped, err, sawContext)
	}
	got, err := provider.Unwrap(ctx, wrapped, []byte("tenant-aad"))
	if err != nil || string(got) != "tenant-key" {
		t.Fatalf("Unwrap = %q, %v", got, err)
	}
}

func TestGCPKMSRESTProviderAndSanitizedError(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, ":encrypt") && !strings.HasSuffix(r.URL.Path, ":decrypt") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request map[string]string
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request["additionalAuthenticatedData"] == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(r.URL.Path, ":encrypt") {
			_ = json.NewEncoder(w).Encode(map[string]string{"ciphertext": request["plaintext"], "name": "gcp-key-version"})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{"plaintext": request["ciphertext"]})
		}
	}))
	defer server.Close()
	provider := &GCPKMSMasterKey{
		Endpoint: server.URL, Resource: "projects/p/locations/l/keyRings/r/cryptoKeys/k", Client: server.Client(),
	}
	wrapped, err := provider.Wrap(ctx, []byte("tenant-key"), []byte("tenant-aad"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Unwrap(ctx, wrapped, []byte("tenant-aad"))
	if err != nil || string(got) != "tenant-key" {
		t.Fatalf("Unwrap = %q, %v", got, err)
	}

	leak := "provider-secret-error-body"
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, leak)
	}))
	defer failing.Close()
	provider.Endpoint = failing.URL
	provider.Client = failing.Client()
	if _, err := provider.Wrap(ctx, []byte("tenant-key"), []byte("aad")); err == nil || strings.Contains(err.Error(), leak) {
		t.Fatalf("provider error leaked response body: %v", err)
	}
}
