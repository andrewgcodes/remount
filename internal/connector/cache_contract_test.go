package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"remount.dev/remount/internal/metrics"
)

// cachedPackageFixture serves one immutable payload and counts registry hits.
type cachedPackageFixture struct {
	store    *Store
	packages *Package
	payload  string
	digest   string
	calls    atomic.Int64
}

func newCachedPackageFixture(t *testing.T, opts StoreOptions, payload string) *cachedPackageFixture {
	t.Helper()
	store, err := NewStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(payload))
	f := &cachedPackageFixture{store: store, payload: payload, digest: hex.EncodeToString(sum[:])}
	f.packages = NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		f.calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(payload)), ContentLength: int64(len(payload)),
		}, nil
	})})
	return f
}

func (f *cachedPackageFixture) fetch(t *testing.T, workspace string, maximum int64) (ConnectorResponse, error) {
	t.Helper()
	return f.packages.Execute(context.Background(), packageRequest(t, workspace, "sha256:"+f.digest, packageRule(maximum)))
}

// mustFetchVerified fails unless the response body is byte-identical to the
// payload and advertises its digest. A cached response is allowed only when
// its bytes match the advertised digest.
func (f *cachedPackageFixture) mustFetchVerified(t *testing.T, workspace string, maximum int64) ConnectorResponse {
	t.Helper()
	response, err := f.fetch(t, workspace, maximum)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	body := readConnectorResponse(t, response)
	sum := sha256.Sum256([]byte(body))
	if body != f.payload || hex.EncodeToString(sum[:]) != f.digest {
		t.Fatalf("bytes do not match advertised digest: body=%q cached=%v", body, response.Provenance.Cached)
	}
	if response.Header.Get(ContentDigestHeader) != "sha256:"+f.digest || response.Provenance.SHA256 != f.digest {
		t.Fatalf("digest headers=%v provenance=%+v", response.Header, response.Provenance)
	}
	return response
}

func rewriteBlob(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPackageCacheHitVerifiesBytesAgainstAdvertisedDigest(t *testing.T) {
	payload := "immutable wheel bytes v1"
	corrupt := strings.Map(func(r rune) rune {
		if r == '1' {
			return '2'
		}
		return r
	}, payload)
	if len(corrupt) != len(payload) || corrupt == payload {
		t.Fatalf("fixture must be a same-size corruption: %q", corrupt)
	}
	opts := StoreOptions{MaxBytes: 4096, MaxBytesPerScope: 1024, MaxObjectBytes: 256}
	cases := []struct {
		name    string
		damage  func(t *testing.T, f *cachedPackageFixture)
		sibling bool
		maximum int64
	}{
		{name: "same size corruption", damage: func(t *testing.T, f *cachedPackageFixture) {
			rewriteBlob(t, f.store.blobPath(f.digest), []byte(corrupt))
		}},
		{name: "same size corruption served to sibling scope", sibling: true, damage: func(t *testing.T, f *cachedPackageFixture) {
			rewriteBlob(t, f.store.blobPath(f.digest), []byte(corrupt))
		}},
		{name: "truncated blob", damage: func(t *testing.T, f *cachedPackageFixture) {
			rewriteBlob(t, f.store.blobPath(f.digest), []byte(payload[:len(payload)/2]))
		}},
		{name: "missing blob", damage: func(t *testing.T, f *cachedPackageFixture) {
			if err := os.Remove(f.store.blobPath(f.digest)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "blob replaced by directory", damage: func(t *testing.T, f *cachedPackageFixture) {
			if err := os.Remove(f.store.blobPath(f.digest)); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(f.store.blobPath(f.digest), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "corrupt reference metadata", damage: func(t *testing.T, f *cachedPackageFixture) {
			ref := f.store.refPath(f.store.scope("tenant-private", "ws_a"), f.digest)
			if err := os.WriteFile(ref, []byte("{not json"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "reference size disagrees with blob", damage: func(t *testing.T, f *cachedPackageFixture) {
			ref := f.store.refPath(f.store.scope("tenant-private", "ws_a"), f.digest)
			data, err := os.ReadFile(ref)
			if err != nil {
				t.Fatal(err)
			}
			data = []byte(strings.Replace(string(data), `"size":`+strconv.Itoa(len(payload)), `"size":`+strconv.Itoa(len(payload)-1), 1))
			if err := os.WriteFile(ref, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		// The rule permits 64 bytes; the intact blob has 24; the reference
		// claims 100. Metadata alone must not turn a fitting object into a
		// permanent response_too_large.
		{name: "inflated reference size above current rule", maximum: 64, damage: func(t *testing.T, f *cachedPackageFixture) {
			ref := f.store.refPath(f.store.scope("tenant-private", "ws_a"), f.digest)
			data, err := os.ReadFile(ref)
			if err != nil {
				t.Fatal(err)
			}
			data = []byte(strings.Replace(string(data), `"size":`+strconv.Itoa(len(payload)), `"size":100`, 1))
			if err := os.WriteFile(ref, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := metrics.PackageCacheIntegrityFailures.Value()
			maximum := tc.maximum
			if maximum == 0 {
				maximum = 256
			}
			f := newCachedPackageFixture(t, opts, payload)
			first := f.mustFetchVerified(t, "ws_a", maximum)
			if first.Provenance.Cached || f.calls.Load() != 1 {
				t.Fatalf("first fetch cached=%v calls=%d", first.Provenance.Cached, f.calls.Load())
			}
			tc.damage(t, f)

			workspace := "ws_a"
			if tc.sibling {
				workspace = "ws_b"
			}
			// The damaged object must never be released under the advertised
			// digest, whichever way the connector chooses to recover.
			response, err := f.fetch(t, workspace, maximum)
			if err == nil {
				body := readConnectorResponse(t, response)
				if body != payload {
					t.Fatalf("cache released bytes that do not match the advertised digest: %q cached=%v", body, response.Provenance.Cached)
				}
			}
			if metrics.PackageCacheIntegrityFailures.Value() == before && !tc.sibling {
				t.Fatal("cache integrity failure was not observable")
			}
			// Recovery: a later fetch by the same workspace delivers verified
			// bytes and is served from the repaired reference afterwards.
			f.mustFetchVerified(t, workspace, maximum)
			calls := f.calls.Load()
			later := f.mustFetchVerified(t, workspace, maximum)
			if !later.Provenance.Cached || f.calls.Load() != calls {
				t.Fatalf("repaired reference not reused: cached=%v calls=%d->%d", later.Provenance.Cached, calls, f.calls.Load())
			}
			blob, err := os.ReadFile(f.store.blobPath(f.digest))
			if err != nil || string(blob) != payload {
				t.Fatalf("blob not repaired: err=%v content=%q", err, blob)
			}
			if info, err := os.Stat(f.store.blobPath(f.digest)); err != nil || info.Mode().Perm()&0o222 != 0 {
				t.Fatalf("repaired blob is not immutable: info=%v err=%v", info, err)
			}
		})
	}
}

func TestPackageCacheRepairIsSafeUnderConcurrentFetches(t *testing.T) {
	payload := "concurrently repaired wheel bytes"
	f := newCachedPackageFixture(t, StoreOptions{MaxBytes: 1 << 20, MaxBytesPerScope: 4096, MaxObjectBytes: 256}, payload)
	for _, workspace := range []string{"ws_0", "ws_1"} {
		f.mustFetchVerified(t, workspace, 256)
	}
	rewriteBlob(t, f.store.blobPath(f.digest), []byte(strings.ToUpper(payload)))

	const workers = 12
	var wg sync.WaitGroup
	released := make(chan string, workers)
	failed := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Two scopes hold references to the damaged blob, two do not.
			workspace := "ws_" + strconv.Itoa(i%4)
			response, err := f.fetch(t, workspace, 256)
			if err != nil {
				failed <- err
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != payload {
				released <- strconv.Quote(string(body)) + " cached=" + strconv.FormatBool(response.Provenance.Cached)
			}
		}(i)
	}
	wg.Wait()
	close(released)
	close(failed)
	for body := range released {
		t.Errorf("released bytes that do not match the advertised digest: %s", body)
	}
	for err := range failed {
		var connectorErr *Error
		if runtime.GOOS == "windows" && errors.As(err, &connectorErr) && connectorErr.Code == "cache_write" {
			// Windows refuses to place a new file under a name whose deleted
			// predecessor is still open by a concurrent verification; the
			// request fails closed and the retry below converges.
			t.Logf("concurrent replacement refused: %v", err)
			continue
		}
		t.Errorf("concurrent fetch failed: %v", err)
	}
	for i := 0; i < 4; i++ {
		hit := f.mustFetchVerified(t, "ws_"+strconv.Itoa(i), 256)
		if !hit.Provenance.Cached {
			t.Fatalf("ws_%d did not settle on a verified reference", i)
		}
	}
}

func TestPackageCacheHitAppliesCurrentResponseSizeRule(t *testing.T) {
	payload := "ten bytes!"
	f := newCachedPackageFixture(t, StoreOptions{MaxBytes: 4096, MaxBytesPerScope: 1024, MaxObjectBytes: 256}, payload)
	first := f.mustFetchVerified(t, "ws_a", 64)
	if first.Provenance.Cached {
		t.Fatal("first fetch must not be cached")
	}
	hitsBefore := metrics.PackageCacheHits.Value()

	// The policy tightened below the cached object's size: the cache hit is
	// governed by the same ceiling as a fresh fetch and releases no bytes.
	_, err := f.fetch(t, "ws_a", int64(len(payload)-1))
	var connectorErr *Error
	if !errors.As(err, &connectorErr) || connectorErr.Code != "response_too_large" {
		t.Fatalf("tightened rule did not constrain cached delivery: %v", err)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("size denial contacted the registry: calls=%d", f.calls.Load())
	}
	if metrics.PackageCacheHits.Value() != hitsBefore {
		t.Fatal("denied cached delivery was counted as a cache hit")
	}

	// A HEAD releases no body, so it mirrors the fresh HEAD path, which is
	// not governed by the byte ceiling either.
	head := packageRequest(t, "ws_a", "sha256:"+f.digest, packageRule(int64(len(payload)-1)))
	head.Method = http.MethodHead
	response, err := f.packages.Execute(context.Background(), head)
	if err != nil {
		t.Fatalf("cached HEAD under tightened rule: %v", err)
	}
	if body := readConnectorResponse(t, response); body != "" || !response.Provenance.Cached ||
		response.Header.Get("Content-Length") != strconv.Itoa(len(payload)) || f.calls.Load() != 1 {
		t.Fatalf("cached HEAD body=%q provenance=%+v header=%v calls=%d", body, response.Provenance, response.Header, f.calls.Load())
	}

	// Exactly at the ceiling, and with the rule delegating to the store's
	// object ceiling, the reference is reused without a registry request.
	for _, maximum := range []int64{int64(len(payload)), 0} {
		hit := f.mustFetchVerified(t, "ws_a", maximum)
		if !hit.Provenance.Cached || f.calls.Load() != 1 {
			t.Fatalf("max=%d cached=%v calls=%d", maximum, hit.Provenance.Cached, f.calls.Load())
		}
	}
}

// stagingUsage totals every retained staging file below the store root so the
// assertion observes physical bytes and inodes rather than in-memory counters.
func stagingUsage(t *testing.T, root string) (bytes, objects int64) {
	t.Helper()
	err := filepath.WalkDir(filepath.Join(root, "scopes"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Base(filepath.Dir(path)) != "tmp" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		objects++
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return bytes, objects
}

func TestStoreRestartReconcilesAbandonedStaging(t *testing.T) {
	root := t.TempDir()
	opts := StoreOptions{MaxBytes: 8, MaxBytesPerScope: 8, MaxObjectBytes: 8, MaxObjects: 1, MaxObjectsPerScope: 1}
	payload := "12345678"

	abandon := func(t *testing.T, store *Store) {
		t.Helper()
		reservation, staged, err := store.begin("tenant", "ws", 8)
		if err != nil {
			t.Fatalf("admission after reconciliation: %v", err)
		}
		if _, err := staged.Write([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		if err := staged.Close(); err != nil {
			t.Fatal(err)
		}
		if reservation == nil {
			t.Fatal("reservation missing")
		}
		// The process "crashes" here: no abort, commit, or detach runs.
	}

	store, err := NewStore(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	abandon(t, store)
	if bytes, objects := stagingUsage(t, root); bytes != 8 || objects != 1 {
		t.Fatalf("precondition: staged bytes=%d objects=%d", bytes, objects)
	}
	orphanMetadata := filepath.Join(store.scopeRoot(store.scope("tenant", "ws")), "refs", ".metadata-orphan")

	for restart := 1; restart <= 2; restart++ {
		if restart == 2 {
			// A crash between metadata staging and rename leaves this behind.
			if err := os.MkdirAll(filepath.Dir(orphanMetadata), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(orphanMetadata, []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		before := metrics.PackageStagingReclaimed.Value()
		store, err = NewStore(root, opts)
		if err != nil {
			t.Fatalf("restart %d: %v", restart, err)
		}
		abandon(t, store)
		bytes, objects := stagingUsage(t, root)
		if bytes > opts.MaxBytes || objects > opts.MaxObjects {
			t.Fatalf("restart %d: physical staging bytes=%d objects=%d exceed ceilings %d/%d", restart, bytes, objects, opts.MaxBytes, opts.MaxObjects)
		}
		if metrics.PackageStagingReclaimed.Value() == before {
			t.Fatalf("restart %d: reconciliation was not observable", restart)
		}
		if _, err := os.Stat(orphanMetadata); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart %d: orphan metadata retained: %v", restart, err)
		}
	}

	// After a final restart the workspace fetches successfully, the object is
	// committed under its digest, and the reference is reused.
	store, err = NewStore(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(payload))
	digest := hex.EncodeToString(sum[:])
	var calls atomic.Int64
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(payload)), ContentLength: 8,
		}, nil
	})})
	req := packageRequest(t, "ws", "sha256:"+digest, packageRule(8))
	req.Tenant = "tenant"
	for attempt := 0; attempt < 2; attempt++ {
		response, err := packages.Execute(context.Background(), req)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if body := readConnectorResponse(t, response); body != payload || response.Provenance.Cached != (attempt == 1) {
			t.Fatalf("attempt %d: body=%q cached=%v", attempt, body, response.Provenance.Cached)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("registry calls=%d", calls.Load())
	}
	if bytes, objects := stagingUsage(t, root); bytes != 0 || objects != 0 {
		t.Fatalf("committed fetch left staging bytes=%d objects=%d", bytes, objects)
	}
}

func TestStoreRestartFailsClosedOnUnexpectedStagingEntries(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(store.scopeRoot(store.scope("tenant", "ws")), "tmp")
	if err := os.MkdirAll(filepath.Join(tmpDir, "body-nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(root, StoreOptions{}); err == nil || !strings.Contains(err.Error(), "unexpected staging entry") {
		t.Fatalf("directory inside staging was not refused: %v", err)
	}
	if err := os.Remove(filepath.Join(tmpDir, "body-nested")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "unowned"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(root, StoreOptions{}); err == nil || !strings.Contains(err.Error(), "unexpected staging entry") {
		t.Fatalf("unowned staging entry was not refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "unowned")); err != nil {
		t.Fatalf("fail-closed reconciliation deleted an unowned entry: %v", err)
	}
}
