package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
)

func TestBlobStoreCRUDAndDigestVerification(t *testing.T) {
	fake, store := newFakeStore(t, Config{})
	var blobs artifact.BlobStore = store

	id, size, err := blobs.Put(strings.NewReader("hello immutable object"))
	if err != nil {
		t.Fatal(err)
	}
	if size != 22 {
		t.Fatalf("Put size = %d, want 22", size)
	}
	if got, err := blobs.Head(id); err != nil || got != size {
		t.Fatalf("Head = %d, %v; want %d", got, err, size)
	}
	reader, gotSize, err := blobs.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := blobs.Delete(id); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("Delete with reader = %v; want in-use error", err)
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if gotSize != size || string(payload) != "hello immutable object" {
		t.Fatalf("Open = %d bytes %q", gotSize, payload)
	}
	ids, err := blobs.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("List = %v, want [%s]", ids, id)
	}

	digest, _ := artifact.Digest(id)
	fake.mu.Lock()
	fake.objects["scope/blobs/"+digest].data[0] ^= 0xff
	fake.mu.Unlock()
	reader, _, err = blobs.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if !errors.Is(readErr, artifact.ErrDigestMismatch) {
		t.Fatalf("corrupt read = %v, want ErrDigestMismatch", readErr)
	}

	if err := blobs.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := blobs.Head(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Head deleted = %v, want os.ErrNotExist", err)
	}
	if _, err := blobs.Head("bad"); err == nil {
		t.Fatal("Head accepted a malformed artifact id")
	}
	if fake.unsigned != 0 {
		t.Fatalf("observed %d unsigned requests", fake.unsigned)
	}
}

func TestBlobPutIsIdempotentAndRejectsCorruptExistingObject(t *testing.T) {
	fake, store := newFakeStore(t, Config{})
	payload := []byte("same content")
	id, _, err := store.Put(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := store.Put(bytes.NewReader(payload)); err != nil || got != id {
		t.Fatalf("idempotent Put = %q, %v; want %q", got, err, id)
	}
	digest, _ := artifact.Digest(id)
	fake.mu.Lock()
	fake.objects["scope/blobs/"+digest].data = []byte("corrupt")
	fake.mu.Unlock()
	if _, _, err := store.Put(bytes.NewReader(payload)); !errors.Is(err, artifact.ErrDigestMismatch) {
		t.Fatalf("Put over corrupt existing object = %v, want digest mismatch", err)
	}
}

func TestMultipartStreamsPublishesAndAbortsFailure(t *testing.T) {
	fake, store := newFakeStore(t, Config{MultipartThreshold: minimumPartSize, PartSize: minimumPartSize, MaxObjectBytes: 32 << 20})
	payload := bytes.Repeat([]byte("abcdefgh"), (11<<20)/8)
	id, size, err := store.Put(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", size, len(payload))
	}
	if err := store.Verify(id); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	completed, copied, active, staging := fake.completed, fake.copied, len(fake.uploads), fake.countPrefix("scope/.staging/")
	fake.failPart = 2
	fake.mu.Unlock()
	if completed != 1 || copied != 1 || active != 0 || staging != 0 {
		t.Fatalf("multipart completed=%d copied=%d active=%d staging=%d", completed, copied, active, staging)
	}
	if _, _, err := store.Put(bytes.NewReader(append([]byte(nil), payload...))); err == nil {
		t.Fatal("multipart part failure was ignored")
	}
	fake.mu.Lock()
	aborted, active := fake.aborted, len(fake.uploads)
	fake.mu.Unlock()
	if aborted != 1 || active != 0 {
		t.Fatalf("failed multipart aborted=%d active=%d, want 1,0", aborted, active)
	}
}

func TestMultipartRejectsOversizeAndAborts(t *testing.T) {
	fake, store := newFakeStore(t, Config{MultipartThreshold: minimumPartSize, PartSize: minimumPartSize, MaxObjectBytes: 6 << 20})
	payload := bytes.Repeat([]byte{'x'}, 7<<20)
	_, got, err := store.Put(bytes.NewReader(payload))
	if !errors.Is(err, artifact.ErrTooLarge) {
		t.Fatalf("Put = %d, %v; want ErrTooLarge", got, err)
	}
	fake.mu.Lock()
	aborted, active := fake.aborted, len(fake.uploads)
	fake.mu.Unlock()
	if aborted != 1 || active != 0 {
		t.Fatalf("oversize multipart aborted=%d active=%d", aborted, active)
	}
}

func TestBlobUsesMultipartCopyPastSingleCopyLimit(t *testing.T) {
	fake, store := newFakeStore(t, Config{MultipartThreshold: minimumPartSize, PartSize: minimumPartSize, MaxObjectBytes: 32 << 20})
	store.copyLimit = 6 << 20
	payload := bytes.Repeat([]byte{'c'}, 11<<20)
	id, _, err := store.Put(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Verify(id); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	completed, active, staging := fake.completed, len(fake.uploads), fake.countPrefix("scope/.staging/")
	fake.mu.Unlock()
	if completed != 2 || active != 0 || staging != 0 {
		t.Fatalf("multipart-copy completed=%d active=%d staging=%d, want 2,0,0", completed, active, staging)
	}
}

func TestNamedObjectsConditionsInventoryAndCleanup(t *testing.T) {
	fake, store := newFakeStore(t, Config{StagingTTL: time.Hour})
	ctx := context.Background()
	if err := store.CheckConditionalWrites(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.PutObject(ctx, "wal/0001", strings.NewReader("entry"), 5, PutOptions{IfNoneMatch: "*", Metadata: map[string]string{"epoch": "7"}})
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag == "" {
		t.Fatal("PutObject returned no ETag")
	}
	if _, err := store.PutObject(ctx, "wal/0001", strings.NewReader("entry"), 5, PutOptions{IfNoneMatch: "*"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("If-None-Match = %v", err)
	}
	if _, err := store.PutObject(ctx, "wal/0001", strings.NewReader("newer"), 5, PutOptions{IfMatch: "wrong"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("wrong If-Match = %v", err)
	}
	if _, err := store.PutObject(ctx, "wal/0001", strings.NewReader("newer"), 5, PutOptions{IfMatch: info.ETag}); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-2 * time.Hour).UTC()
	fake.mu.Lock()
	fake.objects["scope/.staging/old"] = &fakeObject{data: []byte("old"), modified: old, metadata: map[string]string{}}
	fake.uploads["old-upload"] = &fakeUpload{key: "scope/.staging/partial", initiated: old, parts: make(map[int][]byte)}
	fake.mu.Unlock()
	inventory, err := store.Inventory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Objects != 1 || inventory.StagingObjects != 1 || inventory.MultipartUploads != 1 {
		t.Fatalf("Inventory = %+v", inventory)
	}
	if err := store.CleanupStaging(ctx); err != nil {
		t.Fatal(err)
	}
	inventory, err = store.Inventory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.StagingObjects != 0 || inventory.MultipartUploads != 0 {
		t.Fatalf("Inventory after cleanup = %+v", inventory)
	}
}

func TestConditionalCapabilityFailsClosed(t *testing.T) {
	fake, store := newFakeStore(t, Config{})
	fake.mu.Lock()
	fake.ignoreConditions = true
	fake.mu.Unlock()
	if err := store.CheckConditionalWrites(context.Background()); !errors.Is(err, ErrConditionalUnsupported) {
		t.Fatalf("CheckConditionalWrites = %v, want ErrConditionalUnsupported", err)
	}
}

func TestInventoryFollowsObjectAndMultipartPages(t *testing.T) {
	fake, store := newFakeStore(t, Config{})
	now := time.Now().UTC()
	fake.mu.Lock()
	fake.objectPageSize = 1
	fake.uploadPageSize = 1
	fake.objects["scope/one"] = newFakeObject([]byte("1"), nil)
	fake.objects["scope/two"] = newFakeObject([]byte("22"), nil)
	fake.uploads["u1"] = &fakeUpload{key: "scope/.staging/a", initiated: now, parts: make(map[int][]byte)}
	fake.uploads["u2"] = &fakeUpload{key: "scope/.staging/b", initiated: now, parts: make(map[int][]byte)}
	fake.mu.Unlock()
	inventory, err := store.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Objects != 2 || inventory.Bytes != 3 || inventory.MultipartUploads != 2 {
		t.Fatalf("paginated Inventory = %+v", inventory)
	}
}

func TestAddressingStylesAndEscaping(t *testing.T) {
	_, pathStore := newFakeStore(t, Config{})
	u := pathStore.requestURL("scope/a b/+", url.Values{"x y": {"a+b"}})
	if u.EscapedPath() != "/bucket/scope/a%20b/%2B" || u.RawQuery != "x%20y=a%2Bb" {
		t.Fatalf("path-style URL = %s", u.String())
	}
	virtual, err := New(Config{Endpoint: "https://objects.example.test/base", Region: "r1", Bucket: "bucket", Prefix: "scope", AccessKeyID: "access", SecretAccessKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	u = virtual.requestURL("scope/object", nil)
	if u.Host != "bucket.objects.example.test" || u.EscapedPath() != "/base/scope/object" {
		t.Fatalf("virtual-host URL = %s", u.String())
	}
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	var receivedSecretHeader bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSecretHeader = r.Header.Get("X-Amz-Security-Token") != ""
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	store, err := New(Config{Endpoint: redirect.URL, Bucket: "bucket", PathStyle: true, AccessKeyID: "access", SecretAccessKey: "secret", SessionToken: "sensitive-session-token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeadObject(context.Background(), "x"); err == nil {
		t.Fatal("redirect was treated as a successful S3 response")
	}
	if receivedSecretHeader {
		t.Fatal("redirect target received the session credential")
	}
}

func TestServiceErrorDoesNotExposeBodyOrCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>sensitive-session-token</Message></Error>`)
	}))
	defer server.Close()
	store, err := New(Config{Endpoint: server.URL, Bucket: "bucket", PathStyle: true, AccessKeyID: "access", SecretAccessKey: "secret", SessionToken: "sensitive-session-token"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.HeadObject(context.Background(), "x")
	if err == nil || strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe service error: %v", err)
	}
}

func TestMinIOIntegration(t *testing.T) {
	endpoint := os.Getenv("REMOUNT_S3_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("REMOUNT_S3_INTEGRATION_ENDPOINT is not set")
	}
	prefix := fmt.Sprintf("integration/%d", time.Now().UnixNano())
	store, err := New(Config{
		Endpoint: endpoint, Region: envOr("REMOUNT_S3_INTEGRATION_REGION", defaultRegion),
		Bucket: os.Getenv("REMOUNT_S3_INTEGRATION_BUCKET"), Prefix: prefix, PathStyle: true,
		AccessKeyID: os.Getenv("REMOUNT_S3_INTEGRATION_ACCESS_KEY"), SecretAccessKey: os.Getenv("REMOUNT_S3_INTEGRATION_SECRET_KEY"),
		MultipartThreshold: minimumPartSize, PartSize: minimumPartSize, MaxObjectBytes: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.copyLimit = 6 << 20
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if os.Getenv("REMOUNT_S3_INTEGRATION_CREATE_BUCKET") == "1" {
		resp, err := store.do(ctx, http.MethodPut, "", nil, nil, nil, 0, emptySHA256)
		if err != nil {
			t.Fatal(err)
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		objects, listErr := store.ListObjects(cleanupCtx, "")
		if listErr != nil {
			t.Errorf("list integration cleanup objects: %v", listErr)
			return
		}
		for _, object := range objects {
			if deleteErr := store.DeleteObject(cleanupCtx, object.Key); deleteErr != nil {
				t.Errorf("delete integration object: %v", deleteErr)
			}
		}
	})
	if _, err := store.PutObject(ctx, "probe/plain", strings.NewReader("ready"), 5, PutOptions{}); err != nil {
		t.Fatalf("plain named-object probe: %v", err)
	}
	if err := store.CheckConditionalWrites(ctx); err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte("minio-small"), bytes.Repeat([]byte("m"), 11<<20)} {
		id, _, err := store.Put(bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Verify(id); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("List returned %d blobs, want 2", len(ids))
	}
	inventory, err := store.Inventory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.StagingObjects != 0 || inventory.MultipartUploads != 0 {
		t.Fatalf("private uploads remain after success: %+v", inventory)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

type fakeS3 struct {
	mu               sync.Mutex
	objects          map[string]*fakeObject
	uploads          map[string]*fakeUpload
	nextUpload       int
	completed        int
	copied           int
	aborted          int
	failPart         int
	ignoreConditions bool
	unsigned         int
	objectPageSize   int
	uploadPageSize   int
}

type fakeObject struct {
	data     []byte
	etag     string
	modified time.Time
	metadata map[string]string
}

type fakeUpload struct {
	key       string
	initiated time.Time
	parts     map[int][]byte
	metadata  map[string]string
}

func newFakeStore(t *testing.T, overrides Config) (*fakeS3, *Store) {
	t.Helper()
	fake := &fakeS3{objects: make(map[string]*fakeObject), uploads: make(map[string]*fakeUpload)}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)
	cfg := Config{
		Endpoint: server.URL, Region: "test-1", Bucket: "bucket", Prefix: "scope", PathStyle: true,
		AccessKeyID: "access", SecretAccessKey: "secret", RequestTimeout: 10 * time.Second,
	}
	if overrides.MultipartThreshold != 0 {
		cfg.MultipartThreshold = overrides.MultipartThreshold
	}
	if overrides.PartSize != 0 {
		cfg.PartSize = overrides.PartSize
	}
	if overrides.MaxObjectBytes != 0 {
		cfg.MaxObjectBytes = overrides.MaxObjectBytes
	}
	if overrides.StagingTTL != 0 {
		cfg.StagingTTL = overrides.StagingTTL
	}
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return fake, store
}

func (f *fakeS3) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Authorization"), "Credential=access/") {
		f.mu.Lock()
		f.unsigned++
		f.mu.Unlock()
		fakeError(w, http.StatusForbidden, "AccessDenied")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.URL.Path == "/bucket" || r.URL.Path == "/bucket/" {
		key = ""
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Has("list-type"):
		f.listObjects(w, r)
	case r.Method == http.MethodGet && r.URL.Query().Has("uploads"):
		f.listUploads(w, r)
	case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
		f.initiate(w, key, r)
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "":
		f.part(w, key, r)
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
		f.complete(w, key, r)
	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") != "":
		f.abort(w, r)
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		f.copy(w, key, r)
	case r.Method == http.MethodPut:
		f.put(w, key, r)
	case r.Method == http.MethodGet:
		f.get(w, key, false)
	case r.Method == http.MethodHead:
		f.get(w, key, true)
	case r.Method == http.MethodDelete:
		f.delete(w, key)
	default:
		fakeError(w, http.StatusBadRequest, "Unsupported")
	}
}

func (f *fakeS3) put(w http.ResponseWriter, key string, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil || int64(len(data)) != r.ContentLength {
		fakeError(w, http.StatusBadRequest, "BadBody")
		return
	}
	if sum := sha256.Sum256(data); r.Header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(sum[:]) {
		fakeError(w, http.StatusBadRequest, "BadDigest")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.conditionOK(key, r) {
		fakeError(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	object := newFakeObject(data, r.Header)
	f.objects[key] = object
	w.Header().Set("ETag", `"`+object.etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, key string, head bool) {
	f.mu.Lock()
	object := f.objects[key]
	if object != nil {
		object = &fakeObject{data: append([]byte(nil), object.data...), etag: object.etag, modified: object.modified, metadata: cloneMap(object.metadata)}
	}
	f.mu.Unlock()
	if object == nil {
		fakeError(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(object.data)))
	w.Header().Set("ETag", `"`+object.etag+`"`)
	w.Header().Set("Last-Modified", object.modified.Format(http.TimeFormat))
	for key, value := range object.metadata {
		w.Header().Set("X-Amz-Meta-"+key, value)
	}
	w.WriteHeader(http.StatusOK)
	if !head {
		_, _ = w.Write(object.data)
	}
}

func (f *fakeS3) delete(w http.ResponseWriter, key string) {
	f.mu.Lock()
	delete(f.objects, key)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) initiate(w http.ResponseWriter, key string, r *http.Request) {
	f.mu.Lock()
	f.nextUpload++
	id := strconv.Itoa(f.nextUpload)
	f.uploads[id] = &fakeUpload{key: key, initiated: time.Now().UTC(), parts: make(map[int][]byte), metadata: metadataFromHeaders(r.Header)}
	f.mu.Unlock()
	writeXML(w, initiateMultipartResult{UploadID: id})
}

func (f *fakeS3) part(w http.ResponseWriter, key string, r *http.Request) {
	partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		f.partCopy(w, key, partNumber, r)
		return
	}
	data, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	upload := f.uploads[r.URL.Query().Get("uploadId")]
	if upload == nil || upload.key != key {
		fakeError(w, http.StatusNotFound, "NoSuchUpload")
		return
	}
	if f.failPart == partNumber {
		fakeError(w, http.StatusInternalServerError, "InjectedFailure")
		return
	}
	upload.parts[partNumber] = append([]byte(nil), data...)
	sum := md5.Sum(data)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) partCopy(w http.ResponseWriter, key string, partNumber int, r *http.Request) {
	source, _ := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/bucket/"))
	var first, last int64
	if _, err := fmt.Sscanf(r.Header.Get("X-Amz-Copy-Source-Range"), "bytes=%d-%d", &first, &last); err != nil {
		fakeError(w, http.StatusBadRequest, "InvalidRange")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	upload := f.uploads[r.URL.Query().Get("uploadId")]
	object := f.objects[source]
	if upload == nil || upload.key != key || object == nil || first < 0 || last < first || last >= int64(len(object.data)) {
		fakeError(w, http.StatusBadRequest, "InvalidCopyPart")
		return
	}
	payload := append([]byte(nil), object.data[first:last+1]...)
	upload.parts[partNumber] = payload
	sum := md5.Sum(payload)
	writeXML(w, completeMultipartResult{ETag: `"` + hex.EncodeToString(sum[:]) + `"`})
}

func (f *fakeS3) complete(w http.ResponseWriter, key string, r *http.Request) {
	var manifest completeMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&manifest); err != nil {
		fakeError(w, http.StatusBadRequest, "BadManifest")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.conditionOK(key, r) {
		fakeError(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	id := r.URL.Query().Get("uploadId")
	upload := f.uploads[id]
	if upload == nil || upload.key != key {
		fakeError(w, http.StatusNotFound, "NoSuchUpload")
		return
	}
	var data []byte
	for _, part := range manifest.Parts {
		data = append(data, upload.parts[part.PartNumber]...)
	}
	object := newFakeObject(data, nil)
	object.metadata = cloneMap(upload.metadata)
	f.objects[key] = object
	delete(f.uploads, id)
	f.completed++
	writeXML(w, completeMultipartResult{ETag: `"` + object.etag + `"`})
}

func (f *fakeS3) abort(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	if _, ok := f.uploads[r.URL.Query().Get("uploadId")]; ok {
		delete(f.uploads, r.URL.Query().Get("uploadId"))
		f.aborted++
	}
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) copy(w http.ResponseWriter, key string, r *http.Request) {
	source, _ := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/bucket/"))
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.conditionOK(key, r) {
		fakeError(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	object := f.objects[source]
	if object == nil {
		fakeError(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	copy := newFakeObject(object.data, r.Header)
	f.objects[key] = copy
	f.copied++
	writeXML(w, completeMultipartResult{ETag: `"` + copy.etag + `"`})
}

func (f *fakeS3) listObjects(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	start, _ := strconv.Atoi(r.URL.Query().Get("continuation-token"))
	end := len(keys)
	if f.objectPageSize > 0 && start+f.objectPageSize < end {
		end = start + f.objectPageSize
	}
	var result listObjectsResult
	for _, key := range keys[start:end] {
		object := f.objects[key]
		result.Contents = append(result.Contents, struct {
			Key          string    `xml:"Key"`
			Size         int64     `xml:"Size"`
			ETag         string    `xml:"ETag"`
			LastModified time.Time `xml:"LastModified"`
		}{Key: key, Size: int64(len(object.data)), ETag: `"` + object.etag + `"`, LastModified: object.modified})
	}
	if end < len(keys) {
		result.IsTruncated = true
		result.NextContinuationToken = strconv.Itoa(end)
	}
	f.mu.Unlock()
	writeXML(w, result)
}

func (f *fakeS3) listUploads(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	f.mu.Lock()
	var result listMultipartUploadsResult
	var uploads []multipartUpload
	for id, upload := range f.uploads {
		if strings.HasPrefix(upload.key, prefix) {
			uploads = append(uploads, multipartUpload{Key: upload.key, UploadID: id, Initiated: upload.initiated})
		}
	}
	sort.Slice(uploads, func(i, j int) bool {
		return uploads[i].Key < uploads[j].Key || (uploads[i].Key == uploads[j].Key && uploads[i].UploadID < uploads[j].UploadID)
	})
	start := 0
	keyMarker := r.URL.Query().Get("key-marker")
	uploadMarker := r.URL.Query().Get("upload-id-marker")
	if keyMarker != "" {
		for start < len(uploads) && (uploads[start].Key < keyMarker || (uploads[start].Key == keyMarker && uploads[start].UploadID <= uploadMarker)) {
			start++
		}
	}
	end := len(uploads)
	if f.uploadPageSize > 0 && start+f.uploadPageSize < end {
		end = start + f.uploadPageSize
	}
	result.Uploads = append(result.Uploads, uploads[start:end]...)
	if end < len(uploads) {
		result.IsTruncated = true
		result.NextKeyMarker = uploads[end-1].Key
		result.NextUploadIDMarker = uploads[end-1].UploadID
	}
	f.mu.Unlock()
	writeXML(w, result)
}

func (f *fakeS3) conditionOK(key string, r *http.Request) bool {
	if f.ignoreConditions {
		return true
	}
	object := f.objects[key]
	if r.Header.Get("If-None-Match") == "*" && object != nil {
		return false
	}
	if match := trimETag(r.Header.Get("If-Match")); match != "" && (object == nil || object.etag != match) {
		return false
	}
	return true
}

func (f *fakeS3) countPrefix(prefix string) int {
	count := 0
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func newFakeObject(data []byte, headers http.Header) *fakeObject {
	sum := md5.Sum(data)
	return &fakeObject{data: append([]byte(nil), data...), etag: hex.EncodeToString(sum[:]), modified: time.Now().UTC(), metadata: metadataFromHeaders(headers)}
}

func metadataFromHeaders(headers http.Header) map[string]string {
	metadata := make(map[string]string)
	for name, values := range headers {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") && len(values) != 0 {
			metadata[strings.TrimPrefix(strings.ToLower(name), "x-amz-meta-")] = values[0]
		}
	}
	return metadata
}

func fakeError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	writeXML(w, struct {
		Code string `xml:"Code"`
	}{Code: code})
}

func writeXML(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(value)
}
