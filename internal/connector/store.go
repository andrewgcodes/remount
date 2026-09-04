package connector

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/metrics"
)

const (
	defaultMaxCacheBytes   int64 = 16 << 30
	defaultMaxScopeBytes   int64 = 2 << 30
	defaultMaxObjectBytes  int64 = 512 << 20
	defaultMaxObjects      int64 = 100_000
	defaultMaxScopeObjects int64 = 4_096
	connectorKeyBytes            = 32

	stagingBodyPrefix     = "body-"
	stagingMetadataPrefix = ".metadata-"
)

// errCachedObjectTooLarge reports a verified-scope reference whose object is
// larger than the ceiling the current rule permits. No bytes were released.
var errCachedObjectTooLarge = errors.New("connector cache object exceeds the current response limit")

// StoreOptions bound both physical shared storage and the logical amount one
// workspace may pin. Limits are enforced before an upstream body is staged.
type StoreOptions struct {
	MaxBytes           int64
	MaxBytesPerScope   int64
	MaxObjectBytes     int64
	MaxObjects         int64
	MaxObjectsPerScope int64
}

// Store owns an immutable content-addressed blob directory and opaque,
// workspace-scoped reference directories. Callers share Store, never a scope.
type Store struct {
	root               string
	key                []byte
	maxBytes           int64
	maxBytesPerScope   int64
	maxObjectBytes     int64
	maxObjects         int64
	maxObjectsPerScope int64

	mu              sync.Mutex
	reserved        int64
	reservedObjects int64
	byScope         map[string]int64
	objectsByScope  map[string]int64
}

type cacheReference struct {
	Digest     string              `json:"digest"`
	Size       int64               `json:"size"`
	StatusCode int                 `json:"status"`
	Header     map[string][]string `json:"header,omitempty"`
	StoredAt   int64               `json:"stored_at"`
}

type reservation struct {
	store    *Store
	scope    string
	reserved int64
	path     string
	released bool
}

// NewStore opens a connector cache. Node-private metadata is mode 0700/0600;
// immutable blobs are mode 0444 and named only by their SHA-256 digest.
func NewStore(root string, opts StoreOptions) (*Store, error) {
	if root == "" {
		return nil, errors.New("connector store: root is required")
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = defaultMaxCacheBytes
	}
	if opts.MaxBytesPerScope <= 0 {
		opts.MaxBytesPerScope = min(defaultMaxScopeBytes, opts.MaxBytes)
	}
	if opts.MaxObjectBytes <= 0 {
		opts.MaxObjectBytes = min(defaultMaxObjectBytes, opts.MaxBytesPerScope)
	}
	if opts.MaxObjects <= 0 {
		opts.MaxObjects = defaultMaxObjects
	}
	if opts.MaxObjectsPerScope <= 0 {
		opts.MaxObjectsPerScope = min(defaultMaxScopeObjects, opts.MaxObjects)
	}
	if opts.MaxObjectBytes > opts.MaxBytesPerScope || opts.MaxBytesPerScope > opts.MaxBytes {
		return nil, errors.New("connector store: require object <= workspace <= total byte limits")
	}
	if opts.MaxObjectsPerScope > opts.MaxObjects {
		return nil, errors.New("connector store: require workspace objects <= total objects")
	}
	for _, dir := range []string{root, filepath.Join(root, "blobs", "sha256"), filepath.Join(root, "scopes")} {
		if err := ensurePrivateDir(dir); err != nil {
			return nil, fmt.Errorf("connector store: create directory: %w", err)
		}
	}
	key, err := loadOrCreateKey(filepath.Join(root, "scope.key"))
	if err != nil {
		return nil, err
	}
	if err := reconcileStaging(filepath.Join(root, "scopes")); err != nil {
		return nil, err
	}
	return &Store{
		root: root, key: key, maxBytes: opts.MaxBytes,
		maxBytesPerScope: opts.MaxBytesPerScope, maxObjectBytes: opts.MaxObjectBytes,
		maxObjects: opts.MaxObjects, maxObjectsPerScope: opts.MaxObjectsPerScope,
		byScope: map[string]int64{}, objectsByScope: map[string]int64{},
	}, nil
}

// reconcileStaging removes staging files a previous process abandoned before
// commit or abort. In-memory reservations do not survive restart, so any file
// left in a scope's tmp directory would otherwise occupy bytes and inodes that
// no admission decision accounts for. Only regular files carrying the store's
// own staging prefixes are removed; anything else in a tmp directory fails
// closed because that directory belongs entirely to this store.
func reconcileStaging(scopesDir string) error {
	scopes, err := os.ReadDir(scopesDir)
	if err != nil {
		return fmt.Errorf("connector store: read scopes: %w", err)
	}
	for _, scope := range scopes {
		if !scope.IsDir() {
			continue
		}
		scopeRoot := filepath.Join(scopesDir, scope.Name())
		if err := removeOwnedStaging(filepath.Join(scopeRoot, "tmp"), stagingBodyPrefix, true); err != nil {
			return err
		}
		if err := removeOwnedStaging(filepath.Join(scopeRoot, "refs"), stagingMetadataPrefix, false); err != nil {
			return err
		}
	}
	return nil
}

func removeOwnedStaging(dir, prefix string, exclusive bool) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("connector store: read staging: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !entry.Type().IsRegular() {
			if exclusive {
				return fmt.Errorf("connector store: unexpected staging entry %q", filepath.Join(dir, name))
			}
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("connector store: remove abandoned staging: %w", err)
		}
		metrics.PackageStagingReclaimed.Inc()
	}
	return nil
}

func loadOrCreateKey(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		key := make([]byte, connectorKeyBytes)
		if _, err := rand.Read(key); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("connector store: generate scope key: %w", err)
		}
		if _, err := f.Write(key); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("connector store: write scope key: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("connector store: sync scope key: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("connector store: close scope key: %w", err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("connector store: create scope key: %w", err)
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("connector store: read scope key: %w", err)
	}
	if len(key) != connectorKeyBytes {
		return nil, errors.New("connector store: corrupt scope key")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("connector store: protect scope key: %w", err)
	}
	return key, nil
}

func (s *Store) opaque(parts ...string) string {
	h := hmac.New(sha256.New, s.key)
	for _, part := range parts {
		_, _ = io.WriteString(h, part)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Store) scope(tenant, workspace string) string {
	return s.opaque("scope", tenant, workspace)
}

func normalizeDigest(raw string) (string, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.TrimPrefix(raw, "sha256:")
	if len(raw) != sha256.Size*2 {
		return "", errors.New("expected digest must be sha256:<64 lowercase hex characters>")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.New("expected digest must be sha256:<64 lowercase hex characters>")
	}
	return raw, nil
}

func (s *Store) blobPath(digest string) string {
	return filepath.Join(s.root, "blobs", "sha256", digest[:2], digest)
}

func (s *Store) scopeRoot(scope string) string { return filepath.Join(s.root, "scopes", scope) }

func (s *Store) refPath(scope, digest string) string {
	return filepath.Join(s.scopeRoot(scope), "refs", digest+".json")
}

// lookup resolves the scope's own reference for digest. A hit is returned
// only after the blob's bytes have been hashed and found to match digest, so
// the returned body is verified content, not a file that merely has the right
// name and size. maxBytes > 0 is the ceiling the current rule permits for a
// delivered body; a verified object larger than that returns
// errCachedObjectTooLarge. The ceiling is applied to the verified size, never
// to reference metadata alone, so corrupt metadata cannot pin a policy
// rejection onto an object that fits. Any other error means the reference
// existed but could not be verified: it has been invalidated and the caller
// should fetch afresh.
func (s *Store) lookup(tenant, workspace, digest string, maxBytes int64) (ConnectorResponse, bool, error) {
	scope := s.scope(tenant, workspace)
	s.mu.Lock()
	data, err := os.ReadFile(s.refPath(scope, digest))
	s.mu.Unlock()
	if errors.Is(err, os.ErrNotExist) {
		return ConnectorResponse{}, false, nil
	}
	if err != nil {
		s.invalidate(scope, digest, nil)
		return ConnectorResponse{}, false, fmt.Errorf("connector cache metadata unavailable: %w", err)
	}
	var ref cacheReference
	if err := json.Unmarshal(data, &ref); err != nil || ref.Digest != digest || ref.Size < 0 || ref.Size > s.maxObjectBytes {
		s.invalidate(scope, digest, nil)
		return ConnectorResponse{}, false, errors.New("connector cache metadata is corrupt")
	}
	f, err := os.Open(s.blobPath(digest))
	if err != nil {
		s.invalidate(scope, digest, nil)
		return ConnectorResponse{}, false, errors.New("connector cache content is unavailable")
	}
	info, err := s.verifyBlob(f, digest)
	if err != nil {
		_ = f.Close()
		s.invalidate(scope, digest, info)
		return ConnectorResponse{}, false, err
	}
	if info.Size() != ref.Size {
		_ = f.Close()
		s.invalidate(scope, digest, nil)
		return ConnectorResponse{}, false, errors.New("connector cache metadata disagrees with verified content")
	}
	if maxBytes > 0 && info.Size() > maxBytes {
		_ = f.Close()
		return ConnectorResponse{}, false, errCachedObjectTooLarge
	}
	return ConnectorResponse{
		StatusCode: ref.StatusCode, Header: cloneHeader(ref.Header), Body: f,
		ContentLength: ref.Size,
	}, true, nil
}

// verifyBlob hashes the open file and reports whether it is a regular file no
// larger than the object ceiling whose bytes hash to digest. On success the
// file is rewound to offset 0. Blobs are only ever linked and removed, never
// written in place, so hashing does not need the store lock.
func (s *Store) verifyBlob(f *os.File, digest string) (os.FileInfo, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("connector cache content unavailable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return info, errors.New("connector cache content is not a regular file")
	}
	if info.Size() > s.maxObjectBytes {
		return info, errors.New("connector cache content exceeds the object ceiling")
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(f, info.Size()+1))
	if err != nil {
		return info, fmt.Errorf("connector cache content unreadable: %w", err)
	}
	if n != info.Size() {
		return info, errors.New("connector cache content changed during verification")
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return info, errors.New("connector cache content does not match its digest")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return info, fmt.Errorf("connector cache content unavailable: %w", err)
	}
	return info, nil
}

// invalidate withdraws the scope's reference so the next request fetches
// afresh. When failed names the blob that failed verification, that blob is
// discarded too, but only while the digest path still refers to the very file
// that was examined: a concurrent commit may already have replaced it with
// verified bytes.
func (s *Store) invalidate(scope, digest string, failed os.FileInfo) {
	metrics.PackageCacheIntegrityFailures.Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.Remove(s.refPath(scope, digest))
	if failed != nil {
		s.discardBlobLocked(digest, failed)
	}
}

func (s *Store) discardBlobLocked(digest string, failed os.FileInfo) {
	path := s.blobPath(digest)
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(current, failed) {
		return
	}
	_ = os.Remove(path)
}

func (s *Store) begin(tenant, workspace string, maximum int64) (*reservation, *os.File, error) {
	if maximum <= 0 || maximum > s.maxObjectBytes {
		maximum = s.maxObjectBytes
	}
	scope := s.scope(tenant, workspace)
	s.mu.Lock()
	logical, logicalObjects, err := s.scopeUsageLocked(scope)
	if err == nil && exceeds(s.maxBytesPerScope, logical, s.byScope[scope], maximum) {
		err = errors.New("package cache workspace quota exhausted")
	}
	if err == nil && exceeds(s.maxObjectsPerScope, logicalObjects, s.objectsByScope[scope], 1) {
		err = errors.New("package cache workspace object quota exhausted")
	}
	physical, physicalObjects := int64(0), int64(0)
	if err == nil {
		physical, physicalObjects, err = s.blobUsageLocked()
	}
	if err == nil && exceeds(s.maxBytes, physical, s.reserved, maximum) {
		err = errors.New("package cache node quota exhausted")
	}
	if err == nil && exceeds(s.maxObjects, physicalObjects, s.reservedObjects, 1) {
		err = errors.New("package cache node object quota exhausted")
	}
	if err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	s.reserved += maximum
	s.reservedObjects++
	s.byScope[scope] += maximum
	s.objectsByScope[scope]++
	s.mu.Unlock()

	tmpDir := filepath.Join(s.scopeRoot(scope), "tmp")
	if err := ensurePrivateDir(tmpDir); err != nil {
		s.release(scope, maximum)
		return nil, nil, fmt.Errorf("connector cache staging unavailable: %w", err)
	}
	f, err := os.CreateTemp(tmpDir, stagingBodyPrefix+"*")
	if err != nil {
		s.release(scope, maximum)
		return nil, nil, fmt.Errorf("connector cache staging unavailable: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		s.release(scope, maximum)
		return nil, nil, fmt.Errorf("connector cache staging unavailable: %w", err)
	}
	return &reservation{store: s, scope: scope, reserved: maximum, path: f.Name()}, f, nil
}

func exceeds(limit int64, values ...int64) bool {
	remaining := limit
	for _, value := range values {
		if value < 0 || value > remaining {
			return true
		}
		remaining -= value
	}
	return false
}

func (s *Store) release(scope string, reserved int64) {
	s.mu.Lock()
	s.reserved -= reserved
	s.reservedObjects--
	s.byScope[scope] -= reserved
	s.objectsByScope[scope]--
	if s.byScope[scope] == 0 {
		delete(s.byScope, scope)
	}
	if s.objectsByScope[scope] == 0 {
		delete(s.objectsByScope, scope)
	}
	s.mu.Unlock()
}

func (r *reservation) abort() {
	if r == nil || r.released {
		return
	}
	r.released = true
	_ = os.Remove(r.path)
	r.store.release(r.scope, r.reserved)
}

func (r *reservation) detachTemporary() (io.ReadCloser, error) {
	if r == nil || r.released {
		return nil, errors.New("connector cache reservation is closed")
	}
	f, err := os.Open(r.path)
	if err != nil {
		_ = os.Remove(r.path)
		r.released = true
		r.store.release(r.scope, r.reserved)
		return nil, err
	}
	r.released = true
	return &removingFile{
		File: f, path: r.path,
		release: func() { r.store.release(r.scope, r.reserved) },
	}, nil
}

func (r *reservation) commit(digest string, size int64, status int, header httpHeader) (string, error) {
	if r == nil || r.released {
		return "", errors.New("connector cache reservation is closed")
	}
	defer func() {
		r.released = true
		r.store.release(r.scope, r.reserved)
	}()
	if size < 0 || size > r.reserved {
		_ = os.Remove(r.path)
		return "", errors.New("connector cache object exceeded its reservation")
	}
	s := r.store
	blob := s.blobPath(digest)
	if err := s.publishBlob(r.path, blob, digest); err != nil {
		_ = os.Remove(r.path)
		return "", err
	}
	_ = os.Remove(r.path)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Chmod(blob, 0o444); err != nil {
		return "", fmt.Errorf("connector cache protect blob: %w", err)
	}
	ref := cacheReference{Digest: digest, Size: size, StatusCode: status, Header: cloneHeader(header), StoredAt: time.Now().UnixMilli()}
	if err := writeJSONAtomic(s.refPath(r.scope, digest), ref); err != nil {
		return "", err
	}
	return blob, nil
}

// publishBlob links the verified staged file at the digest path. When a blob
// already exists there it is reused only after its own bytes hash to digest;
// an existing file that fails verification is discarded and replaced by the
// staged bytes, so a same-named object that does not match its digest can
// never be reached through a committed reference. Hashing runs outside the
// store lock, so the replacement is guarded by an identity check on the file
// that was examined and by a single retry.
func (s *Store) publishBlob(staged, blob, digest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ensurePrivateDir(filepath.Dir(blob)); err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		err := os.Link(staged, blob)
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("connector cache commit: %w", err)
		}
		existing, openErr := os.Open(blob)
		var failed os.FileInfo
		verifyErr := openErr
		if openErr == nil {
			s.mu.Unlock()
			failed, verifyErr = s.verifyBlob(existing, digest)
			_ = existing.Close()
			s.mu.Lock()
		}
		if verifyErr == nil {
			return nil
		}
		if attempt > 0 {
			return fmt.Errorf("connector cache commit could not replace unverified blob: %w", verifyErr)
		}
		if failed != nil {
			s.discardBlobLocked(digest, failed)
		}
	}
}

// httpHeader avoids importing net/http in the storage layer while retaining
// its map representation.
type httpHeader map[string][]string

func cloneHeader[V ~map[string][]string](in V) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func writeJSONAtomic(path string, value any) error {
	if err := ensurePrivateDir(filepath.Dir(path)); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".metadata-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

func (s *Store) scopeUsageLocked(scope string) (int64, int64, error) {
	entries, err := os.ReadDir(filepath.Join(s.scopeRoot(scope), "refs"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.scopeRoot(scope), "refs", entry.Name()))
		if err != nil {
			return 0, 0, err
		}
		var ref cacheReference
		if err := json.Unmarshal(data, &ref); err != nil || ref.Size < 0 {
			return 0, 0, errors.New("connector cache reference is corrupt")
		}
		if total > s.maxBytesPerScope-ref.Size {
			return 0, 0, errors.New("connector cache workspace quota exhausted")
		}
		total += ref.Size
	}
	return total, int64(len(entries)), nil
}

func (s *Store) blobUsageLocked() (int64, int64, error) {
	var total, objects int64
	err := filepath.WalkDir(filepath.Join(s.root, "blobs", "sha256"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || total > s.maxBytes-info.Size() {
			return errors.New("connector cache node quota exhausted")
		}
		total += info.Size()
		objects++
		return nil
	})
	return total, objects, err
}

type removingFile struct {
	*os.File
	path    string
	release func()
	once    sync.Once
}

func (f *removingFile) Close() error {
	err := f.File.Close()
	removeErr := os.Remove(f.path)
	f.once.Do(func() {
		if f.release != nil {
			f.release()
		}
	})
	if err != nil {
		return err
	}
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	return nil
}
