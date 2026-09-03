// Package encrypted stores tenant-scoped artifacts encrypted at rest while
// preserving the plaintext content-addressed artifact id.
package encrypted

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"remount.dev/remount/internal/artifact"
)

var (
	// ErrObjectExists is returned by KeyedStore.Create when the key is already
	// present. Create must never replace the existing object.
	ErrObjectExists = errors.New("encrypted artifact: object already exists")
	// ErrTooLarge means one plaintext artifact exceeded its configured bound.
	ErrTooLarge = errors.New("encrypted artifact: plaintext exceeds limit")
	// ErrStagingFull means concurrent plaintext staging exhausted its byte or
	// writer budget.
	ErrStagingFull = errors.New("encrypted artifact: staging capacity exhausted")
	// ErrPhysicalStoreFull means retained ciphertext plus in-flight object
	// reservations exhausted the local keyed store's configured capacity.
	ErrPhysicalStoreFull = errors.New("encrypted artifact: physical store capacity exhausted")
	// ErrMalformed means an encrypted object or key record is structurally
	// invalid. Callers must treat it as corruption, never as absence.
	ErrMalformed = errors.New("encrypted artifact: malformed metadata")
	// ErrIntegrity means authenticated decryption or the final plaintext digest
	// failed. The error deliberately contains no key or plaintext material.
	ErrIntegrity = errors.New("encrypted artifact: integrity check failed")
	// ErrKeyUnavailable means required tenant key material is missing.
	ErrKeyUnavailable = errors.New("encrypted artifact: key unavailable")
)

// KeyedStore is the physical-object boundary beneath encryption. Create must
// atomically publish the complete size-byte body only when key is absent. A
// short body, long body, cancellation, or reader error must leave no visible
// object. Unknown keys return errors compatible with fs.ErrNotExist.
type KeyedStore interface {
	Create(ctx context.Context, key string, body io.Reader, size int64) error
	Open(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Head(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// KeyVersion is a tenant encryption-key version. A retired version remains
// readable until every object using it has been migrated and durably verified.
type KeyVersion struct {
	Version     string
	Retired     bool
	MasterKeyID string
}

// KeyProvider returns tenant encryption keys. Implementations must return a
// distinct 32-byte value per tenant and version. The returned array is a copy;
// callers erase it after constructing the required cipher.
type KeyProvider interface {
	Current(ctx context.Context, tenant string) (version string, key [32]byte, err error)
	Get(ctx context.Context, tenant, version string) (key [32]byte, err error)
	Versions(ctx context.Context, tenant string) ([]KeyVersion, error)
}

// Options bounds plaintext staging. Zero fields receive finite defaults.
type Options struct {
	ChunkSize           int
	MaxPlaintextBytes   int64
	MaxStagingBytes     int64
	MaxConcurrentWrites int
}

// Stats describes current plaintext staging use. Durable ciphertext capacity
// is owned and reported by the KeyedStore implementation.
type Stats struct {
	StagingBytes        int64
	ConcurrentWrites    int
	MaxStagingBytes     int64
	MaxConcurrentWrites int
	MaxPlaintextBytes   int64
}

// ObjectInfo is one physical encrypted representation. KeyAvailable is false
// when ciphertext remains but its tenant-key metadata cannot be found.
type ObjectInfo struct {
	ID              string
	KeyVersion      string
	Retired         bool
	KeyAvailable    bool
	MasterKeyID     string
	CiphertextBytes int64
}

// Store encrypts artifacts into a tenant- and key-version-scoped KeyedStore.
// Plaintext exists on disk only in its private, bounded staging directory and
// is removed before Put returns.
type Store struct {
	objects KeyedStore
	keys    KeyProvider
	stage   string
	opts    Options

	mu            sync.Mutex
	stagingBytes  int64
	activeWriters int
}

const (
	defaultChunkSize           = 1 << 20
	defaultMaxPlaintextBytes   = int64(8 << 30)
	defaultMaxStagingBytes     = int64(8 << 30)
	defaultMaxConcurrentWrites = 4
)

// NewStore constructs an encrypted store and removes private staging files
// left by a crashed prior process. Unexpected entries fail closed.
func NewStore(objects KeyedStore, keys KeyProvider, stagingDir string, opts Options) (*Store, error) {
	if objects == nil || keys == nil {
		return nil, errors.New("encrypted artifact: object store and key provider are required")
	}
	if opts.ChunkSize < 0 || opts.MaxPlaintextBytes < 0 || opts.MaxStagingBytes < 0 || opts.MaxConcurrentWrites < 0 {
		return nil, errors.New("encrypted artifact: limits must not be negative")
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	if opts.MaxPlaintextBytes == 0 {
		opts.MaxPlaintextBytes = defaultMaxPlaintextBytes
	}
	if opts.MaxStagingBytes == 0 {
		opts.MaxStagingBytes = defaultMaxStagingBytes
	}
	if opts.MaxConcurrentWrites == 0 {
		opts.MaxConcurrentWrites = defaultMaxConcurrentWrites
	}
	if opts.ChunkSize > maxChunkSize {
		return nil, fmt.Errorf("encrypted artifact: chunk size exceeds %d bytes", maxChunkSize)
	}
	if stagingDir == "" {
		return nil, errors.New("encrypted artifact: staging directory is required")
	}
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return nil, err
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".encrypt-") {
			return nil, fmt.Errorf("encrypted artifact: unexpected staging entry %q", entry.Name())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("encrypted artifact: staging entry %q is a symlink", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("encrypted artifact: staging entry %q is not a regular file", entry.Name())
		}
		if err := os.Remove(filepath.Join(stagingDir, entry.Name())); err != nil {
			return nil, err
		}
		removed = true
	}
	if removed {
		if err := syncDirectory(stagingDir); err != nil {
			return nil, err
		}
	}
	return &Store{objects: objects, keys: keys, stage: stagingDir, opts: opts}, nil
}

// Stats returns a point-in-time view of private staging reservations.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		StagingBytes: s.stagingBytes, ConcurrentWrites: s.activeWriters,
		MaxStagingBytes:     s.opts.MaxStagingBytes,
		MaxConcurrentWrites: s.opts.MaxConcurrentWrites,
		MaxPlaintextBytes:   s.opts.MaxPlaintextBytes,
	}
}

// ObjectKey returns the physical key for one tenant, key version, and logical
// artifact id. Segments are deliberately restricted instead of escaped so two
// spellings can never identify the same authority domain.
func ObjectKey(tenant, keyVersion, id string) (string, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return "", err
	}
	if err := validateSegment("key version", keyVersion); err != nil {
		return "", err
	}
	if _, err := artifact.Digest(id); err != nil {
		return "", err
	}
	return "tenants/" + tenant + "/" + keyVersion + "/" + id, nil
}

// Put stores plaintext for tenant and returns its plaintext content id.
func (s *Store) Put(ctx context.Context, tenant string, r io.Reader) (string, int64, error) {
	return s.put(ctx, tenant, "", r)
}

// PutExpected stores plaintext only when it hashes to expected.
func (s *Store) PutExpected(ctx context.Context, tenant, expected string, r io.Reader) (int64, error) {
	if _, err := artifact.Digest(expected); err != nil {
		return 0, err
	}
	_, n, err := s.put(ctx, tenant, expected, r)
	return n, err
}

func (s *Store) put(ctx context.Context, tenant, expected string, r io.Reader) (string, int64, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return "", 0, err
	}
	reservation, err := s.reserveWriter()
	if err != nil {
		return "", 0, err
	}
	defer reservation.release()

	tmp, err := os.CreateTemp(s.stage, ".encrypt-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	w := io.MultiWriter(tmp, h)
	n, err := copyPlaintext(ctx, w, r, reservation, s.opts.MaxPlaintextBytes)
	if err != nil {
		_ = tmp.Close()
		return "", n, err
	}
	id := artifact.ID(h.Sum(nil))
	if expected != "" && id != expected {
		_ = tmp.Close()
		return "", n, fmt.Errorf("%w", artifact.ErrDigestMismatch)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return "", n, err
	}
	version, tenantKey, err := s.keys.Current(ctx, tenant)
	if err != nil {
		_ = tmp.Close()
		return "", n, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	defer erase32(&tenantKey)
	key, err := ObjectKey(tenant, version, id)
	if err != nil {
		_ = tmp.Close()
		return "", n, err
	}
	if _, err := s.objects.Head(ctx, key); err == nil {
		_ = tmp.Close()
		if err := s.Verify(ctx, tenant, id); err != nil {
			return "", n, err
		}
		return id, n, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = tmp.Close()
		return "", n, err
	}
	body, encryptedSize, err := newEncryptReader(tmp, tenant, version, id, n, s.opts.ChunkSize, tenantKey)
	if err != nil {
		_ = tmp.Close()
		return "", n, err
	}
	err = s.objects.Create(ctx, key, body, encryptedSize)
	closeErr := tmp.Close()
	created := err == nil
	if errors.Is(err, ErrObjectExists) {
		if verifyErr := s.Verify(ctx, tenant, id); verifyErr != nil {
			return "", n, verifyErr
		}
		err = nil
	}
	if err != nil {
		return "", n, err
	}
	if closeErr != nil {
		return "", n, closeErr
	}
	if created {
		if err := s.Verify(ctx, tenant, id); err != nil {
			_ = s.objects.Delete(context.WithoutCancel(ctx), key)
			return "", n, err
		}
	}
	return id, n, nil
}

// Open returns verified plaintext. Reading to EOF validates the logical digest;
// Close drains unread plaintext and returns any integrity failure.
func (s *Store) Open(ctx context.Context, tenant, id string) (io.ReadCloser, int64, error) {
	key, version, err := s.locate(ctx, tenant, id)
	if err != nil {
		return nil, 0, err
	}
	return s.openVersion(ctx, tenant, version, id, key)
}

func (s *Store) openVersion(ctx context.Context, tenant, version, id, key string) (io.ReadCloser, int64, error) {
	raw, size, err := s.objects.Open(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	r, plaintextSize, err := newDecryptReader(ctx, raw, size, tenant, version, id, s.keys)
	if err != nil {
		_ = raw.Close()
		return nil, 0, err
	}
	return r, plaintextSize, nil
}

// Head returns the authenticated-header plaintext size without decrypting the
// body. Verify must be used when ciphertext integrity is required.
func (s *Store) Head(ctx context.Context, tenant, id string) (int64, error) {
	key, version, err := s.locate(ctx, tenant, id)
	if err != nil {
		return 0, err
	}
	r, size, err := s.objects.Open(ctx, key)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	header, _, err := readHeader(r, size, tenant, version, id)
	if err != nil {
		return 0, err
	}
	tenantKey, err := s.keys.Get(ctx, tenant, version)
	if err != nil {
		return 0, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	dataKey, err := unwrapDataKey(header, tenant, version, id, tenantKey)
	erase32(&tenantKey)
	erase32(&dataKey)
	if err != nil {
		return 0, err
	}
	return int64(header.PlaintextSize), nil
}

// RewrapResult describes one bounded retired-key migration pass.
type RewrapResult struct {
	Scanned   int
	Migrated  int
	Remaining int
}

// RewrapRetired migrates at most limit logical artifacts that still have an
// object under a retired key version. A positive bound is required so a
// background invocation cannot monopolize the store indefinitely.
func (s *Store) RewrapRetired(ctx context.Context, tenant string, limit int) (RewrapResult, error) {
	if limit <= 0 {
		return RewrapResult{}, errors.New("encrypted artifact: rewrap limit must be positive")
	}
	versions, err := s.keys.Versions(ctx, tenant)
	if err != nil {
		return RewrapResult{}, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	retired := make(map[string]struct{})
	for _, version := range versions {
		if version.Retired {
			retired[version.Version] = struct{}{}
		}
	}
	objects, err := s.objects.List(ctx, "tenants/"+tenant+"/")
	if err != nil {
		return RewrapResult{}, err
	}
	ids := make(map[string]struct{})
	for _, key := range objects {
		keyTenant, version, id, parseErr := parseObjectKey(key)
		if parseErr != nil || keyTenant != tenant {
			return RewrapResult{}, fmt.Errorf("%w", ErrMalformed)
		}
		if _, ok := retired[version]; ok {
			ids[id] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	result := RewrapResult{Scanned: len(ordered)}
	for i, id := range ordered {
		if i >= limit {
			result.Remaining++
			continue
		}
		migrated, err := s.Rewrap(ctx, tenant, id)
		if err != nil {
			result.Remaining += len(ordered) - i
			return result, err
		}
		if migrated {
			result.Migrated++
		}
	}
	return result, nil
}

// PruneKeyVersion removes wrapped tenant-key metadata only after the physical
// namespace for that retired version is empty. Current versions are refused by
// the key provider.
func (s *Store) PruneKeyVersion(ctx context.Context, tenant, version string) error {
	if err := validateSegment("tenant", tenant); err != nil {
		return err
	}
	if err := validateSegment("key version", version); err != nil {
		return err
	}
	objects, err := s.objects.List(ctx, "tenants/"+tenant+"/"+version+"/")
	if err != nil {
		return err
	}
	if len(objects) != 0 {
		return errors.New("encrypted artifact: key version is still referenced")
	}
	pruner, ok := s.keys.(interface {
		deleteRetired(context.Context, string, string) error
	})
	if !ok {
		return errors.New("encrypted artifact: key provider does not support version pruning")
	}
	return pruner.deleteRetired(ctx, tenant, version)
}

// Verify decrypts the complete object and re-hashes its plaintext logical id.
func (s *Store) Verify(ctx context.Context, tenant, id string) error {
	r, _, err := s.Open(ctx, tenant, id)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, r)
	closeErr := r.Close()
	return errors.Join(readErr, closeErr)
}

// VerifyVersion decrypts and hashes one exact physical representation. Deep
// diagnostics use this instead of logical Verify so a good current copy cannot
// hide corruption in a retained retired-key copy.
func (s *Store) VerifyVersion(ctx context.Context, tenant, version, id string) error {
	key, err := ObjectKey(tenant, version, id)
	if err != nil {
		return err
	}
	r, _, err := s.openVersion(ctx, tenant, version, id, key)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, r)
	return errors.Join(readErr, r.Close())
}

// Inventory returns every physical representation for a tenant, including
// objects whose tenant-key metadata is unavailable. It does not claim body
// integrity; callers use VerifyVersion for that proof.
func (s *Store) Inventory(ctx context.Context, tenant string) ([]ObjectInfo, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return nil, err
	}
	versions, err := s.keys.Versions(ctx, tenant)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	known := make(map[string]KeyVersion, len(versions))
	for _, version := range versions {
		known[version.Version] = version
	}
	keys, err := s.objects.List(ctx, "tenants/"+tenant+"/")
	if err != nil {
		return nil, err
	}
	out := make([]ObjectInfo, 0, len(keys))
	for _, key := range keys {
		keyTenant, version, id, parseErr := parseObjectKey(key)
		if parseErr != nil || keyTenant != tenant {
			return nil, fmt.Errorf("%w", ErrMalformed)
		}
		size, err := s.objects.Head(ctx, key)
		if err != nil {
			return nil, err
		}
		info := ObjectInfo{ID: id, KeyVersion: version, CiphertextBytes: size}
		if keyInfo, ok := known[version]; ok {
			info.KeyAvailable = true
			info.Retired = keyInfo.Retired
			info.MasterKeyID = keyInfo.MasterKeyID
		} else {
			info.Retired = true
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].KeyVersion < out[j].KeyVersion
	})
	return out, nil
}

// List returns unique logical ids for one tenant. Unexpected, cross-shaped, or
// keyless objects make the inventory unavailable rather than silently partial.
func (s *Store) List(ctx context.Context, tenant string) ([]string, error) {
	objects, err := s.Inventory(ctx, tenant)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]struct{})
	for _, object := range objects {
		if !object.KeyAvailable {
			return nil, fmt.Errorf("%w", ErrKeyUnavailable)
		}
		ids[object.ID] = struct{}{}
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Delete removes every physical version of one tenant's logical artifact.
func (s *Store) Delete(ctx context.Context, tenant, id string) error {
	if err := validateSegment("tenant", tenant); err != nil {
		return err
	}
	if _, err := artifact.Digest(id); err != nil {
		return err
	}
	keys, err := s.objects.List(ctx, "tenants/"+tenant+"/")
	if err != nil {
		return err
	}
	var errs []error
	found := false
	for _, key := range keys {
		keyTenant, _, keyID, parseErr := parseObjectKey(key)
		if parseErr != nil || keyTenant != tenant {
			errs = append(errs, fmt.Errorf("%w", ErrMalformed))
			continue
		}
		if keyID != id {
			continue
		}
		found = true
		if err := s.objects.Delete(ctx, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if !found && len(errs) == 0 {
		return fs.ErrNotExist
	}
	return errors.Join(errs...)
}

// Rewrap migrates an old-version object to the current tenant key without
// decrypting or rewriting its data ciphertext. The old object is deleted only
// after the new object has been fully decrypted and its plaintext id verified.
func (s *Store) Rewrap(ctx context.Context, tenant, id string) (bool, error) {
	currentVersion, currentKey, err := s.keys.Current(ctx, tenant)
	if err != nil {
		return false, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	defer erase32(&currentKey)
	physical, err := s.objectKeysForID(ctx, tenant, id)
	if err != nil {
		return false, err
	}
	if len(physical) == 0 {
		return false, fs.ErrNotExist
	}
	targetKey, err := ObjectKey(tenant, currentVersion, id)
	if err != nil {
		return false, err
	}
	for _, candidate := range physical {
		if candidate == targetKey {
			if err := s.Verify(ctx, tenant, id); err != nil {
				return false, err
			}
			removed := false
			for _, old := range physical {
				if old == targetKey {
					continue
				}
				if err := s.objects.Delete(ctx, old); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return removed, err
				}
				removed = true
			}
			return removed, nil
		}
	}
	sourceKey, sourceVersion := physical[0], ""
	_, sourceVersion, _, err = parseObjectKey(sourceKey)
	if err != nil {
		return false, err
	}
	raw, rawSize, err := s.objects.Open(ctx, sourceKey)
	if err != nil {
		return false, err
	}
	header, oldHeaderBytes, err := readHeader(raw, rawSize, tenant, sourceVersion, id)
	if err != nil {
		_ = raw.Close()
		return false, err
	}
	oldKey, err := s.keys.Get(ctx, tenant, sourceVersion)
	if err != nil {
		_ = raw.Close()
		return false, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	dataKey, err := unwrapDataKey(header, tenant, sourceVersion, id, oldKey)
	erase32(&oldKey)
	if err != nil {
		_ = raw.Close()
		return false, err
	}
	newHeader, err := rewrapHeader(header, tenant, currentVersion, id, currentKey, dataKey)
	erase32(&dataKey)
	if err != nil {
		_ = raw.Close()
		return false, err
	}
	ciphertextSize := rawSize - int64(len(oldHeaderBytes))
	newSize := int64(len(newHeader)) + ciphertextSize
	body := io.MultiReader(bytes.NewReader(newHeader), io.LimitReader(raw, ciphertextSize))
	err = s.objects.Create(ctx, targetKey, body, newSize)
	created := err == nil
	closeErr := raw.Close()
	if err != nil && !errors.Is(err, ErrObjectExists) {
		return false, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return false, closeErr
	}
	if err := s.Verify(ctx, tenant, id); err != nil {
		if created {
			_ = s.objects.Delete(context.WithoutCancel(ctx), targetKey)
		}
		return false, err
	}
	for _, old := range physical {
		if old == targetKey {
			continue
		}
		if err := s.objects.Delete(ctx, old); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

func (s *Store) objectKeysForID(ctx context.Context, tenant, id string) ([]string, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return nil, err
	}
	if _, err := artifact.Digest(id); err != nil {
		return nil, err
	}
	objects, err := s.objects.List(ctx, "tenants/"+tenant+"/")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, key := range objects {
		keyTenant, _, keyID, parseErr := parseObjectKey(key)
		if parseErr != nil || keyTenant != tenant {
			return nil, fmt.Errorf("%w", ErrMalformed)
		}
		if keyID == id {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) locate(ctx context.Context, tenant, id string) (string, string, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return "", "", err
	}
	if _, err := artifact.Digest(id); err != nil {
		return "", "", err
	}
	versions, err := s.keys.Versions(ctx, tenant)
	if err != nil {
		return "", "", fmt.Errorf("%w", ErrKeyUnavailable)
	}
	for _, version := range versions {
		key, err := ObjectKey(tenant, version.Version, id)
		if err != nil {
			return "", "", err
		}
		if _, err := s.objects.Head(ctx, key); err == nil {
			return key, version.Version, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", "", err
		}
	}
	// Detect an object whose key metadata disappeared. Reporting not-found here
	// would disguise an unreadable retained artifact as an absent artifact.
	objects, listErr := s.objects.List(ctx, "tenants/"+tenant+"/")
	if listErr != nil {
		return "", "", listErr
	}
	for _, key := range objects {
		keyTenant, _, keyID, parseErr := parseObjectKey(key)
		if parseErr != nil {
			return "", "", fmt.Errorf("%w", ErrMalformed)
		}
		if keyTenant == tenant && keyID == id {
			return "", "", fmt.Errorf("%w", ErrKeyUnavailable)
		}
	}
	return "", "", fs.ErrNotExist
}

type stageReservation struct {
	store *Store
	bytes int64
	done  bool
}

func (s *Store) reserveWriter() (*stageReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeWriters >= s.opts.MaxConcurrentWrites {
		return nil, fmt.Errorf("%w: maximum is %d concurrent writes", ErrStagingFull, s.opts.MaxConcurrentWrites)
	}
	s.activeWriters++
	return &stageReservation{store: s}, nil
}

func (r *stageReservation) addBytes(n int64) error {
	if n <= 0 {
		return nil
	}
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if r.done {
		return errors.New("encrypted artifact: staging reservation closed")
	}
	if r.store.stagingBytes+n > r.store.opts.MaxStagingBytes {
		return fmt.Errorf("%w: maximum is %d bytes", ErrStagingFull, r.store.opts.MaxStagingBytes)
	}
	r.store.stagingBytes += n
	r.bytes += n
	return nil
}

func (r *stageReservation) release() {
	if r == nil {
		return
	}
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if !r.done {
		r.store.stagingBytes -= r.bytes
		r.store.activeWriters--
		r.done = true
	}
}

func copyPlaintext(ctx context.Context, dst io.Writer, src io.Reader, reservation *stageReservation, max int64) (int64, error) {
	buf := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if int64(n) > max-total {
				return total + int64(n), fmt.Errorf("%w: maximum is %d bytes", ErrTooLarge, max)
			}
			if err := reservation.addBytes(int64(n)); err != nil {
				return total, err
			}
			written, err := dst.Write(buf[:n])
			total += int64(written)
			if err != nil {
				return total, err
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
}

// TenantStore binds one tenant and implements artifact.BlobStore for adapters
// that do not yet propagate contexts through the artifact interface.
type TenantStore struct {
	store  *Store
	tenant string
}

var _ artifact.BlobStore = (*TenantStore)(nil)

// ForTenant returns a BlobStore view that cannot address another tenant.
func (s *Store) ForTenant(tenant string) (*TenantStore, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return nil, err
	}
	return &TenantStore{store: s, tenant: tenant}, nil
}

// Put implements artifact.BlobStore.
func (s *TenantStore) Put(r io.Reader) (string, int64, error) {
	return s.store.Put(context.Background(), s.tenant, r)
}

// Open implements artifact.BlobStore.
func (s *TenantStore) Open(id string) (io.ReadCloser, int64, error) {
	return s.store.Open(context.Background(), s.tenant, id)
}

// Head implements artifact.BlobStore.
func (s *TenantStore) Head(id string) (int64, error) {
	return s.store.Head(context.Background(), s.tenant, id)
}

// Delete implements artifact.BlobStore.
func (s *TenantStore) Delete(id string) error {
	return s.store.Delete(context.Background(), s.tenant, id)
}

// List implements artifact.BlobStore.
func (s *TenantStore) List() ([]string, error) {
	return s.store.List(context.Background(), s.tenant)
}

// PutExpected stores a body under its asserted plaintext id.
func (s *TenantStore) PutExpected(expected string, r io.Reader) (int64, error) {
	return s.store.PutExpected(context.Background(), s.tenant, expected, r)
}

// Verify decrypts and hashes one complete artifact.
func (s *TenantStore) Verify(id string) error {
	return s.store.Verify(context.Background(), s.tenant, id)
}

func validateSegment(kind, value string) error {
	if value == "" || len(value) > 128 || value == "." || value == ".." {
		return fmt.Errorf("encrypted artifact: invalid %s", kind)
	}
	for _, c := range []byte(value) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			continue
		}
		return fmt.Errorf("encrypted artifact: invalid %s", kind)
	}
	return nil
}

func parseObjectKey(key string) (tenant, version, id string, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "tenants" {
		return "", "", "", fmt.Errorf("%w", ErrMalformed)
	}
	if err := validateSegment("tenant", parts[1]); err != nil {
		return "", "", "", fmt.Errorf("%w", ErrMalformed)
	}
	if err := validateSegment("key version", parts[2]); err != nil {
		return "", "", "", fmt.Errorf("%w", ErrMalformed)
	}
	if _, err := artifact.Digest(parts[3]); err != nil {
		return "", "", "", fmt.Errorf("%w", ErrMalformed)
	}
	return parts[1], parts[2], parts[3], nil
}

func erase32(key *[32]byte) {
	for i := range key {
		key[i] = 0
	}
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func random32() ([32]byte, error) {
	var out [32]byte
	_, err := io.ReadFull(rand.Reader, out[:])
	return out, err
}
