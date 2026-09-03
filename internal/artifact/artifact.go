// Package artifact stores immutable, content-addressed blobs and builds
// workspace snapshots as tar.gz artifacts.
//
// An artifact id is "art_sha256:<hex>". A snapshot is the workspace
// filesystem (files, dirs, symlinks, modes, mtimes) minus excluded globs.
// Snapshots are portable across nodes, vendors, OSes and architectures
// because they contain only files; process state is never included
// (docs/adr/0006-snapshots-are-files.md).
package artifact

import (
	"archive/tar"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/metrics"
)

const (
	// Prefix of every artifact id.
	Prefix = "art_sha256:"
	// POSIX tar permits a NUL type flag for legacy regular-file entries.
	// archive/tar's TypeRegA name is deprecated, but readers must still accept
	// existing archives that use the wire value.
	legacyRegularFile byte = 0
)

var (
	// ErrDigestMismatch means a caller-supplied artifact id did not match the
	// bytes received. The temporary bytes are never published in this case.
	ErrDigestMismatch = errors.New("artifact: digest mismatch")
	// ErrTooLarge means an input exceeded the configured compressed-byte
	// limit. At most limit+1 bytes are read before the upload is rejected.
	ErrTooLarge = errors.New("artifact: compressed data exceeds limit")
	// ErrStoreFull means publishing another artifact would exceed the store's
	// configured byte or object budget. Active temporary uploads count toward
	// both budgets so concurrent callers cannot overcommit the disk.
	ErrStoreFull = errors.New("artifact: store capacity exhausted")
)

// StoreOptions bounds one content-addressed artifact store. Zero values mean
// unlimited; production callers should always configure finite values.
type StoreOptions struct {
	MaxBytes   int64
	MaxObjects int
}

// StoreStats is a point-in-time view of durable blobs and active staging
// reservations. Reserved resources are included separately so diagnostics can
// distinguish retained data from uploads that are still in progress.
type StoreStats struct {
	Bytes           int64
	Objects         int
	ReservedBytes   int64
	ReservedObjects int
	MaxBytes        int64
	MaxObjects      int
}

// GCResult describes one resumable garbage-collection pass.
type GCResult struct {
	Scanned       int
	Referenced    int
	GraceRetained int
	Removed       int
	RemovedBytes  int64
}

// RestoreLimits bounds the resources consumed while expanding an artifact.
// The compressed size is bounded by the artifact store; these limits protect
// the node from a small gzip stream expanding until it fills the workspace
// volume.
type RestoreLimits struct {
	MaxCompressedBytes  int64
	MaxExpandedBytes    int64
	MaxFileBytes        int64
	MaxEntries          int
	MaxPathBytes        int
	MaxDepth            int
	MaxCompressionRatio int64
}

// DefaultRestoreLimits are deliberately generous enough for ordinary source
// trees while still putting a finite ceiling on hostile archives.
var DefaultRestoreLimits = RestoreLimits{
	MaxCompressedBytes:  8 << 30,
	MaxExpandedBytes:    8 << 30,
	MaxFileBytes:        2 << 30,
	MaxEntries:          1_000_000,
	MaxPathBytes:        4096,
	MaxDepth:            256,
	MaxCompressionRatio: 1000,
}

// ID formats a digest.
func ID(sum []byte) string { return Prefix + hex.EncodeToString(sum) }

// Digest extracts the hex digest from an id.
func Digest(id string) (string, error) {
	if !strings.HasPrefix(id, Prefix) {
		return "", fmt.Errorf("artifact: bad id %q", id)
	}
	h := strings.TrimPrefix(id, Prefix)
	if len(h) != 64 {
		return "", fmt.Errorf("artifact: bad digest length in %q", id)
	}
	if _, err := hex.DecodeString(h); err != nil {
		return "", fmt.Errorf("artifact: bad digest in %q", id)
	}
	return h, nil
}

// BlobStore is the content-addressed blob contract every artifact backend
// satisfies. Ids are "sha256:<hex>"; Put computes the id, so a caller can
// never store bytes under a name that does not verify. Open and Head report
// os.ErrNotExist-compatible errors for unknown ids so callers branch on
// errors.Is rather than on backend-specific text.
type BlobStore interface {
	// Put stores the bytes of r and returns their id and size.
	Put(r io.Reader) (id string, size int64, err error)
	// Open returns the blob and its size.
	Open(id string) (io.ReadCloser, int64, error)
	// Head returns the size of a blob without opening it.
	Head(id string) (int64, error)
	// Delete removes a blob; a blob with active readers is refused.
	Delete(id string) error
	// List returns every id the store holds.
	List() ([]string, error)
}

var _ BlobStore = (*Store)(nil)

// Store is a directory of blobs named by digest.
type Store struct {
	dir  string
	opts StoreOptions

	mu              sync.Mutex
	bytes           int64
	objects         int
	reservedBytes   int64
	reservedObjects int
	pins            map[string]int // digest -> active idempotent readers
}

// NewStore creates the directory if needed.
func NewStore(dir string) (*Store, error) {
	return NewStoreWithOptions(dir, StoreOptions{})
}

// NewStoreWithOptions opens a store with fleet-wide byte and object budgets.
// Existing blobs are inventoried before the store becomes available. Private
// staging files left by a crashed prior process are removed during that scan.
func NewStoreWithOptions(dir string, opts StoreOptions) (*Store, error) {
	if opts.MaxBytes < 0 || opts.MaxObjects < 0 {
		return nil, errors.New("artifact: store limits must not be negative")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, opts: opts, pins: make(map[string]int)}
	if err := s.inventory(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) pathFor(digest string) string {
	return filepath.Join(s.dir, digest[:2], digest)
}

func (s *Store) inventory() error {
	var bytes int64
	var objects int
	removedStaging := false
	err := filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Dir(p) == s.dir && strings.HasPrefix(d.Name(), ".put-") {
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("artifact: staging path %q is a symlink", p)
			}
			if err := os.Remove(p); err != nil {
				return err
			}
			removedStaging = true
			return nil
		}
		digest, ok := storedDigest(p)
		if !ok {
			return fmt.Errorf("artifact: unexpected store entry %q", p)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact: blob %q is not a regular file", digest)
		}
		if info.Size() < 0 || bytes > int64(^uint64(0)>>1)-info.Size() {
			return errors.New("artifact: store byte count overflow")
		}
		bytes += info.Size()
		objects++
		return nil
	})
	if err != nil {
		return err
	}
	if removedStaging {
		if err := syncDir(s.dir); err != nil {
			return err
		}
	}
	s.bytes = bytes
	s.objects = objects
	return nil
}

func storedDigest(p string) (string, bool) {
	digest := filepath.Base(p)
	if len(digest) != sha256.Size*2 || filepath.Base(filepath.Dir(p)) != digest[:2] {
		return "", false
	}
	if _, err := hex.DecodeString(digest); err != nil || strings.ToLower(digest) != digest {
		return "", false
	}
	return digest, true
}

// Stats returns durable usage plus in-progress reservations.
func (s *Store) Stats() StoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StoreStats{
		Bytes: s.bytes, Objects: s.objects,
		ReservedBytes: s.reservedBytes, ReservedObjects: s.reservedObjects,
		MaxBytes: s.opts.MaxBytes, MaxObjects: s.opts.MaxObjects,
	}
}

// Put stores the stream and returns its id. Content is hashed while written;
// an existing blob is left untouched (it is identical by construction).
func (s *Store) Put(r io.Reader) (string, int64, error) {
	return s.put(r, "", 0)
}

// PutLimit is Put with a compressed-byte limit. A non-positive limit means
// unlimited. Rejected bytes remain private temporary files and are removed.
func (s *Store) PutLimit(r io.Reader, maxBytes int64) (string, int64, error) {
	return s.put(r, "", maxBytes)
}

// PutExpected stores r only if its digest is expected. This is the safe path
// for HTTP uploads/downloads: a mismatched body is never made visible and
// therefore cannot collide with, or prompt deletion of, an existing blob.
func (s *Store) PutExpected(expected string, r io.Reader, maxBytes int64) (int64, error) {
	digest, err := Digest(expected)
	if err != nil {
		return 0, err
	}
	if s.pin(digest) {
		defer s.unpin(digest)
		if err := s.Verify(expected); err != nil {
			return 0, fmt.Errorf("artifact: existing object %s failed verification: %w", expected, err)
		}
		n, err := verifyExpectedReader(expected, r, maxBytes)
		return n, err
	}
	_, n, err := s.put(r, expected, maxBytes)
	return n, err
}

func (s *Store) pin(digest string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := os.Lstat(s.pathFor(digest))
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	s.pins[digest]++
	return true
}

func (s *Store) unpin(digest string) {
	s.mu.Lock()
	if s.pins[digest] <= 1 {
		delete(s.pins, digest)
	} else {
		s.pins[digest]--
	}
	s.mu.Unlock()
}

func verifyExpectedReader(expected string, r io.Reader, maxBytes int64) (int64, error) {
	h := sha256.New()
	source := r
	if maxBytes > 0 {
		source = &io.LimitedReader{R: r, N: maxBytes + 1}
	}
	n, err := io.Copy(h, source)
	if err != nil {
		return n, err
	}
	if maxBytes > 0 && n > maxBytes {
		return n, fmt.Errorf("%w: maximum is %d bytes", ErrTooLarge, maxBytes)
	}
	if ID(h.Sum(nil)) != expected {
		return n, fmt.Errorf("%w: body is %s", ErrDigestMismatch, ID(h.Sum(nil)))
	}
	return n, nil
}

func (s *Store) put(r io.Reader, expected string, maxBytes int64) (string, int64, error) {
	reservation, err := s.reserveObject()
	if err != nil {
		metrics.ArtifactQuotaRejected.Inc()
		return "", 0, err
	}
	defer reservation.release()
	tmp, err := os.CreateTemp(s.dir, ".put-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	destination := io.MultiWriter(tmp, h)
	destination = &reservationWriter{reservation: reservation, writer: destination}
	source := r
	if maxBytes > 0 {
		source = &io.LimitedReader{R: r, N: maxBytes + 1}
	}
	n, err := io.Copy(destination, source)
	if err != nil {
		_ = tmp.Close()
		return "", 0, err
	}
	if maxBytes > 0 && n > maxBytes {
		_ = tmp.Close()
		return "", n, fmt.Errorf("%w: maximum is %d bytes", ErrTooLarge, maxBytes)
	}
	got := ID(h.Sum(nil))
	if expected != "" && got != expected {
		_ = tmp.Close()
		return "", n, fmt.Errorf("%w: body is %s", ErrDigestMismatch, got)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", 0, err
	}
	if err := tmp.Close(); err != nil {
		return "", 0, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	dst := s.pathFor(digest)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return "", 0, err
	}
	created, err := s.publish(tmpName, dst, n, reservation)
	if err != nil {
		return "", 0, err
	}
	if created {
		if err := syncDir(filepath.Dir(dst)); err != nil {
			// The rename is already visible and accounted for. Returning an error
			// makes the caller retry safely without pretending durability.
			return "", 0, err
		}
	}
	return got, n, nil
}

type storeReservation struct {
	store  *Store
	bytes  int64
	object bool
	done   bool
}

func (s *Store) reserveObject() (*storeReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opts.MaxObjects > 0 && s.objects+s.reservedObjects+1 > s.opts.MaxObjects {
		return nil, fmt.Errorf("%w: maximum is %d objects", ErrStoreFull, s.opts.MaxObjects)
	}
	s.reservedObjects++
	return &storeReservation{store: s, object: true}, nil
}

func (r *storeReservation) addBytes(n int64) error {
	if n <= 0 {
		return nil
	}
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.done {
		return errors.New("artifact: closed store reservation")
	}
	if s.opts.MaxBytes > 0 && s.bytes+s.reservedBytes+n > s.opts.MaxBytes {
		metrics.ArtifactQuotaRejected.Inc()
		return fmt.Errorf("%w: maximum is %d bytes", ErrStoreFull, s.opts.MaxBytes)
	}
	s.reservedBytes += n
	r.bytes += n
	return nil
}

func (r *storeReservation) releaseBytes(n int64) {
	if n <= 0 {
		return
	}
	s := r.store
	s.mu.Lock()
	if n > r.bytes {
		n = r.bytes
	}
	r.bytes -= n
	s.reservedBytes -= n
	s.mu.Unlock()
}

func (r *storeReservation) release() {
	if r == nil {
		return
	}
	s := r.store
	s.mu.Lock()
	if !r.done {
		s.reservedBytes -= r.bytes
		if r.object {
			s.reservedObjects--
		}
		r.bytes = 0
		r.object = false
		r.done = true
	}
	s.mu.Unlock()
}

type reservationWriter struct {
	reservation *storeReservation
	writer      io.Writer
}

func (w *reservationWriter) Write(p []byte) (int, error) {
	if err := w.reservation.addBytes(int64(len(p))); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if n < len(p) {
		w.reservation.releaseBytes(int64(len(p) - n))
	}
	return n, err
}

func (s *Store) publish(tmpName, dst string, size int64, reservation *storeReservation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reservation.done || !reservation.object || reservation.bytes != size {
		return false, errors.New("artifact: invalid staging reservation")
	}
	if st, err := os.Lstat(dst); err == nil {
		if !st.Mode().IsRegular() || st.Size() != size {
			return false, fmt.Errorf("artifact: existing blob %q is not the expected immutable object", filepath.Base(dst))
		}
		if err := verifyBlobPath(dst, filepath.Base(dst)); err != nil {
			metrics.ArtifactMiss.Inc()
			return false, err
		}
		s.reservedBytes -= reservation.bytes
		s.reservedObjects--
		reservation.bytes = 0
		reservation.object = false
		reservation.done = true
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if err := os.Chmod(tmpName, 0o444); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return false, err
	}
	s.bytes += size
	s.objects++
	s.reservedBytes -= reservation.bytes
	s.reservedObjects--
	reservation.bytes = 0
	reservation.object = false
	reservation.done = true
	return true, nil
}

// Open returns a reader for id.
func (s *Store) Open(id string) (io.ReadCloser, int64, error) {
	digest, err := Digest(id)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(s.pathFor(digest))
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// Head returns the size of a stored blob without opening it.
func (s *Store) Head(id string) (int64, error) {
	digest, err := Digest(id)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := os.Lstat(s.pathFor(digest))
	if err != nil {
		return 0, err
	}
	if !st.Mode().IsRegular() {
		return 0, fmt.Errorf("artifact: blob %q is not a regular file", digest)
	}
	return st.Size(), nil
}

// Has reports whether id is present.
func (s *Store) Has(id string) bool {
	digest, err := Digest(id)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := os.Lstat(s.pathFor(digest))
	return err == nil && st.Mode().IsRegular()
}

// Verify re-hashes a stored blob.
func (s *Store) Verify(id string) error {
	digest, err := Digest(id)
	if err != nil {
		return err
	}
	if err := verifyBlobPath(s.pathFor(digest), digest); err != nil {
		metrics.ArtifactMiss.Inc()
		return err
	}
	return nil
}

func verifyBlobPath(blobPath, digest string) error {
	st, err := os.Lstat(blobPath)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("artifact: blob %q is not a regular file", digest)
	}
	r, err := os.Open(blobPath)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return ErrDigestMismatch
	}
	return nil
}

// Delete removes a blob.
func (s *Store) Delete(id string) error {
	digest, err := Digest(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pathFor(digest)
	if s.pins[digest] > 0 {
		return fmt.Errorf("artifact: blob %q is in use", digest)
	}
	st, err := os.Lstat(p)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("artifact: blob %q is not a regular file", digest)
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	s.bytes -= st.Size()
	s.objects--
	return syncDir(filepath.Dir(p))
}

// List returns all ids.
func (s *Store) List() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}

func (s *Store) listLocked() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Dir(p) == s.dir && strings.HasPrefix(d.Name(), ".put-") {
			return nil // an active private upload
		}
		if digest, ok := storedDigest(p); ok {
			out = append(out, Prefix+digest)
			return nil
		}
		return fmt.Errorf("artifact: unexpected store entry %q", p)
	})
	sort.Strings(out)
	return out, err
}

// Collect removes blobs not present in referenced and older than cutoff.
// Callers must serialize creation of new durable references across this call;
// newly uploaded but not-yet-referenced blobs are protected by cutoff. Each
// unlink is independently durable, so an interrupted pass can be retried.
func (s *Store) Collect(referenced []string, cutoff time.Time) (GCResult, error) {
	keep := make(map[string]struct{}, len(referenced))
	for _, id := range referenced {
		digest, err := Digest(id)
		if err != nil {
			return GCResult{}, fmt.Errorf("artifact: refusing GC with invalid reference %q: %w", id, err)
		}
		keep[digest] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result GCResult
	var errs []error
	err := filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			errs = append(errs, walkErr)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		digest, ok := storedDigest(p)
		if !ok {
			if filepath.Dir(p) == s.dir && strings.HasPrefix(d.Name(), ".put-") {
				return nil // an active upload protected by its reservation
			}
			errs = append(errs, fmt.Errorf("artifact: unexpected store entry %q", p))
			return nil
		}
		result.Scanned++
		if _, ok := keep[digest]; ok {
			result.Referenced++
			return nil
		}
		if s.pins[digest] > 0 {
			result.GraceRetained++
			return nil
		}
		info, err := d.Info()
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if !info.Mode().IsRegular() {
			errs = append(errs, fmt.Errorf("artifact: blob %q is not a regular file", digest))
			return nil
		}
		if !cutoff.IsZero() && !info.ModTime().Before(cutoff) {
			result.GraceRetained++
			return nil
		}
		if err := os.Remove(p); err != nil {
			errs = append(errs, err)
			return nil
		}
		s.bytes -= info.Size()
		s.objects--
		result.Removed++
		result.RemovedBytes += info.Size()
		metrics.ArtifactGCObjects.Inc()
		metrics.ArtifactGCBytes.Add(uint64(info.Size()))
		if err := syncDir(filepath.Dir(p)); err != nil {
			errs = append(errs, err)
		}
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}
	return result, errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// Excluded reports whether rel (slash-separated, no leading slash) matches
// any exclude pattern. Patterns match against the full relative path and
// against each path component, so "node_modules" excludes it anywhere and
// "build/*.o" matches only under build.
func Excluded(rel string, excludes []string) bool {
	for _, pat := range excludes {
		pat = strings.TrimPrefix(pat, "/")
		if ok, _ := path.Match(pat, rel); ok {
			return true
		}
		if !strings.Contains(pat, "/") {
			for _, comp := range strings.Split(rel, "/") {
				if ok, _ := path.Match(pat, comp); ok {
					return true
				}
			}
		}
	}
	return false
}

// Snapshot writes a tar.gz of root to w. Entries are written in sorted
// order so identical trees produce identical bytes (and thus ids).
func Snapshot(root string, excludes []string, w io.Writer) error {
	_, err := SnapshotFiltered(root, func(rel string, _ bool) bool { return Excluded(rel, excludes) }, w)
	return err
}

// SnapshotStats counts what one snapshot contained.
type SnapshotStats struct {
	Files    int
	Dirs     int
	Symlinks int
	// Bytes is the uncompressed regular-file payload.
	Bytes int64
	// Skipped counts entries the filter rejected; a skipped directory counts
	// once, not per descendant.
	Skipped int
}

// SnapshotFiltered is Snapshot with a caller-supplied filter. skip receives
// the slash-separated relative path and whether it is a directory; returning
// true omits the entry (and, for a directory, everything under it). The tar
// layout is identical to Snapshot, so Restore and ApplyOverlay accept the
// output unchanged.
func SnapshotFiltered(root string, skip func(rel string, isDir bool) bool, w io.Writer) (SnapshotStats, error) {
	var stats SnapshotStats
	root, err := filepath.Abs(root)
	if err != nil {
		return stats, err
	}
	rr, err := os.OpenRoot(root)
	if err != nil {
		return stats, err
	}
	defer rr.Close()
	var paths []string
	err = fs.WalkDir(rr.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		rel := filepath.ToSlash(p)
		if skip != nil && skip(rel, d.IsDir()) {
			stats.Skipped++
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return stats, err
	}
	sort.Strings(paths)
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	writeErr := func(err error) (SnapshotStats, error) { return stats, err }
	for _, rel := range paths {
		name := filepath.FromSlash(rel)
		info, err := rr.Lstat(name)
		if err != nil {
			return writeErr(err)
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = rr.Readlink(name)
			if err != nil {
				return writeErr(err)
			}
			if err := validateSymlinkTarget(rel, link); err != nil {
				return writeErr(err)
			}
		}
		var file *os.File
		if info.Mode().IsRegular() {
			// Open first and derive the header from that descriptor. The path
			// can change while a workspace is live; using one descriptor for
			// metadata and bytes prevents a stale-size tar header.
			file, err = openSnapshotFile(rr, name)
			if err != nil {
				return writeErr(err)
			}
			opened, statErr := file.Stat()
			if statErr != nil {
				file.Close()
				return writeErr(statErr)
			}
			if !opened.Mode().IsRegular() {
				file.Close()
				return writeErr(fmt.Errorf("artifact: %q changed type during snapshot", rel))
			}
			info = opened
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			if file != nil {
				file.Close()
			}
			return writeErr(err)
		}
		hdr.Name = rel
		switch {
		case info.IsDir():
			hdr.Name += "/"
			stats.Dirs++
		case link != "":
			stats.Symlinks++
		default:
			stats.Files++
			stats.Bytes += hdr.Size
		}
		hdr.Uid, hdr.Gid = 0, 0
		hdr.Uname, hdr.Gname = "", ""
		hdr.AccessTime, hdr.ChangeTime = hdr.ModTime, hdr.ModTime
		hdr.Format = tar.FormatPAX
		if err := tw.WriteHeader(hdr); err != nil {
			if file != nil {
				file.Close()
			}
			return writeErr(err)
		}
		if file != nil {
			_, err = io.CopyN(tw, file, hdr.Size)
			closeErr := file.Close()
			if err != nil {
				return writeErr(err)
			}
			if closeErr != nil {
				return writeErr(closeErr)
			}
		}
	}
	if err := tw.Close(); err != nil {
		return stats, err
	}
	return stats, gz.Close()
}

// OverlayResult reports what ApplyOverlay placed into the target tree.
type OverlayResult struct {
	// Paths lists every regular file and symlink that was replaced or created,
	// slash-separated and relative to root, in archive order.
	Paths []string
	Dirs  int
	Bytes int64
}

// OverlayStageDir is the directory under root where ApplyOverlay stages an
// archive before moving entries into place. It is the same directory the node
// reserves for `.remount/env`, which snapshots already exclude.
const OverlayStageDir = ".remount"

// ApplyOverlay extracts a snapshot archive over an existing tree without
// removing anything the archive does not name. The archive is fully expanded
// and validated in a staging directory inside root first, so a hostile or
// truncated archive changes nothing; entries are then moved into place one at
// a time with rename, so every file is either its old bytes or its new bytes
// and never a partial write. Directories are created as needed. An entry that
// would replace a directory with a file (or vice versa), or that names
// anything under OverlayStageDir, is refused after the entries before it have
// already landed; the returned result names exactly what landed.
func ApplyOverlay(root string, r io.Reader, limits RestoreLimits) (OverlayResult, error) {
	var res OverlayResult
	limits = normalizeRestoreLimits(limits)
	root, err := filepath.Abs(root)
	if err != nil {
		return res, err
	}
	rr, err := os.OpenRoot(root)
	if err != nil {
		return res, err
	}
	defer rr.Close()
	if err := rr.MkdirAll(OverlayStageDir, 0o755); err != nil {
		return res, err
	}
	stageRel, err := mkdirTempIn(rr, OverlayStageDir, "overlay-")
	if err != nil {
		return res, err
	}
	defer func() { _ = rr.RemoveAll(stageRel) }()
	if err := extract(filepath.Join(root, stageRel), r, limits); err != nil {
		return res, err
	}
	stage, err := rr.OpenRoot(stageRel)
	if err != nil {
		return res, err
	}
	defer stage.Close()
	// Validate every destination before the first rename so a refusal never
	// leaves the tree half-overlaid. Directories are created afterwards and
	// files land last, each by a single rename.
	var dirs, files []string
	err = fs.WalkDir(stage.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		rel := filepath.ToSlash(p)
		if rel == OverlayStageDir || strings.HasPrefix(rel, OverlayStageDir+"/") {
			return fmt.Errorf("artifact: overlay may not write %q", rel)
		}
		osName := filepath.FromSlash(rel)
		if err := rejectSymlinkParents(rr, osName); err != nil {
			return fmt.Errorf("artifact: overlay %q: %w", rel, err)
		}
		if d.IsDir() {
			if st, err := rr.Lstat(osName); err == nil && !st.IsDir() {
				return fmt.Errorf("artifact: overlay %q would replace a file with a directory", rel)
			}
			dirs = append(dirs, rel)
			return nil
		}
		if st, err := rr.Lstat(osName); err == nil && st.IsDir() {
			return fmt.Errorf("artifact: overlay %q would replace a directory", rel)
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return res, err
	}
	for _, rel := range dirs {
		if err := rr.MkdirAll(filepath.FromSlash(rel), 0o755); err != nil {
			return res, err
		}
		res.Dirs++
	}
	for _, rel := range files {
		osName := filepath.FromSlash(rel)
		info, err := stage.Lstat(osName)
		if err != nil {
			return res, err
		}
		if err := rr.Rename(filepath.Join(stageRel, osName), osName); err != nil {
			return res, err
		}
		res.Paths = append(res.Paths, rel)
		if info.Mode().IsRegular() {
			res.Bytes += info.Size()
		}
	}
	return res, syncDir(root)
}

// mkdirTempIn creates a uniquely named directory under parent (relative to
// rr) and returns its path relative to rr.
func mkdirTempIn(rr *os.Root, parent, prefix string) (string, error) {
	var random [8]byte
	for attempt := 0; attempt < 100; attempt++ {
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		name := filepath.Join(parent, prefix+hex.EncodeToString(random[:]))
		err := rr.Mkdir(name, 0o700)
		if err == nil {
			return name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	return "", errors.New("artifact: could not create staging directory")
}

// Restore extracts a snapshot into root, which must exist. Extraction first
// happens in a private sibling directory and is committed only after every
// entry has passed containment and resource-limit checks.
func Restore(root string, r io.Reader) error {
	return RestoreWithLimits(root, r, DefaultRestoreLimits)
}

// RestoreWithLimits is Restore with caller-supplied expansion limits. Zero
// fields inherit the corresponding default.
func RestoreWithLimits(root string, r io.Reader, limits RestoreLimits) error {
	limits = normalizeRestoreLimits(limits)
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	st, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("artifact: restore root must be a real directory")
	}
	stage, err := os.MkdirTemp(filepath.Dir(root), ".remount-restore-*")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()
	if err := extract(stage, r, limits); err != nil {
		return err
	}
	backup := stage + ".old"
	if err := os.Rename(root, backup); err != nil {
		return err
	}
	if err := os.Rename(stage, root); err != nil {
		_ = os.Rename(backup, root)
		return err
	}
	if err := syncDir(filepath.Dir(root)); err != nil {
		// Best-effort rollback while both directory names are still reserved.
		_ = os.Rename(root, stage)
		_ = os.Rename(backup, root)
		return err
	}
	committed = true
	// Cleanup after the durable commit is maintenance, not restore failure. A
	// leftover backup is preferable to a caller deleting the new live tree
	// because it received an error after the commit point.
	if err := os.RemoveAll(backup); err != nil {
		return nil
	}
	_ = syncDir(filepath.Dir(root))
	return nil
}

func normalizeRestoreLimits(l RestoreLimits) RestoreLimits {
	d := DefaultRestoreLimits
	if l.MaxCompressedBytes > 0 {
		d.MaxCompressedBytes = l.MaxCompressedBytes
	}
	if l.MaxExpandedBytes > 0 {
		d.MaxExpandedBytes = l.MaxExpandedBytes
	}
	if l.MaxFileBytes > 0 {
		d.MaxFileBytes = l.MaxFileBytes
	}
	if l.MaxEntries > 0 {
		d.MaxEntries = l.MaxEntries
	}
	if l.MaxPathBytes > 0 {
		d.MaxPathBytes = l.MaxPathBytes
	}
	if l.MaxDepth > 0 {
		d.MaxDepth = l.MaxDepth
	}
	if l.MaxCompressionRatio > 0 {
		d.MaxCompressionRatio = l.MaxCompressionRatio
	}
	return d
}

func extract(root string, r io.Reader, limits RestoreLimits) error {
	compressed := &boundedCompressedReader{reader: r, limit: limits.MaxCompressedBytes}
	gz, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	rr, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer rr.Close()
	type deferredMode struct {
		name  string
		mode  os.FileMode
		mtime time.Time
	}
	var dirs []deferredMode
	seen := make(map[string]struct{})
	var entries int
	var expanded int64
	var expandedWritten int64
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		entries++
		if entries > limits.MaxEntries {
			return fmt.Errorf("artifact: archive has more than %d entries", limits.MaxEntries)
		}
		name, err := validateArchiveName(hdr.Name, limits)
		if err != nil {
			return err
		}
		if name == "." {
			continue
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("artifact: duplicate entry %q", hdr.Name)
		}
		seen[name] = struct{}{}
		if hdr.Size < 0 || hdr.Size > limits.MaxExpandedBytes-expanded {
			return fmt.Errorf("artifact: expanded data exceeds %d bytes", limits.MaxExpandedBytes)
		}
		if hdr.Size > limits.MaxFileBytes {
			return fmt.Errorf("artifact: file %q exceeds %d bytes", hdr.Name, limits.MaxFileBytes)
		}
		expanded += hdr.Size
		osName := filepath.FromSlash(name)
		if err := rejectSymlinkParents(rr, osName); err != nil {
			return fmt.Errorf("artifact: entry %q: %w", hdr.Name, err)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := rr.MkdirAll(osName, 0o755); err != nil {
				return err
			}
			dirs = append(dirs, deferredMode{osName, os.FileMode(hdr.Mode).Perm(), hdr.ModTime})
		case tar.TypeReg, legacyRegularFile:
			if err := rr.MkdirAll(filepath.Dir(osName), 0o755); err != nil {
				return err
			}
			f, err := rr.OpenFile(osName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(hdr.Mode).Perm()|0o200)
			if err != nil {
				return err
			}
			destination := &expansionWriter{
				writer: f, expanded: &expandedWritten, compressed: compressed,
				ratio: limits.MaxCompressionRatio,
			}
			if _, err := io.CopyN(destination, tr, hdr.Size); err != nil {
				f.Close()
				return err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			if err := rr.Chmod(osName, os.FileMode(hdr.Mode).Perm()); err != nil {
				return err
			}
			if err := rr.Chtimes(osName, hdr.ModTime, hdr.ModTime); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if len(hdr.Linkname) > limits.MaxPathBytes {
				return fmt.Errorf("artifact: symlink %q target exceeds %d bytes", hdr.Name, limits.MaxPathBytes)
			}
			if err := validateSymlinkTarget(name, hdr.Linkname); err != nil {
				return err
			}
			if err := rr.MkdirAll(filepath.Dir(osName), 0o755); err != nil {
				return err
			}
			if err := rr.Symlink(filepath.FromSlash(hdr.Linkname), osName); err != nil {
				return err
			}
		default:
			return fmt.Errorf("artifact: unsupported entry type %d for %q", hdr.Typeflag, hdr.Name)
		}
	}
	// tar stops at its end marker before gzip necessarily verifies the footer.
	// Drain gzip first for checksum validation, then the underlying stream so
	// the compressed-size bound also covers trailing input.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, compressed); err != nil {
		return err
	}
	if err := checkCompressionRatio(expandedWritten, compressed.count, limits.MaxCompressionRatio); err != nil {
		return err
	}
	// Apply directory modes last so read-only dirs don't block extraction.
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := rr.Chmod(dirs[i].name, dirs[i].mode); err != nil {
			return err
		}
		if err := rr.Chtimes(dirs[i].name, dirs[i].mtime, dirs[i].mtime); err != nil {
			return err
		}
		f, err := rr.Open(dirs[i].name)
		if err != nil {
			return err
		}
		err = f.Sync()
		_ = f.Close()
		if err != nil {
			return err
		}
	}
	f, err := rr.Open(".")
	if err != nil {
		return err
	}
	err = f.Sync()
	_ = f.Close()
	return err
}

type boundedCompressedReader struct {
	reader io.Reader
	limit  int64
	count  int64
}

type expansionWriter struct {
	writer     io.Writer
	expanded   *int64
	compressed *boundedCompressedReader
	ratio      int64
}

func (w *expansionWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	*w.expanded += int64(n)
	if err == nil {
		err = checkCompressionRatio(*w.expanded, w.compressed.count, w.ratio)
	}
	return n, err
}

func (r *boundedCompressedReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		n, err := r.reader.Read(p)
		r.count += int64(n)
		return n, err
	}
	remaining := r.limit - r.count
	if remaining <= 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n > 0 {
			r.count += int64(n)
			return 0, fmt.Errorf("%w: maximum is %d bytes", ErrTooLarge, r.limit)
		}
		return 0, err
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.reader.Read(p)
	r.count += int64(n)
	return n, err
}

func checkCompressionRatio(expanded, compressed, ratio int64) error {
	const grace = int64(1 << 20)
	if ratio <= 0 || expanded <= grace || compressed <= 0 {
		return nil
	}
	// Division avoids overflowing compressed*ratio for hostile headers.
	if (expanded-grace+ratio-1)/ratio > compressed {
		return fmt.Errorf("artifact: expansion ratio exceeds %d:1", ratio)
	}
	return nil
}

func validateArchiveName(name string, limits RestoreLimits) (string, error) {
	if name == "" || strings.IndexByte(name, 0) >= 0 || path.IsAbs(name) || filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return "", fmt.Errorf("artifact: invalid entry name %q", name)
	}
	name = strings.ReplaceAll(name, "\\", "/")
	clean := path.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("artifact: entry %q escapes root", name)
	}
	if len(clean) > limits.MaxPathBytes {
		return "", fmt.Errorf("artifact: entry path exceeds %d bytes", limits.MaxPathBytes)
	}
	if clean != "." && len(strings.Split(clean, "/")) > limits.MaxDepth {
		return "", fmt.Errorf("artifact: entry path exceeds depth %d", limits.MaxDepth)
	}
	return clean, nil
}

func validateSymlinkTarget(name, target string) error {
	if target == "" || strings.IndexByte(target, 0) >= 0 || path.IsAbs(target) || filepath.IsAbs(target) || filepath.VolumeName(target) != "" {
		return fmt.Errorf("artifact: symlink %q has invalid target %q", name, target)
	}
	target = strings.ReplaceAll(target, "\\", "/")
	resolved := path.Clean(path.Join(path.Dir(name), target))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("artifact: symlink %q escapes root", name)
	}
	return nil
}

func rejectSymlinkParents(root *os.Root, name string) error {
	parent := filepath.Dir(name)
	if parent == "." {
		return nil
	}
	cur := ""
	for _, part := range strings.Split(filepath.ToSlash(parent), "/") {
		if cur == "" {
			cur = part
		} else {
			cur = filepath.Join(cur, part)
		}
		st, err := root.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink parent %q is not permitted", cur)
		}
		if !st.IsDir() {
			return fmt.Errorf("parent %q is not a directory", cur)
		}
	}
	return nil
}

func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// SnapshotToStore snapshots root straight into the store and returns the id.
func SnapshotToStore(s *Store, root string, excludes []string) (string, int64, error) {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(Snapshot(root, excludes, pw))
	}()
	id, n, err := s.Put(pr)
	if err != nil {
		pr.CloseWithError(err)
		return "", 0, err
	}
	return id, n, nil
}

// RestoreFromStore restores id into root.
func RestoreFromStore(s *Store, id, root string) error {
	r, _, err := s.Open(id)
	if err != nil {
		return err
	}
	defer r.Close()
	return Restore(root, r)
}
