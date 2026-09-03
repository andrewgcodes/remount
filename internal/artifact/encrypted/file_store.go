package encrypted

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileStoreOptions bounds retained ciphertext and in-flight reservations. Zero
// fields receive finite defaults.
type FileStoreOptions struct {
	MaxBytes   int64
	MaxObjects int
}

// FileStoreStats describes retained ciphertext and in-flight creation.
type FileStoreStats struct {
	Bytes           int64
	Objects         int
	ReservedBytes   int64
	ReservedObjects int
	MaxBytes        int64
	MaxObjects      int
}

// FileStore is a bounded local implementation of KeyedStore. It publishes by
// hard-linking a complete private staging file, which is atomic and refuses to
// replace an existing object.
type FileStore struct {
	dir  string
	opts FileStoreOptions

	mu              sync.Mutex
	bytes           int64
	objects         int
	reservedBytes   int64
	reservedObjects int
	pins            map[string]int
	pending         map[string]chan struct{}
}

var _ KeyedStore = (*FileStore)(nil)

// NewFileStore opens a local physical-object store and inventories retained
// use before accepting writes. Crash staging is removed during the scan.
func NewFileStore(dir string, opts FileStoreOptions) (*FileStore, error) {
	if opts.MaxBytes < 0 || opts.MaxObjects < 0 {
		return nil, errors.New("encrypted artifact: file-store limits must not be negative")
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 64 << 30
	}
	if opts.MaxObjects == 0 {
		opts.MaxObjects = 100_000
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, opts: opts, pins: make(map[string]int), pending: make(map[string]chan struct{})}
	if err := s.inventory(); err != nil {
		return nil, err
	}
	return s, nil
}

// Stats returns retained and reserved physical capacity.
func (s *FileStore) Stats() FileStoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return FileStoreStats{
		Bytes: s.bytes, Objects: s.objects,
		ReservedBytes: s.reservedBytes, ReservedObjects: s.reservedObjects,
		MaxBytes: s.opts.MaxBytes, MaxObjects: s.opts.MaxObjects,
	}
}

func (s *FileStore) inventory() error {
	var bytes int64
	var objects int
	removed := false
	err := filepath.WalkDir(s.dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if filepath.Dir(path) == s.dir && strings.HasPrefix(entry.Name(), ".create-") {
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("encrypted artifact: file-store staging %q is a symlink", entry.Name())
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("encrypted artifact: file-store staging %q is not a regular file", entry.Name())
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			removed = true
			return nil
		}
		if _, _, _, err := parseObjectKey(key); err != nil {
			return fmt.Errorf("encrypted artifact: unexpected file-store entry %q", key)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("encrypted artifact: physical object %q is not a regular file", key)
		}
		if info.Size() < 0 || info.Size() > int64(^uint64(0)>>1)-bytes {
			return errors.New("encrypted artifact: file-store byte count overflow")
		}
		bytes += info.Size()
		objects++
		return nil
	})
	if err != nil {
		return err
	}
	if bytes > s.opts.MaxBytes || objects > s.opts.MaxObjects {
		return errors.New("encrypted artifact: retained file-store contents exceed configured capacity")
	}
	if removed {
		if err := syncDirectory(s.dir); err != nil {
			return err
		}
	}
	s.bytes, s.objects = bytes, objects
	return nil
}

// Create atomically publishes one exact-size ciphertext object if absent.
func (s *FileStore) Create(ctx context.Context, key string, body io.Reader, size int64) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	reservation, err := s.reserve(ctx, key, path, size)
	if err != nil {
		return err
	}
	defer reservation.release()
	tmp, err := os.CreateTemp(s.dir, ".create-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	written, err := io.Copy(tmp, io.LimitReader(&contextReader{ctx: ctx, r: body}, size+1))
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if written != size {
		_ = tmp.Close()
		return fmt.Errorf("encrypted artifact: physical object size is %d, expected %d", written, size)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o444); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := os.Link(name, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrObjectExists
		}
		return err
	}
	reservation.commit()
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		// Publication and accounting already committed. A retry observes the
		// existing immutable object and verifies it.
		return err
	}
	return nil
}

// Open pins one physical object until Close.
func (s *FileStore) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("encrypted artifact: physical object is not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	s.pins[key]++
	return &pinnedFile{File: f, store: s, key: key}, info.Size(), nil
}

// Head returns one physical ciphertext size.
func (s *FileStore) Head(_ context.Context, key string) (int64, error) {
	path, err := s.path(key)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, errors.New("encrypted artifact: physical object is not a regular file")
	}
	return info.Size(), nil
}

// Delete removes an unpinned physical object.
func (s *FileStore) Delete(_ context.Context, key string) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[key] != 0 {
		return errors.New("encrypted artifact: physical object is in use")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("encrypted artifact: physical object is not a regular file")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	s.bytes -= info.Size()
	s.objects--
	return syncDirectory(filepath.Dir(path))
}

// List returns sorted physical keys below prefix.
func (s *FileStore) List(_ context.Context, prefix string) ([]string, error) {
	if !validObjectPrefix(prefix) {
		return nil, errors.New("encrypted artifact: invalid physical prefix")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	err := filepath.WalkDir(s.dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Dir(path) == s.dir && strings.HasPrefix(entry.Name(), ".create-") {
			return nil
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if _, _, _, err := parseObjectKey(key); err != nil {
			return fmt.Errorf("encrypted artifact: unexpected file-store entry %q", key)
		}
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func (s *FileStore) path(key string) (string, error) {
	if _, _, _, err := parseObjectKey(key); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, filepath.FromSlash(key)), nil
}

func validObjectPrefix(prefix string) bool {
	parts := strings.Split(strings.TrimSuffix(prefix, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "tenants" {
		return false
	}
	if validateSegment("tenant", parts[1]) != nil {
		return false
	}
	return len(parts) != 3 || validateSegment("key version", parts[2]) == nil
}

type fileReservation struct {
	store *FileStore
	key   string
	size  int64
	done  bool
}

func (s *FileStore) reserve(ctx context.Context, key, path string, size int64) (*fileReservation, error) {
	if size < 0 {
		return nil, errors.New("encrypted artifact: physical object size must not be negative")
	}
	for {
		s.mu.Lock()
		info, err := os.Lstat(path)
		if err == nil {
			s.mu.Unlock()
			if !info.Mode().IsRegular() {
				return nil, errors.New("encrypted artifact: physical object is not a regular file")
			}
			return nil, ErrObjectExists
		}
		if !errors.Is(err, fs.ErrNotExist) {
			s.mu.Unlock()
			return nil, err
		}
		if wait := s.pending[key]; wait != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-wait:
				continue
			}
		}
		if s.objects+s.reservedObjects+1 > s.opts.MaxObjects || size > s.opts.MaxBytes-s.bytes-s.reservedBytes {
			s.mu.Unlock()
			return nil, ErrPhysicalStoreFull
		}
		s.reservedObjects++
		s.reservedBytes += size
		s.pending[key] = make(chan struct{})
		s.mu.Unlock()
		return &fileReservation{store: s, key: key, size: size}, nil
	}
}

func (r *fileReservation) release() {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if !r.done {
		r.store.reservedObjects--
		r.store.reservedBytes -= r.size
		wait := r.store.pending[r.key]
		delete(r.store.pending, r.key)
		close(wait)
		r.done = true
	}
}

func (r *fileReservation) commit() {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	if r.done {
		return
	}
	r.store.reservedObjects--
	r.store.reservedBytes -= r.size
	r.store.objects++
	r.store.bytes += r.size
	wait := r.store.pending[r.key]
	delete(r.store.pending, r.key)
	close(wait)
	r.done = true
}

type pinnedFile struct {
	*os.File
	store *FileStore
	key   string
	once  sync.Once
}

func (f *pinnedFile) Close() error {
	err := f.File.Close()
	f.once.Do(func() {
		f.store.mu.Lock()
		if f.store.pins[f.key] <= 1 {
			delete(f.store.pins, f.key)
		} else {
			f.store.pins[f.key]--
		}
		f.store.mu.Unlock()
	})
	return err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
