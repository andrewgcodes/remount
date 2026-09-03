package firecracker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"remount.dev/remount/internal/proto"
)

const (
	defaultImageLimit = 128
	defaultImageBytes = int64(1 << 40)
)

// ReflinkStoreOptions configure durable CoW root images. Logical bytes are
// charged at the full image size even when the filesystem initially shares
// extents, so later guest writes cannot silently overcommit admission.
type ReflinkStoreOptions struct {
	Dir             string
	MaxImages       int
	MaxLogicalBytes int64
	OwnerUID        int
	OwnerGID        int
}

// ReflinkStore creates and adopts fixed-path CoW root images. It deliberately
// does not expose a host filesystem view: a VolumeProvider must add a probed,
// coherent guest bridge rather than mounting an ext4 image on host and guest
// concurrently.
type ReflinkStore struct {
	dir      string
	maxCount int
	maxBytes int64
	uid, gid int
	clone    func(string, string) error

	mu      sync.Mutex
	images  map[string]*RootImage
	bytes   int64
	pending int
}

// RootImage is one durable CoW root drive owned by a ReflinkStore.
type RootImage struct {
	store *ReflinkStore
	id    string
	path  string
	size  int64

	mu      sync.Mutex
	removed bool
}

// NewReflinkStore reconstructs accounting from disk and selects the host
// FICLONE implementation. Probe must succeed before the store is advertised.
func NewReflinkStore(opts ReflinkStoreOptions) (*ReflinkStore, error) {
	return newReflinkStore(opts, hostReflink)
}

func newReflinkStore(opts ReflinkStoreOptions, clone func(string, string) error) (*ReflinkStore, error) {
	if opts.Dir == "" {
		return nil, errors.New("firecracker: reflink store directory is required")
	}
	if opts.MaxImages <= 0 {
		opts.MaxImages = defaultImageLimit
	}
	if opts.MaxLogicalBytes <= 0 {
		opts.MaxLogicalBytes = defaultImageBytes
	}
	if opts.OwnerUID < 0 || opts.OwnerGID < 0 {
		return nil, errors.New("firecracker: invalid root image owner")
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if st, err := os.Lstat(dir); err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("firecracker: root image store %q must be a private real directory", dir)
	}
	store := &ReflinkStore{dir: dir, maxCount: opts.MaxImages, maxBytes: opts.MaxLogicalBytes, uid: opts.OwnerUID, gid: opts.OwnerGID, clone: clone, images: make(map[string]*RootImage)}
	if err := store.reconstruct(); err != nil {
		return nil, err
	}
	return store, nil
}

// Probe proves that base is a regular image and the configured directory
// supports copy-on-write cloning. Unsupported filesystems fail closed.
func (s *ReflinkStore) Probe(ctx context.Context, base string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := regularFile(base)
	if err != nil {
		return fmt.Errorf("base rootfs: %w", err)
	}
	if st.Size() > s.maxBytes {
		return fmt.Errorf("base rootfs size %d exceeds logical byte limit %d", st.Size(), s.maxBytes)
	}
	probe, err := os.CreateTemp(s.dir, ".reflink-probe-")
	if err != nil {
		return err
	}
	path := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	defer os.Remove(path)
	if err := s.clone(base, path); err != nil {
		return fmt.Errorf("reflink (FICLONE) unavailable: %w", err)
	}
	return nil
}

// Create atomically publishes one CoW image after reserving both object and
// worst-case logical-byte capacity.
func (s *ReflinkStore) Create(ctx context.Context, id, base string) (*RootImage, error) {
	if err := validateWorkspaceID(id); err != nil {
		return nil, err
	}
	st, err := regularFile(base)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if _, exists := s.images[id]; exists {
		s.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "firecracker root image for %s already exists", id)
	}
	if len(s.images)+s.pending >= s.maxCount || st.Size() > s.maxBytes-s.bytes {
		s.mu.Unlock()
		return nil, proto.Err(proto.CodeResourceExhausted, "firecracker root image capacity exhausted")
	}
	s.pending++
	s.bytes += st.Size()
	s.mu.Unlock()
	committed := false
	defer func() {
		if committed {
			return
		}
		s.mu.Lock()
		s.pending--
		s.bytes -= st.Size()
		s.mu.Unlock()
	}()

	tmp, err := os.CreateTemp(s.dir, ".image-stage-")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	if err := s.clone(base, tmpPath); err != nil {
		return nil, err
	}
	cloned, err := regularFile(tmpPath)
	if err != nil || cloned.Size() != st.Size() {
		return nil, fmt.Errorf("firecracker: cloned root image size differs from admitted base")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return nil, err
	}
	if err := os.Chown(tmpPath, s.uid, s.gid); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil || closeErr != nil {
		return nil, errors.Join(syncErr, closeErr)
	}
	path := filepath.Join(s.dir, imageName(id))
	if err := os.Rename(tmpPath, path); err != nil {
		return nil, err
	}
	if err := syncDirectory(s.dir); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	image := &RootImage{store: s, id: id, path: path, size: st.Size()}
	s.mu.Lock()
	s.pending--
	s.images[id] = image
	s.mu.Unlock()
	committed = true
	return image, nil
}

// Adopt returns an accounted image retained across a node restart.
func (s *ReflinkStore) Adopt(id string) (*RootImage, error) {
	if err := validateWorkspaceID(id); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	image, ok := s.images[id]
	if !ok {
		return nil, fmt.Errorf("firecracker: no retained root image for %s", id)
	}
	return image, nil
}

// Path returns the host path to the writable root drive.
func (i *RootImage) Path() string { return i.path }

// Size returns the worst-case logical bytes charged for this image.
func (i *RootImage) Size() int64 { return i.size }

// Destroy removes the exact regular image and releases admission only after
// the parent directory sync makes deletion durable.
func (i *RootImage) Destroy() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.removed {
		return nil
	}
	st, err := os.Lstat(i.path)
	if errors.Is(err, os.ErrNotExist) {
		if err := syncDirectory(i.store.dir); err != nil {
			return err
		}
		i.store.mu.Lock()
		if current := i.store.images[i.id]; current == i {
			delete(i.store.images, i.id)
			i.store.bytes -= i.size
		}
		i.store.mu.Unlock()
		i.removed = true
		return nil
	}
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("firecracker: refusing to remove non-regular root image %q", i.path)
	}
	if err := os.Remove(i.path); err != nil {
		return err
	}
	if err := syncDirectory(i.store.dir); err != nil {
		return err
	}
	i.store.mu.Lock()
	if current := i.store.images[i.id]; current == i {
		delete(i.store.images, i.id)
		i.store.bytes -= i.size
	}
	i.store.mu.Unlock()
	i.removed = true
	return nil
}

func (s *ReflinkStore) reconstruct() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(s.dir, name)
		if strings.HasPrefix(name, ".image-stage-") || strings.HasPrefix(name, ".reflink-probe-") {
			st, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !st.Mode().IsRegular() {
				return fmt.Errorf("firecracker: refusing to clean non-regular staging path %q", path)
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			continue
		}
		id, ok := parseImageName(name)
		if !ok {
			return fmt.Errorf("firecracker: unrecognized object in root image store: %s", name)
		}
		st, err := entry.Info()
		if err != nil || !st.Mode().IsRegular() {
			return fmt.Errorf("firecracker: invalid retained root image %q", path)
		}
		if len(s.images) >= s.maxCount || st.Size() > s.maxBytes-s.bytes {
			return fmt.Errorf("firecracker: retained root images exceed configured capacity")
		}
		image := &RootImage{store: s, id: id, path: path, size: st.Size()}
		s.images[id] = image
		s.bytes += st.Size()
	}
	return syncDirectory(s.dir)
}

func imageName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("vm-%x-%s.ext4", sum[:8], id)
}

func parseImageName(name string) (string, bool) {
	if !strings.HasPrefix(name, "vm-") || !strings.HasSuffix(name, ".ext4") {
		return "", false
	}
	parts := strings.SplitN(strings.TrimSuffix(strings.TrimPrefix(name, "vm-"), ".ext4"), "-", 2)
	if len(parts) != 2 || len(parts[0]) != 16 || validateWorkspaceID(parts[1]) != nil || imageName(parts[1]) != name {
		return "", false
	}
	return parts[1], true
}

func regularFile(path string) (os.FileInfo, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%q is not a regular file", path)
	}
	return st, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	return errors.Join(err, dir.Close())
}
