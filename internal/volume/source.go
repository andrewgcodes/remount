package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"remount.dev/remount/internal/artifact"
)

// DirectoryResolver resolves <root>/<tenant>/<artifact-id> through os.Root.
// root may itself be a local directory or an NFS mount managed by the host.
type DirectoryResolver struct {
	root *os.Root
}

// OpenDirectoryResolver opens a descriptor-rooted immutable artifact cache.
func OpenDirectoryResolver(dir string) (*DirectoryResolver, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &DirectoryResolver{root: root}, nil
}

// OpenArtifact returns a directory descriptor without following an escape
// through a tenant or artifact symlink.
func (r *DirectoryResolver) OpenArtifact(_ context.Context, tenant, id string) (*os.File, error) {
	if err := validateTenant(tenant); err != nil {
		return nil, err
	}
	if _, err := artifact.Digest(id); err != nil {
		return nil, fmt.Errorf("volume: invalid artifact: %w", err)
	}
	tenantRoot, err := openRealRoot(r.root, tenant)
	if err != nil {
		return nil, err
	}
	defer tenantRoot.Close()
	before, err := tenantRoot.Lstat(id)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, errors.Join(ErrUnsafePath, errors.New("volume: artifact source must be a real directory"))
	}
	f, err := tenantRoot.Open(id)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	after, lstatErr := tenantRoot.Lstat(id)
	if lstatErr != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, after) || !os.SameFile(after, info) {
		f.Close()
		return nil, errors.Join(ErrUnsafePath, lstatErr, errors.New("volume: artifact source changed or traversed a symlink"))
	}
	return f, nil
}

func openRealRoot(parent *os.Root, name string) (*os.Root, error) {
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, errors.Join(ErrUnsafePath, errors.New("volume: source path must be a real directory"))
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, statErr := child.Stat(".")
	after, lstatErr := parent.Lstat(name)
	if statErr != nil || lstatErr != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, after) || !os.SameFile(after, opened) {
		child.Close()
		return nil, errors.Join(ErrUnsafePath, statErr, lstatErr, errors.New("volume: source path changed or traversed a symlink"))
	}
	return child, nil
}

// Close releases the resolver root descriptor.
func (r *DirectoryResolver) Close() error { return r.root.Close() }

func validateSegment(name, value string) error {
	if value == "" || len(value) > 255 || value == "." || value == ".." {
		return fmt.Errorf("volume: invalid %s", name)
	}
	for _, c := range value {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("._-", c) {
			continue
		}
		return fmt.Errorf("volume: invalid %s", name)
	}
	return nil
}

func validateTenant(value string) error {
	if value == "" || len(value) > 128 || value == "." || value == ".." {
		return errors.New("volume: invalid tenant")
	}
	for _, c := range value {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("._:@-", c) {
			continue
		}
		return errors.New("volume: invalid tenant")
	}
	return nil
}

func cleanMountPath(value string) (string, string, error) {
	if value == "" || !strings.HasPrefix(value, "/") || len(value) > 4096 {
		return "", "", ErrUnsafePath
	}
	clean := path.Clean(value)
	if clean != value || clean == "/" {
		return "", "", ErrUnsafePath
	}
	rel := strings.TrimPrefix(clean, "/")
	if rel == ".remount" || strings.HasPrefix(rel, ".remount/") {
		return "", "", ErrUnsafePath
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return "", "", ErrUnsafePath
		}
	}
	return clean, rel, nil
}
