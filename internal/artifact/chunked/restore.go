package chunked

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"remount.dev/remount/internal/artifact"
)

// LoadManifest fetches, hashes, decodes, and canonicality-checks a manifest.
func LoadManifest(store artifact.BlobStore, id string, limits Limits) (Manifest, error) {
	if store == nil {
		return Manifest{}, errors.New("chunked artifact: blob store is required")
	}
	limits = normalizedLimits(limits)
	r, size, err := store.Open(id)
	if err != nil {
		return Manifest{}, err
	}
	if size > limits.MaxManifestBytes {
		_ = r.Close()
		return Manifest{}, errors.New("chunked artifact: manifest exceeds limit")
	}
	manifest, _, decodeErr := decodeManifest(r, id, limits)
	closeErr := r.Close()
	return manifest, errors.Join(decodeErr, closeErr)
}

// Restore eagerly fetches and verifies every chunk into a private sibling
// directory, then atomically replaces root only after the complete tree is
// durable.
func Restore(ctx context.Context, store artifact.BlobStore, manifestID, root string, limits Limits) error {
	limits = normalizedLimits(limits)
	manifest, err := LoadManifest(store, manifestID, limits)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("chunked artifact: restore root must be a real directory")
	}
	stage, err := os.MkdirTemp(filepath.Dir(root), ".remount-chunked-restore-*")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()
	if err := materialize(ctx, store, manifest, stage); err != nil {
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
		_ = os.Rename(root, stage)
		_ = os.Rename(backup, root)
		return err
	}
	committed = true
	if err := os.RemoveAll(backup); err == nil {
		_ = syncDir(filepath.Dir(root))
	}
	return nil
}

func materialize(ctx context.Context, store artifact.BlobStore, manifest Manifest, root string) error {
	// Sorted manifests put every parent directory before its descendants.
	var dirs []Entry
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(root, filepath.FromSlash(entry.Path))
		switch entry.Type {
		case "dir":
			if err := os.Mkdir(path, 0o700); err != nil {
				return err
			}
			if err := applyXattrsPath(path, entry.Xattrs); err != nil {
				return err
			}
			dirs = append(dirs, entry)
		case "file":
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			for _, chunk := range entry.Chunks {
				if err := copyChunk(ctx, store, chunk, file); err != nil {
					_ = file.Close()
					return err
				}
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return err
			}
			if err := applyXattrs(file, entry.Xattrs); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Chmod(os.FileMode(entry.Mode)); err != nil {
				_ = file.Close()
				return err
			}
			mtime := time.Unix(0, entry.ModTimeUnix)
			if err := os.Chtimes(path, mtime, mtime); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		case "symlink":
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			if err := os.Symlink(filepath.FromSlash(entry.Link), path); err != nil {
				return err
			}
		default:
			return errors.New("chunked artifact: invalid materialize entry")
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		path := filepath.Join(root, filepath.FromSlash(dirs[i].Path))
		if err := os.Chmod(path, os.FileMode(dirs[i].Mode)); err != nil {
			return err
		}
		mtime := time.Unix(0, dirs[i].ModTimeUnix)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			return err
		}
		if err := syncDir(path); err != nil {
			return err
		}
	}
	return syncDir(root)
}

func copyChunk(ctx context.Context, store artifact.BlobStore, chunk ChunkRef, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, size, err := store.Open(chunk.ID)
	if err != nil {
		return err
	}
	if size != int64(chunk.Size) {
		_ = r.Close()
		return errors.New("chunked artifact: chunk size mismatch")
	}
	h := sha256Writer()
	written, copyErr := io.Copy(io.MultiWriter(dst, h), io.LimitReader(&ctxReader{ctx: ctx, r: r}, int64(chunk.Size)+1))
	closeErr := r.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	if written != int64(chunk.Size) || artifact.ID(h.Sum(nil)) != chunk.ID {
		return fmt.Errorf("%w", artifact.ErrDigestMismatch)
	}
	return nil
}

// ExportTar emits the deterministic full-tar representation used by the
// original artifact snapshot path for doctor and interoperability.
func ExportTar(ctx context.Context, store artifact.BlobStore, manifestID string, w io.Writer, limits Limits) error {
	manifest, err := LoadManifest(store, manifestID, limits)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr := &tar.Header{
			Name: entry.Path, Mode: int64(entry.Mode), ModTime: time.Unix(0, entry.ModTimeUnix),
			AccessTime: time.Unix(0, entry.ModTimeUnix), ChangeTime: time.Unix(0, entry.ModTimeUnix),
			Format: tar.FormatPAX,
		}
		switch entry.Type {
		case "dir":
			hdr.Name += "/"
			hdr.Typeflag = tar.TypeDir
		case "symlink":
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = entry.Link
		case "file":
			hdr.Typeflag = tar.TypeReg
			hdr.Size = entry.Size
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if entry.Type == "file" {
			for _, chunk := range entry.Chunks {
				if err := copyChunk(ctx, store, chunk, tw); err != nil {
					return err
				}
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func sha256Writer() hash.Hash { return sha256.New() }
