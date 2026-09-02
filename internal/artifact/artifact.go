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

	"remount.dev/remount/internal/metrics"
)

// Prefix of every artifact id.
const Prefix = "art_sha256:"

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

// Store is a directory of blobs named by digest.
type Store struct {
	dir string
}

// NewStore creates the directory if needed.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) pathFor(digest string) string {
	return filepath.Join(s.dir, digest[:2], digest)
}

// Put stores the stream and returns its id. Content is hashed while written;
// an existing blob is left untouched (it is identical by construction).
func (s *Store) Put(r io.Reader) (string, int64, error) {
	tmp, err := os.CreateTemp(s.dir, ".put-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", 0, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	dst := s.pathFor(digest)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return "", 0, err
	}
	if _, err := os.Stat(dst); err == nil {
		return ID(h.Sum(nil)), n, nil
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return "", 0, err
	}
	return ID(h.Sum(nil)), n, nil
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

// Has reports whether id is present.
func (s *Store) Has(id string) bool {
	digest, err := Digest(id)
	if err != nil {
		return false
	}
	_, err = os.Stat(s.pathFor(digest))
	return err == nil
}

// Verify re-hashes a stored blob.
func (s *Store) Verify(id string) error {
	r, _, err := s.Open(id)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	if ID(h.Sum(nil)) != id {
		metrics.ArtifactMiss.Inc()
		return errors.New("artifact: digest mismatch")
	}
	return nil
}

// Delete removes a blob.
func (s *Store) Delete(id string) error {
	digest, err := Digest(id)
	if err != nil {
		return err
	}
	return os.Remove(s.pathFor(digest))
}

// List returns all ids.
func (s *Store) List() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		if len(d.Name()) == 64 {
			out = append(out, Prefix+d.Name())
		}
		return nil
	})
	sort.Strings(out)
	return out, err
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
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	var paths []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if Excluded(rel, excludes) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(paths)
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, rel := range paths {
		host := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Lstat(host)
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(host)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		if info.IsDir() {
			hdr.Name += "/"
		}
		hdr.Uid, hdr.Gid = 0, 0
		hdr.Uname, hdr.Gname = "", ""
		hdr.AccessTime, hdr.ChangeTime = hdr.ModTime, hdr.ModTime
		hdr.Format = tar.FormatPAX
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(host)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			f.Close()
			if err != nil {
				return err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// Restore extracts a snapshot into root, which must exist. Entries that
// would escape root are rejected.
func Restore(root string, r io.Reader) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	type deferredMode struct {
		path string
		mode os.FileMode
	}
	var dirs []deferredMode
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		for _, comp := range strings.Split(hdr.Name, "/") {
			if comp == ".." {
				return fmt.Errorf("artifact: entry %q escapes root", hdr.Name)
			}
		}
		name := path.Clean("/" + hdr.Name)
		if name == "/" {
			continue
		}
		host := filepath.Join(root, filepath.FromSlash(name))
		if !strings.HasPrefix(host, root+string(filepath.Separator)) {
			return fmt.Errorf("artifact: entry %q escapes root", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(host, 0o755); err != nil {
				return err
			}
			dirs = append(dirs, deferredMode{host, os.FileMode(hdr.Mode).Perm()})
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(host, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode).Perm()|0o200)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			_ = os.Chmod(host, os.FileMode(hdr.Mode).Perm())
			_ = os.Chtimes(host, hdr.ModTime, hdr.ModTime)
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
				return err
			}
			_ = os.Remove(host)
			if err := os.Symlink(hdr.Linkname, host); err != nil {
				return err
			}
		default:
			// Devices, fifos, hard links: not part of a portable snapshot.
		}
	}
	// Apply directory modes last so read-only dirs don't block extraction.
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Chmod(dirs[i].path, dirs[i].mode)
	}
	return nil
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
