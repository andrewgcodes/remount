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
	"time"

	"remount.dev/remount/internal/metrics"
)

// Prefix of every artifact id.
const Prefix = "art_sha256:"

var (
	// ErrDigestMismatch means a caller-supplied artifact id did not match the
	// bytes received. The temporary bytes are never published in this case.
	ErrDigestMismatch = errors.New("artifact: digest mismatch")
	// ErrTooLarge means an input exceeded the configured compressed-byte
	// limit. At most limit+1 bytes are read before the upload is rejected.
	ErrTooLarge = errors.New("artifact: compressed data exceeds limit")
)

// RestoreLimits bounds the resources consumed while expanding an artifact.
// The compressed size is bounded by the artifact store; these limits protect
// the node from a small gzip stream expanding until it fills the workspace
// volume.
type RestoreLimits struct {
	MaxExpandedBytes int64
	MaxEntries       int
	MaxPathBytes     int
	MaxDepth         int
}

// DefaultRestoreLimits are deliberately generous enough for ordinary source
// trees while still putting a finite ceiling on hostile archives.
var DefaultRestoreLimits = RestoreLimits{
	MaxExpandedBytes: 8 << 30,
	MaxEntries:       1_000_000,
	MaxPathBytes:     4096,
	MaxDepth:         256,
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
	if _, err := Digest(expected); err != nil {
		return 0, err
	}
	_, n, err := s.put(r, expected, maxBytes)
	return n, err
}

func (s *Store) put(r io.Reader, expected string, maxBytes int64) (string, int64, error) {
	tmp, err := os.CreateTemp(s.dir, ".put-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	source := r
	if maxBytes > 0 {
		source = &io.LimitedReader{R: r, N: maxBytes + 1}
	}
	n, err := io.Copy(io.MultiWriter(tmp, h), source)
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
	if _, err := os.Stat(dst); err == nil {
		return got, n, nil
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return "", 0, err
	}
	return got, n, nil
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
	rr, err := os.OpenRoot(root)
	if err != nil {
		return err
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
		if Excluded(rel, excludes) {
			if d.IsDir() {
				return fs.SkipDir
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
		name := filepath.FromSlash(rel)
		info, err := rr.Lstat(name)
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = rr.Readlink(name)
			if err != nil {
				return err
			}
			if err := validateSymlinkTarget(rel, link); err != nil {
				return err
			}
		}
		var file *os.File
		if info.Mode().IsRegular() {
			// Open first and derive the header from that descriptor. The path
			// can change while a workspace is live; using one descriptor for
			// metadata and bytes prevents a stale-size tar header.
			file, err = openSnapshotFile(rr, name)
			if err != nil {
				return err
			}
			opened, statErr := file.Stat()
			if statErr != nil {
				file.Close()
				return statErr
			}
			if !opened.Mode().IsRegular() {
				file.Close()
				return fmt.Errorf("artifact: %q changed type during snapshot", rel)
			}
			info = opened
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			if file != nil {
				file.Close()
			}
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
			if file != nil {
				file.Close()
			}
			return err
		}
		if file != nil {
			_, err = io.CopyN(tw, file, hdr.Size)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
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
	if l.MaxExpandedBytes > 0 {
		d.MaxExpandedBytes = l.MaxExpandedBytes
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
	return d
}

func extract(root string, r io.Reader, limits RestoreLimits) error {
	gz, err := gzip.NewReader(r)
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
		case tar.TypeReg, tar.TypeRegA:
			if err := rr.MkdirAll(filepath.Dir(osName), 0o755); err != nil {
				return err
			}
			f, err := rr.OpenFile(osName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(hdr.Mode).Perm()|0o200)
			if err != nil {
				return err
			}
			if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
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
