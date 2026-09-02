// Package fsops implements workspace filesystem operations on a host
// directory, jailed so no path can escape the workspace root. All paths in
// requests are workspace-relative or absolute-within-workspace ("/src/a.go"
// and "src/a.go" mean the same file).
package fsops

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"remount.dev/remount/internal/proto"
)

// FS is a jailed view of a directory tree.
type FS struct {
	root   string
	handle *os.Root
}

// New returns an FS rooted at dir (which must exist).
func New(dir string) (*FS, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	handle, err := os.OpenRoot(real)
	if err != nil {
		return nil, err
	}
	return &FS{root: real, handle: handle}, nil
}

// Root returns the host path of the workspace root.
func (f *FS) Root() string { return f.root }

// Close releases the directory handle used to make operations race-safe.
func (f *FS) Close() error { return f.handle.Close() }

// Limits.
const (
	DefaultReadLimit = 1 << 20 // bytes returned by a single read
	MaxReadLimit     = 3 << 20 // leaves room for CBOR inside the 4 MiB frame
	MaxSearchFile    = 8 << 20 // files larger than this are skipped by search
	DefaultSearchMax = 500
	MaxLineText      = 4096
)

var errEscape = proto.Err(proto.CodeDenied, "path escapes workspace")

func cleanName(p string) (string, error) {
	if strings.IndexByte(p, 0) >= 0 {
		return "", proto.Err(proto.CodeBadRequest, "path contains NUL")
	}
	clean := path.Clean("/" + strings.ReplaceAll(p, "\\", "/"))
	name := strings.TrimPrefix(clean, "/")
	if name == "" {
		name = "."
	}
	return filepath.FromSlash(name), nil
}

// Resolve maps a workspace path to a host path, refusing escapes. Symlinks
// inside the tree are followed only for the existing prefix and must stay
// within the root.
func (f *FS) Resolve(p string) (string, error) {
	name, err := cleanName(p)
	if err != nil {
		return "", err
	}
	host := filepath.Join(f.root, name)
	// Walk down resolving symlinks of the existing prefix.
	existing := host
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			break
		}
		existing = parent
	}
	real, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	if real != f.root && !strings.HasPrefix(real, f.root+string(filepath.Separator)) {
		return "", errEscape
	}
	if existing != host {
		rest := strings.TrimPrefix(host, existing)
		real = real + rest
	}
	return real, nil
}

// Read returns up to limit bytes from offset.
func (f *FS) Read(p string, offset, limit int64) (*proto.FSReadRes, error) {
	name, err := cleanName(p)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultReadLimit
	}
	if limit > MaxReadLimit {
		limit = MaxReadLimit
	}
	if st, err := f.handle.Lstat(name); err != nil {
		return nil, mapErr(err)
	} else if !st.Mode().IsRegular() {
		return nil, proto.Err(proto.CodeBadRequest, "not a regular file")
	}
	fh, err := openRead(f.handle, name)
	if err != nil {
		return nil, mapErr(err)
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return nil, mapErr(err)
	}
	if st.IsDir() {
		return nil, proto.Err(proto.CodeBadRequest, "is a directory")
	}
	if offset > 0 {
		if _, err := fh.Seek(offset, io.SeekStart); err != nil {
			return nil, mapErr(err)
		}
	}
	buf := make([]byte, limit)
	n, err := io.ReadFull(fh, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, mapErr(err)
	}
	return &proto.FSReadRes{Data: buf[:n], Size: st.Size(), EOF: offset+int64(n) >= st.Size()}, nil
}

// Write creates or replaces a file atomically (write temp + rename), or
// appends. Mode 0 means 0644 for new files (existing mode preserved).
func (f *FS) Write(p string, data []byte, mode uint32, appendMode, mkdirp bool) error {
	name, err := cleanName(p)
	if err != nil {
		return err
	}
	if name == "." {
		return proto.Err(proto.CodeBadRequest, "is a directory")
	}
	parent := filepath.Dir(name)
	if mkdirp {
		if err := f.handle.MkdirAll(parent, 0o755); err != nil {
			return mapErr(err)
		}
	}
	if appendMode {
		if st, err := f.handle.Lstat(name); err == nil && !st.Mode().IsRegular() {
			return proto.Err(proto.CodeBadRequest, "not a regular file")
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return mapErr(err)
		}
		fm := os.FileMode(mode)
		if fm == 0 {
			fm = 0o644
		}
		fh, err := openAppend(f.handle, name, fm)
		if err != nil {
			return mapErr(err)
		}
		if st, statErr := fh.Stat(); statErr != nil || !st.Mode().IsRegular() {
			fh.Close()
			return proto.Err(proto.CodeBadRequest, "not a regular file")
		}
		if _, err = io.Copy(fh, bytes.NewReader(data)); err == nil {
			err = fh.Sync()
		}
		if closeErr := fh.Close(); err == nil {
			err = closeErr
		}
		return mapErr(err)
	}
	fm := os.FileMode(mode)
	if st, err := f.handle.Lstat(name); err == nil {
		if !st.Mode().IsRegular() {
			return proto.Err(proto.CodeBadRequest, "not a regular file")
		}
		if fm == 0 {
			fm = st.Mode().Perm()
		}
	}
	if fm == 0 {
		fm = 0o644
	}
	tmp, tmpName, err := f.createTemp(parent, fm)
	if err != nil {
		return mapErr(err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.handle.Remove(tmpName)
		}
	}()
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		tmp.Close()
		return mapErr(err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return mapErr(err)
	}
	if err := tmp.Chmod(fm); err != nil {
		tmp.Close()
		return mapErr(err)
	}
	if err := tmp.Close(); err != nil {
		return mapErr(err)
	}
	if err := f.handle.Rename(tmpName, name); err != nil {
		return mapErr(err)
	}
	ok = true
	return nil
}

// List returns directory entries sorted by name.
func (f *FS) List(p string) ([]proto.FSEntry, error) {
	name, err := cleanName(p)
	if err != nil {
		return nil, err
	}
	dir, err := f.handle.Open(name)
	if err != nil {
		return nil, mapErr(err)
	}
	defer dir.Close()
	ents, err := dir.ReadDir(-1)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]proto.FSEntry, 0, len(ents))
	for _, e := range ents {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, entry(e.Name(), info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Stat returns one entry.
func (f *FS) Stat(p string) (*proto.FSEntry, error) {
	name, err := cleanName(p)
	if err != nil {
		return nil, err
	}
	info, err := f.handle.Lstat(name)
	if err != nil {
		return nil, mapErr(err)
	}
	entryName := filepath.Base(name)
	if name == "." {
		entryName = filepath.Base(f.root)
	}
	e := entry(entryName, info)
	return &e, nil
}

// Mkdir creates a directory and parents.
func (f *FS) Mkdir(p string) error {
	name, err := cleanName(p)
	if err != nil {
		return err
	}
	return mapErr(f.handle.MkdirAll(name, 0o755))
}

// Remove deletes a file, or a tree when recursive.
func (f *FS) Remove(p string, recursive bool) error {
	name, err := cleanName(p)
	if err != nil {
		return err
	}
	if name == "." {
		return proto.Err(proto.CodeDenied, "refusing to remove workspace root")
	}
	if recursive {
		return mapErr(f.handle.RemoveAll(name))
	}
	return mapErr(f.handle.Remove(name))
}

// Rename moves within the workspace.
func (f *FS) Rename(from, to string) error {
	a, err := cleanName(from)
	if err != nil {
		return err
	}
	b, err := cleanName(to)
	if err != nil {
		return err
	}
	if a == "." || b == "." {
		return proto.Err(proto.CodeDenied, "refusing to rename workspace root")
	}
	return mapErr(f.handle.Rename(a, b))
}

// Search greps for an RE2 pattern under p, optionally filtered by a filename
// glob. Binary files (NUL in the first 8 KiB) and oversized files are skipped.
func (f *FS) Search(p, pattern, glob string, max int) (*proto.FSSearchRes, error) {
	name, err := cleanName(p)
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "bad pattern: %v", err)
	}
	if glob != "" {
		if _, err := path.Match(glob, "x"); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "bad glob: %v", err)
		}
	}
	if max <= 0 {
		max = DefaultSearchMax
	}
	res := &proto.FSSearchRes{}
	walkErr := fs.WalkDir(f.handle.FS(), filepath.ToSlash(name), func(fp string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable: skip
		}
		if d.IsDir() {
			if d.Name() == ".git" && fp != filepath.ToSlash(name) {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if glob != "" {
			if ok, _ := path.Match(glob, d.Name()); !ok {
				return nil
			}
		}
		info, err := d.Info()
		if err != nil || info.Size() > MaxSearchFile {
			return nil
		}
		fh, err := openRead(f.handle, filepath.FromSlash(fp))
		if err != nil {
			return nil
		}
		defer fh.Close()
		rel := "/" + filepath.ToSlash(fp)
		br := bufio.NewReaderSize(fh, 64<<10)
		head, _ := br.Peek(8 << 10)
		if bytes.IndexByte(head, 0) >= 0 {
			return nil
		}
		sc := bufio.NewScanner(br)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		line := 0
		for sc.Scan() {
			line++
			text := sc.Text()
			if re.MatchString(text) {
				if len(text) > MaxLineText {
					text = text[:MaxLineText]
				}
				res.Matches = append(res.Matches, proto.FSMatch{Path: rel, Line: line, Text: text})
				if len(res.Matches) >= max {
					res.Truncated = true
					return errStop
				}
			}
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, errStop) {
		return nil, mapErr(walkErr)
	}
	return res, nil
}

var errStop = errors.New("stop")

// Edit applies find/replace edits atomically. Each non-All edit must match
// exactly once; otherwise nothing is written and an error names the edit.
func (f *FS) Edit(p string, edits []proto.FSEdit) (int, error) {
	name, err := cleanName(p)
	if err != nil {
		return 0, err
	}
	st, err := f.handle.Lstat(name)
	if err != nil {
		return 0, mapErr(err)
	}
	if !st.Mode().IsRegular() {
		return 0, proto.Err(proto.CodeBadRequest, "not a regular file")
	}
	if st.Size() > MaxReadLimit {
		return 0, proto.Err(proto.CodeBadRequest, "file exceeds edit limit of %d bytes", MaxReadLimit)
	}
	data, err := f.handle.ReadFile(name)
	if err != nil {
		return 0, mapErr(err)
	}
	s := string(data)
	total := 0
	for i, e := range edits {
		if e.Old == "" {
			return 0, proto.Err(proto.CodeBadRequest, "edit %d: empty old string", i)
		}
		n := strings.Count(s, e.Old)
		switch {
		case n == 0:
			return 0, proto.Err(proto.CodeConflict, "edit %d: old string not found", i)
		case n > 1 && !e.All:
			return 0, proto.Err(proto.CodeConflict, "edit %d: old string matches %d times; use all", i, n)
		}
		if e.All {
			s = strings.ReplaceAll(s, e.Old, e.New)
			total += n
		} else {
			s = strings.Replace(s, e.Old, e.New, 1)
			total++
		}
		if len(s) > MaxReadLimit {
			return 0, proto.Err(proto.CodeResourceExhausted, "edited file exceeds limit of %d bytes", MaxReadLimit)
		}
	}
	mode := uint32(0o644)
	mode = uint32(st.Mode().Perm())
	if err := f.Write(p, []byte(s), mode, false, false); err != nil {
		return 0, err
	}
	return total, nil
}

func (f *FS) createTemp(parent string, mode os.FileMode) (*os.File, string, error) {
	for i := 0; i < 100; i++ {
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := filepath.Join(parent, ".remount-"+hex.EncodeToString(random[:]))
		file, err := f.handle.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return file, name, err
	}
	return nil, "", proto.Err(proto.CodeInternal, "could not allocate temporary file")
}

func entry(name string, info os.FileInfo) proto.FSEntry {
	return proto.FSEntry{
		Name:    name,
		Size:    info.Size(),
		Mode:    uint32(info.Mode().Perm()),
		IsDir:   info.IsDir(),
		ModTime: info.ModTime().UnixMilli(),
		IsLink:  info.Mode()&os.ModeSymlink != 0,
	}
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var pe *proto.Error
	if errors.As(err, &pe) {
		return err
	}
	switch {
	case strings.Contains(err.Error(), "path escapes from parent"):
		// os.Root intentionally keeps its sentinel private. Translate its
		// containment failure into the protocol's stable denial code.
		return errEscape
	case errors.Is(err, os.ErrInvalid), strings.Contains(err.Error(), "file name too long"), strings.Contains(err.Error(), "not a directory"):
		return proto.Err(proto.CodeBadRequest, "%v", err)
	case errors.Is(err, fs.ErrNotExist):
		return proto.Err(proto.CodeNotFound, "%v", err)
	case errors.Is(err, fs.ErrPermission):
		return proto.Err(proto.CodeDenied, "%v", err)
	case errors.Is(err, fs.ErrExist):
		return proto.Err(proto.CodeConflict, "%v", err)
	}
	return proto.Err(proto.CodeInternal, "%v", err)
}
