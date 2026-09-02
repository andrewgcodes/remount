// Package fsops implements workspace filesystem operations on a host
// directory, jailed so no path can escape the workspace root. All paths in
// requests are workspace-relative or absolute-within-workspace ("/src/a.go"
// and "src/a.go" mean the same file).
package fsops

import (
	"bufio"
	"bytes"
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
	root string
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
	return &FS{root: real}, nil
}

// Root returns the host path of the workspace root.
func (f *FS) Root() string { return f.root }

// Limits.
const (
	DefaultReadLimit = 4 << 20 // bytes returned by a single read
	MaxReadLimit     = 32 << 20
	MaxSearchFile    = 8 << 20 // files larger than this are skipped by search
	DefaultSearchMax = 500
	MaxLineText      = 4096
)

var errEscape = proto.Err(proto.CodeDenied, "path escapes workspace")

// Resolve maps a workspace path to a host path, refusing escapes. Symlinks
// inside the tree are followed only for the existing prefix and must stay
// within the root.
func (f *FS) Resolve(p string) (string, error) {
	clean := path.Clean("/" + strings.ReplaceAll(p, "\\", "/"))
	host := filepath.Join(f.root, filepath.FromSlash(clean))
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
	host, err := f.Resolve(p)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultReadLimit
	}
	if limit > MaxReadLimit {
		limit = MaxReadLimit
	}
	fh, err := os.Open(host)
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
	host, err := f.Resolve(p)
	if err != nil {
		return err
	}
	if mkdirp {
		if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
			return mapErr(err)
		}
	}
	if appendMode {
		fm := os.FileMode(mode)
		if fm == 0 {
			fm = 0o644
		}
		fh, err := os.OpenFile(host, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fm)
		if err != nil {
			return mapErr(err)
		}
		defer fh.Close()
		_, err = fh.Write(data)
		return mapErr(err)
	}
	fm := os.FileMode(mode)
	if st, err := os.Stat(host); err == nil {
		if st.IsDir() {
			return proto.Err(proto.CodeBadRequest, "is a directory")
		}
		if fm == 0 {
			fm = st.Mode().Perm()
		}
	}
	if fm == 0 {
		fm = 0o644
	}
	tmp, err := os.CreateTemp(filepath.Dir(host), ".remount-*")
	if err != nil {
		return mapErr(err)
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
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
	if err := os.Rename(tmpName, host); err != nil {
		return mapErr(err)
	}
	ok = true
	return nil
}

// List returns directory entries sorted by name.
func (f *FS) List(p string) ([]proto.FSEntry, error) {
	host, err := f.Resolve(p)
	if err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(host)
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
	host, err := f.Resolve(p)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(host)
	if err != nil {
		return nil, mapErr(err)
	}
	e := entry(filepath.Base(host), info)
	return &e, nil
}

// Mkdir creates a directory and parents.
func (f *FS) Mkdir(p string) error {
	host, err := f.Resolve(p)
	if err != nil {
		return err
	}
	return mapErr(os.MkdirAll(host, 0o755))
}

// Remove deletes a file, or a tree when recursive.
func (f *FS) Remove(p string, recursive bool) error {
	host, err := f.Resolve(p)
	if err != nil {
		return err
	}
	if host == f.root {
		return proto.Err(proto.CodeDenied, "refusing to remove workspace root")
	}
	if recursive {
		return mapErr(os.RemoveAll(host))
	}
	return mapErr(os.Remove(host))
}

// Rename moves within the workspace.
func (f *FS) Rename(from, to string) error {
	a, err := f.Resolve(from)
	if err != nil {
		return err
	}
	b, err := f.Resolve(to)
	if err != nil {
		return err
	}
	return mapErr(os.Rename(a, b))
}

// Search greps for an RE2 pattern under p, optionally filtered by a filename
// glob. Binary files (NUL in the first 8 KiB) and oversized files are skipped.
func (f *FS) Search(p, pattern, glob string, max int) (*proto.FSSearchRes, error) {
	host, err := f.Resolve(p)
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
	walkErr := filepath.WalkDir(host, func(fp string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable: skip
		}
		if d.IsDir() {
			if d.Name() == ".git" && fp != host {
				return filepath.SkipDir
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
		fh, err := os.Open(fp)
		if err != nil {
			return nil
		}
		defer fh.Close()
		rel, _ := filepath.Rel(f.root, fp)
		rel = "/" + filepath.ToSlash(rel)
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
	host, err := f.Resolve(p)
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(host)
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
	}
	st, _ := os.Stat(host)
	mode := uint32(0o644)
	if st != nil {
		mode = uint32(st.Mode().Perm())
	}
	if err := f.Write(p, []byte(s), mode, false, false); err != nil {
		return 0, err
	}
	return total, nil
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
	case errors.Is(err, fs.ErrNotExist):
		return proto.Err(proto.CodeNotFound, "%v", err)
	case errors.Is(err, fs.ErrPermission):
		return proto.Err(proto.CodeDenied, "%v", err)
	case errors.Is(err, fs.ErrExist):
		return proto.Err(proto.CodeConflict, "%v", err)
	}
	return proto.Err(proto.CodeInternal, "%v", err)
}
