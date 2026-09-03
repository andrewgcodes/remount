// Package localfs turns a directory on the user's machine into a workspace
// artifact and back. Pack honors .gitignore and .remountignore plus a short
// list of reproducible directories, and writes through the same tar writer
// as workspace snapshots so Restore and ApplyOverlay accept its output
// unchanged. Unpack is the inverse for `remount pull`: it writes what differs
// and never deletes.
package localfs

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"remount.dev/remount/internal/artifact"
)

// DefaultExcludes are directories every build can regenerate. They are
// skipped at any depth unless PackOptions.NoDefaultExcludes is set.
var DefaultExcludes = []string{"node_modules", ".venv", "target", "dist", "__pycache__"}

// GitPackWarnBytes is the size above which .git/objects/pack is left out of
// the archive with a warning. Past that point the history dominates the
// upload and the agent still has the working tree, index and refs.
const GitPackWarnBytes = 100 << 20

// PackOptions tune Pack.
type PackOptions struct {
	// ExcludeGit drops .git entirely. The default keeps it so the agent can
	// commit inside the workspace.
	ExcludeGit bool
	// Excludes are additional glob patterns in artifact.Excluded syntax.
	Excludes []string
	// NoDefaultExcludes disables DefaultExcludes.
	NoDefaultExcludes bool
	// NoIgnoreFiles disables .gitignore and .remountignore processing.
	NoIgnoreFiles bool
}

// Manifest describes what Pack wrote.
type Manifest struct {
	Files    int      `json:"files"`
	Dirs     int      `json:"dirs"`
	Symlinks int      `json:"symlinks"`
	Bytes    int64    `json:"bytes"`
	Excluded int      `json:"excluded"`
	Warnings []string `json:"warnings,omitempty"`
}

// Pack writes a deterministic tar.gz of dir to w. The root directory itself
// is not an entry; `.remount` is always omitted because the node owns it.
func Pack(dir string, opts PackOptions, w io.Writer) (Manifest, error) {
	var m Manifest
	dir, err := filepath.Abs(dir)
	if err != nil {
		return m, err
	}
	st, err := os.Stat(dir)
	if err != nil {
		return m, err
	}
	if !st.IsDir() {
		return m, fmt.Errorf("localfs: %s is not a directory", dir)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return m, err
	}
	defer root.Close()
	excludes := append([]string(nil), opts.Excludes...)
	if !opts.NoDefaultExcludes {
		excludes = append(excludes, DefaultExcludes...)
	}
	var ig *ignorer
	if !opts.NoIgnoreFiles {
		ig = newIgnorer(root)
	}
	skipGitPack := false
	if !opts.ExcludeGit {
		size, err := dirSize(root, ".git/objects/pack")
		if err == nil && size > GitPackWarnBytes {
			skipGitPack = true
			m.Warnings = append(m.Warnings, fmt.Sprintf(".git/objects/pack is %d MiB; left out of the upload (working tree, index and refs are included)", size>>20))
		}
	}
	skip := func(rel string, isDir bool) bool {
		if rel == artifact.OverlayStageDir || strings.HasPrefix(rel, artifact.OverlayStageDir+"/") {
			return true
		}
		if rel == ".git" || strings.HasPrefix(rel, ".git/") {
			if opts.ExcludeGit {
				return true
			}
			if skipGitPack && rel == ".git/objects/pack" {
				return true
			}
			// Ignore files never apply inside .git.
			return false
		}
		if artifact.Excluded(rel, excludes) {
			return true
		}
		return ig != nil && ig.ignored(rel, isDir)
	}
	stats, err := artifact.SnapshotFiltered(dir, skip, w)
	if err != nil {
		return m, err
	}
	m.Files, m.Dirs, m.Symlinks, m.Bytes, m.Excluded = stats.Files, stats.Dirs, stats.Symlinks, stats.Bytes, stats.Skipped
	return m, nil
}

func dirSize(root *os.Root, rel string) (int64, error) {
	var total int64
	err := fs.WalkDir(root.FS(), filepath.FromSlash(rel), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// UnpackOptions tune Unpack.
type UnpackOptions struct {
	// Force proceeds even when dir has uncommitted changes (or is a non-empty
	// directory that is not a git checkout).
	Force bool
	// Limits bound archive expansion; zero fields take artifact defaults.
	Limits artifact.RestoreLimits
}

// UnpackResult reports what Unpack changed locally.
type UnpackResult struct {
	Written   []string `json:"written"`           // files created or replaced
	Unchanged int      `json:"unchanged"`         // files already byte-identical
	Extra     []string `json:"extra,omitempty"`   // local files absent from the snapshot; never removed
	Dirty     bool     `json:"dirty"`             // dir had local changes and Force was set
	Bytes     int64    `json:"bytes"`             // bytes written
	Skipped   []string `json:"skipped,omitempty"` // entries refused (type conflicts, .remount)
}

// ErrDirty means the local directory has changes Unpack would overwrite.
var ErrDirty = errors.New("localfs: local directory has uncommitted changes (use --force to overwrite)")

// Unpack expands a snapshot archive into dir, replacing files whose bytes
// differ and creating the ones that are missing. Files that exist locally
// but not in the archive are reported, never deleted. Every replaced file is
// written to a temporary sibling and renamed into place. dir is created if
// absent.
func Unpack(dir string, r io.Reader, opts UnpackOptions) (UnpackResult, error) {
	var res UnpackResult
	dir, err := filepath.Abs(dir)
	if err != nil {
		return res, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return res, err
	}
	dirty, err := isDirty(dir)
	if err != nil {
		return res, err
	}
	if dirty {
		if !opts.Force {
			return res, ErrDirty
		}
		res.Dirty = true
	}
	stage, err := os.MkdirTemp(dir, ".remount-pull-*")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(stage)
	if err := artifact.RestoreWithLimits(stage, r, opts.Limits); err != nil {
		return res, err
	}
	src, err := os.OpenRoot(stage)
	if err != nil {
		return res, err
	}
	defer src.Close()
	dst, err := os.OpenRoot(dir)
	if err != nil {
		return res, err
	}
	defer dst.Close()
	stageRel := filepath.Base(stage)
	inArchive := map[string]bool{}
	err = fs.WalkDir(src.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		rel := filepath.ToSlash(p)
		inArchive[rel] = true
		if rel == artifact.OverlayStageDir || strings.HasPrefix(rel, artifact.OverlayStageDir+"/") {
			res.Skipped = append(res.Skipped, rel)
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			if st, err := dst.Lstat(p); err == nil && !st.IsDir() {
				res.Skipped = append(res.Skipped, rel)
				return fs.SkipDir
			}
			return dst.MkdirAll(p, 0o755)
		}
		if st, err := dst.Lstat(p); err == nil && st.IsDir() {
			res.Skipped = append(res.Skipped, rel)
			return nil
		}
		same, err := sameContent(src, dst, p)
		if err != nil {
			return err
		}
		if same {
			res.Unchanged++
			return nil
		}
		info, err := src.Lstat(p)
		if err != nil {
			return err
		}
		if err := dst.Rename(filepath.Join(stageRel, p), p); err != nil {
			return err
		}
		res.Written = append(res.Written, rel)
		if info.Mode().IsRegular() {
			res.Bytes += info.Size()
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	err = fs.WalkDir(dst.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		rel := filepath.ToSlash(p)
		if rel == stageRel || rel == artifact.OverlayStageDir {
			return fs.SkipDir
		}
		if d.IsDir() {
			if inArchive[rel] {
				return nil
			}
			res.Extra = append(res.Extra, rel)
			return fs.SkipDir
		}
		if !inArchive[rel] {
			res.Extra = append(res.Extra, rel)
		}
		return nil
	})
	sort.Strings(res.Extra)
	return res, err
}

// sameContent reports whether p in src and dst are the same kind of entry
// with identical bytes (or symlink target).
func sameContent(src, dst *os.Root, p string) (bool, error) {
	a, err := src.Lstat(p)
	if err != nil {
		return false, err
	}
	b, err := dst.Lstat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if a.Mode().Type() != b.Mode().Type() {
		return false, nil
	}
	if a.Mode()&os.ModeSymlink != 0 {
		la, err := src.Readlink(p)
		if err != nil {
			return false, err
		}
		lb, err := dst.Readlink(p)
		if err != nil {
			return false, err
		}
		return la == lb, nil
	}
	if a.Size() != b.Size() || a.Mode().Perm() != b.Mode().Perm() {
		return false, nil
	}
	ha, err := hashFile(src, p)
	if err != nil {
		return false, err
	}
	hb, err := hashFile(dst, p)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ha, hb), nil
}

func hashFile(root *os.Root, p string) ([]byte, error) {
	f, err := root.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// isDirty decides whether pulling into dir could destroy local work. A git
// checkout is dirty when `git status --porcelain` prints anything; a
// directory that is not a checkout is dirty whenever it is non-empty, because
// there is no way to tell what in it was committed.
func isDirty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		return true, nil
	}
	git, err := exec.LookPath("git")
	if err != nil {
		return true, nil
	}
	cmd := exec.Command(git, "-C", dir, "status", "--porcelain", "--untracked-files=normal")
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.Output()
	if err != nil {
		return true, fmt.Errorf("localfs: git status: %w", err)
	}
	return len(bytes.TrimSpace(out)) > 0, nil
}
