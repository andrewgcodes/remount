package compliance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	stagingPrefix       = ".remount-audit-staging-"
	defaultMaxFileBytes = int64(8<<30) + 1<<20
	maximumMaxFileBytes = int64(1 << 40)
)

// CommitError means the final name became visible but a later durability or
// staging-cleanup step failed. Callers must inspect/verify instead of blindly
// retrying an overwrite.
type CommitError struct {
	Err       error
	Committed bool
}

func (e *CommitError) Error() string {
	return "compliance: atomic bundle commit incomplete: " + e.Err.Error()
}

// Unwrap exposes the filesystem error without losing the committed flag.
func (e *CommitError) Unwrap() error { return e.Err }

// AtomicFileSink writes a mode-0600 temporary file in the destination
// directory, syncs it, and atomically publishes the completed bundle.
type AtomicFileSink struct {
	Path     string
	Replace  bool
	MaxBytes int64
}

// Commit implements BundleSink. With Replace false an existing destination is
// never overwritten. With Replace true the old path remains intact until the
// new complete file is atomically renamed over it.
func (s *AtomicFileSink) Commit(ctx context.Context, produce func(io.Writer) error) error {
	if s == nil || produce == nil || s.Path == "" || strings.ContainsRune(s.Path, '\x00') {
		return errors.New("compliance: destination path and producer are required")
	}
	maximum := s.MaxBytes
	if maximum == 0 {
		maximum = defaultMaxFileBytes
	}
	if maximum < 1 || maximum > maximumMaxFileBytes {
		return errors.New("compliance: invalid atomic-file byte bound")
	}
	path := filepath.Clean(s.Path)
	if filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return errors.New("compliance: destination must name a file")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	temporary, err := os.CreateTemp(parent, stagingPrefix)
	if err != nil {
		return fmt.Errorf("compliance: create staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("compliance: protect staging file: %w", err)
	}
	bounded := &boundedWriter{writer: temporary, remaining: maximum}
	if err := produce(bounded); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("compliance: sync staging file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("compliance: close staging file: %w", err)
	}
	if s.Replace {
		if err := os.Rename(temporaryPath, path); err != nil {
			return fmt.Errorf("compliance: publish bundle: %w", err)
		}
		committed = true
	} else {
		if err := os.Link(temporaryPath, path); err != nil {
			return fmt.Errorf("compliance: publish bundle without overwrite: %w", err)
		}
		committed = true
		cleanupErr := os.Remove(temporaryPath)
		syncErr := syncDirectory(parent)
		if cleanupErr != nil || syncErr != nil {
			return &CommitError{Err: errors.Join(cleanupErr, syncErr), Committed: true}
		}
		return nil
	}
	if err := syncDirectory(parent); err != nil {
		return &CommitError{Err: err, Committed: true}
	}
	return nil
}

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, ErrLimit
	}
	written, err := w.writer.Write(data)
	w.remaining -= int64(written)
	return written, err
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// CleanupStaging removes at most limit owned, regular staging files older than
// before. Symlinks, directories, unrelated files, and recent exports remain.
func (s *AtomicFileSink) CleanupStaging(ctx context.Context, before time.Time, limit int) (int, error) {
	if s == nil || s.Path == "" || before.IsZero() || limit < 1 || limit > 1024 {
		return 0, errors.New("compliance: invalid staging cleanup request")
	}
	parent := filepath.Dir(filepath.Clean(s.Path))
	directory, err := os.Open(parent)
	if err != nil {
		return 0, err
	}
	defer directory.Close()
	removed := 0
	scanned := 0
	exhausted := false
	for removed < limit && scanned < 8192 {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			scanned++
			if err := ctx.Err(); err != nil {
				return removed, err
			}
			if !strings.HasPrefix(entry.Name(), stagingPrefix) || entry.Type()&fs.ModeSymlink != 0 || entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return removed, err
			}
			if !info.Mode().IsRegular() || !info.ModTime().Before(before) {
				continue
			}
			candidate := filepath.Join(parent, entry.Name())
			if err := os.Remove(candidate); err != nil {
				return removed, err
			}
			removed++
			if removed >= limit {
				break
			}
		}
		if errors.Is(readErr, io.EOF) {
			exhausted = true
			break
		}
		if readErr != nil {
			return removed, readErr
		}
	}
	if removed < limit && !exhausted && scanned >= 8192 {
		return removed, ErrLimit
	}
	return removed, nil
}

var _ BundleSink = (*AtomicFileSink)(nil)
