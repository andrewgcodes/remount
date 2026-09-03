package replicate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SQLiteSource captures a live WAL-mode database. The supplied *sql.DB must
// be the writer's own serialized handle; using a second pool could checkpoint
// across an in-flight control transaction.
type SQLiteSource struct {
	db          *sql.DB
	path        string
	tempDir     string
	maxSnapshot int64
	maxWAL      int64
	now         func() time.Time
	mu          sync.Mutex
}

// NewSQLiteSource disables automatic checkpoints on the writer connection so
// a WAL lineage changes only when Full deliberately checkpoints it.
func NewSQLiteSource(db *sql.DB, databasePath, tempDir string, maxSnapshot, maxWAL int64, now func() time.Time) (*SQLiteSource, error) {
	if db == nil || databasePath == "" || databasePath == ":memory:" {
		return nil, errors.New("control replication: a file-backed SQLite writer is required")
	}
	if maxSnapshot < 1 || maxWAL < walHeaderBytes {
		return nil, errors.New("control replication: SQLite capture bounds are invalid")
	}
	abs, err := filepath.Abs(databasePath)
	if err != nil {
		return nil, err
	}
	if tempDir == "" {
		tempDir = filepath.Dir(abs)
	}
	if now == nil {
		now = time.Now
	}
	if _, err := db.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		return nil, fmt.Errorf("disable SQLite auto-checkpoint: %w", err)
	}
	return &SQLiteSource{db: db, path: abs, tempDir: tempDir, maxSnapshot: maxSnapshot, maxWAL: maxWAL, now: now}, nil
}

// Full checkpoints the previous lineage, takes an exact database-file copy
// while BEGIN IMMEDIATE excludes writers, then captures the next WAL lineage.
func (s *SQLiteSource) Full(ctx context.Context) (*Bundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var busy, logFrames, checkpointed int
	if err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		return nil, fmt.Errorf("truncate SQLite WAL: %w", err)
	}
	if busy != 0 || logFrames != checkpointed {
		return nil, fmt.Errorf("control replication: SQLite checkpoint incomplete (busy=%d log=%d checkpointed=%d)", busy, logFrames, checkpointed)
	}
	path, err := vacantTempPath(s.tempDir, "control-full-*.db")
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path)
		}
	}()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return nil, fmt.Errorf("lock SQLite writer for snapshot: %w", err)
	}
	copyErr := copyBoundedFile(s.path, path, s.maxSnapshot)
	_, commitErr := conn.ExecContext(ctx, `COMMIT`)
	if err := errors.Join(copyErr, commitErr); err != nil {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		return nil, fmt.Errorf("snapshot SQLite database: %w", err)
	}
	snapshot, err := payloadFromStableFile(path, s.maxSnapshot, "")
	if err != nil {
		return nil, err
	}
	keep = true
	wal, err := s.captureWALLocked("")
	if err != nil {
		snapshot.Close()
		return nil, err
	}
	return &Bundle{Snapshot: snapshot, WAL: wal, At: s.now().UTC()}, nil
}

func copyBoundedFile(source, destination string, max int64) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	stat, err := in.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < 1 || stat.Size() > max {
		return fmt.Errorf("control replication: snapshot size %d outside configured bound %d", stat.Size(), max)
	}
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyN(out, in, stat.Size())
	return errors.Join(copyErr, out.Sync(), out.Close())
}

// WAL captures exactly through the last checksum-valid commit frame.
func (s *SQLiteSource) WAL(_ context.Context, expectedLineage string) (*Payload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captureWALLocked(expectedLineage)
}

func (s *SQLiteSource) captureWALLocked(expectedLineage string) (*Payload, error) {
	walPath := s.path + "-wal"
	for attempt := 0; attempt < 3; attempt++ {
		file, err := os.Open(walPath)
		if errors.Is(err, os.ErrNotExist) {
			if expectedLineage != "" {
				return nil, ErrWALReset
			}
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		if stat.Size() == 0 {
			file.Close()
			if expectedLineage != "" {
				return nil, ErrWALReset
			}
			return nil, nil
		}
		inspection, err := inspectWAL(file, stat.Size(), s.maxWAL, false)
		if err != nil {
			file.Close()
			return nil, err
		}
		if expectedLineage != "" && inspection.Lineage != expectedLineage {
			file.Close()
			return nil, ErrWALReset
		}
		if inspection.CommittedBytes == 0 {
			file.Close()
			return nil, nil
		}
		path, err := vacantTempPath(s.tempDir, "control-wal-*.wal")
		if err != nil {
			file.Close()
			return nil, err
		}
		destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			file.Close()
			return nil, err
		}
		_, copyErr := io.CopyN(destination, io.NewSectionReader(file, 0, inspection.CommittedBytes), inspection.CommittedBytes)
		syncErr := destination.Sync()
		closeErr := errors.Join(destination.Close(), file.Close())
		if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
			_ = os.Remove(path)
			return nil, err
		}
		payload, err := payloadFromStableFile(path, s.maxWAL, inspection.Lineage)
		if err != nil {
			_ = os.Remove(path)
			if errors.Is(err, ErrCorruptRecoveryPoint) {
				continue
			}
			return nil, err
		}
		captured := payload.Reader.(*removingFile)
		if err := validateStoredWAL(captured.File, payload.Size, s.maxWAL, inspection.Lineage); err != nil {
			payload.Close()
			continue
		}
		if _, err := captured.Seek(0, io.SeekStart); err != nil {
			payload.Close()
			return nil, err
		}
		return payload, nil
	}
	return nil, errors.New("control replication: WAL changed during three capture attempts")
}

type removingFile struct {
	*os.File
	path string
}

func (f *removingFile) Close() error { return errors.Join(f.File.Close(), os.Remove(f.path)) }

func vacantTempPath(dir, pattern string) (string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(path); err != nil {
		return "", err
	}
	return path, nil
}

func payloadFromStableFile(path string, max int64, lineage string) (*Payload, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if stat.Size() < 1 || stat.Size() > max {
		file.Close()
		return nil, fmt.Errorf("control replication: capture size %d outside configured bound %d", stat.Size(), max)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return &Payload{Reader: &removingFile{File: file, path: path}, Size: stat.Size(), SHA256: hex.EncodeToString(hash.Sum(nil)), Lineage: lineage}, nil
}
