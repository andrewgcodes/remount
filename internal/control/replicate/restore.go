package replicate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Restorer verifies a committed recovery point, performs SQLite recovery in a
// same-directory staging file, and atomically replaces the destination.
type Restorer struct {
	store Store
	lease *Coordinator
	opts  Options
}

// NewRestorer constructs a promotion restorer.
func NewRestorer(store Store, lease *Coordinator, options Options) (*Restorer, error) {
	if store == nil || lease == nil {
		return nil, errors.New("control replication: store and lease are required")
	}
	opts, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Restorer{store: store, lease: lease, opts: opts}, nil
}

// Promote acquires a higher epoch, restores the last committed pointer, and
// renews immediately before the atomic database replacement. The returned
// metadata must be emitted only after node-authoritative reconciliation.
func (r *Restorer) Promote(ctx context.Context, destination string) (Recovery, error) {
	epoch, previous, err := r.lease.Acquire(ctx)
	if err != nil {
		return Recovery{}, err
	}
	manifest, err := r.loadManifest(ctx)
	if err != nil {
		return Recovery{}, err
	}
	if manifest.Epoch > previous {
		return Recovery{}, fmt.Errorf("%w: manifest epoch %d exceeds predecessor %d", ErrCorruptRecoveryPoint, manifest.Epoch, previous)
	}
	stage, err := r.materialize(ctx, manifest, destination)
	if err != nil {
		return Recovery{}, err
	}
	defer os.Remove(stage)
	guardEpoch, err := r.lease.Guard(ctx)
	if err != nil {
		return Recovery{}, err
	}
	if guardEpoch != epoch {
		return Recovery{}, ErrFenced
	}
	if err := atomicReplace(stage, destination); err != nil {
		return Recovery{}, err
	}
	now := r.opts.Now().UTC()
	lost := now.Sub(manifest.CapturedAt)
	if lost < 0 {
		lost = 0
	}
	return Recovery{Epoch: epoch, PreviousEpoch: previous, Manifest: manifest, PromotedAt: now, LastReplicatedAt: manifest.CapturedAt, LostWindow: lost, NeedsReconcile: true}, nil
}

func (r *Restorer) loadManifest(ctx context.Context) (Manifest, error) {
	var pointer Manifest
	_, err := readJSON(ctx, r.store, r.opts.Prefix+"/current.json", r.opts.MaxManifestBytes, &pointer)
	if isNotFound(err) {
		return Manifest{}, ErrNoRecoveryPoint
	}
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(pointer, r.opts); err != nil {
		return Manifest{}, err
	}
	var immutable Manifest
	_, err = readJSON(ctx, r.store, pointer.Key, r.opts.MaxManifestBytes, &immutable)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(immutable, r.opts); err != nil {
		return Manifest{}, err
	}
	a, _ := json.Marshal(pointer)
	b, _ := json.Marshal(immutable)
	if !bytes.Equal(a, b) {
		return Manifest{}, fmt.Errorf("%w: pointer does not match immutable manifest", ErrCorruptRecoveryPoint)
	}
	return pointer, nil
}

func (r *Restorer) materialize(ctx context.Context, m Manifest, destination string) (string, error) {
	if destination == "" {
		return "", errors.New("control replication: empty restore destination")
	}
	dir, base := filepath.Dir(destination), filepath.Base(destination)
	stageFile, err := os.CreateTemp(dir, "."+base+".restore-*.db")
	if err != nil {
		return "", err
	}
	stage := stageFile.Name()
	if err := stageFile.Close(); err != nil {
		os.Remove(stage)
		return "", err
	}
	os.Remove(stage)
	if err := r.fetch(ctx, m.Snapshot, stage, r.opts.MaxSnapshotBytes); err != nil {
		os.Remove(stage)
		return "", err
	}
	walPath := stage + "-wal"
	if m.WAL != nil {
		if err := r.fetch(ctx, *m.WAL, walPath, r.opts.MaxWALBytes); err != nil {
			os.Remove(stage)
			return "", err
		}
		wal, err := os.Open(walPath)
		if err != nil {
			os.Remove(stage)
			os.Remove(walPath)
			return "", err
		}
		validateErr := validateStoredWAL(wal, m.WAL.Size, r.opts.MaxWALBytes, m.WALLineage)
		closeErr := wal.Close()
		if err := errors.Join(validateErr, closeErr); err != nil {
			os.Remove(stage)
			os.Remove(walPath)
			return "", err
		}
	}
	db, err := sql.Open("sqlite", stage)
	if err != nil {
		os.Remove(stage)
		os.Remove(walPath)
		return "", err
	}
	db.SetMaxOpenConns(1)
	var check string
	err = db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&check)
	if err == nil && check != "ok" {
		err = fmt.Errorf("SQLite quick_check: %s", check)
	}
	if err == nil {
		var busy, log, done int
		err = db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &log, &done)
		if err == nil && (busy != 0 || log != done) {
			err = errors.New("SQLite recovery checkpoint incomplete")
		}
	}
	err = errors.Join(err, db.Close())
	os.Remove(walPath)
	os.Remove(stage + "-shm")
	if err != nil {
		os.Remove(stage)
		return "", fmt.Errorf("%w: %v", ErrCorruptRecoveryPoint, err)
	}
	file, err := os.OpenFile(stage, os.O_RDWR, 0)
	if err != nil {
		os.Remove(stage)
		return "", err
	}
	err = errors.Join(file.Sync(), file.Close())
	if err != nil {
		os.Remove(stage)
		return "", err
	}
	return stage, nil
}

func (r *Restorer) fetch(ctx context.Context, ref ObjectRef, destination string, max int64) error {
	if ref.Size < 1 || ref.Size > max {
		return ErrCorruptRecoveryPoint
	}
	reader, info, err := r.store.OpenObject(ctx, ref.Key)
	if err != nil {
		return err
	}
	defer reader.Close()
	if info.Size != ref.Size {
		return fmt.Errorf("%w: object size mismatch", ErrCorruptRecoveryPoint)
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(reader, max+1))
	err = errors.Join(copyErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if n != ref.Size || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("%w: object digest mismatch", ErrCorruptRecoveryPoint)
	}
	return nil
}

func atomicReplace(stage, destination string) error {
	for _, sidecar := range []string{destination + "-wal", destination + "-shm"} {
		if _, err := os.Lstat(sidecar); err == nil {
			return fmt.Errorf("control replication: destination sidecar %s exists", sidecar)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(stage, destination); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(destination))
}
