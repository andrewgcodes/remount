package replicate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact/s3"
)

// Shipper publishes immutable captures and advances one conditional manifest
// pointer. Publication is serialized so sequence numbers cannot fork locally.
type Shipper struct {
	store       Store
	source      Source
	lease       *Coordinator
	opts        Options
	mu          sync.Mutex
	current     *Manifest
	currentETag string
	lastFull    time.Time
}

// LastManifest returns a copy of the newest recovery point this shipper
// successfully committed in this process.
func (s *Shipper) LastManifest() (Manifest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return Manifest{}, false
	}
	return *s.current, true
}

// NewShipper constructs a bounded shipper. Acquire must succeed before Ship.
func NewShipper(store Store, source Source, lease *Coordinator, options Options) (*Shipper, error) {
	if store == nil || source == nil || lease == nil {
		return nil, errors.New("control replication: store, source, and lease are required")
	}
	opts, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Shipper{store: store, source: source, lease: lease, opts: opts}, nil
}

// Ship captures and conditionally commits a new recovery point. forceFull is
// required on first publication and after any WAL lineage reset.
func (s *Shipper) Ship(ctx context.Context, sourceEventSeq uint64, forceFull bool) (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refresh(ctx); err != nil && !errors.Is(err, ErrNoRecoveryPoint) {
		return Manifest{}, err
	}
	epoch, err := s.lease.CanDecide()
	if err != nil {
		return Manifest{}, err
	}
	if s.current != nil && sourceEventSeq < s.current.SourceEventSeq {
		return Manifest{}, errors.New("control replication: source event sequence regressed")
	}
	if s.current != nil && s.current.Epoch > epoch {
		s.lease.markFenced()
		return Manifest{}, fmt.Errorf("%w: recovery pointer epoch %d exceeds lease epoch %d", ErrFenced, s.current.Epoch, epoch)
	}
	if s.current == nil || s.current.Epoch != epoch || s.opts.Now().Sub(s.lastFull) >= s.opts.SnapshotInterval {
		forceFull = true
	}
	var snapshot *Payload
	var wal *Payload
	var captured time.Time
	if forceFull {
		bundle, err := s.source.Full(ctx)
		if err != nil {
			return Manifest{}, err
		}
		defer bundle.Close()
		snapshot, wal, captured = bundle.Snapshot, bundle.WAL, bundle.At
		if snapshot == nil {
			return Manifest{}, errors.New("control replication: full source returned no snapshot")
		}
	} else {
		wal, err = s.source.WAL(ctx, s.current.WALLineage)
		if errors.Is(err, ErrWALReset) {
			return s.shipFullLocked(ctx, sourceEventSeq)
		}
		if err != nil {
			return Manifest{}, err
		}
		if wal == nil {
			return *s.current, nil
		}
		defer wal.Close()
		captured = s.opts.Now().UTC()
	}
	return s.publish(ctx, epoch, sourceEventSeq, snapshot, wal, captured, forceFull)
}

func (s *Shipper) shipFullLocked(ctx context.Context, sourceEventSeq uint64) (Manifest, error) {
	bundle, err := s.source.Full(ctx)
	if err != nil {
		return Manifest{}, err
	}
	defer bundle.Close()
	if bundle.Snapshot == nil {
		return Manifest{}, errors.New("control replication: full source returned no snapshot")
	}
	epoch, err := s.lease.CanDecide()
	if err != nil {
		return Manifest{}, err
	}
	return s.publish(ctx, epoch, sourceEventSeq, bundle.Snapshot, bundle.WAL, bundle.At, true)
}

func (s *Shipper) publish(ctx context.Context, epoch, eventSeq uint64, snapshot, wal *Payload, captured time.Time, full bool) (Manifest, error) {
	var snapRef ObjectRef
	var err error
	if full {
		snapRef, err = s.putPayload(ctx, epoch, "snapshots", ".db", snapshot)
	} else {
		snapRef = s.current.Snapshot
	}
	if err != nil {
		return Manifest{}, err
	}
	var walRef *ObjectRef
	lineage := ""
	if wal != nil {
		ref, err := s.putPayload(ctx, epoch, "wal", ".wal", wal)
		if err != nil {
			return Manifest{}, err
		}
		walRef, lineage = &ref, wal.Lineage
	}
	if !full && walRef == nil {
		walRef, lineage = s.current.WAL, s.current.WALLineage
	}
	if _, err := s.lease.Guard(ctx); err != nil {
		return Manifest{}, err
	}
	sequence := uint64(1)
	if s.current != nil && s.current.Epoch == epoch {
		if s.current.Sequence == math.MaxUint64 {
			return Manifest{}, errors.New("control replication: manifest sequence exhausted")
		}
		sequence = s.current.Sequence + 1
	}
	m := Manifest{Version: formatVersion, Epoch: epoch, Sequence: sequence, CapturedAt: captured.UTC(), CommittedAt: s.opts.Now().UTC(), Snapshot: snapRef, WAL: walRef, WALLineage: lineage, SourceEventSeq: eventSeq}
	m.Key = s.key("epochs", fmt.Sprintf("%020d", epoch), "manifests", fmt.Sprintf("%020d.json", sequence))
	if err := validateManifest(m, s.opts); err != nil {
		return Manifest{}, err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return Manifest{}, err
	}
	if int64(len(body)) > s.opts.MaxManifestBytes {
		return Manifest{}, errors.New("control replication: manifest exceeds configured bound")
	}
	_, err = s.store.PutObject(ctx, m.Key, bytes.NewReader(body), int64(len(body)), s3.PutOptions{IfNoneMatch: "*", ContentType: "application/json", Metadata: epochMetadata(epoch)})
	if errors.Is(err, s3.ErrPreconditionFailed) {
		reader, _, openErr := s.store.OpenObject(ctx, m.Key)
		if openErr != nil {
			return Manifest{}, openErr
		}
		existing, readErr := io.ReadAll(io.LimitReader(reader, s.opts.MaxManifestBytes+1))
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return Manifest{}, err
		}
		if !bytes.Equal(existing, body) {
			s.lease.markFenced()
			return Manifest{}, fmt.Errorf("%w: manifest sequence collision", ErrFenced)
		}
	} else if err != nil {
		return Manifest{}, err
	}
	if _, err = s.lease.CanDecide(); err != nil {
		return Manifest{}, err
	}
	put := s3.PutOptions{ContentType: "application/json", Metadata: epochMetadata(epoch)}
	if s.currentETag == "" {
		put.IfNoneMatch = "*"
	} else {
		put.IfMatch = s.currentETag
	}
	info, err := s.store.PutObject(ctx, s.pointerKey(), bytes.NewReader(body), int64(len(body)), put)
	if err != nil {
		if errors.Is(err, s3.ErrPreconditionFailed) {
			s.lease.markFenced()
			return Manifest{}, ErrFenced
		}
		return Manifest{}, err
	}
	if info.ETag == "" {
		return Manifest{}, errors.New("control replication: manifest pointer write returned no ETag")
	}
	s.current, s.currentETag = &m, info.ETag
	if full {
		s.lastFull = captured
	}
	return m, nil
}

func (s *Shipper) putPayload(ctx context.Context, epoch uint64, kind, suffix string, p *Payload) (ObjectRef, error) {
	if p == nil || p.Reader == nil || p.Size < 1 || !validDigest(p.SHA256) {
		return ObjectRef{}, errors.New("control replication: invalid capture")
	}
	max := s.opts.MaxWALBytes
	if kind == "snapshots" {
		max = s.opts.MaxSnapshotBytes
	}
	if p.Size > max {
		return ObjectRef{}, fmt.Errorf("control replication: %s exceeds configured bound", kind)
	}
	key := s.key("epochs", fmt.Sprintf("%020d", epoch), kind, p.SHA256+suffix)
	info, err := s.store.PutObject(ctx, key, p.Reader, p.Size, s3.PutOptions{IfNoneMatch: "*", ContentSHA256: p.SHA256, Metadata: epochMetadata(epoch)})
	if errors.Is(err, s3.ErrPreconditionFailed) {
		info, err = s.store.HeadObject(ctx, key)
		if err == nil {
			err = verifyObject(ctx, s.store, ObjectRef{Key: key, Size: p.Size, SHA256: p.SHA256}, max)
		}
	}
	if err != nil {
		return ObjectRef{}, err
	}
	if info.Size != p.Size {
		return ObjectRef{}, fmt.Errorf("%w: immutable object size mismatch", ErrCorruptRecoveryPoint)
	}
	return ObjectRef{Key: key, Size: p.Size, SHA256: p.SHA256}, nil
}

func (s *Shipper) refresh(ctx context.Context) error {
	var m Manifest
	info, err := readJSON(ctx, s.store, s.pointerKey(), s.opts.MaxManifestBytes, &m)
	if isNotFound(err) {
		s.current, s.currentETag = nil, ""
		return ErrNoRecoveryPoint
	}
	if err != nil {
		return err
	}
	if err := validateManifest(m, s.opts); err != nil {
		return err
	}
	if info.ETag == "" {
		return fmt.Errorf("%w: manifest pointer has no ETag", ErrCorruptRecoveryPoint)
	}
	s.current, s.currentETag = &m, info.ETag
	return nil
}

// Run ships at the configured interval until cancellation. It does not hide
// fencing: callers must stop serving writes when ErrFenced is returned.
func (s *Shipper) Run(ctx context.Context, sourceEventSeq func() uint64) error {
	if sourceEventSeq == nil {
		return errors.New("control replication: source event sequence callback is required")
	}
	t := time.NewTicker(s.opts.ShipInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := s.Ship(ctx, sourceEventSeq(), false); err != nil {
				return err
			}
		}
	}
}

// Collect performs a conservative mark-and-sweep. It retains the newest
// manifests and their payloads, skips young or timestamp-less orphans, and
// bounds deletions per pass.
func (s *Shipper) Collect(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refresh(ctx); err != nil {
		return 0, err
	}
	objects, err := s.store.ListObjects(ctx, s.key("epochs")+"/")
	if err != nil {
		return 0, err
	}
	manifests := make([]string, 0)
	byKey := make(map[string]s3.ObjectInfo, len(objects))
	for _, object := range objects {
		byKey[object.Key] = object
		if path.Ext(object.Key) == ".json" && strings.Contains(object.Key, "/manifests/") {
			manifests = append(manifests, object.Key)
		}
	}
	sort.Strings(manifests)
	if len(manifests) > s.opts.RetainManifests {
		manifests = manifests[len(manifests)-s.opts.RetainManifests:]
	}
	referenced := map[string]bool{s.current.Key: true, s.current.Snapshot.Key: true}
	if s.current.WAL != nil {
		referenced[s.current.WAL.Key] = true
	}
	for _, key := range manifests {
		var manifest Manifest
		if _, err := readJSON(ctx, s.store, key, s.opts.MaxManifestBytes, &manifest); err != nil {
			return 0, err
		}
		if err := validateManifest(manifest, s.opts); err != nil {
			return 0, err
		}
		referenced[key], referenced[manifest.Snapshot.Key] = true, true
		if manifest.WAL != nil {
			referenced[manifest.WAL.Key] = true
		}
	}
	cutoff := s.opts.Now().Add(-s.opts.OrphanGrace)
	candidates := make([]string, 0)
	for key, info := range byKey {
		if !referenced[key] && !info.LastModified.IsZero() && info.LastModified.Before(cutoff) {
			candidates = append(candidates, key)
		}
	}
	sort.Strings(candidates)
	if len(candidates) > s.opts.MaxDeletesPerRun {
		candidates = candidates[:s.opts.MaxDeletesPerRun]
	}
	for index, key := range candidates {
		if err := s.store.DeleteObject(ctx, key); err != nil {
			return index, err
		}
	}
	return len(candidates), nil
}

func validateManifest(m Manifest, opts Options) error {
	epochRoot := opts.Prefix + "/epochs/" + fmt.Sprintf("%020d", m.Epoch) + "/"
	expectedManifest := epochRoot + "manifests/" + fmt.Sprintf("%020d.json", m.Sequence)
	if m.Version != formatVersion || m.Key != expectedManifest || m.Epoch == 0 || m.Sequence == 0 || m.CapturedAt.IsZero() || m.CommittedAt.IsZero() || m.CapturedAt.After(m.CommittedAt) || m.Snapshot.Key != epochRoot+"snapshots/"+m.Snapshot.SHA256+".db" || m.Snapshot.Size < 1 || m.Snapshot.Size > opts.MaxSnapshotBytes || !validDigest(m.Snapshot.SHA256) {
		return fmt.Errorf("%w: invalid manifest", ErrCorruptRecoveryPoint)
	}
	if m.WAL != nil {
		if m.WAL.Key != epochRoot+"wal/"+m.WAL.SHA256+".wal" || m.WAL.Size < walHeaderBytes || m.WAL.Size > opts.MaxWALBytes || !validDigest(m.WAL.SHA256) || len(m.WALLineage) != 16 {
			return fmt.Errorf("%w: invalid WAL reference", ErrCorruptRecoveryPoint)
		}
		if _, err := hex.DecodeString(m.WALLineage); err != nil {
			return fmt.Errorf("%w: WAL lineage", ErrCorruptRecoveryPoint)
		}
	} else if m.WALLineage != "" {
		return fmt.Errorf("%w: WAL lineage without WAL", ErrCorruptRecoveryPoint)
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func verifyObject(ctx context.Context, store Store, ref ObjectRef, max int64) error {
	reader, info, err := store.OpenObject(ctx, ref.Key)
	if err != nil {
		return err
	}
	defer reader.Close()
	if info.Size != ref.Size || info.Size < 1 || info.Size > max {
		return fmt.Errorf("%w: immutable object size mismatch", ErrCorruptRecoveryPoint)
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(reader, max+1))
	if err != nil {
		return err
	}
	if n != ref.Size || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("%w: immutable object digest mismatch", ErrCorruptRecoveryPoint)
	}
	return nil
}

func (s *Shipper) pointerKey() string         { return s.key("current.json") }
func (s *Shipper) key(parts ...string) string { return s.opts.Prefix + "/" + path.Join(parts...) }
