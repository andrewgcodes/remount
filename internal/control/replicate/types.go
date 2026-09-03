// Package replicate provides the storage and fencing core for an active/passive
// Remount control plane. It deliberately does not decide workspace state; the
// control package remains authoritative for reconciliation after promotion.
package replicate

import (
	"context"
	"errors"
	"io"
	"time"

	"remount.dev/remount/internal/artifact/s3"
)

var (
	// ErrLeaseHeld means another unexpired controller owns the writer lease.
	ErrLeaseHeld = errors.New("control replication: writer lease is held")
	// ErrFenced means this controller no longer owns the current epoch.
	ErrFenced = errors.New("control replication: controller is fenced")
	// ErrWALReset means SQLite started a new WAL lineage and a full snapshot is required.
	ErrWALReset = errors.New("control replication: WAL lineage changed")
	// ErrNoRecoveryPoint means no committed replication manifest exists.
	ErrNoRecoveryPoint = errors.New("control replication: no recovery point")
	// ErrCorruptRecoveryPoint means stored bytes or metadata failed validation.
	ErrCorruptRecoveryPoint = errors.New("control replication: corrupt recovery point")
)

// Store is the named-object subset required from the hand-written S3 store.
// CheckConditionalWrites must perform a real negative capability probe.
type Store interface {
	PutObject(context.Context, string, io.Reader, int64, s3.PutOptions) (s3.ObjectInfo, error)
	OpenObject(context.Context, string) (io.ReadCloser, s3.ObjectInfo, error)
	HeadObject(context.Context, string) (s3.ObjectInfo, error)
	ListObjects(context.Context, string) ([]s3.ObjectInfo, error)
	DeleteObject(context.Context, string) error
	CheckConditionalWrites(context.Context) error
}

// Payload is one immutable, bounded capture. Close removes any staging state.
type Payload struct {
	Reader  io.ReadCloser
	Size    int64
	SHA256  string
	Lineage string
}

// Close releases a captured payload.
func (p *Payload) Close() error {
	if p == nil || p.Reader == nil {
		return nil
	}
	err := p.Reader.Close()
	p.Reader = nil
	return err
}

// Bundle is a full SQLite snapshot plus the committed WAL prefix created after
// its checkpoint. WAL may be nil when no post-snapshot transaction committed.
type Bundle struct {
	Snapshot *Payload
	WAL      *Payload
	At       time.Time
}

// Close releases all staging held by a bundle.
func (b *Bundle) Close() error {
	if b == nil {
		return nil
	}
	return errors.Join(b.Snapshot.Close(), b.WAL.Close())
}

// Source captures a transactionally consistent full snapshot and later full
// committed WAL images. A lineage mismatch must return ErrWALReset.
type Source interface {
	Full(context.Context) (*Bundle, error)
	WAL(context.Context, string) (*Payload, error)
}

// ObjectRef identifies immutable recovery bytes.
type ObjectRef struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest is the only recovery point a standby may restore. The pointer to a
// manifest is conditionally committed after every referenced object exists.
type Manifest struct {
	Version        int        `json:"version"`
	Key            string     `json:"key"`
	Epoch          uint64     `json:"epoch"`
	Sequence       uint64     `json:"sequence"`
	CapturedAt     time.Time  `json:"captured_at"`
	CommittedAt    time.Time  `json:"committed_at"`
	Snapshot       ObjectRef  `json:"snapshot"`
	WAL            *ObjectRef `json:"wal,omitempty"`
	WALLineage     string     `json:"wal_lineage,omitempty"`
	SourceEventSeq uint64     `json:"source_event_seq,omitempty"`
}

// Recovery is explicit evidence about the point restored during promotion.
// LostWindow is an upper-bound estimate, not proof that commits were lost.
type Recovery struct {
	Epoch            uint64
	PreviousEpoch    uint64
	Manifest         Manifest
	PromotedAt       time.Time
	LastReplicatedAt time.Time
	LostWindow       time.Duration
	NeedsReconcile   bool
}

// Options bounds lease, payload, manifest, and retained-object state.
type Options struct {
	Prefix           string
	LeaseTTL         time.Duration
	RenewInterval    time.Duration
	ShipInterval     time.Duration
	SnapshotInterval time.Duration
	OrphanGrace      time.Duration
	MaxClockSkew     time.Duration
	MaxManifestBytes int64
	MaxSnapshotBytes int64
	MaxWALBytes      int64
	RetainManifests  int
	MaxDeletesPerRun int
	Now              func() time.Time
}

func (o Options) withDefaults() (Options, error) {
	if o.Prefix == "" {
		o.Prefix = "control/replication"
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = 3 * time.Second
	}
	if o.RenewInterval == 0 {
		o.RenewInterval = o.LeaseTTL / 3
	}
	if o.ShipInterval == 0 {
		o.ShipInterval = time.Second
	}
	if o.SnapshotInterval == 0 {
		o.SnapshotInterval = 5 * time.Minute
	}
	if o.OrphanGrace == 0 {
		o.OrphanGrace = max(o.SnapshotInterval, 2*o.LeaseTTL)
	}
	if o.MaxClockSkew == 0 {
		o.MaxClockSkew = 250 * time.Millisecond
	}
	if o.MaxManifestBytes == 0 {
		o.MaxManifestBytes = 1 << 20
	}
	if o.MaxSnapshotBytes == 0 {
		o.MaxSnapshotBytes = 1 << 30
	}
	if o.MaxWALBytes == 0 {
		o.MaxWALBytes = 512 << 20
	}
	if o.RetainManifests == 0 {
		o.RetainManifests = 8
	}
	if o.MaxDeletesPerRun == 0 {
		o.MaxDeletesPerRun = 128
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.LeaseTTL <= 0 || o.RenewInterval <= 0 || o.RenewInterval > o.LeaseTTL/3 || o.ShipInterval <= 0 || o.SnapshotInterval < o.ShipInterval || o.OrphanGrace < o.LeaseTTL || o.MaxClockSkew < 0 || o.MaxManifestBytes < 1 || o.MaxSnapshotBytes < 1 || o.MaxWALBytes < 32 || o.RetainManifests < 1 || o.MaxDeletesPerRun < 1 {
		return Options{}, errors.New("control replication: invalid time, size, or retention bounds")
	}
	return o, nil
}
