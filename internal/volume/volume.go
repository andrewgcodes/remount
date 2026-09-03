// Package volume manages immutable, tenant-scoped data volumes and their
// read-only workspace attachments.
//
// A volume version is an artifact reference, not a mutable filesystem. An
// attachment pins one version so publishing a replacement cannot change bytes
// underneath a running workspace. The local backend persists lifecycle intent
// before invoking the mount engine and reconciles incomplete intent on restart.
package volume

import (
	"context"
	"errors"
	"os"
	"time"

	"remount.dev/remount/internal/proto"
)

var (
	// ErrNotFound reports that a tenant-scoped volume does not exist.
	ErrNotFound = errors.New("volume: not found")
	// ErrConflict reports an incompatible replay or a busy resource.
	ErrConflict = errors.New("volume: conflict")
	// ErrStaleGeneration reports work fenced by a newer workspace generation.
	ErrStaleGeneration = errors.New("volume: stale workspace generation")
	// ErrQuota reports bounded catalog or attachment capacity exhaustion.
	ErrQuota = errors.New("volume: quota exceeded")
	// ErrUnsafePath reports an invalid or escaping workspace mount path.
	ErrUnsafePath = errors.New("volume: unsafe mount path")
	// ErrReadOnlyRequired reports an attempt to create a live writable mount.
	ErrReadOnlyRequired = errors.New("volume: shared mounts must be read-only")
	// ErrUnsupported reports a host that cannot enforce a requested mount.
	ErrUnsupported = errors.New("volume: mount operation unsupported")
)

// Volume is a tenant-owned pointer to the latest immutable artifact version.
type Volume struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Artifact  string    `json:"artifact"`
	Version   uint64    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Version records one immutable value previously published for a volume.
type Version struct {
	Number      uint64    `json:"number"`
	Artifact    string    `json:"artifact"`
	PublishedBy string    `json:"published_by"`
	Generation  uint64    `json:"generation"`
	PublishedAt time.Time `json:"published_at"`
}

// Attachment pins an immutable version at one path in one workspace generation.
type Attachment struct {
	Tenant        string    `json:"tenant"`
	Workspace     string    `json:"workspace"`
	Generation    uint64    `json:"generation"`
	VolumeID      string    `json:"volume_id"`
	VolumeVersion uint64    `json:"volume_version"`
	Artifact      string    `json:"artifact"`
	Path          string    `json:"path"`
	WorkspaceRoot string    `json:"workspace_root"`
	TargetDevice  uint64    `json:"target_device,omitempty"`
	TargetInode   uint64    `json:"target_inode,omitempty"`
	ReadOnly      bool      `json:"read_only"`
	AttachedAt    time.Time `json:"attached_at"`
	State         string    `json:"state"`
	MountPresent  bool      `json:"mount_present,omitempty"`
	MountVerified bool      `json:"mount_verified,omitempty"`
}

// Detail is the inspect result for one volume.
type Detail struct {
	Volume      Volume       `json:"volume"`
	Versions    []Version    `json:"versions"`
	Attachments []Attachment `json:"attachments"`
}

// CreateRequest creates the first immutable version of a volume.
type CreateRequest struct {
	OperationID string
	ID          string
	Tenant      string
	Artifact    string
}

// DeleteRequest deletes catalog authority after all attachments are gone.
type DeleteRequest struct {
	OperationID string
	ID          string
	Tenant      string
}

// PublishRequest atomically advances a volume from ExpectedVersion.
type PublishRequest struct {
	OperationID     string
	ID              string
	Tenant          string
	Artifact        string
	Workspace       string
	Generation      uint64
	ExpectedVersion uint64
}

// AttachRequest mounts the latest volume version into a workspace.
type AttachRequest struct {
	OperationID   string
	ID            string
	Tenant        string
	Workspace     string
	Generation    uint64
	Path          string
	WorkspaceRoot string
	ReadOnly      bool
	Version       uint64
	Artifact      string
}

// DetachRequest removes an attachment only for its current generation.
type DetachRequest struct {
	OperationID string
	Tenant      string
	Workspace   string
	Generation  uint64
	Path        string
}

// Options bound durable state retained by a local backend.
type Options struct {
	MaxVolumesPerTenant  int
	MaxMountsPerTenant   int
	MaxVersionsPerVolume int
	MaxVolumes           int
	MaxMounts            int
	MaxWorkspaceFences   int
	MaxOperations        int
	OperationRetention   time.Duration
	Clock                func() time.Time
}

// Stats are monotonic observability counters plus current retained use.
type Stats struct {
	Volumes          int    `json:"volumes"`
	Attachments      int    `json:"attachments"`
	WorkspaceFences  int    `json:"workspace_fences"`
	Operations       int    `json:"operations"`
	QuotaRejections  uint64 `json:"quota_rejections"`
	PrunedVolumes    uint64 `json:"pruned_volumes"`
	PrunedFences     uint64 `json:"pruned_fences"`
	StaleRejections  uint64 `json:"stale_rejections"`
	ReconciledAttach uint64 `json:"reconciled_attach"`
	ReconciledDetach uint64 `json:"reconciled_detach"`
}

// Backend is the complete shared-data volume contract. Implementations must
// make mutating operations idempotent by OperationID and tenant scope.
type Backend interface {
	EnsureVersion(context.Context, string, string, uint64, string) error
	Create(context.Context, CreateRequest) (Volume, error)
	Delete(context.Context, DeleteRequest) error
	Publish(context.Context, PublishRequest) (Volume, error)
	Attach(context.Context, AttachRequest) (Attachment, error)
	Detach(context.Context, DetachRequest) error
	List(context.Context, string) ([]Volume, error)
	Inspect(context.Context, string, string) (Detail, error)
	SetWorkspaceGeneration(context.Context, string, string, uint64) error
	Stats() Stats
	Close() error
}

// ArtifactReferenceReporter is the optional GC-root contract implemented by
// backends with durable local recovery records. Callers use a fresh snapshot;
// they must not retain or mutate backend state through it.
type ArtifactReferenceReporter interface {
	ArtifactReferences() []string
}

// SourceResolver opens a verified immutable artifact directory. The returned
// descriptor, rather than a string path, is handed across the mount boundary.
// This permits local caches and host-mounted NFS stores without making NFS a
// source of Remount lifecycle authority.
type SourceResolver interface {
	OpenArtifact(context.Context, string, string) (*os.File, error)
}

// MountEngine performs host-specific mount operations using already-open
// source and target directory descriptors.
type MountEngine interface {
	MountReadOnly(context.Context, *os.File, *os.File) error
	Unmount(context.Context, *os.File) error
	Inspect(context.Context, *os.File) (MountStatus, error)
}

// MountStatus is the minimum evidence needed to recover an interrupted attach.
type MountStatus struct {
	Mounted  bool
	ReadOnly bool
	Source   os.FileInfo
}

// ValidateTenant rejects values that cannot be used as one descriptor-rooted
// source-cache segment. Tenant authority still comes from authentication.
func ValidateTenant(tenant string) error { return validateTenant(tenant) }

func validateVolumeID(id string) error { return proto.ValidateVolumeID(id) }
