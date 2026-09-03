package proto

const (
	// OpSessionLogCommit atomically replaces a node-produced durable segment
	// record after control revalidates the live workspace generation.
	OpSessionLogCommit = "session.log.commit"
	// OpSessionLogGet lets the current workspace holder reconstruct a retained
	// exited session after a move or node restart.
	OpSessionLogGet = "session.log.get"
	// OpSessionLogDelete releases an expired record and its GC roots.
	OpSessionLogDelete      = "session.log.delete"
	EvSessionLogCommitted   = "session.log.committed"
	EvSessionLogDeleted     = "session.log.deleted"
	EvSessionLogUnavailable = "session.log.unavailable"
)

// SessionLogSegment is one immutable, tenant-local encoded sequence range.
type SessionLogSegment struct {
	First    uint64 `cbor:"first" json:"first"`
	Next     uint64 `cbor:"next" json:"next"`
	Artifact string `cbor:"artifact" json:"artifact"`
	Bytes    int64  `cbor:"bytes" json:"bytes"`
}

// SessionLogRecord is the control-owned replay authority. Tenant is derived
// from Workspace and cannot be selected by a node.
type SessionLogRecord struct {
	Session   string              `cbor:"session" json:"session"`
	Workspace string              `cbor:"workspace" json:"workspace"`
	Tenant    string              `cbor:"tenant" json:"tenant"`
	Principal string              `cbor:"principal" json:"principal"`
	Kind      string              `cbor:"kind" json:"kind"`
	Info      SessionInfo         `cbor:"info" json:"info"`
	Exit      ExitInfo            `cbor:"exit" json:"exit"`
	MaxChunk  int                 `cbor:"max_chunk" json:"max_chunk"`
	Segments  []SessionLogSegment `cbor:"segments,omitempty" json:"segments,omitempty"`
	Complete  bool                `cbor:"complete,omitempty" json:"complete,omitempty"`
	UpdatedAt int64               `cbor:"updated_at" json:"updated_at"`
	ExpiresAt int64               `cbor:"expires_at,omitempty" json:"expires_at,omitempty"`
}

// SessionLogCommitReq carries a full replacement record. Generation and node
// connection fence stale producers; Tenant is intentionally absent.
type SessionLogCommitReq struct {
	Session    string              `cbor:"session" json:"session"`
	Workspace  string              `cbor:"workspace" json:"workspace"`
	Generation uint64              `cbor:"gen" json:"gen"`
	Principal  string              `cbor:"principal" json:"principal"`
	Kind       string              `cbor:"kind" json:"kind"`
	Info       SessionInfo         `cbor:"info,omitempty" json:"info,omitempty"`
	Exit       ExitInfo            `cbor:"exit,omitempty" json:"exit,omitempty"`
	MaxChunk   int                 `cbor:"max_chunk" json:"max_chunk"`
	Segments   []SessionLogSegment `cbor:"segments,omitempty" json:"segments,omitempty"`
	Complete   bool                `cbor:"complete,omitempty" json:"complete,omitempty"`
}

// SessionLogGetReq asks for a record only after the requester became the live
// holder of Workspace/Generation.
type SessionLogGetReq struct {
	Session    string `cbor:"session" json:"session"`
	Workspace  string `cbor:"workspace" json:"workspace"`
	Generation uint64 `cbor:"gen" json:"gen"`
}

type SessionLogDeleteReq = SessionLogGetReq
