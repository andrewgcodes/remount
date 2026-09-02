// Package proto defines the Remount wire protocol: one frame type, encoded
// with CBOR, carried over any ordered, reliable byte transport.
//
// Design rules (see docs/adr/0002-one-frame-type.md):
//
//   - There is exactly one frame shape. The kind is a short string in T.
//     Unknown kinds and unknown fields are ignored so peers of different
//     versions can talk; hello negotiates the intersection of capabilities.
//   - Frames are routed by To/From. A relay forwards frames by To without
//     interpreting Body. The control plane is the peer named "control".
//   - Request/response correlate by ID. Every mutating request carries an
//     idempotency key so a retry after a dropped connection is safe.
//   - Session output is carried in "chunk" frames with a per-session
//     monotonically increasing Seq. A client resumes a session by asking for
//     replay from the last Seq it saw.
package proto

import (
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Version is the protocol version carried in every frame.
const Version = 1

// Well-known peer names.
const (
	PeerControl = "control"
)

// Frame kinds.
const (
	KindHello = "hello" // first frame on a connection; carries identity + caps
	KindReq   = "req"   // request; expects exactly one res with the same ID
	KindRes   = "res"   // response
	KindChunk = "chunk" // session output: Seq, S, Body=ChunkBody
	KindEvent = "ev"    // one-way notification (event log entries, offers, peer.gone)
	KindPing  = "ping"
	KindPong  = "pong"
)

// Frame is the single wire unit.
type Frame struct {
	V    uint8  `cbor:"v" json:"v"`
	T    string `cbor:"t" json:"t"`
	ID   uint64 `cbor:"id,omitempty" json:"id,omitempty"`     // req/res correlation
	Seq  uint64 `cbor:"seq,omitempty" json:"seq,omitempty"`   // chunk: per-session output sequence
	S    string `cbor:"s,omitempty" json:"s,omitempty"`       // session id
	WS   string `cbor:"ws,omitempty" json:"ws,omitempty"`     // workspace id
	To   string `cbor:"to,omitempty" json:"to,omitempty"`     // destination peer
	From string `cbor:"from,omitempty" json:"from,omitempty"` // source peer (set by the relay, never trusted from the sender)
	Op   string `cbor:"op,omitempty" json:"op,omitempty"`     // req: operation name; ev: event type
	Body []byte `cbor:"body,omitempty" json:"body,omitempty"` // CBOR-encoded payload, kind/op specific
	Err  *Error `cbor:"err,omitempty" json:"err,omitempty"`   // res: non-nil on failure
}

// Error is a machine-readable failure. Code is stable; Msg is for humans.
type Error struct {
	Code string `cbor:"code" json:"code"`
	Msg  string `cbor:"msg,omitempty" json:"msg,omitempty"`
	// Oldest is set with CodeEvicted: the oldest seq still replayable.
	Oldest uint64 `cbor:"oldest,omitempty" json:"oldest,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Msg == "" {
		return e.Code
	}
	return e.Code + ": " + e.Msg
}

// Is lets errors.Is match on Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Stable error codes.
const (
	CodeBadRequest        = "bad_request"
	CodeNotFound          = "not_found"
	CodeUnsupported       = "unsupported"
	CodeUnauthorized      = "unauthorized"
	CodeConflict          = "conflict"
	CodeEvicted           = "evicted"     // requested seq is older than the retained log
	CodeUnreachable       = "unreachable" // relay: destination peer not connected
	CodeInternal          = "internal"
	CodeTimeout           = "timeout"
	CodeClosed            = "closed"
	CodeDenied            = "denied" // policy denied
	CodeResourceExhausted = "resource_exhausted"
)

// Err builds an *Error.
func Err(code, format string, a ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, a...)}
}

var encMode cbor.EncMode
var decMode cbor.DecMode

func init() {
	em, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	encMode = em
	dm, err := cbor.DecOptions{
		// Ignore unknown fields (default) and cap sizes so a hostile peer
		// cannot make us allocate unboundedly from a header.
		MaxArrayElements: 1 << 20,
		MaxMapPairs:      1 << 20,
	}.DecMode()
	if err != nil {
		panic(err)
	}
	decMode = dm
}

// Marshal encodes any value with the protocol's deterministic CBOR mode.
func Marshal(v any) ([]byte, error) { return encMode.Marshal(v) }

// Unmarshal decodes CBOR produced by Marshal (or any peer).
func Unmarshal(b []byte, v any) error { return decMode.Unmarshal(b, v) }

// MustMarshal panics on error; for values we construct ourselves.
func MustMarshal(v any) []byte {
	b, err := Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// HelloProofBytes returns the deterministic bytes a node signs. Proof itself
// is omitted to avoid a recursive signature.
func HelloProofBytes(h Hello) []byte {
	h.Proof = nil
	return MustMarshal(h)
}

// EncodeFrame serializes a frame. The transport prefixes its own length.
func EncodeFrame(f *Frame) ([]byte, error) {
	if f.V == 0 {
		f.V = Version
	}
	return encMode.Marshal(f)
}

// DecodeFrame parses a frame. It rejects frames whose version is newer than
// ours only if the kind is unknown; otherwise it is lenient by design.
func DecodeFrame(b []byte) (*Frame, error) {
	var f Frame
	if err := decMode.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("proto: decode frame: %w", err)
	}
	if f.T == "" {
		return nil, errors.New("proto: frame has no kind")
	}
	return &f, nil
}

// Body decodes f.Body into v.
func (f *Frame) Decode(v any) error {
	if len(f.Body) == 0 {
		return nil
	}
	return decMode.Unmarshal(f.Body, v)
}

// NewReq builds a request frame.
func NewReq(id uint64, to, op string, body any) *Frame {
	return &Frame{V: Version, T: KindReq, ID: id, To: to, Op: op, Body: MustMarshal(body)}
}

// NewRes builds a successful response to req.
func NewRes(req *Frame, body any) *Frame {
	var b []byte
	if body != nil {
		b = MustMarshal(body)
	}
	return &Frame{V: Version, T: KindRes, ID: req.ID, To: req.From, Op: req.Op, Body: b}
}

// NewErrRes builds a failed response to req.
func NewErrRes(req *Frame, e *Error) *Frame {
	return &Frame{V: Version, T: KindRes, ID: req.ID, To: req.From, Op: req.Op, Err: e}
}

// NewEvent builds a one-way event frame.
func NewEvent(to, typ string, body any) *Frame {
	var b []byte
	if body != nil {
		b = MustMarshal(body)
	}
	return &Frame{V: Version, T: KindEvent, To: to, Op: typ, Body: b}
}

// NormalizeSecurity applies profile defaults without weakening explicit
// requirements. Callers must still validate the selected backend.
func NormalizeSecurity(s SecuritySpec) (SecuritySpec, error) {
	if s.Profile == "" {
		s.Profile = SecurityLocal
	}
	switch s.Profile {
	case SecurityLocal:
		if s.MinIsolation == "" {
			s.MinIsolation = "none"
		}
	case SecurityIsolated:
		if s.MinIsolation == "" {
			s.MinIsolation = "container"
		}
		if s.Network.Default == "" {
			s.Network.Default = "deny"
		}
		s.Audit.Required = true
	case SecurityMultiTenant:
		if s.MinIsolation == "" {
			s.MinIsolation = "microvm"
		}
		s.RequireSiblingIsolation = true
		s.RequireEnforcedEgress = true
		if s.SecretMode == "" {
			s.SecretMode = "brokered"
		}
		s.Network.Default = "deny"
		s.Audit.Required = true
	default:
		return SecuritySpec{}, Err(CodeBadRequest, "unknown security profile %q", s.Profile)
	}
	if isolationRank(s.MinIsolation) < 0 {
		return SecuritySpec{}, Err(CodeBadRequest, "unknown minimum isolation %q", s.MinIsolation)
	}
	if s.SecretMode != "" && s.SecretMode != "none" && s.SecretMode != "brokered" {
		return SecuritySpec{}, Err(CodeBadRequest, "unknown secret mode %q", s.SecretMode)
	}
	if s.Network.Default != "" && s.Network.Default != "deny" && s.Network.Default != "allow" {
		return SecuritySpec{}, Err(CodeBadRequest, "unknown network default %q", s.Network.Default)
	}
	if s.Profile != SecurityLocal && s.Network.Default == "allow" {
		return SecuritySpec{}, Err(CodeDenied, "non-local security profiles cannot default-allow egress")
	}
	return s, nil
}

func isolationRank(s string) int {
	switch s {
	case "none", "":
		return 0
	case "process_sandbox":
		return 1
	case "container":
		return 2
	case "microvm":
		return 3
	default:
		return -1
	}
}

// ValidateBackendSecurity checks one backend descriptor against a workspace's
// normalized security contract.
func ValidateBackendSecurity(policy SecuritySpec, backend BackendDescriptor) error {
	p, err := NormalizeSecurity(policy)
	if err != nil {
		return err
	}
	if isolationRank(backend.Security.Isolation) < isolationRank(p.MinIsolation) {
		return Err(CodeDenied, "isolation %s is weaker than required %s", backend.Security.Isolation, p.MinIsolation)
	}
	if p.RequireSiblingIsolation && !backend.Security.SiblingIsolation {
		return Err(CodeDenied, "sibling isolation is required")
	}
	if p.RequireEnforcedEgress && backend.Security.EgressMode != "enforced_gateway" {
		return Err(CodeDenied, "enforced egress is required")
	}
	if p.SecretMode == "brokered" && backend.Security.BrokerIdentity == "none" {
		return Err(CodeDenied, "an authenticated broker is required")
	}
	if p.Profile == SecurityMultiTenant {
		if !backend.Security.MultiTenant || !backend.Security.NetworkNamespace || !backend.Security.DeviceIsolation {
			return Err(CodeDenied, "backend is not approved for multi-tenant execution")
		}
	}
	return nil
}

// StrengthenSecurity raises requested to at least floor. It never clears an
// explicit requirement from requested.
func StrengthenSecurity(requested SecuritySpec, floor string) (SecuritySpec, error) {
	p, err := NormalizeSecurity(requested)
	if err != nil {
		return SecuritySpec{}, err
	}
	f, err := NormalizeSecurity(SecuritySpec{Profile: floor})
	if err != nil {
		return SecuritySpec{}, err
	}
	profileRank := func(profile string) int {
		switch profile {
		case SecurityLocal:
			return 0
		case SecurityIsolated:
			return 1
		case SecurityMultiTenant:
			return 2
		default:
			return -1
		}
	}
	if profileRank(f.Profile) > profileRank(p.Profile) {
		p.Profile = f.Profile
	}
	if isolationRank(f.MinIsolation) > isolationRank(p.MinIsolation) {
		p.MinIsolation = f.MinIsolation
	}
	p.RequireSiblingIsolation = p.RequireSiblingIsolation || f.RequireSiblingIsolation
	p.RequireEnforcedEgress = p.RequireEnforcedEgress || f.RequireEnforcedEgress
	p.Audit.Required = p.Audit.Required || f.Audit.Required
	if f.SecretMode == "brokered" {
		p.SecretMode = "brokered"
	}
	if f.Network.Default == "deny" {
		p.Network.Default = "deny"
	}
	return NormalizeSecurity(p)
}
