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
	"net"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

// Version is the only protocol version this release accepts. A peer must
// negotiate CapabilityV1 during hello before either side exchanges requests.
const Version = 1

// CapabilityV1 identifies the complete v1 semantic contract. Capabilities
// are exact, case-sensitive identifiers; an empty list does not mean a
// wildcard or implicit compatibility.
const CapabilityV1 = "v1"

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
		copyFrame := *f
		copyFrame.V = Version
		return encMode.Marshal(&copyFrame)
	}
	return encMode.Marshal(f)
}

// DecodeFrame parses a frame. Unknown fields remain forward-compatible within
// a negotiated version, but frames from any other version fail closed before
// their kind or body can be interpreted.
func DecodeFrame(b []byte) (*Frame, error) {
	var f Frame
	if err := decMode.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("proto: decode frame: %w", err)
	}
	if f.T == "" {
		return nil, errors.New("proto: frame has no kind")
	}
	if f.V != Version {
		return nil, fmt.Errorf("proto: unsupported frame version %d (supported: %d)", f.V, Version)
	}
	return &f, nil
}

// Named capabilities. A change an old peer could ignore without weakening a
// security property is an additive field; a change an old peer ignoring it
// would weaken is a named capability, negotiated at hello and required by
// the security profiles that depend on it (ADR 0040).
const (
	// CapabilityAuthzPush: the control plane pushes authorization revisions
	// on renew and the node refuses stale grants and closes their sessions.
	CapabilityAuthzPush = "authz-push"
	// CapabilityControllerEpoch: frames carry the controller epoch and a
	// node fences itself against a superseded controller.
	CapabilityControllerEpoch = "controller-epoch"
	// CapabilitySessionCap: sessions carry a principal-bound capability that
	// a principal revocation invalidates.
	CapabilitySessionCap = "session-cap"
	// CapabilityChunkedArtifacts: artifacts move as verified chunks so a
	// truncated transfer can never be restored as complete.
	CapabilityChunkedArtifacts = "chunked-artifacts"
	// CapabilityApprovals: an operation may be held for an approval decision
	// and the peer honours the held state rather than proceeding.
	CapabilityApprovals = "approvals"
	// CapabilityEncryptedArtifacts: snapshot artifacts are encrypted at
	// rest with the artifact key and refused when the key is absent.
	CapabilityEncryptedArtifacts = "encrypted-artifacts"
)

// knownCapabilities is the canonical order in which negotiated
// capabilities are reported. v1 is always first.
var knownCapabilities = []string{
	CapabilityV1,
	CapabilityAuthzPush,
	CapabilityControllerEpoch,
	CapabilitySessionCap,
	CapabilityChunkedArtifacts,
	CapabilityApprovals,
	CapabilityEncryptedArtifacts,
}

// implementedCapabilities is what a peer built from this release offers.
// A capability joins this list in the same commit that lands its semantics
// on both the node and the control plane; the identifiers above may be
// defined ahead of that so the spec table and profile requirements are
// stable.
var implementedCapabilities = []string{
	CapabilityV1,
	CapabilityAuthzPush,
}

// PeerCapabilities returns the capabilities a peer built from this release
// offers at hello. The slice is a fresh copy.
func PeerCapabilities() []string {
	return append([]string(nil), implementedCapabilities...)
}

// profileCapabilities maps a security profile to the named capabilities
// every peer serving it must have negotiated. local requires none so an
// older node keeps working there; isolated and multi_tenant require every
// capability this release implements beyond v1, because each one closes a
// hole those profiles promise is closed.
var profileCapabilities = map[string][]string{
	SecurityLocal:       nil,
	SecurityIsolated:    {CapabilityAuthzPush},
	SecurityMultiTenant: {CapabilityAuthzPush},
}

// SecurityCapabilities returns the named capabilities a peer must have
// negotiated before it may serve a workspace, or a deployment, under
// profile. An unknown or empty profile is treated as local. The slice is a
// fresh copy.
func SecurityCapabilities(profile string) []string {
	return append([]string(nil), profileCapabilities[profile]...)
}

// NegotiateCapabilities returns the ordered intersection of offered and the
// capabilities this release knows. v1 is a semantic baseline rather than an
// optional extension, so peers that do not offer it are rejected explicitly.
// Unknown identifiers are dropped, never echoed, so a peer can only rely on
// what both sides understand.
func NegotiateCapabilities(offered []string) ([]string, error) {
	if !HasCapability(offered, CapabilityV1) {
		return nil, Err(CodeUnsupported, "peer does not offer required capability %q", CapabilityV1)
	}
	negotiated := make([]string, 0, len(implementedCapabilities))
	for _, capability := range implementedCapabilities {
		if HasCapability(offered, capability) {
			negotiated = append(negotiated, capability)
		}
	}
	return negotiated, nil
}

// MissingCapabilities returns, in canonical order, every identifier in
// required that negotiated lacks. An empty result means the peer may serve
// whatever required was derived from.
func MissingCapabilities(negotiated, required []string) []string {
	var missing []string
	for _, capability := range knownCapabilities {
		if HasCapability(required, capability) && !HasCapability(negotiated, capability) {
			missing = append(missing, capability)
		}
	}
	return missing
}

// HasCapability reports whether capabilities contains the exact identifier.
func HasCapability(capabilities []string, required string) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
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
	// Security policies are commonly read directly from a live workspace while
	// other goroutines are serializing that workspace for events or responses.
	// Clone every mutable layer before canonicalizing rules: accepting the
	// struct by value does not copy its slice backing arrays.
	if len(s.Network.Rules) > 0 {
		s.Network.Rules = append([]EgressRule(nil), s.Network.Rules...)
		for i := range s.Network.Rules {
			rule := &s.Network.Rules[i]
			rule.Hosts = append([]string(nil), rule.Hosts...)
			rule.Repos = append([]string(nil), rule.Repos...)
			rule.Ports = append([]uint16(nil), rule.Ports...)
			rule.Methods = append([]string(nil), rule.Methods...)
			rule.PathPrefixes = append([]string(nil), rule.PathPrefixes...)
		}
	}
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
			s.Network.Default = NetworkDefaultDeny
		}
		s.RequireEnforcedEgress = true
		if s.SecretMode == "" {
			s.SecretMode = "brokered"
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
		s.Network.Default = NetworkDefaultDeny
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
	if len(s.Network.Rules) > 0 && s.Network.Default == "" {
		s.Network.Default = NetworkDefaultDeny
	}
	if s.Network.Default != "" && s.Network.Default != NetworkDefaultDeny && s.Network.Default != NetworkDefaultAllow {
		return SecuritySpec{}, Err(CodeBadRequest, "unknown network default %q", s.Network.Default)
	}
	if s.Profile != SecurityLocal && s.Network.Default == NetworkDefaultAllow {
		return SecuritySpec{}, Err(CodeDenied, "non-local security profiles cannot default-allow egress")
	}
	seenRules := make(map[string]struct{}, len(s.Network.Rules))
	for i := range s.Network.Rules {
		if err := normalizeEgressRule(&s.Network.Rules[i], seenRules); err != nil {
			return SecuritySpec{}, err
		}
	}
	return s, nil
}

func normalizeEgressRule(rule *EgressRule, seen map[string]struct{}) error {
	rule.ID = strings.TrimSpace(rule.ID)
	if rule.ID == "" {
		return Err(CodeBadRequest, "egress rule id is required")
	}
	if _, exists := seen[rule.ID]; exists {
		return Err(CodeBadRequest, "duplicate egress rule id %q", rule.ID)
	}
	seen[rule.ID] = struct{}{}
	rule.Protocol = strings.ToLower(strings.TrimSpace(rule.Protocol))
	rule.Connector = strings.ToLower(strings.TrimSpace(rule.Connector))
	switch rule.Connector {
	case "", EgressConnectorPackage, EgressConnectorGit:
	default:
		return Err(CodeBadRequest, "egress rule %q has unsupported connector %q", rule.ID, rule.Connector)
	}
	if rule.Connector != EgressConnectorGit && (len(rule.Repos) != 0 || rule.Push) {
		return Err(CodeBadRequest, "egress rule %q sets repos/push but is not a git connector rule", rule.ID)
	}
	switch rule.Protocol {
	case EgressProtocolHTTP, EgressProtocolHTTPS, EgressProtocolConnect:
	default:
		return Err(CodeBadRequest, "egress rule %q has unsupported protocol %q", rule.ID, rule.Protocol)
	}
	if len(rule.Hosts) == 0 {
		return Err(CodeBadRequest, "egress rule %q requires at least one host", rule.ID)
	}
	for i, rawHost := range rule.Hosts {
		host, err := normalizeEgressHostPattern(rawHost)
		if err != nil {
			return Err(CodeBadRequest, "egress rule %q has invalid host pattern %q", rule.ID, rawHost)
		}
		rule.Hosts[i] = host
	}
	for _, port := range rule.Ports {
		if port == 0 {
			return Err(CodeBadRequest, "egress rule %q contains port zero", rule.ID)
		}
	}
	for i, method := range rule.Methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" || strings.ContainsAny(method, " \t\r\n") {
			return Err(CodeBadRequest, "egress rule %q has invalid method %q", rule.ID, method)
		}
		rule.Methods[i] = method
	}
	for i, prefix := range rule.PathPrefixes {
		cleanTarget := prefix
		if len(cleanTarget) > 1 {
			cleanTarget = strings.TrimSuffix(cleanTarget, "/")
		}
		if prefix == "" || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#\\%\x00") || path.Clean(cleanTarget) != cleanTarget {
			return Err(CodeBadRequest, "egress rule %q has invalid path prefix %q", rule.ID, prefix)
		}
		rule.PathPrefixes[i] = prefix
	}
	if rule.MaxRequests < 0 || rule.MaxRequestBytes < 0 || rule.MaxResponseBytes < 0 {
		return Err(CodeBadRequest, "egress rule %q has a negative limit", rule.ID)
	}
	if rule.SharedState == "" {
		if rule.Connector == EgressConnectorPackage {
			rule.SharedState = SharedStateImmutableRead
		} else {
			rule.SharedState = SharedStateNone
		}
	}
	switch rule.SharedState {
	case SharedStateNone, SharedStateImmutableRead, SharedStateScopedWrite, SharedStateGlobalWrite:
	default:
		return Err(CodeBadRequest, "egress rule %q has unknown shared state %q", rule.ID, rule.SharedState)
	}
	if rule.Protocol == EgressProtocolConnect {
		if rule.Connector != "" {
			return Err(CodeBadRequest, "CONNECT rule %q cannot invoke a managed connector", rule.ID)
		}
		if len(rule.PathPrefixes) != 0 || rule.MaxRequestBytes != 0 || rule.MaxResponseBytes != 0 {
			return Err(CodeBadRequest, "CONNECT rule %q cannot claim unenforceable path or byte limits", rule.ID)
		}
		if rule.SharedState != SharedStateNone {
			return Err(CodeBadRequest, "CONNECT rule %q cannot claim enforceable shared-state semantics", rule.ID)
		}
		for _, method := range rule.Methods {
			if method != "CONNECT" {
				return Err(CodeBadRequest, "CONNECT rule %q may only name method CONNECT", rule.ID)
			}
		}
		if len(rule.Methods) == 0 {
			rule.Methods = []string{"CONNECT"}
		}
	}
	if rule.SharedState == SharedStateImmutableRead {
		if len(rule.Methods) == 0 {
			rule.Methods = []string{"GET", "HEAD"}
		}
		for _, method := range rule.Methods {
			if method != "GET" && method != "HEAD" {
				return Err(CodeBadRequest, "immutable-read rule %q permits mutating method %s", rule.ID, method)
			}
		}
	}
	if rule.Connector == EgressConnectorPackage {
		if rule.Protocol != EgressProtocolHTTPS {
			return Err(CodeBadRequest, "package connector rule %q requires HTTPS", rule.ID)
		}
		if rule.SharedState != SharedStateImmutableRead {
			return Err(CodeBadRequest, "package connector rule %q must use immutable_read shared state", rule.ID)
		}
		if rule.MaxRequestBytes != 0 {
			return Err(CodeBadRequest, "package connector rule %q cannot permit a request body", rule.ID)
		}
	}
	if rule.Connector == EgressConnectorGit {
		if rule.Protocol != EgressProtocolHTTPS {
			return Err(CodeBadRequest, "git connector rule %q requires HTTPS", rule.ID)
		}
		if len(rule.Repos) == 0 {
			return Err(CodeBadRequest, "git connector rule %q requires at least one repo pattern", rule.ID)
		}
		rule.Repos = append([]string(nil), rule.Repos...)
		for i, pattern := range rule.Repos {
			pattern = strings.TrimSpace(pattern)
			if err := ValidateRepoPattern(pattern); err != nil {
				return Err(CodeBadRequest, "git connector rule %q: %v", rule.ID, err)
			}
			rule.Repos[i] = pattern
		}
		if len(rule.PathPrefixes) != 0 {
			return Err(CodeBadRequest, "git connector rule %q scopes by repos, not path prefixes", rule.ID)
		}
		if len(rule.Methods) == 0 {
			rule.Methods = []string{"GET", "POST"}
		}
		for _, method := range rule.Methods {
			if method != "GET" && method != "POST" {
				return Err(CodeBadRequest, "git connector rule %q permits non-smart-HTTP method %s", rule.ID, method)
			}
		}
		if rule.SharedState == SharedStateImmutableRead {
			return Err(CodeBadRequest, "git connector rule %q cannot claim immutable-read semantics", rule.ID)
		}
	}
	sort.Slice(rule.Ports, func(i, j int) bool { return rule.Ports[i] < rule.Ports[j] })
	sort.Strings(rule.Methods)
	return nil
}

func normalizeEgressHostPattern(raw string) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || strings.ContainsAny(raw, "/?#@\\%\x00\t\r\n ") {
		return "", errors.New("invalid host pattern")
	}
	for _, r := range raw {
		if r > 0x7f {
			return "", errors.New("host pattern must be ASCII")
		}
	}
	host, port := raw, ""
	if splitHost, splitPort, err := net.SplitHostPort(raw); err == nil {
		host, port = splitHost, splitPort
	} else if strings.HasPrefix(raw, "[") {
		if !strings.HasSuffix(raw, "]") {
			return "", errors.New("invalid bracketed host")
		}
		host = strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	} else if strings.Count(raw, ":") == 1 {
		return "", errors.New("invalid port")
	} else if strings.Count(raw, ":") > 1 && net.ParseIP(raw) == nil {
		return "", errors.New("invalid IPv6 host")
	}
	if strings.HasSuffix(raw, ":") && port == "" {
		return "", errors.New("invalid empty port")
	}
	if port != "" {
		parsed, err := strconv.ParseUint(port, 10, 16)
		if err != nil || parsed == 0 {
			return "", errors.New("invalid port")
		}
		port = strconv.FormatUint(parsed, 10)
	}
	host = strings.TrimSuffix(host, ".")
	wildcard := false
	switch {
	case host == "*":
	case strings.HasPrefix(host, "*."):
		wildcard = true
		host = strings.TrimPrefix(host, "*.")
	case strings.Contains(host, "*"):
		return "", errors.New("invalid wildcard")
	}
	if host != "*" {
		if ip := net.ParseIP(host); ip != nil {
			if wildcard {
				return "", errors.New("IP wildcard is invalid")
			}
			host = ip.String()
		} else if !validEgressDNSName(host) {
			return "", errors.New("invalid DNS name")
		}
	}
	if wildcard {
		host = "*." + host
	}
	if port == "" {
		return host, nil
	}
	if strings.Contains(host, ":") {
		return net.JoinHostPort(host, port), nil
	}
	return host + ":" + port, nil
}

func validEgressDNSName(host string) bool {
	if host == "" || len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' {
				return false
			}
		}
	}
	return true
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
