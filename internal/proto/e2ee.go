package proto

// End-to-end payload confidentiality between two authenticated peers
// (docs/adr/0081-relay-payload-confidentiality.md). The relay routes by To and
// From; with this capability negotiated it can no longer read what it routes.
//
// Everything here is additive. A peer that does not implement the capability
// never receives a sealed frame, because sealing only happens after a key
// agreement that such a peer never answers.

// CapabilityE2EEPayloads names end-to-end sealed operation payloads. A peer
// ignoring it would hand the relay every operation body and response in
// plaintext, which is exactly the property the capability promises is closed.
const CapabilityE2EEPayloads = "e2ee-payloads"

// E2EESuiteX25519 is the only cipher suite in this release: X25519 key
// agreement, HKDF-SHA-256 key schedule, AES-256-GCM record protection. It is
// named on the wire so a future suite is a negotiation, not a version bump.
const E2EESuiteX25519 = "x25519-hkdf-sha256-aes256gcm"

// Operations the confidentiality layer owns. Both are invisible to the peers
// above it: OpE2EEKeyExchange frames are consumed by the layer that sent
// them, and OpE2EESealed is the constant Op every sealed frame carries so the
// real Op is not exposed to the relay.
const (
	OpE2EEKeyExchange = "e2ee.kx"
	OpE2EESealed      = "e2ee.sealed"
)

// Key-exchange message types.
const (
	E2EEOffer   = "offer"   // I propose a session; here is my ephemeral key
	E2EEAccept  = "accept"  // I accept it; here is mine
	E2EEReject  = "reject"  // I will not accept this offer
	E2EERestart = "restart" // I hold no key for the session you sealed under
)

// PeerBinding binds a peer id to the ed25519 key that peer authenticates key
// agreements with. It is issued and signed by the control plane, which is the
// only party that knows which identity owns a peer id. Long-term identity,
// enrollment, grants and authorization are unchanged: a binding confers no
// authority, only the right to be recognized as the far end of a channel.
type PeerBinding struct {
	Peer   string `cbor:"peer" json:"peer"`
	Key    []byte `cbor:"key" json:"key"`                           // ed25519 public key
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"` // tenant the peer belongs to
	Issued int64  `cbor:"issued,omitempty" json:"issued,omitempty"` // unix seconds
	Exp    int64  `cbor:"exp" json:"exp"`                           // unix seconds
	Sig    []byte `cbor:"sig,omitempty" json:"sig,omitempty"`       // control's ed25519 signature over PeerBindingBytes
}

// PeerBindingBytes returns the deterministic bytes the control plane signs.
// Sig is omitted to avoid a recursive signature, exactly as with a grant.
func PeerBindingBytes(b PeerBinding) []byte {
	b.Sig = nil
	return MustMarshal(b)
}

// E2EEKeyExchange is the plaintext body of an OpE2EEKeyExchange event. It is
// plaintext by necessity: it is what establishes the keys. Every field a
// receiver acts on is covered by Sig, and the transcript names both peer ids,
// so a relay can drop or delay it but cannot redirect or rewrite it.
type E2EEKeyExchange struct {
	Type    string      `cbor:"type" json:"type"`
	Suite   string      `cbor:"suite,omitempty" json:"suite,omitempty"`
	Caps    []string    `cbor:"caps,omitempty" json:"caps,omitempty"` // capability identifiers the sender implements
	Session []byte      `cbor:"session" json:"session"`               // 16 random bytes chosen by the offerer
	From    string      `cbor:"from" json:"from"`                     // claimed sender; checked against the relay stamp and the binding
	To      string      `cbor:"to" json:"to"`                         // intended recipient; a redirected offer fails here
	Eph     []byte      `cbor:"eph,omitempty" json:"eph,omitempty"`   // X25519 public key
	Nonce   []byte      `cbor:"nonce,omitempty" json:"nonce,omitempty"`
	TS      int64       `cbor:"ts,omitempty" json:"ts,omitempty"` // offerer's unix seconds; bounds offer replay
	Binding PeerBinding `cbor:"binding,omitempty" json:"binding,omitempty"`
	Sig     []byte      `cbor:"sig,omitempty" json:"sig,omitempty"`       // ed25519 over the transcript, by Binding.Key
	Reason  string      `cbor:"reason,omitempty" json:"reason,omitempty"` // reject/restart diagnostics; never acted on
}

// E2EESealed is the body of every sealed frame. Only the fields the relay
// needs to route stay outside it.
type E2EESealed struct {
	Session []byte `cbor:"sid" json:"sid"` // which key agreement produced the key
	N       uint64 `cbor:"n" json:"n"`     // per-key, per-direction record counter
	CT      []byte `cbor:"ct" json:"ct"`   // AES-256-GCM ciphertext of E2EEPayload
}

// E2EEPayload is everything a sealed frame hides. Op is inside because the
// operation a client asks a node to perform is itself information the relay
// does not need to route the frame.
type E2EEPayload struct {
	Op   string `cbor:"op,omitempty" json:"op,omitempty"`
	S    string `cbor:"s,omitempty" json:"s,omitempty"`
	WS   string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Seq  uint64 `cbor:"seq,omitempty" json:"seq,omitempty"`
	Body []byte `cbor:"body,omitempty" json:"body,omitempty"`
	Err  *Error `cbor:"err,omitempty" json:"err,omitempty"`
}
