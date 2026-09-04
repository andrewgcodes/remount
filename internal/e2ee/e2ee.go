// Package e2ee seals Remount operation payloads between two authenticated
// peers so the relay routes frames it cannot read
// (docs/adr/0081-relay-payload-confidentiality.md).
//
// It is a transport.Conn wrapper rather than a change to the frame codec:
// frames addressed to another peer are sealed on the way out and opened on
// the way in, and frames addressed to the control plane are untouched,
// because the control plane is the party that must read them. Long-term
// identity, generation, grant and authorization checks are unchanged; this
// package adds confidentiality, not authority.
//
// What stays visible to the relay is the routing envelope only: protocol
// version, frame kind, correlation id, source and destination peer ids, and
// the controller epoch it stamps itself. Frame length, timing and traffic
// volume remain visible; this package does not conceal traffic analysis
// metadata.
package e2ee

import (
	"errors"
	"time"
)

// Policy decides what happens when a peer cannot negotiate a sealed session.
type Policy int

const (
	// PolicyDisabled never seals. It exists so the same code path can be run
	// with confidentiality off, which is what makes a leak scan's positive
	// control meaningful.
	PolicyDisabled Policy = iota
	// PolicyPreferred seals when the far peer answers a key agreement and
	// falls back to plaintext when it does not. A peer built before this
	// capability keeps working exactly as before.
	PolicyPreferred
	// PolicyRequired never lets an operation payload reach the wire in
	// plaintext. A peer that cannot negotiate gets an error before the frame
	// is transmitted, so the far side cannot have applied it.
	PolicyRequired
)

// String names the policy for diagnostics.
func (p Policy) String() string {
	switch p {
	case PolicyDisabled:
		return "disabled"
	case PolicyPreferred:
		return "preferred"
	case PolicyRequired:
		return "required"
	default:
		return "unknown"
	}
}

// Errors the guard reports to its caller. Wire-visible failures are reported
// as *proto.Error with a stable code; these are the internal reasons.
var (
	// ErrNotNegotiated means the far peer did not complete a key agreement.
	ErrNotNegotiated = errors.New("e2ee: peer did not negotiate end-to-end encryption")
	// ErrRekeyRequired means the record counter for a key is exhausted. It
	// can only be reached by sealing more frames under one key than a
	// connection will ever carry; the guard refuses rather than reusing a
	// nonce.
	ErrRekeyRequired = errors.New("e2ee: record counter exhausted, rekey required")
	// ErrReplayed means a record was already accepted, or is older than the
	// replay window still remembers.
	ErrReplayed = errors.New("e2ee: record replayed or outside the replay window")
	// ErrUnknownSession means no key exists for the session id a frame names.
	ErrUnknownSession = errors.New("e2ee: no key for that session")
	// ErrClosed means the guard was closed.
	ErrClosed = errors.New("e2ee: closed")
)

// Bounds. Every one of these caps state a peer on the far side of a relay can
// cause this process to retain. Unbounded state here is a memory-exhaustion
// vector reachable by a peer, so each has a default and none is optional.
const (
	defaultHandshakeTimeout = 5 * time.Second
	defaultIdleTimeout      = 10 * time.Minute
	defaultMaxSessions      = 64
	defaultMaxOutstanding   = 4096
	defaultMaxAuthFailures  = 64
	defaultFailureTTL       = 5 * time.Second
	// maxRestartsPerPeer bounds how often an unauthenticated restart hint may
	// make us discard a working key. A relay that forges them past this point
	// achieves nothing it could not achieve by dropping frames.
	maxRestartsPerPeer = 16
	// maxOfferSkew bounds how old or how far in the future an offer may claim
	// to be, so a captured offer cannot be presented much later.
	maxOfferSkew = 5 * time.Minute
	// maxPeerIDBytes bounds an identifier before it is length-prefixed into a
	// signature transcript or an AAD, where a 16-bit length is used.
	maxPeerIDBytes = 256
	// identityRenewMargin is how long a binding must still be valid for to
	// authenticate a key agreement. A binding closer to expiry than this is
	// rebound rather than used, so it cannot expire between this peer signing
	// an offer and the far peer verifying it.
	identityRenewMargin = 30 * time.Second
)

// EventKind classifies what the guard observed. Observation is for tests,
// diagnostics and future metrics; nothing behavioural depends on it.
type EventKind string

// Observable guard events.
const (
	EventEstablished EventKind = "established" // a sealed session is usable
	EventReplaced    EventKind = "replaced"    // a new key agreement replaced an older session
	EventDropped     EventKind = "dropped"     // an inbound frame was refused
	EventFallback    EventKind = "fallback"    // preferred policy sent plaintext
	EventRefused     EventKind = "refused"     // required policy refused to send
	EventDiscarded   EventKind = "discarded"   // a session was forgotten
)

// Event is one observation.
type Event struct {
	Kind    EventKind
	Peer    string
	Session string // hex session id, when one applies
	Reason  string
}
