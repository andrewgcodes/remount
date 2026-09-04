package e2ee

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/hex"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
)

// Session is one agreed key pair between two peers: one key for each
// direction, each with its own record counter and its own replay window.
//
// A session belongs to one connection. Reconnecting produces a new
// connection, a new key agreement and a new session, which is what "rekey on
// reconnect" means here: there is no key that outlives the connection it was
// agreed on.
type Session struct {
	// Self, Peer and SID are fixed at agreement time and are what the record
	// AAD binds every ciphertext to.
	Self string
	Peer string
	SID  [sidLen]byte
	// Offerer decides which direction key this side sends under.
	Offerer bool
	// TS is the offer timestamp this session was agreed from. A later offer
	// may replace this session; an older one may not.
	TS int64

	mu       sync.Mutex
	send     cipher.AEAD
	sendIV   []byte
	sendDir  byte
	sendSeq  uint64
	recv     cipher.AEAD
	recvIV   []byte
	recvDir  byte
	recvHigh uint64
	recvMask uint64 // bitmap of the 64 records below recvHigh
	lastUsed time.Time
	restarts int
}

// newSession builds the two directional AEADs from the derived key material.
func newSession(self, peer string, sid [sidLen]byte, offerer bool, ts int64, keys directionKeys, now time.Time) (*Session, error) {
	sendKey, recvKey := keys.o2aKey, keys.a2oKey
	sendIV, recvIV := keys.o2aIV, keys.a2oIV
	sendDir, recvDir := dirO2A, dirA2O
	if !offerer {
		sendKey, recvKey = recvKey, sendKey
		sendIV, recvIV = recvIV, sendIV
		sendDir, recvDir = recvDir, sendDir
	}
	send, err := newAEAD(sendKey)
	if err != nil {
		return nil, err
	}
	recv, err := newAEAD(recvKey)
	if err != nil {
		return nil, err
	}
	return &Session{
		Self: self, Peer: peer, SID: sid, Offerer: offerer, TS: ts,
		send: send, sendIV: sendIV, sendDir: sendDir,
		recv: recv, recvIV: recvIV, recvDir: recvDir,
		lastUsed: now,
	}, nil
}

// ID is the hex session id, for diagnostics and tests.
func (s *Session) ID() string { return hex.EncodeToString(s.SID[:]) }

// Seal encrypts everything about f except the routing envelope. The returned
// frame keeps only v, t, id, to, from and the controller epoch the relay
// stamps; op is replaced by a constant marker so the operation name itself is
// not exposed.
func (s *Session) Seal(f *proto.Frame, now time.Time) (*proto.Frame, error) {
	payload, err := proto.Marshal(proto.E2EEPayload{Op: f.Op, S: f.S, WS: f.WS, Seq: f.Seq, Body: f.Body, Err: f.Err})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The counter is assigned under the lock that guards the AEAD, so two
	// concurrent Sends can never be handed the same value. It never wraps: at
	// maxRecord the session refuses rather than reusing a nonce.
	if s.sendSeq >= maxRecord {
		return nil, ErrRekeyRequired
	}
	s.sendSeq++
	n := s.sendSeq
	aad, err := recordAAD(s.sendDir, s.SID[:], n, f.T, s.Self, s.Peer, f.ID)
	if err != nil {
		return nil, err
	}
	iv := nonce(s.sendIV, n)
	ct := s.send.Seal(nil, iv[:], payload, aad)
	s.lastUsed = now
	body, err := proto.Marshal(proto.E2EESealed{Session: s.SID[:], N: n, CT: ct})
	if err != nil {
		return nil, err
	}
	return &proto.Frame{
		V: proto.Version, T: f.T, ID: f.ID, To: f.To, From: f.From,
		ControllerEpoch: f.ControllerEpoch,
		Op:              proto.OpE2EESealed,
		Body:            body,
	}, nil
}

// Open reverses Seal. It authenticates before it changes any state: the
// replay window advances only after the AEAD accepts, so a forged record with
// a high counter cannot push the window past records that have not arrived.
func (s *Session) Open(f *proto.Frame, now time.Time) (*proto.Frame, error) {
	var sealed proto.E2EESealed
	if err := f.Decode(&sealed); err != nil {
		return nil, err
	}
	if len(sealed.Session) != sidLen || subtle.ConstantTimeCompare(sealed.Session, s.SID[:]) != 1 {
		return nil, ErrUnknownSession
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.windowAllows(sealed.N) {
		return nil, ErrReplayed
	}
	aad, err := recordAAD(s.recvDir, s.SID[:], sealed.N, f.T, s.Peer, s.Self, f.ID)
	if err != nil {
		return nil, err
	}
	iv := nonce(s.recvIV, sealed.N)
	plain, err := s.recv.Open(nil, iv[:], sealed.CT, aad)
	if err != nil {
		return nil, err
	}
	var payload proto.E2EEPayload
	if err := proto.Unmarshal(plain, &payload); err != nil {
		return nil, err
	}
	s.windowAccept(sealed.N)
	s.lastUsed = now
	return &proto.Frame{
		V: proto.Version, T: f.T, ID: f.ID, To: f.To, From: f.From,
		ControllerEpoch: f.ControllerEpoch,
		Op:              payload.Op, S: payload.S, WS: payload.WS, Seq: payload.Seq,
		Body: payload.Body, Err: payload.Err,
	}, nil
}

// windowAllows reports whether counter n is new. The relay may reorder, so a
// strictly increasing check would drop honest frames; a 64-record sliding
// window accepts reordering and still rejects every replay it can still see.
// Anything older than the window is refused rather than guessed at.
func (s *Session) windowAllows(n uint64) bool {
	if n == 0 || n > maxRecord {
		return false
	}
	if n > s.recvHigh {
		return true
	}
	if s.recvHigh-n >= 64 {
		return false
	}
	return s.recvMask&(1<<(s.recvHigh-n)) == 0
}

// windowAccept records n as seen. State is one counter and one 64-bit mask
// per direction: bounded, whatever a peer sends.
func (s *Session) windowAccept(n uint64) {
	if n > s.recvHigh {
		shift := n - s.recvHigh
		if shift >= 64 {
			s.recvMask = 0
		} else {
			s.recvMask <<= shift
		}
		s.recvMask |= 1
		s.recvHigh = n
		return
	}
	s.recvMask |= 1 << (s.recvHigh - n)
}

// idle reports whether the session has gone unused for d.
func (s *Session) idle(now time.Time, d time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Sub(s.lastUsed) >= d
}

// allowRestart consumes one of the bounded restart credits. An unauthenticated
// restart hint is only a liveness aid; a relay that forges them past this
// bound gains nothing it could not get by dropping frames.
func (s *Session) allowRestart() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restarts >= maxRestartsPerPeer {
		return false
	}
	s.restarts++
	return true
}

// used reports whether anything has been sealed under this session, so a
// restart hint cannot discard a key that has not been used yet.
func (s *Session) used() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendSeq > 0
}

// lastUse is when a record was last sealed or opened under this session.
func (s *Session) lastUse() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastUsed
}
