package e2ee

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"time"

	"remount.dev/remount/internal/proto"
)

// pendingOffer is one key agreement this side started. It holds the ephemeral
// private key until the accept arrives and is discarded either way, so the
// secret exists for at most one handshake timeout.
type pendingOffer struct {
	peer    string
	sid     [sidLen]byte
	priv    *ecdh.PrivateKey
	nonce   []byte
	ts      int64
	done    chan struct{}
	session *Session
	err     error
}

// buildOffer creates the ephemeral key and the signed offer. The offer names
// the peer it is for, so a relay that redirects it to a different peer
// produces a message that peer refuses rather than one it answers.
func buildOffer(self, peer string, id *Identity, now time.Time) (*pendingOffer, *proto.E2EEKeyExchange, error) {
	if err := checkPeerIDs(self, peer); err != nil {
		return nil, nil, err
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	offer := &pendingOffer{peer: peer, priv: priv, ts: now.Unix(), done: make(chan struct{})}
	if _, err := rand.Read(offer.sid[:]); err != nil {
		return nil, nil, err
	}
	offer.nonce = make([]byte, sidLen)
	if _, err := rand.Read(offer.nonce); err != nil {
		return nil, nil, err
	}
	half, err := transcript(self, peer, offer.sid[:], priv.PublicKey().Bytes(), offer.nonce, offer.ts, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	return offer, &proto.E2EEKeyExchange{
		Type:    proto.E2EEOffer,
		Suite:   proto.E2EESuiteX25519,
		Caps:    []string{proto.CapabilityV1, proto.CapabilityE2EEPayloads},
		Session: offer.sid[:],
		From:    self,
		To:      peer,
		Eph:     priv.PublicKey().Bytes(),
		Nonce:   offer.nonce,
		TS:      offer.ts,
		Binding: id.Binding,
		Sig:     ed25519.Sign(id.Key, signedOffer(half)),
	}, nil
}

// acceptOffer verifies an offer and answers it. Everything it checks is
// covered by the offerer's signature or by the control plane's signature over
// the binding, so the relay between them can drop this message but cannot
// author one.
func acceptOffer(self string, id *Identity, controlKey ed25519.PublicKey, kx *proto.E2EEKeyExchange, from string, now time.Time) (*Session, *proto.E2EEKeyExchange, error) {
	if err := checkPeerIDs(self, from); err != nil {
		return nil, nil, err
	}
	if kx.Suite != proto.E2EESuiteX25519 {
		return nil, nil, proto.Err(proto.CodeUnsupported, "e2ee: unsupported suite %q", kx.Suite)
	}
	if !proto.HasCapability(kx.Caps, proto.CapabilityE2EEPayloads) {
		return nil, nil, proto.Err(proto.CodeUnsupported, "e2ee: offer does not name capability %q", proto.CapabilityE2EEPayloads)
	}
	// From is what the relay stamped and is authoritative for routing; the
	// offer's own claim and its binding must agree with it, or the frame has
	// been moved between peers.
	if kx.From != from || kx.To != self {
		return nil, nil, proto.Err(proto.CodeUnauthorized, "e2ee: offer from %q for %q arrived from %q at %q", kx.From, kx.To, from, self)
	}
	if len(kx.Session) != sidLen || len(kx.Eph) == 0 || len(kx.Nonce) == 0 {
		return nil, nil, proto.Err(proto.CodeBadRequest, "e2ee: malformed offer")
	}
	if skew := now.Sub(time.Unix(kx.TS, 0)); skew > maxOfferSkew || skew < -maxOfferSkew {
		return nil, nil, proto.Err(proto.CodeUnauthorized, "e2ee: offer timestamp is outside the accepted skew")
	}
	if err := VerifyBinding(controlKey, kx.Binding, from, now); err != nil {
		return nil, nil, err
	}
	if id.Binding.Tenant != "" && kx.Binding.Tenant != "" && id.Binding.Tenant != kx.Binding.Tenant {
		return nil, nil, proto.Err(proto.CodeUnauthorized, "e2ee: peer %q belongs to another tenant", from)
	}
	half, err := transcript(from, self, kx.Session, kx.Eph, kx.Nonce, kx.TS, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	if !ed25519.Verify(ed25519.PublicKey(kx.Binding.Key), signedOffer(half), kx.Sig) {
		return nil, nil, proto.Err(proto.CodeUnauthorized, "e2ee: offer signature from %q does not verify", from)
	}

	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	replyNonce := make([]byte, sidLen)
	if _, err := rand.Read(replyNonce); err != nil {
		return nil, nil, err
	}
	full, err := transcript(from, self, kx.Session, kx.Eph, kx.Nonce, kx.TS, priv.PublicKey().Bytes(), replyNonce)
	if err != nil {
		return nil, nil, err
	}
	shared, err := agree(priv, kx.Eph)
	if err != nil {
		return nil, nil, err
	}
	keys, err := deriveKeys(shared, full)
	if err != nil {
		return nil, nil, err
	}
	var sid [sidLen]byte
	copy(sid[:], kx.Session)
	session, err := newSession(self, from, sid, false, kx.TS, keys, now)
	if err != nil {
		return nil, nil, err
	}
	return session, &proto.E2EEKeyExchange{
		Type:    proto.E2EEAccept,
		Suite:   proto.E2EESuiteX25519,
		Caps:    []string{proto.CapabilityV1, proto.CapabilityE2EEPayloads},
		Session: kx.Session,
		From:    self,
		To:      from,
		Eph:     priv.PublicKey().Bytes(),
		Nonce:   replyNonce,
		TS:      kx.TS,
		Binding: id.Binding,
		Sig:     ed25519.Sign(id.Key, signedAccept(full)),
	}, nil
}

// completeOffer turns a verified accept into a session on the offerer's side.
func completeOffer(offer *pendingOffer, self string, id *Identity, controlKey ed25519.PublicKey, kx *proto.E2EEKeyExchange, from string, now time.Time) (*Session, error) {
	if kx.Suite != proto.E2EESuiteX25519 {
		return nil, proto.Err(proto.CodeUnsupported, "e2ee: accept names suite %q", kx.Suite)
	}
	if from != offer.peer || kx.From != from || kx.To != self {
		return nil, proto.Err(proto.CodeUnauthorized, "e2ee: accept from %q does not belong to the offer to %q", from, offer.peer)
	}
	if len(kx.Eph) == 0 || len(kx.Nonce) == 0 {
		return nil, proto.Err(proto.CodeBadRequest, "e2ee: malformed accept")
	}
	if err := VerifyBinding(controlKey, kx.Binding, from, now); err != nil {
		return nil, err
	}
	if id.Binding.Tenant != "" && kx.Binding.Tenant != "" && id.Binding.Tenant != kx.Binding.Tenant {
		return nil, proto.Err(proto.CodeUnauthorized, "e2ee: peer %q belongs to another tenant", from)
	}
	full, err := transcript(self, offer.peer, offer.sid[:], offer.priv.PublicKey().Bytes(), offer.nonce, offer.ts, kx.Eph, kx.Nonce)
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(ed25519.PublicKey(kx.Binding.Key), signedAccept(full), kx.Sig) {
		return nil, proto.Err(proto.CodeUnauthorized, "e2ee: accept signature from %q does not verify", from)
	}
	shared, err := agree(offer.priv, kx.Eph)
	if err != nil {
		return nil, err
	}
	keys, err := deriveKeys(shared, full)
	if err != nil {
		return nil, err
	}
	return newSession(self, offer.peer, offer.sid, true, offer.ts, keys, now)
}

// checkPeerIDs refuses identifiers that cannot be safely length-prefixed into
// a transcript. Peer ids are assigned by the control plane and are short; an
// oversized one arrived from a relay that is making things up.
func checkPeerIDs(ids ...string) error {
	for _, id := range ids {
		if id == "" || len(id) > maxPeerIDBytes {
			return proto.Err(proto.CodeBadRequest, "e2ee: unusable peer id of %d bytes", len(id))
		}
	}
	return nil
}
