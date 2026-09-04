package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"time"

	"remount.dev/remount/internal/proto"
)

// Identity is what one peer authenticates its key agreements with: an
// ed25519 private key and the control-plane-signed binding that says which
// peer id that key belongs to.
//
// The binding is issued by the control plane because the control plane is the
// only party that knows which identity owns a peer id. Deriving it from the
// relay's view would make the relay the trust anchor, which is precisely the
// party this package removes trust from.
type Identity struct {
	Binding proto.PeerBinding
	Key     ed25519.PrivateKey
}

// NewIdentity generates a key pair and returns the unsigned binding the
// control plane must sign, together with the identity that will use it.
func NewIdentity(peer, tenant string, now time.Time, ttl time.Duration) (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{
		Binding: proto.PeerBinding{
			Peer:   peer,
			Key:    pub,
			Tenant: tenant,
			Issued: now.Unix(),
			Exp:    now.Add(ttl).Unix(),
		},
		Key: priv,
	}, nil
}

// SignBinding signs an identity's binding with the control plane's key. It is
// the operation the control plane performs when it issues a binding; it lives
// here so the verification and the issuance cannot drift apart.
func SignBinding(controlKey ed25519.PrivateKey, b proto.PeerBinding) proto.PeerBinding {
	b.Sig = ed25519.Sign(controlKey, proto.PeerBindingBytes(b))
	return b
}

// VerifyBinding checks a binding presented by a peer. It fails closed: an
// absent control key, a wrong peer id, an expired binding or a bad signature
// are all refusals, never a downgrade to an unauthenticated channel.
func VerifyBinding(controlKey ed25519.PublicKey, b proto.PeerBinding, peer string, now time.Time) error {
	if len(controlKey) != ed25519.PublicKeySize {
		return proto.Err(proto.CodeUnauthorized, "e2ee: no control-plane key to verify peer bindings against")
	}
	if b.Peer == "" || b.Peer != peer {
		return proto.Err(proto.CodeUnauthorized, "e2ee: binding names peer %q, frame came from %q", b.Peer, peer)
	}
	if len(b.Peer) > maxPeerIDBytes {
		return proto.Err(proto.CodeUnauthorized, "e2ee: binding peer id is too long")
	}
	if len(b.Key) != ed25519.PublicKeySize {
		return proto.Err(proto.CodeUnauthorized, "e2ee: binding carries no usable key")
	}
	if b.Exp == 0 || now.Unix() >= b.Exp {
		return proto.Err(proto.CodeUnauthorized, "e2ee: binding for %q expired", peer)
	}
	if !ed25519.Verify(controlKey, proto.PeerBindingBytes(b), b.Sig) {
		return proto.Err(proto.CodeUnauthorized, "e2ee: binding for %q is not signed by the control plane", peer)
	}
	return nil
}
