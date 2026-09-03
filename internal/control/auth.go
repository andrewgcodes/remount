package control

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// Authenticate checks the token and assigns/validates the peer id.
func (c *Control) Authenticate(ctx context.Context, h *proto.Hello) (string, *proto.HelloOK, error) {
	caps, err := proto.NegotiateCapabilities(h.Caps)
	if err != nil {
		return "", nil, err
	}
	if err := c.requireDeploymentCapabilities(h.Role, caps); err != nil {
		return "", nil, err
	}
	ok := &proto.HelloOK{Caps: caps, Server: "remount", Now: c.now().UnixMilli(), PubKey: c.PublicKey(), LeaseSec: c.opts.LeaseSec}
	switch h.Role {
	case proto.RoleNode:
		if h.Peer == "" || !strings.HasPrefix(h.Peer, "n_") || len(h.PubKey) != ed25519.PublicKeySize {
			return "", nil, proto.Err(proto.CodeBadRequest, "node hello needs an n_ id and an ed25519 public key")
		}
		if err := c.verifyNodeProofLocked(h); err != nil {
			return "", nil, err
		}
		if c.opts.NodeAuthenticator != nil {
			identity, err := c.opts.NodeAuthenticator.AuthenticateNode(ctx, h.Peer, h.Token, append([]byte(nil), h.PubKey...))
			if err != nil || identity.Tenant == "" {
				return "", nil, proto.Err(proto.CodeUnauthorized, "node enrollment failed")
			}
			h.Labels = cloneMap(identity.Labels)
			if h.Labels == nil {
				h.Labels = map[string]string{}
			}
			h.Labels["tenant"] = identity.Tenant
			if identity.Pool != "" {
				h.Labels["pool"] = identity.Pool
			}
			if err := c.validateDynamicNode(h); err != nil {
				return "", nil, err
			}
			ok.NodeToken = identity.Token
		} else if c.opts.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.opts.Token)) != 1 {
			return "", nil, proto.Err(proto.CodeUnauthorized, "bad node token")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.opts.NodeAuthenticator == nil {
			if approval, required := c.opts.ApprovedNodes[h.Peer]; required || c.opts.ApprovedNodes != nil {
				if !required || subtle.ConstantTimeCompare(approval.PubKey, h.PubKey) != 1 {
					return "", nil, proto.Err(proto.CodeUnauthorized, "node is not operator-approved")
				}
				h.Labels = cloneMap(approval.Labels)
				info := approval.Info
				h.Node = &info
			}
		}
		if n, exists := c.nodes[h.Peer]; exists && len(n.PubKey) > 0 && subtle.ConstantTimeCompare(n.PubKey, h.PubKey) != 1 {
			return "", nil, proto.Err(proto.CodeUnauthorized, "node id %s is registered to a different key", h.Peer)
		}
		ok.Peer = h.Peer
		return h.Peer, ok, nil
	case proto.RoleClient:
		var subject Subject
		var err error
		if c.opts.Authenticator != nil {
			subject, err = c.opts.Authenticator.Authenticate(ctx, Credential{Token: h.Token, Role: h.Role, Peer: h.Peer})
		} else {
			if c.opts.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.opts.Token)) != 1 {
				return "", nil, proto.Err(proto.CodeUnauthorized, "bad token")
			}
			subject = c.opts.SharedSubject
			if subject.ID == "" {
				subject = Subject{ID: "local-user", Tenant: "local", Roles: []string{"admin"}}
			}
		}
		if err != nil || subject.ID == "" || subject.Tenant == "" {
			return "", nil, proto.Err(proto.CodeUnauthorized, "authentication failed")
		}
		id := h.Peer
		if id == "" || !strings.HasPrefix(id, "c_") {
			id = ids.New("c")
		}
		ok.Peer = id
		ok.Subject, ok.Tenant = subject.ID, subject.Tenant
		c.mu.Lock()
		c.subjects[id] = subject
		c.mu.Unlock()
		return id, ok, nil
	default:
		return "", nil, proto.Err(proto.CodeBadRequest, "unknown role %q", h.Role)
	}
}

func (c *Control) validateDynamicNode(h *proto.Hello) error {
	if h.Node == nil || len(h.Node.BackendDescriptors) == 0 {
		return proto.Err(proto.CodeUnauthorized, "enrolled node has no backend descriptors")
	}
	for _, descriptor := range h.Node.BackendDescriptors {
		if err := proto.ValidateBackendSecurity(proto.SecuritySpec{Profile: c.opts.SecurityProfileFloor}, descriptor); err != nil {
			return proto.Err(proto.CodeUnauthorized, "enrolled node backend %s cannot satisfy %s", descriptor.Name, c.opts.SecurityProfileFloor)
		}
	}
	return nil
}

func (c *Control) verifyNodeProofLocked(h *proto.Hello) error {
	now := c.now().UnixMilli()
	if len(h.Nonce) < 16 || len(h.Proof) != ed25519.SignatureSize || h.IssuedAt < now-60_000 || h.IssuedAt > now+60_000 {
		return proto.Err(proto.CodeUnauthorized, "missing or stale node proof")
	}
	if !ed25519.Verify(ed25519.PublicKey(h.PubKey), proto.HelloProofBytes(*h), h.Proof) {
		return proto.Err(proto.CodeUnauthorized, "invalid node proof")
	}
	proofID := h.Peer + "|" + hex.EncodeToString(h.Nonce)
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, expires := range c.proofs {
		if expires < now {
			delete(c.proofs, id)
		}
	}
	if _, replayed := c.proofs[proofID]; replayed {
		return proto.Err(proto.CodeUnauthorized, "replayed node proof")
	}
	c.proofs[proofID] = now + 120_000
	return nil
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
