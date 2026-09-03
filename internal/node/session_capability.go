package node

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

// localSessionCapability keeps the bearer exposed to an untrusted process
// stable while the signed control-plane proof behind it rotates. Neither the
// handle nor the signed proof is persisted.
type localSessionCapability struct {
	session    string
	principal  string
	tenant     string
	workspace  string
	generation uint64
	proof      string
	expiresAt  time.Time
}

func (n *Node) sessionCapabilityHandle(claims proto.GrantClaims, idempotencyKey string) (string, error) {
	var material []byte
	if idempotencyKey == "" {
		material = make([]byte, 32)
		if _, err := rand.Read(material); err != nil {
			return "", err
		}
	} else {
		mac := hmac.New(sha256.New, n.priv)
		_, _ = mac.Write([]byte("remount/session-capability-handle/v1\x00"))
		_, _ = mac.Write(proto.MustMarshal(struct {
			Client, Workspace, Principal, Tenant, IdempotencyKey string
			Generation, AuthzRevision                            uint64
		}{claims.Client, claims.WS, claims.Principal, claims.Tenant, idempotencyKey, claims.Gen, claims.AuthzRevision}))
		material = mac.Sum(nil)
	}
	return "sc_" + base64.RawURLEncoding.EncodeToString(material), nil
}

func (n *Node) matchingSessionCapability(handle string, claims proto.GrantClaims, w *ws) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	capability := n.sessionCapabilities[handle]
	return capability != nil && capability.principal == claims.Principal && capability.tenant == claims.Tenant &&
		capability.workspace == w.ID && capability.generation == w.Generation
}

func (n *Node) installSessionCapability(handle, proof string, expiresAt time.Time, claims proto.GrantClaims, w *ws) error {
	if handle == "" || proof == "" || !expiresAt.After(time.Now()) {
		return proto.Err(proto.CodeUnauthorized, "control returned an invalid session capability")
	}
	limit := n.sessions.Stats().MaxSessions
	n.mu.Lock()
	defer n.mu.Unlock()
	if existing := n.sessionCapabilities[handle]; existing != nil {
		if existing.principal == claims.Principal && existing.tenant == claims.Tenant && existing.workspace == w.ID && existing.generation == w.Generation {
			return nil
		}
		return proto.Err(proto.CodeConflict, "session capability handle collision")
	}
	if len(n.sessionCapabilities) >= limit {
		return proto.Err(proto.CodeResourceExhausted, "node session capability limit %d reached", limit)
	}
	n.sessionCapabilities[handle] = &localSessionCapability{
		principal: claims.Principal, tenant: claims.Tenant, workspace: w.ID, generation: w.Generation,
		proof: proof, expiresAt: expiresAt,
	}
	return nil
}

func (n *Node) bindSessionCapability(handle string, s *session.Session) {
	n.mu.Lock()
	capability := n.sessionCapabilities[handle]
	if capability == nil {
		n.mu.Unlock()
		return
	}
	capability.session = s.ID
	n.capabilityWG.Add(1)
	n.mu.Unlock()
	go n.maintainSessionCapability(handle)
}

func (n *Node) dropSessionCapability(handle string) {
	n.mu.Lock()
	delete(n.sessionCapabilities, handle)
	n.mu.Unlock()
}

func (n *Node) dropSessionCapabilities(sessionID string) {
	n.mu.Lock()
	for handle, capability := range n.sessionCapabilities {
		if capability.session == sessionID {
			delete(n.sessionCapabilities, handle)
		}
	}
	n.mu.Unlock()
}

func (n *Node) maintainSessionCapability(handle string) {
	defer n.capabilityWG.Done()
	for {
		n.mu.Lock()
		capability := n.sessionCapabilities[handle]
		if capability == nil {
			n.mu.Unlock()
			return
		}
		remaining := time.Until(capability.expiresAt)
		n.mu.Unlock()
		if remaining <= 0 {
			return
		}
		delay := remaining / 2
		if delay < 25*time.Millisecond {
			delay = 25 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-n.stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
		if err := n.renewSessionCapability(handle); err != nil {
			var protocol *proto.Error
			if errors.As(err, &protocol) && (protocol.Code == proto.CodeDenied || protocol.Code == proto.CodeUnauthorized || protocol.Code == proto.CodeNotFound) {
				n.mu.Lock()
				current := n.sessionCapabilities[handle]
				delete(n.sessionCapabilities, handle)
				n.mu.Unlock()
				if current != nil && current.session != "" {
					n.sessions.Terminate(current.session, proto.ExitReasonRevoked)
				}
				return
			}
			// A transient control outage never authorizes a request. Retry while
			// the current short-lived proof remains usable.
			select {
			case <-time.After(50 * time.Millisecond):
			case <-n.stop:
				return
			}
		}
	}
}

func (n *Node) renewSessionCapability(handle string) error {
	n.mu.Lock()
	capability := n.sessionCapabilities[handle]
	p := n.peer
	if capability == nil || p == nil {
		n.mu.Unlock()
		return proto.Err(proto.CodeUnreachable, "control connection lost before session capability renewal")
	}
	proof, workspaceID, generation := capability.proof, capability.workspace, capability.generation
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var response proto.SessionCapabilityIssueRes
	if err := p.Call(ctx, proto.PeerControl, proto.OpSessionCapabilityRenew, proto.SessionCapabilityRenewReq{
		Workspace: workspaceID, Generation: generation, Capability: proof,
	}, &response); err != nil {
		return err
	}
	expiresAt := time.UnixMilli(response.ExpiresAt)
	if response.Capability == "" || !expiresAt.After(time.Now()) {
		return proto.Err(proto.CodeUnauthorized, "control returned an invalid renewed session capability")
	}
	n.mu.Lock()
	current := n.sessionCapabilities[handle]
	if current == capability && current.proof == proof {
		current.proof, current.expiresAt = response.Capability, expiresAt
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) verifyLocalSessionCapability(ctx context.Context, w *ws, handle string) (string, error) {
	n.mu.Lock()
	capability := n.sessionCapabilities[handle]
	p := n.peer
	if capability == nil || capability.workspace != w.ID || capability.generation != w.Generation || p == nil {
		n.mu.Unlock()
		return "", proto.Err(proto.CodeDenied, "session capability is unknown or stale")
	}
	proof, principal := capability.proof, capability.principal
	n.mu.Unlock()
	var result proto.SessionCapabilityCheckRes
	if err := p.Call(ctx, proto.PeerControl, proto.OpSessionCapabilityCheck, proto.SessionCapabilityCheckReq{
		Workspace: w.ID, Generation: w.Generation, Capability: proof,
	}, &result); err != nil {
		return "", err
	}
	if result.Principal == "" || result.Principal != principal || result.Tenant != w.Tenant {
		return "", proto.Err(proto.CodeDenied, "session capability attribution mismatch")
	}
	return result.Principal, nil
}
