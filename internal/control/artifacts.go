package control

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"net/http"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

const (
	artifactProofTTL       = 30 * time.Second
	artifactProofClockSkew = 5 * time.Second
	artifactProofMaxBytes  = 4096
)

var artifactProofDomain = []byte("remount/artifact-proof/v1\x00")

// IssueArtifactProof returns a short-lived proof for one exact HTTP transfer.
// Node and tenant come from authenticated control state, never the request.
func (c *Control) IssueArtifactProof(_ context.Context, node string, req proto.ArtifactProofReq) (*proto.ArtifactProofRes, error) {
	if _, err := artifact.Digest(req.Artifact); err != nil || req.Workspace == "" || req.Generation == 0 || !artifactProofMethod(req.Method) {
		return nil, proto.Err(proto.CodeBadRequest, "invalid artifact proof request")
	}
	now := c.now()
	c.mu.Lock()
	ws := c.workspaces[req.Workspace]
	if ws == nil || ws.Node != node || ws.Generation != req.Generation || !artifactProofState(ws.State) || ws.Tenant == "" {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeDenied, "node does not hold workspace generation")
	}
	tenant := ws.Tenant
	expires := now.Add(artifactProofTTL).UnixMilli()
	if ws.LeaseUntil > 0 && ws.LeaseUntil < expires {
		expires = ws.LeaseUntil
	}
	c.mu.Unlock()
	if expires <= now.UnixMilli() {
		return nil, proto.Err(proto.CodeDenied, "workspace lease is expired")
	}
	claims := proto.ArtifactProofClaims{
		ID: ids.New("arp"), Node: node, Tenant: tenant, Workspace: req.Workspace,
		Generation: req.Generation, Method: req.Method, Artifact: req.Artifact,
		IssuedAt: now.UnixMilli(), ExpiresAt: expires,
	}
	signature := ed25519.Sign(c.key, artifactProofBytes(claims))
	token := base64.RawURLEncoding.EncodeToString(proto.MustMarshal(proto.ArtifactProofEnvelope{Claims: claims, Signature: signature}))
	return &proto.ArtifactProofRes{Proof: token}, nil
}

// AuthorizeNodeArtifact verifies a control signature, exact authenticated
// request binding, expiry, and current live assignment before storage access.
func (c *Control) AuthorizeNodeArtifact(_ context.Context, node, tenant, workspace string, generation uint64, method, id, proof string) error {
	if len(proof) == 0 || len(proof) > artifactProofMaxBytes || !artifactProofMethod(method) {
		return proto.Err(proto.CodeUnauthorized, "invalid artifact authorization")
	}
	raw, err := base64.RawURLEncoding.DecodeString(proof)
	if err != nil || len(raw) == 0 || len(raw) > artifactProofMaxBytes {
		return proto.Err(proto.CodeUnauthorized, "invalid artifact authorization")
	}
	var envelope proto.ArtifactProofEnvelope
	if proto.Unmarshal(raw, &envelope) != nil || len(envelope.Signature) != ed25519.SignatureSize ||
		!ed25519.Verify(c.key.Public().(ed25519.PublicKey), artifactProofBytes(envelope.Claims), envelope.Signature) {
		return proto.Err(proto.CodeUnauthorized, "invalid artifact authorization")
	}
	claims := envelope.Claims
	if claims.Node != node || claims.Tenant != tenant || claims.Workspace != workspace || claims.Generation != generation ||
		claims.Method != method || claims.Artifact != id || claims.ID == "" {
		return proto.Err(proto.CodeUnauthorized, "artifact authorization does not cover request")
	}
	now := c.now().UnixMilli()
	if claims.ExpiresAt < now || claims.IssuedAt > now+artifactProofClockSkew.Milliseconds() ||
		claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > artifactProofTTL.Milliseconds() {
		return proto.Err(proto.CodeUnauthorized, "artifact authorization expired")
	}
	c.mu.Lock()
	ws := c.workspaces[workspace]
	valid := ws != nil && ws.Node == node && ws.Tenant == tenant && ws.Generation == generation && artifactProofState(ws.State)
	proofID := "artifact|" + claims.ID
	if valid {
		for seen, expires := range c.proofs {
			if expires < now {
				delete(c.proofs, seen)
			}
		}
		if _, replayed := c.proofs[proofID]; replayed {
			valid = false
		} else {
			c.proofs[proofID] = claims.ExpiresAt
		}
	}
	c.mu.Unlock()
	if !valid {
		return proto.Err(proto.CodeDenied, "artifact authorization is no longer valid")
	}
	return nil
}

func artifactProofBytes(claims proto.ArtifactProofClaims) []byte {
	body := proto.MustMarshal(claims)
	out := make([]byte, 0, len(artifactProofDomain)+len(body))
	out = append(out, artifactProofDomain...)
	return append(out, body...)
}

func artifactProofMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodPut
}

func artifactProofState(state string) bool {
	switch state {
	case proto.WSClaiming, proto.WSClaimed, proto.WSCheckpointing, proto.WSQuiescing, proto.WSDestroying:
		return true
	default:
		return false
	}
}
