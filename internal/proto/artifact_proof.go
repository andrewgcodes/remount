package proto

// OpArtifactProof asks the control plane for a short-lived authorization for
// one node artifact HTTP request.
const OpArtifactProof = "artifact.proof"

const (
	// ArtifactWorkspaceHeader binds an artifact request to a workspace.
	ArtifactWorkspaceHeader = "X-Remount-Workspace"
	// ArtifactGenerationHeader fences an artifact request to one assignment.
	ArtifactGenerationHeader = "X-Remount-Generation"
	// ArtifactProofHeader carries the opaque control-plane authorization.
	ArtifactProofHeader = "X-Remount-Artifact-Proof"
)

// ArtifactProofReq binds a proof request to one held workspace generation and
// exact artifact operation. Tenant and node are deliberately absent: control
// derives both from authenticated assignment state.
type ArtifactProofReq struct {
	Workspace  string `cbor:"ws" json:"ws"`
	Generation uint64 `cbor:"gen" json:"gen"`
	Method     string `cbor:"method" json:"method"`
	Artifact   string `cbor:"artifact" json:"artifact"`
}

// ArtifactProofRes contains an opaque control-plane-signed token suitable for
// the X-Remount-Artifact-Proof HTTP header.
type ArtifactProofRes struct {
	Proof string `cbor:"proof" json:"proof"`
}

// ArtifactProofClaims are signed by control and independently compared with
// the authenticated node and request before storage access.
type ArtifactProofClaims struct {
	ID         string `cbor:"id" json:"id"`
	Node       string `cbor:"node" json:"node"`
	Tenant     string `cbor:"tenant" json:"tenant"`
	Workspace  string `cbor:"ws" json:"ws"`
	Generation uint64 `cbor:"gen" json:"gen"`
	Method     string `cbor:"method" json:"method"`
	Artifact   string `cbor:"artifact" json:"artifact"`
	IssuedAt   int64  `cbor:"issued_at" json:"issued_at"`
	ExpiresAt  int64  `cbor:"expires_at" json:"expires_at"`
}

// ArtifactProofEnvelope is encoded into the opaque HTTP proof token.
type ArtifactProofEnvelope struct {
	Claims    ArtifactProofClaims `cbor:"claims" json:"claims"`
	Signature []byte              `cbor:"signature" json:"signature"`
}
