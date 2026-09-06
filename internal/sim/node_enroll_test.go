package sim

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// TestNodeEnrollmentFromOperatorClaimsAWorkspace walks the operator path that
// did not exist: a production-mode control plane with no shared token, an
// operator bootstrapped from nothing, a one-time node credential minted
// through `node.enroll`, a node that presents it, and a workspace that
// actually reaches claimed on that node.
//
// The second half is the part that makes the credential worth having: the
// same credential presented by a second machine is refused, with a reason a
// caller can match on rather than a message it has to parse.
func TestNodeEnrollmentFromOperatorClaimsAWorkspace(t *testing.T) {
	w := newWorldWith(t, func(options *server.Options) {
		options.Token = ""
		options.Mode = server.ModeProductionSingleTenant
		options.TenantArtifacts = identityEncryptedResolver(t)
	})
	ctx := ctxT(t, 90*time.Second)
	if w.srv.Identity == nil {
		t.Fatal("production server has no identity authority")
	}
	// Exactly what `--bootstrap-principal` does on a first production start.
	operatorToken, _, err := w.srv.BootstrapOperator(ctx, "root", 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	operator := w.clientWithToken("enroll-operator", operatorToken)

	// The bootstrap operator is global (tenant "*"), so it must name the
	// tenant, exactly as `principal create` and `token issue` require.
	if _, err := operator.EnrollNode(ctx, "", "enroll-pool", nil, time.Minute, "enroll-no-tenant"); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("enrollment without an exact tenant = %v, want bad_request", err)
	}
	// Bad requests answer bad_request, not `unreachable`. The authority owns
	// the label and name rules, so its refusal has to arrive typed.
	for name, bad := range map[string]func() error{
		"unprintable label": func() error {
			_, err := operator.EnrollNode(ctx, "tenant-a", "enroll-pool", map[string]string{"zone": "eu west"}, time.Minute, "enroll-bad-label")
			return err
		},
		"ttl beyond the ceiling": func() error {
			_, err := operator.EnrollNode(ctx, "tenant-a", "enroll-pool", nil, time.Hour, "enroll-bad-ttl")
			return err
		},
		"no name": func() error {
			_, err := operator.EnrollNode(ctx, "tenant-a", "", nil, time.Minute, "enroll-bad-name")
			return err
		},
		"no idempotency key": func() error {
			_, err := operator.EnrollNode(ctx, "tenant-a", "enroll-pool", nil, time.Minute, "")
			return err
		},
	} {
		if err := bad(); codeOf(err) != proto.CodeBadRequest {
			t.Fatalf("%s = %v, want bad_request", name, err)
		}
	}

	res, err := operator.EnrollNode(ctx, "tenant-a", "enroll-pool", map[string]string{"zone": "eu-west"}, time.Minute, "enroll-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.EnrollmentToken == "" || res.Tenant != "tenant-a" || res.Name != "enroll-pool" {
		t.Fatalf("enrollment response = %+v", res)
	}
	if res.ExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("enrollment expires_at %d is not in the future", res.ExpiresAt)
	}

	w.nodeWith("enrolled-n1", func(options *node.Options) {
		process, processErr := workspace.NewProcess(filepath.Join(options.DataDir, "enroll-process"))
		if processErr != nil {
			t.Fatal(processErr)
		}
		options.Token = res.EnrollmentToken
		options.Backends = workspace.NewRegistry(&identityTestBackend{process: process})
	})

	agentToken, _, err := w.srv.Identity.IssueTokens(ctx, "agent:enroll", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	agent := w.clientWithToken("enroll-agent", agentToken)
	ws := mustWS(t, agent, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "identity-test"}})
	if ws.State != proto.WSClaimed {
		t.Fatalf("workspace state = %s, want claimed", ws.State)
	}

	nodes, err := operator.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var listed *proto.NodeStatus
	for i := range nodes {
		if nodes[i].Online {
			listed = &nodes[i]
		}
	}
	if listed == nil {
		t.Fatal("the enrolled node is not visible to node.list")
	}
	// The operator's trusted labels are authority the node cannot alter, and
	// the tenant is stamped by the control plane, never by the hello.
	if listed.Labels["zone"] != "eu-west" || listed.Labels["tenant"] != "tenant-a" {
		t.Fatalf("enrolled node labels = %v", listed.Labels)
	}

	// One-time means one time. A second machine with the same credential is
	// refused, and the refusal carries a reason rather than only a message.
	// It advertises the same qualifying backend as the first node, so the
	// refusal under test is the spent credential and not the security floor.
	err = rawNodeHello(ctx, w.dialer("enroll-replay"), res.EnrollmentToken, "n_enroll_replay", identityTestNodeInfo(t))
	if codeOf(err) != proto.CodeUnauthorized {
		t.Fatalf("replayed enrollment = %v, want unauthorized", err)
	}
	if reason := proto.ErrorReason(err); reason != proto.ReasonRevoked {
		t.Fatalf("replayed enrollment reason = %q, want %q", reason, proto.ReasonRevoked)
	}

	// The audit trail names the operator who minted it and never the bearer.
	// It is read as the tenant, because the enrollment event belongs to
	// tenant-a while the bootstrap operator's own tenant is the wildcard.
	events, err := agent.ReadEvents(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	issued := false
	for _, event := range events {
		if event.Type == proto.EvIdentityNodeEnrollmentIssued {
			issued = issued || event.Actor == "root"
		}
		if len(event.Payload) > 0 && containsSecret(event.Payload, res.EnrollmentToken) {
			t.Fatalf("event %s carries the enrollment bearer", event.Type)
		}
	}
	if !issued {
		t.Fatalf("no %s event attributed to the operator", proto.EvIdentityNodeEnrollmentIssued)
	}
}

// TestNodeEnrollmentRequiresAnAuthority proves the standalone refusal is typed
// rather than a silent success: without an identity authority there is nothing
// to mint against, and `unsupported` says so.
func TestNodeEnrollmentRequiresAnAuthority(t *testing.T) {
	w := newWorld(t)
	ctx := ctxT(t, 30*time.Second)
	c := w.client("enroll-standalone")
	if _, err := c.EnrollNode(ctx, "local", "pool", nil, time.Minute, "enroll-standalone"); codeOf(err) != proto.CodeUnsupported {
		t.Fatalf("standalone node enrollment = %v, want unsupported", err)
	}
}

// rawNodeHello presents one node hello and returns the control plane's answer.
// It is a whole second machine as far as enrollment is concerned: a fresh key,
// a fresh id, and the credential someone else already spent.
func rawNodeHello(ctx context.Context, dial transport.Dialer, token, id string, info proto.NodeInfo) error {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	hello := proto.Hello{
		// The full capability set and descriptors a real node offers, so the
		// refusal under test is the enrollment decision and neither a
		// negotiation failure nor the deployment's security floor.
		Peer: id, Role: proto.RoleNode, Token: token, Caps: proto.PeerCapabilities(),
		PubKey: public, IssuedAt: time.Now().UnixMilli(), Nonce: nonce, Node: &info,
	}
	hello.Proof = ed25519.Sign(private, proto.HelloProofBytes(hello))
	conn, err := dial.Dial(ctx)
	if err != nil {
		return err
	}
	peer := transport.NewPeer(conn, nil)
	defer peer.Close()
	_, err = transport.Hello(ctx, peer, hello)
	return err
}

// identityTestNodeInfo is the hello NodeInfo of a node whose only backend
// satisfies the production security floor, derived from the backend rather
// than hand-written so the two cannot drift apart.
func identityTestNodeInfo(t *testing.T) proto.NodeInfo {
	t.Helper()
	process, err := workspace.NewProcess(filepath.Join(t.TempDir(), "replay-process"))
	if err != nil {
		t.Fatal(err)
	}
	return workspace.HostInfoForRegistry(workspace.NewRegistry(&identityTestBackend{process: process}))
}

func containsSecret(payload []byte, secret string) bool {
	return secret != "" && bytes.Contains(payload, []byte(secret))
}
