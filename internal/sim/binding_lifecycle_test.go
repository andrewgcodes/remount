package sim

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/tenant"
	"remount.dev/remount/internal/workspace"
)

func reasonOf(err error) string {
	var protocol *proto.Error
	if errors.As(err, &protocol) {
		return protocol.Reason
	}
	return ""
}

// brokeredUpstream is a TLS upstream that records the Authorization header it
// was sent, so a test can see exactly which credential reached the provider.
type brokeredUpstream struct {
	host string
	auth *atomic.Value
	pool *x509.CertPool
}

func newBrokeredUpstream(t *testing.T) *brokeredUpstream {
	t.Helper()
	seen := &atomic.Value{}
	seen.Store("")
	up := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
		io.WriteString(w, "ok")
	}))
	t.Cleanup(up.Close)
	pool := x509.NewCertPool()
	pool.AddCert(up.Certificate())
	return &brokeredUpstream{host: strings.TrimPrefix(up.URL, "https://"), auth: seen, pool: pool}
}

// curlStatus asks the workspace to send its placeholder through the broker and
// returns the HTTP status the broker answered with.
func curlStatus(t *testing.T, ctx context.Context, c *client.Client, wsID string) string {
	t.Helper()
	out, _, _, err := c.Run(ctx, wsID, "sh", "-c",
		`curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$API_URL/v1/thing"`)
	if err != nil {
		t.Fatalf("broker request failed to run: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// awaitUpstreamAuth drives brokered requests until the upstream stops seeing
// stale and returns what it saw. Each attempt opens a session, so the interval
// is deliberately larger than the world's renew period rather than a spin.
func awaitUpstreamAuth(t *testing.T, ctx context.Context, c *client.Client, wsID string, up *brokeredUpstream, stale string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if status := curlStatus(t, ctx, c, wsID); status != "200" {
			t.Fatalf("brokered request returned %s", status)
		}
		seen, _ := up.auth.Load().(string)
		if seen != stale {
			return seen
		}
		if !time.Now().Before(deadline) {
			return seen
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// countCredentialUse counts substitutions recorded for one workspace and host.
func countCredentialUse(t *testing.T, ctx context.Context, c *client.Client, wsID, host string) int {
	t.Helper()
	records, err := c.CredentialEvents(ctx, client.CredentialFilter{WS: wsID, Host: host, Since: 1})
	if err != nil {
		t.Fatal(err)
	}
	used := 0
	for _, record := range records {
		if record.Type == proto.EvCredUsed {
			used++
		}
	}
	return used
}

// bindingEventsOf returns the binding lifecycle events in the whole log. They
// are streamed under the binding id rather than a workspace, so they are read
// from the tenant-wide stream.
func bindingEventsOf(t *testing.T, ctx context.Context, c *client.Client, binding string) []proto.Event {
	t.Helper()
	events, err := c.ReadEvents(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	var out []proto.Event
	for _, event := range events {
		switch event.Type {
		case proto.EvBindingCreated, proto.EvBindingRotated, proto.EvBindingRevoked:
			if event.Stream == binding {
				out = append(out, event)
			}
		}
	}
	return out
}

// TestBindingRevokeStopsSubstitutionWithinRenew is the revocation-propagation
// proof. A workspace substitutes a credential successfully, the operator
// revokes the binding, and within one renew interval the credential stops
// reaching the upstream - the workspace's inert placeholder goes instead - and
// every request is still audited. The lease TTL here is an hour, so only
// renew-driven invalidation can produce that result.
func TestBindingRevokeStopsSubstitutionWithinRenew(t *testing.T) {
	up := newBrokeredUpstream(t)
	w := newWorld(t)
	w.nodeWithBrokerRoots("n1", nil, up.pool)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)

	if _, err := c.CreateBinding(ctx, proto.BindingSpec{
		ID: "b_revocable", Kind: proto.BindingKindAPIKey, Secret: "sk-LIVE-SECRET",
		Destinations: []string{up.host}, TTLSec: 3600,
	}, client.WithIdempotencyKey("bind-revocable")); err != nil {
		t.Fatalf("create binding: %v", err)
	}
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Bindings: []string{"b_revocable"},
		Env:      map[string]string{"API_KEY": "ref:b_revocable", "API_URL": "${REMOUNT_BROKER}/d/" + up.host},
	})
	if status := curlStatus(t, ctx, c, ws.ID); status != "200" {
		t.Fatalf("brokered request before revocation = %s", status)
	}
	if got := up.auth.Load(); got != "Bearer sk-LIVE-SECRET" {
		t.Fatalf("upstream saw %v before revocation", got)
	}

	// The lease TTL is an hour, so only renew-driven invalidation can stop the
	// substitution inside the deadline below. That is the whole proof: without
	// it the credential would keep flowing for the rest of the hour.
	revokedAt := time.Now()
	if _, err := c.RevokeBinding(ctx, "", "b_revocable", "credential leaked",
		client.WithIdempotencyKey("revoke-revocable")); err != nil {
		t.Fatalf("revoke binding: %v", err)
	}
	seen := awaitUpstreamAuth(t, ctx, c, ws.ID, up, "Bearer sk-LIVE-SECRET")
	if seen == "Bearer sk-LIVE-SECRET" {
		t.Fatalf("substitution continued %s after revocation", time.Since(revokedAt))
	}
	// What reaches the upstream now is the inert placeholder the workspace
	// holds, never the credential.
	if !strings.Contains(seen, "ref:b_revocable") {
		t.Fatalf("post-revocation upstream credential = %q", seen)
	}

	// Substitution has stopped; prove it stays stopped and stays audited.
	substituted := countCredentialUse(t, ctx, c, ws.ID, up.host)
	if substituted == 0 {
		t.Fatal("the pre-revocation request was not audited as a substitution")
	}
	unbrokered := 0
	for i := 0; i < 2; i++ {
		if status := curlStatus(t, ctx, c, ws.ID); status != "200" {
			t.Fatalf("post-revocation request returned %s", status)
		}
		if seen, _ := up.auth.Load().(string); seen == "Bearer sk-LIVE-SECRET" {
			t.Fatal("a later request was substituted again after revocation")
		}
		unbrokered++
	}
	if again := countCredentialUse(t, ctx, c, ws.ID, up.host); again != substituted {
		t.Fatalf("substitution resumed after revocation: %d then %d", substituted, again)
	}
	audit, err := c.CredentialEvents(ctx, client.CredentialFilter{WS: ws.ID, Binding: "", Since: 1})
	if err != nil {
		t.Fatal(err)
	}
	decisions := 0
	for _, record := range audit {
		switch record.Type {
		case proto.EvEgressAllowed, proto.EvEgressDenied:
			decisions++
		}
	}
	if decisions < unbrokered {
		t.Fatalf("only %d of %d post-revocation requests were audited", decisions, unbrokered)
	}
	// The revocation itself is durable and recorded without the credential.
	lifecycle := bindingEventsOf(t, ctx, c, "b_revocable")
	sawRevoked := false
	for _, event := range lifecycle {
		if event.Type == proto.EvBindingRevoked {
			sawRevoked = true
		}
		if strings.Contains(string(event.Payload), "sk-LIVE-SECRET") {
			t.Fatal("binding event carried the credential")
		}
	}
	if !sawRevoked {
		t.Fatal("binding.revoked was not emitted")
	}
}

// TestBindingRotateInvalidatesOldLease proves a rotation reaches a running
// workspace: the upstream stops seeing the old credential and starts seeing
// the new one without the workspace being restarted or the lease expiring.
func TestBindingRotateInvalidatesOldLease(t *testing.T) {
	up := newBrokeredUpstream(t)
	w := newWorld(t)
	w.nodeWithBrokerRoots("n1", nil, up.pool)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)

	if _, err := c.CreateBinding(ctx, proto.BindingSpec{
		ID: "b_rotating", Kind: proto.BindingKindBearer, Secret: "sk-FIRST",
		Destinations: []string{up.host}, TTLSec: 3600,
	}, client.WithIdempotencyKey("bind-rotating")); err != nil {
		t.Fatalf("create binding: %v", err)
	}
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Bindings: []string{"b_rotating"},
		Env:      map[string]string{"API_KEY": "ref:b_rotating", "API_URL": "${REMOUNT_BROKER}/d/" + up.host},
	})
	if status := curlStatus(t, ctx, c, ws.ID); status != "200" || up.auth.Load() != "Bearer sk-FIRST" {
		t.Fatalf("before rotation: status=%s auth=%v", status, up.auth.Load())
	}

	rotated, err := c.RotateBinding(ctx, proto.BindingRotateReq{ID: "b_rotating", Secret: "sk-SECOND"},
		client.WithIdempotencyKey("rotate-rotating"))
	if err != nil || rotated.Revision != 2 {
		t.Fatalf("rotate = %+v, %v", rotated, err)
	}
	if seen := awaitUpstreamAuth(t, ctx, c, ws.ID, up, "Bearer sk-FIRST"); seen != "Bearer sk-SECOND" {
		t.Fatalf("upstream credential after rotation = %q", seen)
	}
	// The rotation is recorded without either credential value.
	sawRotated := false
	for _, event := range bindingEventsOf(t, ctx, c, "b_rotating") {
		if event.Type == proto.EvBindingRotated {
			sawRotated = true
		}
		payload := string(event.Payload)
		if strings.Contains(payload, "sk-FIRST") || strings.Contains(payload, "sk-SECOND") {
			t.Fatalf("rotation event carried a credential: %s", payload)
		}
	}
	if !sawRotated {
		t.Fatal("binding.rotated was not emitted")
	}
}

// sessionTestBackend is identityTestBackend with an honest Backend() answer,
// which a release needs: the node compares the control plane's backend name
// against the handle's before it will hand a workspace on.
type sessionTestBackend struct{ process *workspace.Process }

func (b *sessionTestBackend) Name() string { return "session-test" }

func (b *sessionTestBackend) Caps() workspace.Caps {
	return workspace.Caps{
		Isolation: "microvm", Snapshots: "fs", EgressEnforced: true, MultiTenant: true,
		SiblingIsolation: true, EgressMode: "enforced_gateway", BrokerIdentity: "token",
		FilesystemBoundary: "test-jail", NetworkNamespace: true, DeviceIsolation: true,
	}
}

func (b *sessionTestBackend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	handle, err := b.process.Create(ctx, id, spec, restore)
	if err != nil {
		return nil, err
	}
	return sessionTestHandle{Handle: handle}, nil
}

func (b *sessionTestBackend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	handle, err := b.process.Adopt(ctx, id)
	if err != nil {
		return nil, err
	}
	return sessionTestHandle{Handle: handle}, nil
}

type sessionTestHandle struct{ workspace.Handle }

func (sessionTestHandle) Backend() string { return "session-test" }
func (sessionTestHandle) ApplyNetworkPolicy(context.Context, proto.NetworkPolicy, workspace.NetworkEndpoint) error {
	return nil
}
func (sessionTestHandle) RevokeNetwork(context.Context) error { return nil }

// identityWorld builds the production multi-tenant world the principal tests
// need: real identity, real tenant policy, one enrolled node.
func identityWorld(t *testing.T, tenantID, nodeName string) *world {
	t.Helper()
	w := newWorldWith(t, func(options *server.Options) {
		options.Token = ""
		options.Mode = server.ModeProductionMultiTenant
		options.TenantArtifacts = identityEncryptedResolver(t)
	})
	ctx := ctxT(t, 60*time.Second)
	if _, err := w.srv.Tenants.Create(ctx, tenantID, tenant.Policy{
		Quotas: tenant.Quotas{MaxWorkspaces: 4, MaxActiveSessions: 8},
	}, tenant.Mutation{OperationID: "binding-tenant-" + tenantID, Actor: "bootstrap"}); err != nil {
		t.Fatal(err)
	}
	enrollment, err := w.srv.Identity.IssueEnrollment(ctx, nodeName, tenantID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.nodeWith(nodeName, func(options *node.Options) {
		process, processErr := workspace.NewProcess(filepath.Join(options.DataDir, "identity-process"))
		if processErr != nil {
			t.Fatal(processErr)
		}
		options.Token = enrollment
		options.Backends = workspace.NewRegistry(
			&identityTestBackend{process: process}, &sessionTestBackend{process: process})
	})
	return w
}

// TestRevokedPrincipalCannotOpenAttachReadOrLease extends the E8 pattern with
// the typed reason: after revocation every authority path an agent has -
// opening a session, attaching to the one it already had, reading a file, and
// asking for a session-scoped credential - is refused with ReasonRevoked.
//
// The codes at those sites are unchanged; revocation surfaces as denied from
// the re-authentication boundary, which is where the credential is checked.
func TestRevokedPrincipalCannotOpenAttachReadOrLease(t *testing.T) {
	const tenantID = "tenant-revoke"
	w := identityWorld(t, tenantID, "revoke-n1")
	ctx := ctxT(t, 60*time.Second)
	operatorToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator", tenantID, []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	aliceToken, _, err := w.srv.Identity.IssueTokens(ctx, "agent:alice", tenantID, []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	operator := w.clientWithToken("revoke-operator", operatorToken)
	alice := w.clientWithToken("revoke-alice", aliceToken)

	ws := mustWS(t, operator, proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "identity-test"},
		ACL:      proto.WorkspaceACL{Writers: []string{"agent:alice"}},
	})
	live, err := alice.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sleep", "60"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alice.ReadFile(ctx, ws.ID, "missing"); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("alice was not authorized before revocation: %v", err)
	}

	if _, err := operator.RevokePrincipal(ctx, tenantID, "agent:alice", "revoke-alice"); err != nil {
		t.Fatal(err)
	}
	// The live session ends because of the revocation, not because it exited.
	exit, err := live.Wait(ctxT(t, 15*time.Second))
	if err != nil || exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("live session exit = %+v, %v", exit, err)
	}

	paths := map[string]func() error{
		"read": func() error {
			_, err := alice.ReadFile(ctx, ws.ID, "missing")
			return err
		},
		"open": func() error {
			_, err := alice.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"true"}})
			return err
		},
		"attach": func() error {
			_, err := alice.Attach(ctx, ws.ID, live.ID, 0)
			return err
		},
		"lease": func() error {
			_, err := alice.CreateSessionPrincipal(ctx, proto.PrincipalSessionCreateReq{
				Tenant: tenantID, Workspace: ws.ID, Roles: []string{identity.RoleAgent},
			}, client.WithIdempotencyKey("revoked-session-principal"))
			return err
		},
	}
	for name, call := range paths {
		err := call()
		if err == nil {
			t.Fatalf("%s succeeded after revocation", name)
		}
		if reason := reasonOf(err); reason != proto.ReasonRevoked {
			t.Fatalf("%s after revocation: reason=%q code=%q err=%v", name, reason, codeOf(err), err)
		}
	}
	// The workspace itself is untouched: revocation is per principal.
	if current, err := operator.GetWorkspace(ctx, ws.ID); err != nil || current.State != proto.WSClaimed {
		t.Fatalf("workspace after revocation = %+v, %v", current, err)
	}
}

// TestSessionPrincipalIsWorkspaceAndGenerationBound proves the one-call
// session principal is scoped to exactly the workspace and generation it was
// issued for: after a real move the capability no longer verifies, a fresh one
// reports the new generation, and revoking the principal kills both.
func TestSessionPrincipalIsWorkspaceAndGenerationBound(t *testing.T) {
	const tenantID = "tenant-session"
	w := identityWorld(t, tenantID, "session-n1")
	ctx := ctxT(t, 90*time.Second)
	operatorToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator", tenantID, []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	operator := w.clientWithToken("session-operator", operatorToken)
	ws := mustWS(t, operator, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "session-test"}})
	other := mustWS(t, operator, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "session-test"}})

	issued, err := operator.CreateSessionPrincipal(ctx, proto.PrincipalSessionCreateReq{
		Tenant: tenantID, Workspace: ws.ID, Roles: []string{identity.RoleAgent}, TTLSec: 60,
	}, client.WithIdempotencyKey("session-principal-one"))
	if err != nil {
		t.Fatalf("create session principal: %v", err)
	}
	if issued.Token == "" || issued.Principal.ID == "" || issued.Workspace != ws.ID {
		t.Fatalf("session principal = %+v", issued)
	}
	if issued.Generation != ws.Generation || issued.Generation == 0 {
		t.Fatalf("session principal generation %d, workspace generation %d", issued.Generation, ws.Generation)
	}
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, issued.Token, tenantID, ws.ID, issued.Generation); err != nil {
		t.Fatalf("freshly issued capability did not verify: %v", err)
	}
	// A leaked capability is useless outside its workspace and generation.
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, issued.Token, tenantID, other.ID, issued.Generation); err == nil {
		t.Fatal("capability verified against another workspace")
	}
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, issued.Token, tenantID, ws.ID, issued.Generation+1); err == nil {
		t.Fatal("capability verified against another generation")
	}

	// One node holds the test backend, so the move re-materializes in place;
	// what matters for the proof is that the generation advances.
	moved, err := operator.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: ""},
		client.WithIdempotencyKey("session-principal-move"))
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	moved, err = operator.WaitClaimed(ctx, moved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Generation == issued.Generation {
		t.Skip("move did not advance the generation; nothing to prove")
	}
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, issued.Token, tenantID, ws.ID, moved.Generation); err == nil {
		t.Fatal("capability from the previous generation survived a move")
	}

	reissued, err := operator.CreateSessionPrincipal(ctx, proto.PrincipalSessionCreateReq{
		Tenant: tenantID, Workspace: ws.ID, Roles: []string{identity.RoleAgent}, TTLSec: 60,
	}, client.WithIdempotencyKey("session-principal-two"))
	if err != nil {
		t.Fatalf("reissue after move: %v", err)
	}
	if reissued.Generation != moved.Generation {
		t.Fatalf("reissued generation %d, workspace generation %d", reissued.Generation, moved.Generation)
	}
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, reissued.Token, tenantID, ws.ID, moved.Generation); err != nil {
		t.Fatalf("capability for the current generation did not verify: %v", err)
	}

	// The ephemeral principal is an ordinary principal: principal.revoke ends
	// it, and its capability stops verifying immediately.
	if _, err := operator.RevokePrincipal(ctx, tenantID, reissued.Principal.ID, "session-principal-revoke"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.srv.Identity.VerifyBrokerCapability(ctx, reissued.Token, tenantID, ws.ID, moved.Generation); err == nil {
		t.Fatal("revoked session principal still verifies")
	}
}
