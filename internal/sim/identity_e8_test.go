package sim

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact/encrypted"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/tenant"
	"remount.dev/remount/internal/workspace"
)

type identityTestBackend struct{ process *workspace.Process }

func (b *identityTestBackend) Name() string { return "identity-test" }

func (b *identityTestBackend) Caps() workspace.Caps {
	return workspace.Caps{
		Isolation: "microvm", Snapshots: "fs", EgressEnforced: true, MultiTenant: true,
		SiblingIsolation: true, EgressMode: "enforced_gateway", BrokerIdentity: "token",
		FilesystemBoundary: "test-jail", NetworkNamespace: true, DeviceIsolation: true,
	}
}

func (b *identityTestBackend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	handle, err := b.process.Create(ctx, id, spec, restore)
	if err != nil {
		return nil, err
	}
	return identityTestHandle{Handle: handle}, nil
}

func (b *identityTestBackend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	handle, err := b.process.Adopt(ctx, id)
	if err != nil {
		return nil, err
	}
	return identityTestHandle{Handle: handle}, nil
}

type identityTestHandle struct{ workspace.Handle }

func (identityTestHandle) ApplyNetworkPolicy(context.Context, proto.NetworkPolicy, workspace.NetworkEndpoint) error {
	return nil
}
func (identityTestHandle) RevokeNetwork(context.Context) error { return nil }

func identityEncryptedResolver(t *testing.T) *encrypted.Resolver {
	t.Helper()
	root := t.TempDir()
	key := sha256.Sum256([]byte("identity E8 encrypted artifact test key"))
	master, err := encrypted.NewAESMasterKey("master-v1", key[:])
	if err != nil {
		t.Fatal(err)
	}
	keys, err := encrypted.NewDirectoryKeyProvider(filepath.Join(root, "keys"), master, encrypted.DirectoryKeyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := encrypted.NewFileStore(filepath.Join(root, "objects"), encrypted.FileStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := encrypted.NewStore(objects, keys, filepath.Join(root, "stage"), encrypted.Options{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := encrypted.NewResolver(store, keys, encrypted.ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func brokerHealth(t *testing.T, base string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, strings.TrimSpace(base)+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("broker health: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func liveBrokerSession(t *testing.T, ctx context.Context, c *client.Client, workspaceID string) (*client.Session, string) {
	t.Helper()
	s, err := c.Exec(ctx, proto.SOpenReq{
		WS: workspaceID, Kind: proto.SessionExec,
		Program: []string{"sh", "-c", `printf '%s\n' "$REMOUNT_BROKER"; sleep 60`},
	})
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for {
		select {
		case chunk, ok := <-s.Chunks():
			if !ok {
				t.Fatalf("broker session exited before publishing its address: %+v", s.Exit())
			}
			if chunk.Stream != proto.StreamStdout {
				continue
			}
			output.Write(chunk.Data)
			if strings.Contains(output.String(), "\n") {
				return s, strings.TrimSpace(output.String())
			}
		case <-ctx.Done():
			t.Fatalf("waiting for broker address: %v", context.Cause(ctx))
		}
	}
}

// TestE8PrincipalRevocationComposes proves the complete authority path rather
// than only identity's token core. Alice's already-open relay connection,
// node calls, live session, and broker capability stop; Bob's session and
// broker capability plus the workspace remain live.
func TestE8PrincipalRevocationComposes(t *testing.T) {
	w := newWorldWith(t, func(options *server.Options) {
		options.LeaseSec = expiringLeaseSec // revocation reaches the node on renewal
		options.Token = ""
		options.Mode = server.ModeProductionMultiTenant
		options.TenantArtifacts = identityEncryptedResolver(t)
		options.SessionCapabilityTTL = 2 * time.Second
	})
	if w.srv.Identity == nil {
		t.Fatal("production server has no identity authority")
	}
	ctx := ctxT(t, 60*time.Second)
	if w.srv.Tenants == nil {
		t.Fatal("production server has no tenant authority")
	}
	if _, err := w.srv.Tenants.Create(ctx, "tenant-a", tenant.Policy{Quotas: tenant.Quotas{MaxWorkspaces: 4, MaxActiveSessions: 8}}, tenant.Mutation{
		OperationID: "e8-create-tenant", Actor: "bootstrap",
	}); err != nil {
		t.Fatal(err)
	}
	operatorToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator", "tenant-a", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	aliceToken, _, err := w.srv.Identity.IssueTokens(ctx, "agent:alice", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	bobToken, _, err := w.srv.Identity.IssueTokens(ctx, "agent:bob", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := w.srv.Identity.IssueEnrollment(ctx, "identity-e8", "tenant-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.nodeWith("identity-n1", func(options *node.Options) {
		process, processErr := workspace.NewProcess(filepath.Join(options.DataDir, "identity-process"))
		if processErr != nil {
			t.Fatal(processErr)
		}
		options.Token = enrollment
		options.Backends = workspace.NewRegistry(&identityTestBackend{process: process})
	})
	operator := w.clientWithToken("identity-operator", operatorToken)
	alice := w.clientWithToken("identity-alice", aliceToken)
	bob := w.clientWithToken("identity-bob", bobToken)

	ws := mustWS(t, operator, proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "identity-test"},
		ACL:      proto.WorkspaceACL{Writers: []string{"agent:alice", "agent:bob"}},
	})
	aliceSession, aliceBroker := liveBrokerSession(t, ctx, alice, ws.ID)
	bobSession, bobBroker := liveBrokerSession(t, ctx, bob, ws.ID)
	if brokerHealth(t, aliceBroker) != http.StatusOK || brokerHealth(t, bobBroker) != http.StatusOK {
		t.Fatal("fresh session capability was not accepted")
	}
	// The process-visible handles do not expire with their original signed
	// proofs. The node rotates those proofs while the sessions keep running.
	time.Sleep(2500 * time.Millisecond)
	if brokerHealth(t, aliceBroker) != http.StatusOK || brokerHealth(t, bobBroker) != http.StatusOK {
		t.Fatal("stable session capability handle failed after signed proof expiry")
	}

	revokedAt := time.Now()
	revision, err := operator.RevokePrincipal(ctx, "tenant-a", "agent:alice", "e8-revoke-alice")
	if err != nil || revision == 0 {
		t.Fatalf("revoke revision=%d err=%v", revision, err)
	}
	if replay, err := operator.RevokePrincipal(ctx, "tenant-a", "agent:alice", "e8-revoke-alice"); err != nil || replay != revision {
		t.Fatalf("revoke replay revision=%d err=%v", replay, err)
	}
	if _, err := alice.ReadFile(ctx, ws.ID, "missing"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("Alice node call after revocation=%v", err)
	}
	if status := brokerHealth(t, aliceBroker); status != http.StatusForbidden {
		t.Fatalf("Alice broker status after revocation=%d", status)
	}
	exit, err := aliceSession.Wait(ctxT(t, 10*time.Second))
	if err != nil || exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("Alice session exit=%+v err=%v", exit, err)
	}
	if elapsed := time.Since(revokedAt); elapsed > 5*time.Second {
		t.Fatalf("revocation propagation took %v", elapsed)
	}
	select {
	case <-bobSession.Done():
		t.Fatalf("Bob session was collateral damage: %+v", bobSession.Exit())
	case <-time.After(100 * time.Millisecond):
	}
	if status := brokerHealth(t, bobBroker); status != http.StatusOK {
		t.Fatalf("Bob broker status after Alice revocation=%d", status)
	}
	if _, err := bob.ReadFile(ctx, ws.ID, "missing"); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("Bob node call after Alice revocation=%v", err)
	}
	if current, err := operator.GetWorkspace(ctx, ws.ID); err != nil || current.State != proto.WSClaimed {
		t.Fatalf("workspace after revocation=%+v err=%v", current, err)
	}
	if err := bobSession.Signal(ctx, "TERM"); err != nil {
		t.Fatal(err)
	}
	if _, err := bobSession.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	events, err := operator.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var identityFence, nodeClosure bool
	for _, event := range events {
		switch event.Type {
		case proto.EvIdentityWorkspaceRevoked:
			identityFence = event.Principal == "operator" && event.Tenant == "tenant-a"
		case proto.EvAuthzRevoked:
			nodeClosure = true
		}
	}
	if !identityFence || !nodeClosure {
		t.Fatalf("missing revocation evidence: identity_fence=%v node_closure=%v", identityFence, nodeClosure)
	}
}

// An operator's ambient role need not appear in a workspace ACL. Revocation
// still advances every tenant workspace and closes only that operator's
// existing sessions; otherwise an old signed grant would survive until TTL.
func TestE8OperatorRevocationClosesAmbientSession(t *testing.T) {
	w := newWorldWith(t, func(options *server.Options) {
		options.Token = ""
		options.Mode = server.ModeProductionMultiTenant
		options.TenantArtifacts = identityEncryptedResolver(t)
	})
	ctx := ctxT(t, 60*time.Second)
	if _, err := w.srv.Tenants.Create(ctx, "tenant-a", tenant.Policy{Quotas: tenant.Quotas{MaxWorkspaces: 2}}, tenant.Mutation{
		OperationID: "e8-operator-tenant", Actor: "bootstrap",
	}); err != nil {
		t.Fatal(err)
	}
	rootToken, _, err := w.srv.Identity.IssueTokens(ctx, "root", "*", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	ownerToken, _, err := w.srv.Identity.IssueTokens(ctx, "agent:owner", "tenant-a", []string{identity.RoleAgent})
	if err != nil {
		t.Fatal(err)
	}
	operatorToken, _, err := w.srv.Identity.IssueTokens(ctx, "operator:alice", "tenant-a", []string{identity.RoleOperator})
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := w.srv.Identity.IssueEnrollment(ctx, "identity-operator-e8", "tenant-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.nodeWith("identity-operator-n1", func(options *node.Options) {
		process, processErr := workspace.NewProcess(filepath.Join(options.DataDir, "identity-process"))
		if processErr != nil {
			t.Fatal(processErr)
		}
		options.Token = enrollment
		options.Backends = workspace.NewRegistry(&identityTestBackend{process: process})
	})
	root := w.clientWithToken("identity-root", rootToken)
	owner := w.clientWithToken("identity-owner", ownerToken)
	operator := w.clientWithToken("identity-tenant-operator", operatorToken)
	ws := mustWS(t, owner, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "identity-test"}})
	s, err := operator.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sleep", "60"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.RevokePrincipal(ctx, "tenant-a", "operator:alice", "e8-revoke-ambient-operator"); err != nil {
		t.Fatal(err)
	}
	exit, err := s.Wait(ctxT(t, 10*time.Second))
	if err != nil || exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("ambient operator session exit=%+v err=%v", exit, err)
	}
	if _, err := owner.ReadFile(ctx, ws.ID, "missing"); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("workspace owner stopped with revoked operator: %v", err)
	}
}
