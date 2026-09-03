package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

func newTestNode(t *testing.T, configure func(*Options)) *Node {
	t.Helper()
	opts := Options{DataDir: t.TempDir(), Dialer: transport.DialFunc(func(context.Context) (transport.Conn, error) {
		return nil, errors.New("unused test dialer")
	})}
	if configure != nil {
		configure(&opts)
	}
	n, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.sessions.Close() })
	return n
}

func nodeDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return artifact.ID(sum[:])
}

func TestCorruptIdentityIsNotSilentlyReplaced(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "identity.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := New(Options{DataDir: dir, Dialer: transport.DialFunc(func(context.Context) (transport.Conn, error) { return nil, nil })})
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("New with corrupt identity = %v", err)
	}
	got, readErr := os.ReadFile(filepath.Join(dir, "identity.json"))
	if readErr != nil || string(got) != "not-json" {
		t.Fatalf("identity was replaced: %q, %v", got, readErr)
	}
}

func TestFetchArtifactMismatchCannotDeleteExistingBlob(t *testing.T) {
	body := "pre-existing body"
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer httpServer.Close()
	n := newTestNode(t, func(opts *Options) {
		opts.ArtifactURL = httpServer.URL
		opts.HTTPClient = httpServer.Client()
		opts.MaxArtifactBytes = 1024
	})
	bodyID, _, err := n.store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	wanted := nodeDigest("different")
	if _, err := n.fetchArtifact(context.Background(), wanted); !errors.Is(err, artifact.ErrDigestMismatch) {
		t.Fatalf("fetch mismatch = %v", err)
	}
	if !n.store.Has(bodyID) {
		t.Fatal("mismatched fetch deleted an existing valid cache entry")
	}
	if n.store.Has(wanted) {
		t.Fatal("mismatched fetch published requested id")
	}
}

func TestMaterializeRevalidatesBackendSecurity(t *testing.T) {
	n := newTestNode(t, nil)
	w := proto.Workspace{
		ID: "ws_secure", Generation: 1, State: proto.WSClaiming,
		Spec: proto.WorkspaceSpec{
			Requires: proto.Requires{Backend: "process"},
			Security: proto.SecuritySpec{Profile: proto.SecurityMultiTenant},
		},
	}
	if err := n.materialize(context.Background(), w, false); err == nil {
		t.Fatal("process backend accepted multi-tenant policy")
	}
	if _, err := os.Stat(filepath.Join(n.opts.DataDir, "ws", w.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected policy materialized bytes: %v", err)
	}
}

type failingHandle struct {
	id          string
	fs          *fsops.FS
	snapshotErr error
	destroyed   atomic.Int32
}

func (h *failingHandle) ID() string                  { return h.id }
func (h *failingHandle) Backend() string             { return "test" }
func (h *failingHandle) FS() *fsops.FS               { return h.fs }
func (h *failingHandle) Prepare(*session.Spec) error { return nil }
func (h *failingHandle) Snapshot(context.Context, []string, io.Writer) error {
	return h.snapshotErr
}
func (h *failingHandle) Destroy(context.Context) error {
	h.destroyed.Add(1)
	return nil
}

type fixedBackend struct {
	name   string
	caps   workspace.Caps
	handle workspace.Handle
}

func (b *fixedBackend) Name() string         { return b.name }
func (b *fixedBackend) Caps() workspace.Caps { return b.caps }
func (b *fixedBackend) Create(context.Context, string, proto.WorkspaceSpec, io.Reader) (workspace.Handle, error) {
	return b.handle, nil
}
func (b *fixedBackend) Adopt(context.Context, string) (workspace.Handle, error) {
	return b.handle, nil
}

type networkHandle struct {
	*failingHandle
	backend   string
	applyErr  error
	revokeErr error
	applied   atomic.Int32
	revoked   atomic.Int32
	endpoint  workspace.NetworkEndpoint
}

func (h *networkHandle) Backend() string { return h.backend }
func (h *networkHandle) ApplyNetworkPolicy(_ context.Context, _ proto.NetworkPolicy, endpoint workspace.NetworkEndpoint) error {
	h.endpoint = endpoint
	h.applied.Add(1)
	return h.applyErr
}
func (h *networkHandle) RevokeNetwork(context.Context) error {
	h.revoked.Add(1)
	return h.revokeErr
}

func TestReleaseSnapshotFailureRestoresSourceWithoutDestroy(t *testing.T) {
	n := newTestNode(t, nil)
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	h := &failingHandle{id: "ws_one", fs: fs, snapshotErr: errors.New("disk full")}
	w := &ws{Workspace: proto.Workspace{ID: h.id, Generation: 7, State: proto.WSClaimed}, handle: h}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = n.started.AddDate(1, 0, 0)
	n.mu.Unlock()
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: 7, Snapshot: true}); err == nil {
		t.Fatal("release succeeded after snapshot failure")
	}
	n.mu.Lock()
	restored := n.workspaces[w.ID]
	_, prepared := n.prepared[w.ID]
	n.mu.Unlock()
	if restored != w || prepared || h.destroyed.Load() != 0 {
		t.Fatalf("source was not restored: workspace=%p prepared=%v destroyed=%d", restored, prepared, h.destroyed.Load())
	}
}

func TestEnforcedGatewayRequiresConcreteNetworkController(t *testing.T) {
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handle := &failingHandle{id: "ws_dishonest", fs: fs}
	backend := &fixedBackend{name: "enforced", handle: handle, caps: workspace.Caps{
		Isolation: "container", EgressMode: "enforced_gateway", BrokerIdentity: "token",
	}}
	n := newTestNode(t, func(opts *Options) { opts.Backends = workspace.NewRegistry(backend) })
	w := proto.Workspace{
		ID: "ws_dishonest", Generation: 1, State: proto.WSClaiming,
		Spec: proto.WorkspaceSpec{Requires: proto.Requires{Backend: "enforced"}},
	}
	n.mu.Lock()
	n.materializing[w.ID] = &materialization{generation: w.Generation, deadline: time.Now().Add(time.Minute), cancel: func() {}}
	n.mu.Unlock()
	err = n.materialize(context.Background(), w, false)
	if err == nil || !strings.Contains(err.Error(), "without a network controller") {
		t.Fatalf("dishonest backend materialize error=%v", err)
	}
	n.mu.Lock()
	_, serving := n.workspaces[w.ID]
	_, quarantined := n.quarantined[w.ID]
	n.mu.Unlock()
	if serving || !quarantined {
		t.Fatalf("dishonest backend serving=%v quarantined=%v", serving, quarantined)
	}
}

func TestEnforcedGatewayAppliesPolicyAndRevokesBeforeFencing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handle := &networkHandle{
		failingHandle: &failingHandle{id: "ws_enforced", fs: fs}, backend: "enforced",
	}
	backend := &fixedBackend{name: "enforced", handle: handle, caps: workspace.Caps{
		Isolation: "container", EgressMode: "enforced_gateway", BrokerIdentity: "token",
	}}
	n := newTestNode(t, func(opts *Options) { opts.Backends = workspace.NewRegistry(backend) })
	w := proto.Workspace{
		ID: "ws_enforced", Generation: 3, State: proto.WSClaiming,
		Spec: proto.WorkspaceSpec{
			Requires: proto.Requires{Backend: "enforced"},
			Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}},
		},
	}
	n.mu.Lock()
	n.materializing[w.ID] = &materialization{generation: w.Generation, deadline: time.Now().Add(time.Minute), cancel: func() {}}
	n.mu.Unlock()
	// With no control peer, materialization reaches the ready boundary and
	// self-fences. Policy must already be installed and revocation must run.
	err = n.materialize(context.Background(), w, false)
	if err == nil || handle.applied.Load() != 1 || handle.revoked.Load() != 1 {
		t.Fatalf("materialize err=%v applied=%d revoked=%d", err, handle.applied.Load(), handle.revoked.Load())
	}
	if handle.endpoint.Workspace != w.ID || handle.endpoint.Generation != w.Generation ||
		handle.endpoint.ReverseProxyURL == "" || handle.endpoint.ForwardProxyURL == "" {
		t.Fatalf("network endpoint=%#v", handle.endpoint)
	}
}

func TestQuarantineDoesNotAffirmFenceWhenNetworkRevocationFails(t *testing.T) {
	n := newTestNode(t, nil)
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handle := &networkHandle{
		failingHandle: &failingHandle{id: "ws_revoke", fs: fs}, backend: "enforced",
		revokeErr: errors.New("firewall unavailable"),
	}
	w := &ws{Workspace: proto.Workspace{ID: "ws_revoke", Generation: 2, State: proto.WSClaimed}, handle: handle}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = time.Now().Add(time.Hour)
	n.mu.Unlock()
	res, err := n.quarantine(context.Background(), &proto.WSQuarantineReq{
		OperationID: "fleet_revoke", WS: w.ID, Gen: w.Generation, Action: proto.FleetActionFreeze,
	})
	if err != nil || res.Fenced || !strings.Contains(res.Warning, "firewall unavailable") || handle.revoked.Load() != 1 {
		t.Fatalf("quarantine=%#v err=%v revoked=%d", res, err, handle.revoked.Load())
	}
}

func TestRetainedQuarantineReopensBackendAndRevokesNetwork(t *testing.T) {
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handle := &networkHandle{
		failingHandle: &failingHandle{id: "ws_retained_network", fs: fs}, backend: "enforced",
	}
	backend := &fixedBackend{name: "enforced", handle: handle, caps: workspace.Caps{
		Isolation: "container", EgressMode: "enforced_gateway", BrokerIdentity: "token",
	}}
	n := newTestNode(t, func(opts *Options) { opts.Backends = workspace.NewRegistry(backend) })
	n.mu.Lock()
	n.quarantined[handle.id] = struct{}{}
	n.mu.Unlock()
	res, err := n.quarantine(context.Background(), &proto.WSQuarantineReq{
		OperationID: "fleet_retained_network", WS: handle.id, Gen: 4,
		Action: proto.FleetActionRevokeEgress, Backend: backend.name,
		Security: proto.SecuritySpec{Profile: proto.SecurityIsolated},
	})
	if err != nil || !res.Fenced || res.Warning != "" || handle.revoked.Load() != 1 {
		t.Fatalf("retained quarantine=%#v err=%v revoked=%d", res, err, handle.revoked.Load())
	}
}

func TestQuarantineWaitsForMaterializationCancellation(t *testing.T) {
	n := newTestNode(t, nil)
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_materializing", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = handle.FS().Close()
	cancelled := make(chan struct{})
	done := make(chan struct{})
	n.mu.Lock()
	n.materializing["ws_materializing"] = &materialization{
		generation: 8, cancel: func() { close(cancelled) }, done: done,
	}
	n.mu.Unlock()
	type result struct {
		res *proto.WSQuarantineRes
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		res, callErr := n.quarantine(context.Background(), &proto.WSQuarantineReq{
			OperationID: "fleet_materializing", WS: "ws_materializing", Gen: 8,
			Action: proto.FleetActionFreeze, Backend: "process",
		})
		resultCh <- result{res: res, err: callErr}
	}()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("quarantine did not cancel materialization")
	}
	select {
	case got := <-resultCh:
		t.Fatalf("quarantine acknowledged before materialization stopped: %#v", got)
	default:
	}
	close(done)
	select {
	case got := <-resultCh:
		if got.err != nil || got.res == nil || !got.res.Fenced {
			t.Fatalf("quarantine after materialization stop=%#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("quarantine did not finish after materialization stopped")
	}
}

func TestShutdownRevokesEnforcedWorkspaceNetwork(t *testing.T) {
	n := newTestNode(t, nil)
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	handle := &networkHandle{
		failingHandle: &failingHandle{id: "ws_shutdown", fs: fs}, backend: "enforced",
	}
	n.mu.Lock()
	n.workspaces[handle.id] = &ws{
		Workspace: proto.Workspace{ID: handle.id, Spec: proto.WorkspaceSpec{
			Security: proto.SecuritySpec{Profile: proto.SecurityIsolated},
		}},
		handle: handle,
	}
	n.mu.Unlock()
	n.shutdown()
	if handle.revoked.Load() != 1 {
		t.Fatalf("shutdown network revocations=%d", handle.revoked.Load())
	}
}

func TestRejectedRenewalFencesAndRetainsFilesystem(t *testing.T) {
	n := newTestNode(t, nil)
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_fence", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := handle.FS().Root()
	if err := handle.FS().Write("keep", []byte("current bytes"), 0o600, false, false); err != nil {
		t.Fatal(err)
	}
	w := &ws{Workspace: proto.Workspace{ID: "ws_fence", Generation: 3, State: proto.WSClaimed}, handle: handle}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = n.started.AddDate(1, 0, 0)
	n.mu.Unlock()
	n.applyRenewResults(proto.WSRenewReq{IDs: []string{w.ID}, Gen: map[string]uint64{w.ID: 3}}, &proto.WSRenewRes{
		Results: []proto.WSRenewResult{{ID: w.ID, Generation: 3, Accepted: false, Action: "fence", AuthoritativeGen: 4}},
	})
	n.mu.Lock()
	_, serving := n.workspaces[w.ID]
	_, quarantined := n.quarantined[w.ID]
	n.mu.Unlock()
	if serving || !quarantined {
		t.Fatalf("fence state: serving=%v quarantined=%v", serving, quarantined)
	}
	got, err := os.ReadFile(filepath.Join(root, "keep"))
	if err != nil || string(got) != "current bytes" {
		t.Fatalf("fence lost filesystem: %q, %v", got, err)
	}
}

func TestWorkspaceRegistryDescriptorsAreDerived(t *testing.T) {
	process, err := workspace.NewProcess(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := workspace.NewRegistry(process)
	d := r.Descriptors()
	if len(d) != 1 || d[0].Name != "process" || d[0].Security.Isolation != "none" || d[0].Security.EgressMode == "enforced_gateway" {
		t.Fatalf("process descriptor overclaims security: %#v", d)
	}
}

func TestHandleRejectsWhenRequestCapacityIsExhausted(t *testing.T) {
	n := newTestNode(t, func(opts *Options) { opts.MaxConcurrentRequests = 1 })
	n.requestSlots <- struct{}{}
	nodeConn, clientConn := transport.Pipe(4)
	p := transport.NewPeer(nodeConn, nil)
	defer p.Close()
	defer clientConn.Close()

	f := &proto.Frame{V: proto.Version, T: proto.KindReq, ID: 77, From: "c_test", Op: proto.OpNodeStatus}
	n.handle(context.Background(), p, f)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := clientConn.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != f.ID || got.Err == nil || got.Err.Code != proto.CodeResourceExhausted {
		t.Fatalf("overload response = %#v", got)
	}
	<-n.requestSlots
}

func TestMutationJournalWritesIntentBeforeEffectAndReplaysDurableResult(t *testing.T) {
	n := newTestNode(t, nil)
	request := struct{ Value string }{Value: "one"}
	key := "subject|workspace|operation|key"
	applied := 0
	result, err := n.runMutation(context.Background(), key, request, func() ([]byte, error) {
		applied++
		loaded, err := loadMutations(n.mutationPath)
		if err != nil {
			t.Fatal(err)
		}
		entry := loaded[key]
		if entry == nil || entry.State != mutationPending {
			t.Fatalf("durable intent before effect = %#v", entry)
		}
		return []byte("result"), nil
	})
	if err != nil || string(result) != "result" || applied != 1 {
		t.Fatalf("first mutation = %q, %v, applied=%d", result, err, applied)
	}

	replayed, err := n.runMutation(context.Background(), key, request, func() ([]byte, error) {
		applied++
		return []byte("duplicate"), nil
	})
	if err != nil || string(replayed) != "result" || applied != 1 {
		t.Fatalf("replay = %q, %v, applied=%d", replayed, err, applied)
	}
	loaded, err := loadMutations(n.mutationPath)
	if err != nil {
		t.Fatal(err)
	}
	if entry := loaded[key]; entry == nil || entry.State != mutationCompleted || string(entry.Result) != "result" {
		t.Fatalf("durable result = %#v", entry)
	}
	if _, err := n.runMutation(context.Background(), key, struct{ Value string }{Value: "two"}, func() ([]byte, error) {
		return nil, nil
	}); err == nil {
		t.Fatal("same key with a different fingerprint was accepted")
	}
}

func TestMutationDoesNotRunWithoutDurableIntent(t *testing.T) {
	n := newTestNode(t, nil)
	n.mutationPath = filepath.Join(t.TempDir(), "missing", "mutations.cbor")
	applied := false
	_, err := n.runMutation(context.Background(), "key", "request", func() ([]byte, error) {
		applied = true
		return nil, nil
	})
	if err == nil || applied {
		t.Fatalf("mutation err=%v applied=%v", err, applied)
	}
}

func TestMutationDoesNotAcknowledgeUntilResultIsDurable(t *testing.T) {
	n := newTestNode(t, nil)
	originalPath := n.mutationPath
	request := struct{ Value string }{Value: "request"}
	applied := 0
	result, err := n.runMutation(context.Background(), "key", request, func() ([]byte, error) {
		applied++
		// The intent is already durable at originalPath. Make only the
		// completion write fail to model disk loss after the side effect.
		n.mutationPath = filepath.Join(t.TempDir(), "missing", "mutations.cbor")
		return []byte("committed-result"), nil
	})
	if err == nil || result != nil || applied != 1 {
		t.Fatalf("first result=%q err=%v applied=%d", result, err, applied)
	}

	// Once storage recovers, an exact retry durably records and returns the
	// original result without executing the effect again.
	n.mutationPath = originalPath
	result, err = n.runMutation(context.Background(), "key", request, func() ([]byte, error) {
		applied++
		return []byte("duplicate"), nil
	})
	if err != nil || string(result) != "committed-result" || applied != 1 {
		t.Fatalf("recovery result=%q err=%v applied=%d", result, err, applied)
	}
}

func TestPendingMutationFromRestartIsNeverReapplied(t *testing.T) {
	n := newTestNode(t, nil)
	request := struct{ Value string }{Value: "request"}
	key := "pending-key"
	fingerprint := sha256.Sum256(proto.MustMarshal(request))
	n.mutationMu.Lock()
	n.mutations[key] = &mutationEntry{
		Fingerprint: fingerprint, State: mutationPending, CompletedAt: time.Now().UnixMilli(),
		done: make(chan struct{}), needsPersist: true,
	}
	if err := n.persistMutationsLocked(); err != nil {
		n.mutationMu.Unlock()
		t.Fatal(err)
	}
	n.mutationMu.Unlock()

	loaded, err := loadMutations(n.mutationPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := &Node{mutations: loaded, mutationPath: n.mutationPath}
	applied := false
	_, err = restarted.runMutation(context.Background(), key, request, func() ([]byte, error) {
		applied = true
		return nil, nil
	})
	var protocolErr *proto.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != proto.CodeConflict || applied {
		t.Fatalf("restart replay err=%v applied=%v", err, applied)
	}
}

func TestQuarantineFencesAndDestroyCommitIsIdempotent(t *testing.T) {
	n := newTestNode(t, nil)
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_quarantine", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.FS().Write("keep", []byte("evidence"), 0o600, false, false); err != nil {
		t.Fatal(err)
	}
	w := &ws{Workspace: proto.Workspace{ID: "ws_quarantine", Generation: 7, State: proto.WSClaimed}, handle: handle}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = time.Now().Add(time.Hour)
	n.mu.Unlock()

	req := &proto.WSQuarantineReq{OperationID: "fleet_one", WS: w.ID, Gen: 7, Action: proto.FleetActionDestroy}
	res, err := n.quarantine(context.Background(), req)
	if err != nil || !res.Fenced || res.Snapshot == "" || res.Backend != "process" {
		t.Fatalf("quarantine=%#v err=%v", res, err)
	}
	n.mu.Lock()
	_, serving := n.workspaces[w.ID]
	_, retained := n.quarantined[w.ID]
	n.mu.Unlock()
	if serving || !retained {
		t.Fatalf("serving=%v retained=%v", serving, retained)
	}
	if got, err := os.ReadFile(filepath.Join(handle.FS().Root(), "keep")); err != nil || string(got) != "evidence" {
		t.Fatalf("evidence before commit=%q err=%v", got, err)
	}
	replayed, err := n.quarantine(context.Background(), req)
	if err != nil || replayed.Snapshot != res.Snapshot {
		t.Fatalf("quarantine replay=%#v err=%v", replayed, err)
	}

	commit := &proto.WSQuarantineCommitReq{
		OperationID: req.OperationID, WS: req.WS, Gen: req.Gen,
		Backend: res.Backend, Snapshot: res.Snapshot,
	}
	mismatched := *commit
	mismatched.Snapshot = "sha256:not-the-checkpoint"
	if err := n.quarantineCommit(context.Background(), &mismatched); err == nil {
		t.Fatal("destroy commit with a mismatched checkpoint was accepted")
	}
	if _, err := os.Stat(handle.FS().Root()); err != nil {
		t.Fatalf("mismatched destroy commit changed source: %v", err)
	}

	// A restarted node must be able to verify the durable phase-one proof and
	// complete destruction without re-running the checkpoint.
	restarted, err := New(Options{
		DataDir: n.opts.DataDir,
		Dialer: transport.DialFunc(func(context.Context) (transport.Conn, error) {
			return nil, errors.New("unused test dialer")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.sessions.Close()
	if err := restarted.quarantineCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(handle.FS().Root()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace still exists after commit: %v", err)
	}
	if err := restarted.quarantineCommit(context.Background(), commit); err != nil {
		t.Fatalf("destroy commit replay: %v", err)
	}
}

func TestQuarantineSnapshotsRetainedWorkspaceBeforeDestroy(t *testing.T) {
	n := newTestNode(t, nil)
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_retained", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.FS().Write("keep", []byte("forensic evidence"), 0o600, false, false); err != nil {
		t.Fatal(err)
	}
	if err := handle.FS().Write("excluded", []byte("reproducible"), 0o600, false, false); err != nil {
		t.Fatal(err)
	}
	if err := handle.FS().Mkdir(EnvFileDir); err != nil {
		t.Fatal(err)
	}
	if err := handle.FS().Write(EnvFilePath, []byte("node-local"), 0o600, false, false); err != nil {
		t.Fatal(err)
	}
	root := handle.FS().Root()
	_ = handle.FS().Close()
	n.mu.Lock()
	n.quarantined["ws_retained"] = struct{}{}
	n.mu.Unlock()

	req := &proto.WSQuarantineReq{
		OperationID: "fleet_retained", WS: "ws_retained", Gen: 4,
		Action: proto.FleetActionDestroy, Backend: "process", Exclude: []string{"excluded"},
	}
	res, err := n.quarantine(context.Background(), req)
	if err != nil || !res.Fenced || res.Backend != "process" || res.Snapshot == "" || res.Warning != "" {
		t.Fatalf("retained quarantine=%#v err=%v", res, err)
	}
	r, _, err := n.store.Open(res.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	restored := t.TempDir()
	if err := artifact.Restore(restored, r); err != nil {
		r.Close()
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(restored, "keep")); err != nil || string(got) != "forensic evidence" {
		t.Fatalf("restored evidence=%q err=%v", got, err)
	}
	for _, omitted := range []string{"excluded", EnvFilePath} {
		if _, err := os.Stat(filepath.Join(restored, omitted)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("excluded path %q restored: %v", omitted, err)
		}
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("retained source disappeared before commit: %v", err)
	}
	if err := n.quarantineCommit(context.Background(), &proto.WSQuarantineCommitReq{
		OperationID: req.OperationID, WS: req.WS, Gen: req.Gen,
		Backend: res.Backend, Snapshot: res.Snapshot,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retained source still exists after proven commit: %v", err)
	}
}

func TestQuarantineCommitWithoutPhaseOneProofCannotDelete(t *testing.T) {
	n := newTestNode(t, nil)
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_unproven", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := handle.FS().Root()
	_ = handle.FS().Close()
	n.mu.Lock()
	n.quarantined["ws_unproven"] = struct{}{}
	n.mu.Unlock()

	err = n.quarantineCommit(context.Background(), &proto.WSQuarantineCommitReq{
		OperationID: "fleet_unproven", WS: "ws_unproven", Gen: 2,
		Backend: "process", Snapshot: "sha256:unproven",
	})
	var protocolErr *proto.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != proto.CodeConflict {
		t.Fatalf("unproven commit error=%v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("unproven commit changed source: %v", err)
	}
}
