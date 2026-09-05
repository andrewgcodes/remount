package node

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/volume"
	"remount.dev/remount/internal/workspace"
)

type cancelClosingConn struct {
	sendStarted chan struct{}
	releaseSend chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
}

func newCancelClosingConn() *cancelClosingConn {
	return &cancelClosingConn{
		sendStarted: make(chan struct{}),
		releaseSend: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (c *cancelClosingConn) Send(ctx context.Context, f *proto.Frame) error {
	if f.T != proto.KindChunk {
		return nil
	}
	c.startOnce.Do(func() { close(c.sendStarted) })
	select {
	case <-ctx.Done():
		_ = c.Close()
		return ctx.Err()
	case <-c.releaseSend:
		return nil
	}
}

func (c *cancelClosingConn) Recv(ctx context.Context) (*proto.Frame, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, transport.ErrClosed
	}
}

func (c *cancelClosingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

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
	t.Cleanup(n.shutdown)
	return n
}

func TestReplacingSessionCursorDoesNotCancelSharedPeerWrite(t *testing.T) {
	n := newTestNode(t, nil)
	s, err := n.sessions.Open(session.Spec{WS: "ws_cursor", Kind: proto.SessionExec, Program: []string{"/bin/cat"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.sessions.Remove(s.ID, true) })

	conn := newCancelClosingConn()
	p := transport.NewPeer(conn, nil)
	t.Cleanup(func() { _ = p.Close() })

	n.subscribe(p, "c_cursor", s, 0, "")
	select {
	case <-conn.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("initial cursor did not start its chunk write")
	}

	n.subscribe(p, "c_cursor", s, 0, "")
	select {
	case <-p.Done():
		t.Fatalf("replacing the cursor closed the shared peer: %v", p.Err())
	case <-time.After(100 * time.Millisecond):
	}
	close(conn.releaseSend)
}

func TestStaleSessionDetachDoesNotCancelReplacementCursor(t *testing.T) {
	n := newTestNode(t, nil)
	s, err := n.sessions.Open(session.Spec{WS: "ws_cursor", Kind: proto.SessionExec, Program: []string{"/bin/cat"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.sessions.Remove(s.ID, true) })

	conn := newCancelClosingConn()
	p := transport.NewPeer(conn, nil)
	t.Cleanup(func() { _ = p.Close() })

	n.subscribe(p, "c_cursor", s, 0, "sub_old")
	n.subscribe(p, "c_cursor", s, 0, "sub_new")
	n.unsubscribe("c_cursor", s.ID, "sub_old")

	n.mu.Lock()
	current := n.subs["c_cursor|"+s.ID]
	n.mu.Unlock()
	if current == nil || current.subscription != "sub_new" {
		t.Fatal("stale detach removed the replacement cursor")
	}
	close(conn.releaseSend)
}

func TestSubscriptionReportsDurableCompletionFailure(t *testing.T) {
	n := newTestNode(t, nil)
	n.sessions.Close()
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("complete record rejected")
	n.sessions = session.NewManager(session.ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1, SpillBytes: 1 << 20, MaxChunk: 16, SegmentBytes: 64,
		Retention:          time.Hour,
		BlobStoreForTenant: func(string) (artifact.BlobStore, error) { return store, nil },
		CommitSessionLogRecord: func(string, session.Spec, session.LogRecord) error {
			return nil
		},
		CompleteSessionLogRecord: func(string, session.Spec, proto.SessionInfo, proto.ExitInfo, session.LogRecord) error {
			return commitErr
		},
	})
	s, err := n.sessions.Open(session.Spec{
		WS: "ws_completion_failure", Tenant: "tenant", Principal: "principal",
		Kind: proto.SessionExec, Program: []string{"sh", "-c", "printf output"},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeConn, clientConn := transport.Pipe(16)
	p := transport.NewPeer(nodeConn, nil)
	t.Cleanup(func() {
		_ = p.Close()
		_ = clientConn.Close()
	})
	n.subscribe(p, "c_completion_failure", s, 0, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		frame, err := clientConn.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var body proto.ChunkBody
		if err := proto.Unmarshal(frame.Body, &body); err != nil {
			t.Fatal(err)
		}
		if body.Stream != proto.StreamExit {
			continue
		}
		var exit proto.ExitInfo
		if err := proto.Unmarshal(body.Data, &exit); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(exit.Error, commitErr.Error()) {
			t.Fatalf("exit = %+v, want durable completion failure", exit)
		}
		return
	}
}

func closeNodeRuntimeForTest(n *Node) {
	n.sessions.Close()
	n.mu.Lock()
	held := make(map[*ws]struct{}, len(n.workspaces)+len(n.prepared))
	for _, w := range n.workspaces {
		held[w] = struct{}{}
	}
	for _, prepared := range n.prepared {
		held[prepared.workspace] = struct{}{}
	}
	n.mu.Unlock()
	for w := range held {
		if w != nil && w.handle != nil {
			_ = w.handle.FS().Close()
		}
	}
	if n.volumeRoot != nil {
		if n.volumes != nil {
			_ = n.volumes.Close()
		}
		_ = n.volumeRoot.Close()
	}
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

func TestProvisionedIdentityIsPinnedAndCannotBeReplaced(t *testing.T) {
	dir := t.TempDir()
	n, err := New(Options{DataDir: dir, ID: "n_planned"})
	if err != nil {
		t.Fatal(err)
	}
	if n.ID() != "n_planned" {
		t.Fatalf("node id = %q", n.ID())
	}
	closeNodeRuntimeForTest(n)
	if _, err := New(Options{DataDir: dir, ID: "n_other"}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("identity replacement error = %v", err)
	}
}

func TestNewRejectsNegativeResourceLimits(t *testing.T) {
	tests := map[string]func(*Options){
		"sessions":                func(o *Options) { o.MaxSessions = -1 },
		"active sessions":         func(o *Options) { o.MaxActiveSessions = -1 },
		"workspace sessions":      func(o *Options) { o.MaxSessionsPerWorkspace = -1 },
		"principal sessions":      func(o *Options) { o.MaxSessionsPerPrincipal = -1 },
		"concurrent requests":     func(o *Options) { o.MaxConcurrentRequests = -1 },
		"concurrent snapshots":    func(o *Options) { o.MaxConcurrentSnapshots = -1 },
		"snapshot min interval":   func(o *Options) { o.SnapshotMinInterval = -1 },
		"artifact retention":      func(o *Options) { o.ArtifactRetention = -1 },
		"artifact gc interval":    func(o *Options) { o.ArtifactGCInterval = -1 },
		"volume source bytes":     func(o *Options) { o.MaxVolumeSourceBytes = -1 },
		"volume source entries":   func(o *Options) { o.MaxVolumeSourceEntries = -1 },
		"connector total bytes":   func(o *Options) { o.MaxConnectorCacheBytes = -1 },
		"connector scope bytes":   func(o *Options) { o.MaxConnectorWorkspaceBytes = -1 },
		"connector object bytes":  func(o *Options) { o.MaxConnectorObjectBytes = -1 },
		"connector objects":       func(o *Options) { o.MaxConnectorObjects = -1 },
		"connector scope objects": func(o *Options) { o.MaxConnectorWorkspaceObjects = -1 },
		"session memory":          func(o *Options) { o.SessionMemoryBytes = -1 },
		"session spill":           func(o *Options) { o.SessionSpillBytes = -1 },
		"session chunk":           func(o *Options) { o.SessionMaxChunkBytes = -1 },
		"session chunk count":     func(o *Options) { o.SessionMaxMemoryChunks = -1 },
	}
	for name, configure := range tests {
		t.Run(name, func(t *testing.T) {
			opts := Options{DataDir: t.TempDir()}
			configure(&opts)
			if _, err := New(opts); err == nil || !strings.Contains(err.Error(), "must not be negative") {
				t.Fatalf("New error = %v, want negative-limit validation", err)
			}
		})
	}
}

func TestNewRemovesOnlyOwnedOrphanSpillFiles(t *testing.T) {
	directory := t.TempDir()
	spillDirectory := filepath.Join(directory, "spill")
	if err := os.MkdirAll(spillDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(spillDirectory, "s_orphan.log")
	keep := filepath.Join(spillDirectory, "operator-note.txt")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	n, err := New(Options{DataDir: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNodeRuntimeForTest(n)
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan spill remains: %v", err)
	}
	if body, err := os.ReadFile(keep); err != nil || string(body) != "keep" {
		t.Fatalf("unowned file changed: %q, %v", body, err)
	}
}

func TestNodeArtifactGCIsReferenceAware(t *testing.T) {
	n := newTestNode(t, func(options *Options) { options.ArtifactRetention = time.Hour })
	referenced, _, err := n.store.Put(strings.NewReader("referenced"))
	if err != nil {
		t.Fatal(err)
	}
	orphan, _, err := n.store.Put(strings.NewReader("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, id := range []string{referenced, orphan} {
		digest := strings.TrimPrefix(id, artifact.Prefix)
		path := filepath.Join(n.opts.DataDir, "artifacts", digest[:2], digest)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		id, body string
	}{{referenced, "referenced"}, {orphan, "orphan"}} {
		dir := filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant", item.id))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "data"), []byte(item.body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	n.workspaces["ws_ref"] = &ws{Workspace: proto.Workspace{ID: "ws_ref", LastSnapshot: referenced}}
	result, err := n.CollectArtifacts(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !n.store.Has(referenced) || n.store.Has(orphan) || result.Removed != 1 {
		t.Fatalf("collection = %+v, referenced=%t orphan=%t", result, n.store.Has(referenced), n.store.Has(orphan))
	}
	if _, err := os.Stat(filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant", referenced))); err != nil {
		t.Fatalf("referenced volume source removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant", orphan))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan volume source remains: %v", err)
	}
	delete(n.workspaces, "ws_ref")
	result, err = n.CollectArtifacts(time.Now())
	if err != nil || n.store.Has(referenced) || result.Removed != 1 {
		t.Fatalf("post-release collection = %+v, retained=%t, err=%v", result, n.store.Has(referenced), err)
	}
	if _, err := os.Stat(filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant", referenced))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released volume source remains: %v", err)
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

func TestFetchArtifactRejectsCorruptCachedBlob(t *testing.T) {
	n := newTestNode(t, nil)
	id, _, err := n.store.Put(strings.NewReader("valid cached body"))
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.TrimPrefix(id, artifact.Prefix)
	path := filepath.Join(n.opts.DataDir, "artifacts", digest[:2], digest)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt cached body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := n.fetchArtifact(context.Background(), id); !errors.Is(err, artifact.ErrDigestMismatch) {
		t.Fatalf("fetch corrupt cached artifact = %v, want digest mismatch", err)
	}
}

func TestProductionCachedArtifactRequiresTenantProof(t *testing.T) {
	body := "tenant-b cached bytes"
	id := nodeDigest(body)
	var artifactRequests atomic.Int64
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		artifactRequests.Add(1)
		if r.Method != http.MethodHead || r.Header.Get("Authorization") != "Bearer dynamic-node-token" ||
			r.Header.Get(proto.ArtifactWorkspaceHeader) != "ws_a" || r.Header.Get(proto.ArtifactGenerationHeader) != "4" ||
			r.Header.Get(proto.ArtifactProofHeader) != "opaque-proof" {
			t.Errorf("artifact authorization headers = method=%s auth=%q ws=%q gen=%q proof=%q", r.Method,
				r.Header.Get("Authorization"), r.Header.Get(proto.ArtifactWorkspaceHeader),
				r.Header.Get(proto.ArtifactGenerationHeader), r.Header.Get(proto.ArtifactProofHeader))
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer httpServer.Close()
	n := newTestNode(t, func(opts *Options) {
		opts.ArtifactURL = httpServer.URL
		opts.HTTPClient = httpServer.Client()
	})
	if stored, _, err := n.store.Put(strings.NewReader(body)); err != nil || stored != id {
		t.Fatalf("seed local cache = %q, %v", stored, err)
	}
	attachArtifactProofPeer(t, n, func(req proto.ArtifactProofReq) proto.ArtifactProofRes {
		if req.Workspace != "ws_a" || req.Generation != 4 || req.Method != http.MethodHead || req.Artifact != id {
			t.Errorf("artifact proof request = %+v", req)
		}
		return proto.ArtifactProofRes{Proof: "opaque-proof"}
	})

	w := &proto.Workspace{ID: "ws_a", Tenant: "tenant-a", Generation: 4, State: proto.WSClaimed}
	if _, err := n.fetchWorkspaceArtifact(context.Background(), w, id); err == nil {
		t.Fatal("cross-tenant cache hit bypassed tenant authority")
	}
	if artifactRequests.Load() != 1 {
		t.Fatalf("artifact HTTP requests = %d, want one authorization HEAD", artifactRequests.Load())
	}
}

func TestProductionArtifactDownloadCarriesExactProofBinding(t *testing.T) {
	body := "tenant-a remote bytes"
	id := nodeDigest(body)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer dynamic-node-token" ||
			r.Header.Get(proto.ArtifactWorkspaceHeader) != "ws_a" || r.Header.Get(proto.ArtifactGenerationHeader) != "4" ||
			r.Header.Get(proto.ArtifactProofHeader) != "opaque-proof" {
			http.Error(w, "missing proof", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	defer httpServer.Close()
	n := newTestNode(t, func(opts *Options) {
		opts.ArtifactURL = httpServer.URL
		opts.HTTPClient = httpServer.Client()
	})
	attachArtifactProofPeer(t, n, func(req proto.ArtifactProofReq) proto.ArtifactProofRes {
		if req.Workspace != "ws_a" || req.Generation != 4 || req.Method != http.MethodGet || req.Artifact != id {
			t.Errorf("artifact proof request = %+v", req)
		}
		return proto.ArtifactProofRes{Proof: "opaque-proof"}
	})

	w := &proto.Workspace{ID: "ws_a", Tenant: "tenant-a", Generation: 4, State: proto.WSClaiming}
	r, err := n.fetchWorkspaceArtifact(context.Background(), w, id)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || string(got) != body {
		t.Fatalf("download = %q, read=%v close=%v", got, readErr, closeErr)
	}
}

func attachArtifactProofPeer(t *testing.T, n *Node, issue func(proto.ArtifactProofReq) proto.ArtifactProofRes) {
	t.Helper()
	nodeConn, controlConn := transport.Pipe(8)
	nodePeer := transport.NewPeer(nodeConn, nil)
	controlPeer := transport.NewPeer(controlConn, transport.HandlerFunc(func(ctx context.Context, p *transport.Peer, frame *proto.Frame) {
		if frame.Op != proto.OpArtifactProof {
			t.Errorf("control operation = %q, want %q", frame.Op, proto.OpArtifactProof)
			_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeBadRequest, "unexpected operation"))
			return
		}
		var req proto.ArtifactProofReq
		if err := frame.Decode(&req); err != nil {
			t.Errorf("decode proof request: %v", err)
			_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeBadRequest, "invalid request"))
			return
		}
		_ = p.Respond(ctx, frame, issue(req))
	}))
	t.Cleanup(func() {
		_ = nodePeer.Close()
		_ = controlPeer.Close()
	})
	n.mu.Lock()
	n.peer = nodePeer
	n.httpToken = "dynamic-node-token"
	n.mu.Unlock()
}

func TestSessionCapabilityHandleRotatesProofBeforeExpiry(t *testing.T) {
	n := newTestNode(t, nil)
	nodeConn, controlConn := transport.Pipe(8)
	nodePeer := transport.NewPeer(nodeConn, nil)
	var renewals atomic.Int64
	controlPeer := transport.NewPeer(controlConn, transport.HandlerFunc(func(ctx context.Context, p *transport.Peer, frame *proto.Frame) {
		switch frame.Op {
		case proto.OpSessionCapabilityRenew:
			var request proto.SessionCapabilityRenewReq
			if err := frame.Decode(&request); err != nil || request.Capability != "signed-proof-1" {
				t.Errorf("renew request=%+v err=%v", request, err)
				_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeBadRequest, "bad renew request"))
				return
			}
			renewals.Add(1)
			_ = p.Respond(ctx, frame, proto.SessionCapabilityIssueRes{Capability: "signed-proof-2", ExpiresAt: time.Now().Add(2 * time.Second).UnixMilli()})
		case proto.OpSessionCapabilityCheck:
			var request proto.SessionCapabilityCheckReq
			if err := frame.Decode(&request); err != nil || request.Capability != "signed-proof-2" {
				t.Errorf("check request=%+v err=%v", request, err)
				_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeDenied, "stale proof"))
				return
			}
			_ = p.Respond(ctx, frame, proto.SessionCapabilityCheckRes{Principal: "agent:alice", Tenant: "tenant-a"})
		default:
			_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeUnsupported, "unexpected operation"))
		}
	}))
	t.Cleanup(func() {
		_ = nodePeer.Close()
		_ = controlPeer.Close()
	})
	n.mu.Lock()
	n.peer = nodePeer
	n.mu.Unlock()
	w := &ws{Workspace: proto.Workspace{ID: "ws_session_cap", Tenant: "tenant-a", Generation: 3}}
	claims := proto.GrantClaims{WS: w.ID, Gen: w.Generation, Principal: "agent:alice", Tenant: w.Tenant}
	handle := "sc_stable_test_handle"
	originalExpiry := time.Now().Add(300 * time.Millisecond)
	if err := n.installSessionCapability(handle, "signed-proof-1", originalExpiry, claims, w); err != nil {
		t.Fatal(err)
	}
	s, err := n.sessions.Open(session.Spec{WS: w.ID, Kind: proto.SessionACP, Principal: claims.Principal, Tenant: claims.Tenant})
	if err != nil {
		t.Fatal(err)
	}
	n.bindSessionCapability(handle, s)
	deadline := time.Now().Add(time.Second)
	for renewals.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if renewals.Load() == 0 {
		t.Fatal("short-lived proof was not renewed")
	}
	if remaining := time.Until(originalExpiry); remaining > 0 {
		time.Sleep(remaining + 25*time.Millisecond)
	}
	principal, err := n.verifyLocalSessionCapability(context.Background(), w, handle)
	if err != nil || principal != claims.Principal {
		t.Fatalf("stable handle after original expiry principal=%q err=%v", principal, err)
	}
	s.End(proto.ExitInfo{})
}

func TestPrepareVolumeArtifactRebuildsIncompleteSource(t *testing.T) {
	n := newTestNode(t, nil)
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "dataset.txt"), []byte("verified bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	var archive strings.Builder
	if err := artifact.Snapshot(source, nil, &archive); err != nil {
		t.Fatal(err)
	}
	id, _, err := n.store.Put(strings.NewReader(archive.String()))
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant-a", id))
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "dataset.txt"), []byte("partial bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := n.prepareVolumeArtifact(context.Background(), "tenant-a", id); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "dataset.txt"))
	if err != nil || string(got) != "verified bytes" {
		t.Fatalf("rebuilt source = %q, %v", got, err)
	}
	markerPath := filepath.Join(filepath.Dir(destination), ".complete-"+volumeArtifactName(id))
	marker, err := os.ReadFile(markerPath)
	markerLines := strings.Split(strings.TrimSpace(string(marker)), "\n")
	if err != nil || len(markerLines) != 2 || markerLines[0] != id || markerLines[1] == "" {
		t.Fatalf("completion marker = %q, %v", marker, err)
	}
	if err := os.WriteFile(filepath.Join(destination, "dataset.txt"), []byte("corrupt after completion"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := n.prepareVolumeArtifact(context.Background(), "tenant-a", id); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(destination, "dataset.txt"))
	if err != nil || string(got) != "verified bytes" {
		t.Fatalf("corrupt completed source was not rebuilt = %q, %v", got, err)
	}
}

func TestPrepareVolumeArtifactEnforcesExpandedEntryLimit(t *testing.T) {
	n := newTestNode(t, func(options *Options) { options.MaxVolumeSourceEntries = 2 })
	source := t.TempDir()
	for _, name := range []string{"one", "two", "three"} {
		if err := os.WriteFile(filepath.Join(source, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var archive strings.Builder
	if err := artifact.Snapshot(source, nil, &archive); err != nil {
		t.Fatal(err)
	}
	id, _, err := n.store.Put(strings.NewReader(archive.String()))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.prepareVolumeArtifact(context.Background(), "tenant-a", id); err == nil {
		t.Fatal("expanded volume source exceeded entry limit without rejection")
	}
	if _, err := os.Stat(filepath.Join(n.opts.DataDir, "volumes", "sources", volume.SourceRelativePath("tenant-a", id))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-limit source was published: %v", err)
	}
}

// A node connected to a control plane that never negotiated a capability an
// isolated or multi-tenant workspace depends on refuses the workspace before
// any bytes land; a local workspace is unaffected, and a current control
// plane moves the decision on to backend security.
func TestMaterializeRequiresUplinkCapabilitiesForProfile(t *testing.T) {
	n := newTestNode(t, nil)
	n.mu.Lock()
	n.protocol = []string{proto.CapabilityV1}
	n.mu.Unlock()
	for _, profile := range []string{proto.SecurityIsolated, proto.SecurityMultiTenant} {
		w := proto.Workspace{
			ID: "ws_" + profile, Generation: 1, State: proto.WSClaiming,
			Spec: proto.WorkspaceSpec{Requires: proto.Requires{Backend: "process"}, Security: proto.SecuritySpec{Profile: profile}},
		}
		err := n.materialize(context.Background(), w, false)
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeUnsupported || !strings.Contains(pe.Msg, proto.CapabilityAuthzPush) || !strings.Contains(pe.Msg, profile) {
			t.Fatalf("%s under an old control plane: %v", profile, err)
		}
		if _, err := os.Stat(filepath.Join(n.opts.DataDir, "ws", w.ID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refused workspace materialized bytes: %v", err)
		}
	}
	if err := n.requireUplinkCapabilities(proto.SecurityLocal); err != nil {
		t.Fatalf("local profile refused under an old control plane: %v", err)
	}
	n.mu.Lock()
	n.protocol = proto.PeerCapabilities()
	n.mu.Unlock()
	for _, profile := range []string{proto.SecurityLocal, proto.SecurityIsolated, proto.SecurityMultiTenant} {
		if err := n.requireUplinkCapabilities(profile); err != nil {
			t.Fatalf("%s refused under a current control plane: %v", profile, err)
		}
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
	fenced      atomic.Int32
	resumed     atomic.Int32
}

func mustHostFS(t *testing.T, handle workspace.Handle) workspace.HostFileSystem {
	t.Helper()
	host, ok := workspace.HostFileSystemOf(handle)
	if !ok {
		t.Fatalf("backend %s has no host filesystem", handle.Backend())
	}
	return host
}

func (h *failingHandle) ID() string                  { return h.id }
func (h *failingHandle) Backend() string             { return "test" }
func (h *failingHandle) FS() workspace.FileSystem    { return h.fs }
func (h *failingHandle) Prepare(*session.Spec) error { return nil }
func (h *failingHandle) Snapshot(context.Context, []string, io.Writer) error {
	return h.snapshotErr
}
func (h *failingHandle) Checkpoint(context.Context, []string, io.Writer) error {
	return h.snapshotErr
}
func (h *failingHandle) CheckpointFenced(context.Context, []string, io.Writer) error {
	h.fenced.Add(1)
	return h.snapshotErr
}
func (h *failingHandle) ResumeFenced(context.Context) error {
	h.resumed.Add(1)
	return nil
}
func (h *failingHandle) Destroy(context.Context) error {
	h.destroyed.Add(1)
	return h.fs.Close()
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
	w := &ws{Workspace: proto.Workspace{ID: h.id, Tenant: "local", Generation: 7, State: proto.WSClaimed}, handle: h}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = n.started.AddDate(1, 0, 0)
	n.mu.Unlock()
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{
		WS: w.ID, Gen: 7, ReleaseEpoch: 1, OperationID: "rel_first", Snapshot: true,
	}); err == nil {
		t.Fatal("release succeeded after snapshot failure")
	}
	n.mu.Lock()
	restored := n.workspaces[w.ID]
	_, prepared := n.prepared[w.ID]
	_, quarantined := n.quarantined[w.ID]
	n.mu.Unlock()
	if restored != nil || !prepared || !quarantined || h.destroyed.Load() != 0 {
		t.Fatalf("source was not retained inert: workspace=%p prepared=%v quarantined=%v destroyed=%d",
			restored, prepared, quarantined, h.destroyed.Load())
	}
	record, ok := n.releaseRecord(w.ID)
	if !ok || record.State != releaseRestored {
		t.Fatalf("release rollback state=%+v present=%v", record, ok)
	}
	if err := n.releaseAbortCommit(context.Background(), &proto.WSReleaseCommitReq{
		ID: w.ID, Gen: w.Generation, OperationID: record.OperationID,
	}); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	restored = n.workspaces[w.ID]
	n.mu.Unlock()
	if restored != w {
		t.Fatal("abort commit did not publish restored source")
	}
	record, ok = n.releaseRecord(w.ID)
	if !ok || record.State != releasePublished {
		t.Fatalf("release abort publication state=%+v present=%v", record, ok)
	}
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{
		WS: w.ID, Gen: w.Generation, ReleaseEpoch: 1, OperationID: "rel_delayed", Reason: "delayed",
	}); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("same-epoch different operation accepted: %v", err)
	}
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{
		WS: w.ID, Gen: w.Generation, ReleaseEpoch: 2, OperationID: record.OperationID, Reason: "reused",
	}); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("new epoch reused old operation identity: %v", err)
	}
	n.mu.Lock()
	restored = n.workspaces[w.ID]
	n.mu.Unlock()
	if restored != w || h.destroyed.Load() != 0 {
		t.Fatalf("stale release changed restored source: workspace=%p destroyed=%d", restored, h.destroyed.Load())
	}
	const nextOperation = "rel_next"
	out, err := n.release(context.Background(), &proto.WSReleaseReq{
		WS: w.ID, Gen: w.Generation, ReleaseEpoch: 2, OperationID: nextOperation, Reason: "retry",
	})
	if err != nil {
		t.Fatalf("later release cycle: %v", err)
	}
	released := out.(proto.WSReleasedReq)
	if released.OperationID != nextOperation {
		t.Fatalf("later release operation = %q", released.OperationID)
	}
}

func TestSnapshotAdmissionBoundsExplicitWorkWithoutBlockingLifecycle(t *testing.T) {
	n := newTestNode(t, func(opts *Options) {
		opts.MaxConcurrentSnapshots = 1
		opts.SnapshotMinInterval = time.Hour
	})
	w1 := &ws{Workspace: proto.Workspace{ID: "ws_one"}}
	w2 := &ws{Workspace: proto.Workspace{ID: "ws_two"}}
	release, err := n.acquireSnapshot(context.Background(), w1, true)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := n.acquireSnapshot(context.Background(), w2, true); second != nil || !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		if second != nil {
			second()
		}
		release()
		t.Fatalf("concurrent snapshot admission = (%v, %v)", second != nil, err)
	}
	release()
	if retry, err := n.acquireSnapshot(context.Background(), w1, true); retry != nil || !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		if retry != nil {
			retry()
		}
		t.Fatalf("snapshot frequency admission = (%v, %v)", retry != nil, err)
	}
	// Safety-critical move/sleep/release checkpoints share the concurrency
	// budget but must not be rejected by the user-facing frequency window.
	lifecycle, err := n.acquireSnapshot(context.Background(), w1, false)
	if err != nil {
		t.Fatalf("lifecycle snapshot admission = %v", err)
	}
	lifecycle()
}

func TestAuthoritativeCheckpointRequiresControlPlaneArtifactStore(t *testing.T) {
	n := newTestNode(t, nil)
	w := &ws{Workspace: proto.Workspace{ID: "ws_checkpoint"}}
	_, err := n.snapshotExplicit(context.Background(), w, true, true, nil)
	if !errors.Is(err, &proto.Error{Code: proto.CodeUnsupported}) {
		t.Fatalf("authoritative checkpoint without artifact store = %v", err)
	}
}

func TestAuthoritativeCheckpointHoldsTreeUntilControlCommit(t *testing.T) {
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upload.Close()
	n := newTestNode(t, func(options *Options) {
		options.ArtifactURL = upload.URL
		options.HTTPClient = upload.Client()
		options.SnapshotMinInterval = time.Nanosecond
	})
	root := t.TempDir()
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	w := &ws{
		Workspace: proto.Workspace{ID: "ws_checkpoint", Generation: 1},
		handle:    &failingHandle{id: "ws_checkpoint", fs: fs},
	}
	n.workspaces[w.ID] = w
	commitEntered := make(chan struct{})
	allowCommit := make(chan struct{})
	checkpointDone := make(chan error, 1)
	go func() {
		_, err := n.snapshotExplicit(context.Background(), w, true, true, func(string) error {
			close(commitEntered)
			<-allowCommit
			return nil
		})
		checkpointDone <- err
	}()
	select {
	case <-commitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not reach control commit")
	}

	operationEntered := make(chan struct{})
	go func() {
		w.treeMu.RLock()
		close(operationEntered)
		w.treeMu.RUnlock()
	}()
	select {
	case <-operationEntered:
		t.Fatal("filesystem operation crossed the archive-to-commit boundary")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowCommit)
	if err := <-checkpointDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-operationEntered:
	case <-time.After(time.Second):
		t.Fatal("filesystem operation was not released after commit")
	}
}

func TestWorkspaceTreeLockRejectsOperationQueuedBeforeRemoval(t *testing.T) {
	n := newTestNode(t, nil)
	w := &ws{Workspace: proto.Workspace{ID: "ws_race", Generation: 1}}
	n.workspaces[w.ID] = w

	// Model an operation that passed grant authorization and then queued behind
	// a lifecycle writer. Removing the workspace before the operation acquires
	// treeMu must make the post-lock check fail without touching its handle.
	w.treeMu.Lock()
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		unlock, err := n.lockWorkspaceTree(w, false)
		if unlock != nil {
			unlock()
		}
		result <- err
	}()
	<-started
	n.mu.Lock()
	delete(n.workspaces, w.ID)
	n.mu.Unlock()
	w.treeMu.Unlock()

	select {
	case err := <-result:
		if !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
			t.Fatalf("queued operation = %v, want conflict", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued operation did not finish")
	}
}

func TestAuthoritativeCheckpointRevalidatesAfterLifecycleRemoval(t *testing.T) {
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upload.Close()
	n := newTestNode(t, func(options *Options) {
		options.ArtifactURL = upload.URL
		options.HTTPClient = upload.Client()
		options.SnapshotMinInterval = time.Nanosecond
	})
	fs, err := fsops.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	w := &ws{
		Workspace: proto.Workspace{ID: "ws_checkpoint_race", Generation: 2},
		handle:    &failingHandle{id: "ws_checkpoint_race", fs: fs},
	}
	n.workspaces[w.ID] = w

	w.treeMu.Lock()
	committed := atomic.Bool{}
	done := make(chan error, 1)
	go func() {
		_, err := n.snapshotExplicit(context.Background(), w, true, true, func(string) error {
			committed.Store(true)
			return nil
		})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		n.mu.Lock()
		checkpointing := w.checkpointing
		n.mu.Unlock()
		if checkpointing {
			break
		}
		if time.Now().After(deadline) {
			w.treeMu.Unlock()
			t.Fatal("checkpoint did not publish its fence")
		}
		time.Sleep(time.Millisecond)
	}
	n.mu.Lock()
	delete(n.workspaces, w.ID)
	n.mu.Unlock()
	w.treeMu.Unlock()

	if err := <-done; !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("checkpoint after removal = %v, want conflict", err)
	}
	if committed.Load() {
		t.Fatal("removed workspace committed an authoritative checkpoint")
	}
}

func TestReleaseRejectsConcurrentAuthoritativeCheckpoint(t *testing.T) {
	n := newTestNode(t, nil)
	w := &ws{Workspace: proto.Workspace{ID: "ws_checkpointing", Generation: 3}, checkpointing: true}
	n.workspaces[w.ID] = w
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("release during checkpoint = %v, want conflict", err)
	}
	if n.workspaces[w.ID] != w || n.prepared[w.ID] != nil {
		t.Fatal("rejected release changed node ownership")
	}
}

func TestStopWorkspaceSessionsCatchesStartupAlreadyInsideTreeBoundary(t *testing.T) {
	n := newTestNode(t, nil)
	w := &ws{Workspace: proto.Workspace{ID: "ws_starting", Generation: 1}}

	// Model sOpen after its post-lock authority check but before Manager.Open.
	// The lifecycle stop must wait for that boundary and then observe/kill the
	// newly registered process rather than taking an earlier empty snapshot.
	w.treeMu.RLock()
	stopped := make(chan error, 1)
	go func() { stopped <- n.stopWorkspaceSessions(w) }()
	s, err := n.sessions.Open(session.Spec{
		WS: w.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 30"},
	})
	if err != nil {
		w.treeMu.RUnlock()
		t.Fatal(err)
	}
	w.treeMu.RUnlock()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle stop did not drain session startup")
	}
	if !s.Exited() {
		t.Fatal("session created inside the drained boundary survived fencing")
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
	root := mustHostFS(t, handle).Root()
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

func TestSessionLogAuthorityDoesNotReadMutableLeaseFields(t *testing.T) {
	w := &ws{Workspace: proto.Workspace{
		ID: "ws_log", Generation: 7, Tenant: "tenant", LeaseUntil: 1,
	}}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				w.LeaseUntil++
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	for i := 0; i < 10000; i++ {
		authority := sessionLogAuthority(w)
		if authority.ID != w.ID || authority.Generation != w.Generation || authority.Tenant != w.Tenant {
			t.Fatalf("session log authority = %+v", authority)
		}
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

func TestMutationJournalQuotaAndCompletedRecordRetention(t *testing.T) {
	n := newTestNode(t, func(opts *Options) {
		opts.MaxMutationRecords = 1
		opts.MutationRetention = time.Hour
	})
	if _, err := n.runMutation(context.Background(), "old", "request", func() ([]byte, error) {
		return []byte("old-result"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.runMutation(context.Background(), "full", "request", func() ([]byte, error) {
		return []byte("must-not-run"), nil
	}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("mutation quota error = %v", err)
	}
	n.mutationMu.Lock()
	n.mutations["old"].CompletedAt = time.Now().Add(-2 * time.Hour).UnixMilli()
	if err := n.persistMutationsLocked(); err != nil {
		n.mutationMu.Unlock()
		t.Fatal(err)
	}
	n.mutationMu.Unlock()
	applied := 0
	if result, err := n.runMutation(context.Background(), "replacement", "request", func() ([]byte, error) {
		applied++
		return []byte("replacement-result"), nil
	}); err != nil || string(result) != "replacement-result" || applied != 1 {
		t.Fatalf("mutation after retention = (%q, %v, applied=%d)", result, err, applied)
	}
	loaded, err := loadMutations(n.mutationPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded["old"] != nil || loaded["replacement"] == nil || len(loaded) != 1 {
		t.Fatalf("retained mutation journal = %#v", loaded)
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
	if got, err := os.ReadFile(filepath.Join(mustHostFS(t, handle).Root(), "keep")); err != nil || string(got) != "evidence" {
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
	if _, err := os.Stat(mustHostFS(t, handle).Root()); err != nil {
		t.Fatalf("mismatched destroy commit changed source: %v", err)
	}

	// A restarted node must be able to verify the durable phase-one proof and
	// complete destruction without re-running the checkpoint.
	closeNodeRuntimeForTest(n)
	restarted, err := New(Options{
		DataDir: n.opts.DataDir,
		Dialer: transport.DialFunc(func(context.Context) (transport.Conn, error) {
			return nil, errors.New("unused test dialer")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNodeRuntimeForTest(restarted)
	if err := restarted.quarantineCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(mustHostFS(t, handle).Root()); !errors.Is(err, os.ErrNotExist) {
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
	root := mustHostFS(t, handle).Root()
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
	root := mustHostFS(t, handle).Root()
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

// TestAuthzPushClosesRevokedPrincipalOnly exercises the node side of a pushed
// revision without a control plane: cached grants under the old revision are
// dropped, only the named principal's sessions end with reason revoked, a
// reset ends every session, and an open that raced the push is undone.
func TestAuthzPushClosesRevokedPrincipalOnly(t *testing.T) {
	n := newTestNode(t, nil)
	w := &ws{Workspace: proto.Workspace{ID: "ws_authz", Generation: 1, AuthzRevision: 3, Tenant: "team"}}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.grants["c_a|ws_authz"] = &proto.Grant{Claims: proto.GrantClaims{WS: w.ID, AuthzRevision: 3}}
	n.grants["c_b|ws_other"] = &proto.Grant{Claims: proto.GrantClaims{WS: "ws_other", AuthzRevision: 3}}
	n.mu.Unlock()
	open := func(principal string) *session.Session {
		s, err := n.sessions.Open(session.Spec{WS: w.ID, Kind: proto.SessionExec, Program: []string{"sleep", "30"}, Principal: principal, Tenant: "team"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	revoked, kept := open("guest"), open("other")

	n.mu.Lock()
	if r := n.applyAuthzLocked(w, proto.WSRenewResult{AuthzRevision: 3}); r != nil {
		t.Fatalf("same revision produced work: %+v", r)
	}
	r := n.applyAuthzLocked(w, proto.WSRenewResult{AuthzRevision: 4, Revoked: []string{"guest"}})
	_, staleKept := n.grants["c_a|ws_authz"]
	_, otherKept := n.grants["c_b|ws_other"]
	n.mu.Unlock()
	if r == nil || w.AuthzRevision != 4 || staleKept || !otherKept {
		t.Fatalf("apply: r=%+v rev=%d stale=%v other=%v", r, w.AuthzRevision, staleKept, otherKept)
	}
	n.closeRevokedSessions(r)
	if exit := waitExit(t, revoked); exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("revoked exit %+v", exit)
	}
	if kept.Exited() {
		t.Fatal("unaffected principal's session closed")
	}

	// An open authorized under revision 4 that lands after a push to 5 is
	// terminated and refused, so no session outlives its grant's revision.
	late := open("other")
	n.mu.Lock()
	n.applyAuthzLocked(w, proto.WSRenewResult{AuthzRevision: 5, Revoked: []string{"other"}})
	n.mu.Unlock()
	err := n.confirmOpenAuthz(w, proto.GrantClaims{AuthzRevision: 4}, late)
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Code != proto.CodeUnauthorized {
		t.Fatalf("late open = %v", err)
	}
	if exit := waitExit(t, late); exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("late exit %+v", exit)
	}
	if err := n.confirmOpenAuthz(w, proto.GrantClaims{AuthzRevision: 5}, kept); err != nil {
		t.Fatalf("current open = %v", err)
	}

	// A reset closes everyone: control could not name the revoked principals.
	n.mu.Lock()
	r = n.applyAuthzLocked(w, proto.WSRenewResult{AuthzRevision: 9, AuthzReset: true})
	n.mu.Unlock()
	n.closeRevokedSessions(r)
	if exit := waitExit(t, kept); exit.Reason != proto.ExitReasonRevoked {
		t.Fatalf("reset exit %+v", exit)
	}
}

func TestAuthorizeClaimsDistinguishesControlAheadRevision(t *testing.T) {
	n := newTestNode(t, nil)
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &ws{Workspace: proto.Workspace{
		ID: "ws_authz_ahead", Node: n.id, Generation: 1, AuthzRevision: 3, Tenant: "team",
	}}
	n.mu.Lock()
	n.ctrlPub = public
	n.workspaces[w.ID] = w
	n.mu.Unlock()
	grant := func(revision uint64) *proto.Grant {
		claims := proto.GrantClaims{
			Client: "c_ahead", WS: w.ID, Node: n.id, Principal: "agent:bob", Tenant: w.Tenant,
			AuthzRevision: revision, ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), Gen: w.Generation,
		}
		return &proto.Grant{Claims: claims, Signature: ed25519.Sign(private, proto.MustMarshal(claims)), Node: n.id}
	}

	_, _, err = n.authorizeClaims("c_ahead", w.ID, grant(4))
	var protocolErr *proto.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != proto.CodeConflict {
		t.Fatalf("control-ahead grant = %v", err)
	}
	n.mu.Lock()
	r := n.applyAuthzLocked(w, proto.WSRenewResult{AuthzRevision: 4, Revoked: []string{"agent:alice"}})
	n.mu.Unlock()
	if r == nil {
		t.Fatal("authorization push did not retain revocation work")
	}
	if _, claims, err := n.authorizeClaims("c_ahead", w.ID, grant(4)); err != nil || claims.AuthzRevision != 4 {
		t.Fatalf("current grant claims=%+v err=%v", claims, err)
	}
	_, _, err = n.authorizeClaims("c_ahead", w.ID, grant(3))
	if !errors.As(err, &protocolErr) || protocolErr.Code != proto.CodeUnauthorized {
		t.Fatalf("stale grant = %v", err)
	}
}

func waitExit(t *testing.T, s *session.Session) *proto.ExitInfo {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s.Exited() {
			return s.ExitInfo()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session did not exit")
	return nil
}
