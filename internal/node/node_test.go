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
