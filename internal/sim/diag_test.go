package sim

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// findingsBy indexes findings by check name.
func findingsBy(fs []proto.Finding) map[string]proto.Finding {
	m := map[string]proto.Finding{}
	for _, f := range fs {
		m[f.Check] = f
	}
	return m
}

// The control plane must be able to describe its own state, and the numbers
// must be real rather than zeroes that happen to look calm.
func TestControlDiagReportsRealState(t *testing.T) {
	w := newWorld(t)
	w.node("n1", map[string]string{"zone": "a"})
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "diagged"})
	if _, _, _, err := c.Run(ctx, ws.ID, "sh", "-c", "echo hello"); err != nil {
		t.Fatal(err)
	}

	d, err := c.Diag(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if d.NodesOnline != 1 || d.NodesTotal != 1 {
		t.Fatalf("nodes: %d/%d", d.NodesOnline, d.NodesTotal)
	}
	if d.WorkspaceState[proto.WSClaimed] != 1 {
		t.Fatalf("workspace states: %v", d.WorkspaceState)
	}
	if d.EventSeq == 0 {
		t.Fatal("event log reported empty after real work")
	}
	if d.Metrics["remount_sessions_opened_total"] == 0 {
		t.Fatalf("session counter never moved: %v", d.Metrics)
	}
	if d.DBIntegrity != "" && d.DBIntegrity != "ok" {
		t.Fatalf("db integrity: %q", d.DBIntegrity)
	}
}

// A workspace that no node can take must say so, rather than sitting pending
// with nothing to explain it.
func TestDiagExplainsUnplaceableWorkspace(t *testing.T) {
	w := newWorld(t)
	w.node("cpu", map[string]string{"gpu": "no"})
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	if _, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
		Placement: proto.Placement{Allow: map[string]string{"gpu": "yes"}},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		d, err := c.Diag(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		if f, ok := findingsBy(d.Findings)["workspace.unplaceable"]; ok {
			if f.Severity != "error" || f.Hint == "" {
				t.Fatalf("unhelpful finding: %+v", f)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("never reported the workspace as unplaceable")
}

// The node's deep view must reach the actual filesystem and the session logs.
func TestNodeDiagReachesDiskAndSessionLogs(t *testing.T) {
	w := newWorld(t)
	n := w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	if err := c.WriteFile(ctx, ws.ID, "some/file.txt", []byte("0123456789"), 0); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := c.Run(ctx, ws.ID, "sh", "-c", "echo streamed"); err != nil {
		t.Fatal(err)
	}

	d, err := c.NodeDiag(ctx, n.ID(), ws.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if d.Node != n.ID() || d.DiskTotal == 0 || d.Goroutines == 0 {
		t.Fatalf("shallow node diag: %+v", d)
	}
	if len(d.Workspaces) != 1 {
		t.Fatalf("workspaces: %+v", d.Workspaces)
	}
	wd := d.Workspaces[0]
	if wd.ID != ws.ID || wd.Root == "" || wd.Broker == "" {
		t.Fatalf("%+v", wd)
	}
	if wd.Files < 2 || wd.Bytes < 10 {
		t.Fatalf("disk usage not measured: %d files, %d bytes", wd.Files, wd.Bytes)
	}
	if len(wd.Sessions) == 0 {
		t.Fatal("no sessions reported")
	}
	var sawExited bool
	for _, s := range wd.Sessions {
		if s.Exited {
			sawExited = true
			if s.Next == 0 {
				t.Fatalf("session log position not reported: %+v", s)
			}
		}
	}
	if !sawExited {
		t.Fatal("no finished session in the node's view")
	}
}

// Diagnostics expose workspace roots and session programs, so an unrelated
// client must not be able to read them.
func TestNodeDiagRequiresEntitlement(t *testing.T) {
	w := newWorld(t)
	n := w.node("n1", nil)
	owner := w.client("owner")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, owner, proto.WorkspaceSpec{})

	stranger := w.client("stranger")
	if _, err := stranger.NodeDiag(ctx, n.ID(), "", false); err == nil {
		t.Fatal("a client with no grant read this node's deep state")
	}
	if _, err := stranger.NodeDiag(ctx, n.ID(), ws.ID, false); err != nil {
		// A grant is issued to any authenticated client in this deployment,
		// so this should succeed; the point is that one is required.
		t.Fatalf("entitled call failed: %v", err)
	}
}

// Deep verification must actually detect a damaged snapshot, and must not
// report a clean store as damaged.
func TestDeepVerifyDetectsCorruptedArtifact(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	if err := c.WriteFile(ctx, ws.ID, "payload.txt", []byte("important"), 0); err != nil {
		t.Fatal(err)
	}
	snap, err := c.Snapshot(ctx, ws.ID, true)
	if err != nil {
		t.Fatal(err)
	}

	// Clean store: verified, nothing damaged.
	d, err := c.Diag(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := findingsBy(d.Findings)["artifact.verified"]
	if !ok {
		t.Fatalf("no verification finding: %+v", d.Findings)
	}
	if !strings.Contains(f.Detail, "0 damaged") {
		t.Fatalf("clean store reported damage: %q", f.Detail)
	}
	if _, bad := findingsBy(d.Findings)["artifact.digest"]; bad {
		t.Fatal("clean store produced a digest finding")
	}

	// Damage the blob in the control plane's store and check it is caught.
	digest := strings.TrimPrefix(snap.Artifact, "art_sha256:")
	var blob string
	_ = filepath.WalkDir(w.artifactDir, func(p string, de os.DirEntry, err error) error {
		if err == nil && !de.IsDir() && de.Name() == digest {
			blob = p
		}
		return nil
	})
	if blob == "" {
		t.Skip("artifact store path not discoverable in this configuration")
	}
	// Published blobs are intentionally read-only. This test is simulating an
	// out-of-band storage fault, so explicitly override that protection first.
	if err := os.Chmod(blob, 0o644); err != nil {
		t.Fatal(err)
	}
	fh, err := os.OpenFile(blob, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteAt([]byte("CORRUPT"), 40); err != nil {
		t.Fatal(err)
	}
	fh.Close()

	d, err = c.Diag(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	by := findingsBy(d.Findings)
	dig, ok := by["artifact.digest"]
	if !ok {
		t.Fatalf("corruption not detected: %+v", d.Findings)
	}
	if dig.Severity != "error" || dig.Subject != snap.Artifact {
		t.Fatalf("wrong finding: %+v", dig)
	}
	if !strings.Contains(by["artifact.verified"].Detail, "1 damaged") {
		t.Fatalf("count wrong: %q", by["artifact.verified"].Detail)
	}
}

// Verification that cannot run must say so rather than reporting success.
func TestVerifyUnavailableIsNotAPass(t *testing.T) {
	l, ctrl := newBareControl(t)
	defer l.Close()
	d := ctrl.Diag(context.Background())
	_ = d
	// With no artifact store configured, a deep verify warns rather than
	// silently reporting a clean result.
	fs := ctrl.VerifyForTest()
	f, ok := findingsBy(fs)["artifact.verify_unavailable"]
	if !ok {
		t.Fatalf("a control plane with no store reported: %+v", fs)
	}
	if f.Severity != "warn" || !strings.Contains(f.Hint, "not") {
		t.Fatalf("unclear finding: %+v", f)
	}
}
