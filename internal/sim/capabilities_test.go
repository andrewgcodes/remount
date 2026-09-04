package sim

import (
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// oldNode starts a node that offers only v1 at hello, as a node built before
// named capabilities existed would. The hello is rewritten on the wire and
// re-signed with the node's own identity key so the proof stays valid.
func (w *world) oldNode(name string) *node.Node {
	w.t.Helper()
	var dir string
	w.mu.Lock()
	w.peerHooks[name] = func(f *proto.Frame) bool {
		if f.T != proto.KindHello {
			return true
		}
		var h proto.Hello
		if err := f.Decode(&h); err != nil {
			w.t.Errorf("old node hook: %v", err)
			return true
		}
		raw, err := os.ReadFile(filepath.Join(dir, "identity.json"))
		if err != nil {
			w.t.Errorf("old node hook: %v", err)
			return true
		}
		var identity struct {
			Priv []byte `json:"priv"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			w.t.Errorf("old node hook: %v", err)
			return true
		}
		h.Caps = []string{proto.CapabilityV1}
		h.Proof = nil
		h.Proof = ed25519.Sign(ed25519.PrivateKey(identity.Priv), proto.HelloProofBytes(h))
		f.Body = proto.MustMarshal(h)
		return true
	}
	w.mu.Unlock()
	return w.nodeWith(name, func(o *node.Options) { dir = o.DataDir })
}

// oldControl makes the server look, to the named peer, like a control plane
// built before named capabilities existed: its hello response echoes v1 only.
func (w *world) oldControl(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.serverHooks[name] = func(f *proto.Frame) bool {
		if f.T != proto.KindRes || f.ID != 1 {
			return true
		}
		var ok proto.HelloOK
		if err := f.Decode(&ok); err != nil || len(ok.Caps) == 0 {
			return true
		}
		ok.Caps = []string{proto.CapabilityV1}
		f.Body = proto.MustMarshal(ok)
		return true
	}
}

func nodeStatus(t *testing.T, statuses []proto.NodeStatus, id string) proto.NodeStatus {
	t.Helper()
	for _, s := range statuses {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("node %s not listed in %+v", id, statuses)
	return proto.NodeStatus{}
}

// Under the local profile the release matrix is symmetric: a current control
// plane accepts an old node and records exactly what it negotiated, and a
// current node keeps working against an old control plane. Refusal under
// isolated and multi_tenant is proved per side in control and node tests,
// where a backend that satisfies those profiles can be faked; here the
// process backend is the only one available.
func TestReleaseMatrixUnderLocalProfile(t *testing.T) {
	w := newWorld(t)
	old := w.oldNode("n_old")
	w.oldControl("n_new")
	fresh := w.node("n_new", nil)
	c := w.client("c1")
	ctx := ctxT(t, 30*time.Second)

	var (
		statuses []proto.NodeStatus
		err      error
	)
	for {
		statuses, err = c.ListNodes(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var oldListed, freshListed bool
		for _, status := range statuses {
			oldListed = oldListed || status.ID == old.ID()
			freshListed = freshListed || status.ID == fresh.ID()
		}
		if oldListed && freshListed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("nodes %s and %s were not both listed: %+v", old.ID(), fresh.ID(), statuses)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if got := nodeStatus(t, statuses, old.ID()).Protocol; !reflect.DeepEqual(got, []string{proto.CapabilityV1}) {
		t.Fatalf("old node negotiated %v", got)
	}
	if got := nodeStatus(t, statuses, fresh.ID()).Protocol; !reflect.DeepEqual(got, proto.PeerCapabilities()) {
		t.Fatalf("current node negotiated %v, want %v", got, proto.PeerCapabilities())
	}
	if got := fresh.Protocol(); !reflect.DeepEqual(got, []string{proto.CapabilityV1}) {
		t.Fatalf("current node saw %v from the old control plane", got)
	}
	if got := old.Protocol(); !reflect.DeepEqual(got, []string{proto.CapabilityV1}) {
		t.Fatalf("old node saw %v", got)
	}

	// New control × old node: a local workspace pinned to the old node runs.
	onOld := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: old.ID()}})
	if out, _, exit, err := c.Run(ctx, onOld.ID, "sh", "-c", "echo old-node-ok"); err != nil || exit.Code != 0 || string(out) != "old-node-ok\n" {
		t.Fatalf("old node run: %q exit=%+v err=%v", out, exit, err)
	}
	// Old control × new node: the node saw only v1 from the server and still
	// serves a local workspace.
	onNew := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: fresh.ID()}})
	if out, _, exit, err := c.Run(ctx, onNew.ID, "sh", "-c", "echo new-node-ok"); err != nil || exit.Code != 0 || string(out) != "new-node-ok\n" {
		t.Fatalf("current node run: %q exit=%+v err=%v", out, exit, err)
	}
	if onOld.Node != old.ID() || onNew.Node != fresh.ID() {
		t.Fatalf("placement: %s on %s, %s on %s", onOld.ID, onOld.Node, onNew.ID, onNew.Node)
	}

	// An isolated workspace pinned to the old node is never claimed by it:
	// the node lacks authz-push, so the control plane holds it pending rather
	// than weakening the profile.
	isolated, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
		Placement: proto.Placement{Node: old.ID()},
		Security:  proto.SecuritySpec{Profile: proto.SecurityIsolated},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	got, err := c.GetWorkspace(ctx, isolated.ID)
	if err != nil || got.State != proto.WSPending {
		t.Fatalf("isolated workspace on old node: state=%s err=%v", got.State, err)
	}
}
