package sim

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// profileTestBackend advertises a chosen capability descriptor over a real
// process tree and answers Reprobe from a script the test can rewrite. It is
// the only way to exercise drift here: gVisor and Firecracker cannot run on
// this host, and their real prerequisites cannot be broken from a test.
type profileTestBackend struct {
	process *workspace.Process
	name    string
	caps    workspace.Caps

	mu     sync.Mutex
	checks []proto.Finding
}

func (b *profileTestBackend) Name() string { return b.name }

func (b *profileTestBackend) Caps() workspace.Caps { return b.caps }

func (b *profileTestBackend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	h, err := b.process.Create(ctx, id, spec, restore)
	if err != nil {
		return nil, err
	}
	return profileTestHandle{Handle: h}, nil
}

func (b *profileTestBackend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	h, err := b.process.Adopt(ctx, id)
	if err != nil {
		return nil, err
	}
	return profileTestHandle{Handle: h}, nil
}

func (b *profileTestBackend) Reprobe(context.Context) []proto.Finding {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]proto.Finding(nil), b.checks...)
}

func (b *profileTestBackend) setChecks(checks ...proto.Finding) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checks = append([]proto.Finding(nil), checks...)
}

type profileTestHandle struct{ workspace.Handle }

func (profileTestHandle) ApplyNetworkPolicy(context.Context, proto.NetworkPolicy, workspace.NetworkEndpoint) error {
	return nil
}
func (profileTestHandle) RevokeNetwork(context.Context) error { return nil }

// isolatedCaps mirrors the gVisor backend's advertised boundary: container
// isolation, sibling isolation, an enforced egress gateway, a private network
// namespace and device isolation.
func isolatedCaps() workspace.Caps {
	return workspace.Caps{
		Isolation: "container", Snapshots: "fs", EgressEnforced: true,
		SiblingIsolation: true, EgressMode: "enforced_gateway",
		BrokerIdentity: "per_session_capability", FilesystemBoundary: "bind_mount",
		NetworkNamespace: true, DeviceIsolation: true,
	}
}

func healthyCheck() proto.Finding {
	return proto.Finding{Severity: "info", Status: proto.CheckPass, Check: "profiletest.network", Detail: "enforced egress is available"}
}

func brokenCheck() proto.Finding {
	return proto.Finding{
		Severity: "error", Status: proto.CheckFail, Check: "profiletest.network",
		Detail: "nftables rules for the sandbox namespace were removed",
		Hint:   "restore the deny-first ruleset",
	}
}

// isolatedNode starts a node whose only backend advertises the isolated
// boundary, claims multi-tenant-isolated, and re-probes fast enough for a
// test to watch drift happen.
func isolatedNode(t *testing.T, w *world, name string) (*node.Node, *profileTestBackend) {
	t.Helper()
	var backend *profileTestBackend
	n := w.nodeWith(name, func(o *node.Options) {
		pb, err := workspace.NewProcess(filepath.Join(o.DataDir, "ws"))
		if err != nil {
			t.Fatal(err)
		}
		backend = &profileTestBackend{process: pb, name: "profiletest", caps: isolatedCaps(),
			checks: []proto.Finding{healthyCheck()}}
		o.Backends = workspace.NewRegistry(backend)
		o.Profile = string(profile.MultiTenantIsolated)
		o.ProfileHealthInterval = 100 * time.Millisecond
	})
	return n, backend
}

// waitProfileStatus polls node.profile.get until the node reports want.
func waitProfileStatus(t *testing.T, ctx context.Context, c *client.Client, nodeID, want string) proto.NodeProfileReport {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last proto.NodeProfileReport
	for time.Now().Before(deadline) {
		res, err := c.NodeProfiles(ctx, nodeID, string(profile.MultiTenantIsolated))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Nodes) != 1 {
			t.Fatalf("node.profile.get for %s returned %d reports", nodeID, len(res.Nodes))
		}
		last = res.Nodes[0]
		if last.Status == want {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s profile status is %q, want %q (checks %+v)", nodeID, last.Status, want, last.Checks)
	return last
}

func waitNodeEvent(t *testing.T, ctx context.Context, c *client.Client, nodeID, typ string) proto.Event {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		events, err := c.ReadEvents(ctx, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if e.Type == typ && e.Node == nodeID {
				return e
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s never emitted %s", nodeID, typ)
	return proto.Event{}
}

// TestProfileSchedulingRequiresEvidence proves that Requires.Profile is
// matched against backend evidence rather than against a label or a node's own
// claim: an isolated node takes the workspace, a process node never does, and
// the pending workspace says why.
func TestProfileSchedulingRequiresEvidence(t *testing.T) {
	w := newWorld(t)
	ctx := ctxT(t, 90*time.Second)
	isolated, _ := isolatedNode(t, w, "pn-isolated")
	c := w.client("pc1")

	ws := mustWS(t, c, proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "profiletest", Profile: string(profile.MultiTenantIsolated)},
	})
	if ws.Node != isolated.ID() {
		t.Fatalf("workspace landed on %s, want the isolated node %s", ws.Node, isolated.ID())
	}

	// A second world with only a process node must never place the same
	// requirement, however it is labelled.
	w2 := newWorld(t)
	ctx2 := ctxT(t, 90*time.Second)
	w2.nodeWith("pn-process", func(o *node.Options) {
		o.Labels = map[string]string{"tier": "production", "isolation": "strong"}
	})
	c2 := w2.client("pc2")
	pending, err := c2.CreateWorkspace(ctx2, proto.WorkspaceSpec{
		Requires: proto.Requires{Profile: string(profile.MultiTenantIsolated)},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(750 * time.Millisecond)
	got, err := c2.GetWorkspace(ctx2, pending.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSPending {
		t.Fatalf("a process node claimed a multi-tenant-isolated workspace: state=%s node=%s", got.State, got.Node)
	}
	if got.PendingReason != proto.ReasonProfileUnschedulable {
		t.Fatalf("pending_reason = %q, want %q", got.PendingReason, proto.ReasonProfileUnschedulable)
	}
	// A dev workspace still runs on that node; the constraint is per profile.
	devWS := mustWS(t, c2, proto.WorkspaceSpec{Requires: proto.Requires{Profile: string(profile.Dev)}})
	if devWS.State != proto.WSClaimed {
		t.Fatalf("dev workspace state %s", devWS.State)
	}
	_ = ctx
}

// TestProfileDriftMakesNodeUnschedulable scripts a host check going bad and
// proves the whole loop: the node re-probes, the control plane records the
// downgrade, the transition is an event, the gauge moves, a new
// profile-requiring workspace stays pending, and a dev workspace still runs on
// the very same node.
func TestProfileDriftMakesNodeUnschedulable(t *testing.T) {
	w := newWorld(t)
	ctx := ctxT(t, 120*time.Second)
	n, backend := isolatedNode(t, w, "pn-drift")
	c := w.client("pc-drift")

	healthy := waitProfileStatus(t, ctx, c, n.ID(), proto.CheckPass)
	if !healthy.Online {
		t.Fatal("a healthy node should report online")
	}
	waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileVerified)

	before := metrics.ProfileDriftDetected.Value()
	backend.setChecks(brokenCheck())

	waitProfileStatus(t, ctx, c, n.ID(), proto.CheckFail)
	ev := waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileUnschedulable)
	var payload proto.NodeProfileEvent
	if err := proto.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Profile != string(profile.MultiTenantIsolated) || len(payload.Failed) == 0 {
		t.Fatalf("unschedulable payload %+v names no failing check", payload)
	}
	if metrics.ProfileDriftDetected.Value() <= before {
		t.Fatal("drift counter did not move")
	}
	if got := metrics.NodesProfileUnschedulable.Value(); got < 1 {
		t.Fatalf("unschedulable gauge = %d, want at least 1", got)
	}

	drifted, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "profiletest", Profile: string(profile.MultiTenantIsolated)},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(750 * time.Millisecond)
	got, err := c.GetWorkspace(ctx, drifted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSPending {
		t.Fatalf("a drifted node claimed a multi-tenant-isolated workspace: state=%s node=%s", got.State, got.Node)
	}
	if got.PendingReason != proto.ReasonProfileUnschedulable {
		t.Fatalf("pending_reason = %q, want %q", got.PendingReason, proto.ReasonProfileUnschedulable)
	}

	// Drift removes only what the profile promised. Work that asks for
	// nothing still runs on the same machine.
	devWS := mustWS(t, c, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "profiletest"}})
	if devWS.Node != n.ID() {
		t.Fatalf("dev workspace landed on %s, want %s", devWS.Node, n.ID())
	}

	// Recovery is observable too, and the parked workspace is placed.
	backend.setChecks(healthyCheck())
	waitProfileStatus(t, ctx, c, n.ID(), proto.CheckPass)
	waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileRestored)
	if _, err := c.WaitClaimed(ctx, drifted.ID); err != nil {
		t.Fatalf("the parked workspace was not placed after recovery: %v", err)
	}
}

// TestNodeProfileGetReportsEveryNode covers the fleet-wide form of the
// operation and the rule that an offline node is never reported as passing.
func TestNodeProfileGetReportsEveryNode(t *testing.T) {
	w := newWorldExpiring(t)
	ctx := ctxT(t, 90*time.Second)
	isolated, _ := isolatedNode(t, w, "pn-a")
	plain := w.node("pn-b", nil)
	c := w.client("pc-list")

	waitProfileStatus(t, ctx, c, isolated.ID(), proto.CheckPass)

	res, err := c.NodeProfiles(ctx, "", string(profile.MultiTenantIsolated))
	if err != nil {
		t.Fatal(err)
	}
	byNode := map[string]proto.NodeProfileReport{}
	for _, r := range res.Nodes {
		byNode[r.Node] = r
	}
	if got := byNode[isolated.ID()].Status; got != proto.CheckPass {
		t.Fatalf("isolated node status %q, want pass", got)
	}
	if got := byNode[plain.ID()].Status; got != proto.CheckFail {
		t.Fatalf("process node status %q, want fail", got)
	}
	if byNode[isolated.ID()].Configured != string(profile.MultiTenantIsolated) {
		t.Fatalf("configured profile %q", byNode[isolated.ID()].Configured)
	}
	for _, check := range byNode[plain.ID()].Checks {
		if check.Status == "" {
			t.Fatalf("check %q has no status", check.Check)
		}
	}

	// An offline node cannot re-prove anything, so its report is unavailable
	// rather than a repeat of its last pass.
	w.stopNode("pn-a")
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, err := c.NodeProfiles(ctx, isolated.ID(), string(profile.MultiTenantIsolated))
		if err != nil {
			t.Fatal(err)
		}
		if res.Nodes[0].Status == proto.CheckUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("offline node still reports %q", res.Nodes[0].Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
