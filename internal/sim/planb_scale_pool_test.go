package sim

// Plan B B30 §15.4 item "provider pool bursts and drains", judged by §15.5's
// goroutine, memory, descriptor and retained-state ceilings.
//
// Two separate questions live here.
//
// The first is the resource slope: a burst creates provider inventory, each
// machine becomes a real node with an uplink, leases, a renew loop, an
// artifact store and GC timers, and a drain destroys all of it. Anything the
// control plane retains per assignment, per node identity or per machine shows
// up as a floor that rises one cycle at a time, so the same cycle is run four
// times and only the later ones are compared.
//
// The second is whether the drain finishes at all. Idle provider inventory is
// the one retained resource in this system that costs money while nobody is
// using it, so "the drain converges" is a ceiling in its own right, and it is
// tested at a width greater than one pool because that is where it stops
// holding.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/server"
)

// planbScalePool is a world wired to a fake provider, plus the bookkeeping a
// burst-and-drain test needs.
type planbScalePool struct {
	world    *world
	provider *simProvisioner
	client   *client.Client
}

// newPlanbScalePool starts a world whose pools are backed by an in-process
// provider that turns every machine into a real node.
func newPlanbScalePool(t *testing.T, adjust func(*server.Options)) *planbScalePool {
	t.Helper()
	enrollment := newSimEnrollment()
	provider := &simProvisioner{machines: map[string]provision.Machine{}, cancels: map[string]context.CancelFunc{}}
	w := newWorldWith(t, func(o *server.Options) {
		o.NodeAuthenticator = enrollment
		o.PoolEnrollmentSource = enrollment
		o.ProvisionDrivers = []provision.Driver{provider}
		o.PoolBootstrap = control.PoolBootstrap{ServerURL: "http://control.invalid", BinaryURL: "http://control.invalid/remount", DataDir: "/var/lib/remount"}
		o.PoolOptions = pool.Options{MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, PendingTimeout: 10 * time.Second}
		if adjust != nil {
			adjust(o)
		}
	})
	root := t.TempDir()
	provider.startNode = func(request provision.Request) (context.CancelFunc, error) {
		dataDir := filepath.Join(root, request.Bootstrap.NodeID)
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			return nil, err
		}
		n, err := node.New(node.Options{
			DataDir: dataDir, ID: request.Bootstrap.NodeID,
			Dialer: w.dialer(request.Bootstrap.NodeID), Token: request.Bootstrap.EnrollmentToken,
			ArtifactURL: w.http.URL + "/v1/artifacts",
			Allow:       []string{"127.0.0.1"}, AllowPrivate: []string{"127.0.0.1", "localhost"},
			MaxSessions: 8, MaxActiveSessions: 4,
		})
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(w.ctx)
		go n.Run(ctx)
		select {
		case <-n.Online():
			return cancel, nil
		case <-time.After(30 * time.Second):
			cancel()
			return nil, errors.New("provisioned node did not enroll")
		}
	}
	return &planbScalePool{world: w, provider: provider, client: w.client("pool-operator")}
}

// TestPlanBScalePoolBurstAndDrainCyclesStayBounded repeats one pool's
// scale-up and scale-down and requires the resources held after each drain to
// stop growing.
func TestPlanBScalePoolBurstAndDrainCyclesStayBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("provider pool burst and drain scale evidence is not a short test")
	}
	cycles := planbScaleSize(t, 4, 2)

	p := newPlanbScalePool(t, func(o *server.Options) {
		o.MaxWorkspacesPerTenant = cycles + 2
		o.MaxWorkspacesPerSubject = cycles + 2
	})
	ctx := ctxT(t, 15*time.Minute)
	if _, err := p.client.CreatePool(ctx, proto.PoolSpec{Name: "iad-z00", Vendor: "fake", Min: 0, Max: 1,
		Backend: "process", Labels: map[string]string{"zone": "z00"}, IdleScaleDownMilli: 50}); err != nil {
		t.Fatal(err)
	}

	baseline := planbScaleFloor(5 * time.Second)
	t.Logf("baseline before the first burst: %s", baseline)

	var settled []planbScaleSample
	for cycle := 1; cycle <= cycles; cycle++ {
		before := planbScaleMetrics()
		ws, err := p.client.CreateWorkspace(ctx, proto.WorkspaceSpec{
			Name:      fmt.Sprintf("burst-%d", cycle),
			Requires:  proto.Requires{Backend: "process"},
			Placement: proto.Placement{Allow: map[string]string{"zone": "z00"}},
		})
		if err != nil {
			t.Fatalf("cycle %d create: %v", cycle, err)
		}
		claimed, err := p.client.WaitClaimed(ctx, ws.ID)
		if err != nil || claimed.Node == "" {
			t.Fatalf("cycle %d never claimed: %+v %v", cycle, claimed, err)
		}
		if machines := planbScaleInventory(t, ctx, p.provider, []string{"z00"}); len(machines) != 1 {
			t.Fatalf("cycle %d provisioned %d machines for a Max=1 pool", cycle, len(machines))
		}

		if err := p.client.DestroyWorkspace(ctx, ws.ID); err != nil {
			t.Fatalf("cycle %d destroy: %v", cycle, err)
		}
		drained := planbScaleAwaitEmptyPool(t, ctx, p, []string{"z00"}, 120*time.Second)
		if state, err := p.client.GetPool(ctx, "iad-z00"); err != nil || state.Current != 0 {
			t.Fatalf("cycle %d durable pool after drain = %+v, %v", cycle, state, err)
		}

		floor := planbScaleFloor(20 * time.Second)
		after := planbScaleMetrics()
		t.Logf("cycle %d: drained in %s; %s scale_actions=+%g provision_failures=+%g pool_machines=%g",
			cycle, drained.Round(time.Millisecond), floor,
			planbScaleDelta(before, after, "remount_pool_scale_actions_total"),
			planbScaleDelta(before, after, "remount_pool_provision_failures_total"),
			after["remount_pool_machines"])

		// A create and a destroy are both scale actions. A drain that
		// destroyed provider inventory without counting it would be an
		// unobserved irreversible provider mutation.
		if actions := planbScaleDelta(before, after, "remount_pool_scale_actions_total"); actions != 2 {
			t.Errorf("cycle %d moved remount_pool_scale_actions_total by %g, want 2 (one create, one destroy)", cycle, actions)
		}
		if failures := planbScaleDelta(before, after, "remount_pool_provision_failures_total"); failures != 0 {
			t.Errorf("cycle %d saw %g provider failures in a healthy burst and drain", cycle, failures)
		}
		if gauge := after["remount_pool_machines"]; gauge != 0 {
			t.Errorf("cycle %d left remount_pool_machines at %g after a full drain", cycle, gauge)
		}
		settled = append(settled, floor)
	}

	// Cycle one pays one-off costs the rest do not.
	first, last := settled[1], settled[len(settled)-1]
	if last.Goroutines > first.Goroutines+16 {
		t.Errorf("goroutines grew across identical burst/drain cycles: %d then %d", first.Goroutines, last.Goroutines)
	}
	if growth := planbScaleGrowth(first.HeapInUse, last.HeapInUse); growth > 0.30 {
		t.Errorf("heap in use grew %.0f%% across identical burst/drain cycles: %s then %s",
			growth*100, planbScaleBytes(first.HeapInUse), planbScaleBytes(last.HeapInUse))
	}
	if first.Descriptor > 0 && last.Descriptor > first.Descriptor+16 {
		t.Errorf("descriptors grew across identical burst/drain cycles: %d then %d", first.Descriptor, last.Descriptor)
	}
}

// TestPlanBScalePoolWideBurstDrainsEveryIdleMachine is the ceiling on retained
// provider inventory. A fleet with more than one pool bursts to one machine
// per pool and, once every workspace is gone, every machine is idle and must
// be destroyed. Inventory that stays up after its pool has no work is
// unbounded retained state that costs money for as long as it survives.
func TestPlanBScalePoolWideBurstDrainsEveryIdleMachine(t *testing.T) {
	if testing.Short() {
		t.Skip("wide provider pool burst and drain evidence is not a short test")
	}
	pools := planbScaleSize(t, 6, 2)
	const idleAfter = 50 * time.Millisecond
	// The control plane reconciles every pool once a second, so a machine that
	// has been idle for 50ms should be destroyed within a second or two. This
	// budget is two orders of magnitude more generous than that.
	const budget = 60 * time.Second

	p := newPlanbScalePool(t, func(o *server.Options) {
		o.MaxWorkspacesPerTenant = pools + 2
		o.MaxWorkspacesPerSubject = pools + 2
	})
	ctx := ctxT(t, 15*time.Minute)

	// One pool per zone. A single pool answers a wide burst with one machine:
	// the reconciler adds at most one machine per pass, and a pending
	// workspace stops counting as demand the moment any online node can take
	// it. A zone per workspace is what makes the burst actually wide.
	zones := make([]string, 0, pools)
	for i := 0; i < pools; i++ {
		zone := fmt.Sprintf("z%02d", i)
		zones = append(zones, zone)
		if _, err := p.client.CreatePool(ctx, proto.PoolSpec{Name: "iad-" + zone, Vendor: "fake", Min: 0, Max: 1,
			Backend: "process", Labels: map[string]string{"zone": zone}, IdleScaleDownMilli: int64(idleAfter / time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
	}

	ids := make([]string, 0, pools)
	for i := 0; i < pools; i++ {
		ws, err := p.client.CreateWorkspace(ctx, proto.WorkspaceSpec{
			Name:      fmt.Sprintf("wide-%d", i),
			Requires:  proto.Requires{Backend: "process"},
			Placement: proto.Placement{Allow: map[string]string{"zone": zones[i]}},
		})
		if err != nil {
			t.Fatalf("burst create %d: %v", i, err)
		}
		ids = append(ids, ws.ID)
	}
	nodes := map[string]struct{}{}
	for i, id := range ids {
		claimed, err := p.client.WaitClaimed(ctx, id)
		if err != nil || claimed.Node == "" {
			t.Fatalf("burst workspace %d never claimed: %+v %v", i, claimed, err)
		}
		nodes[claimed.Node] = struct{}{}
	}
	machines := planbScaleInventory(t, ctx, p.provider, zones)
	t.Logf("wide burst: %d workspaces across %d nodes and %d provider machines", len(ids), len(nodes), len(machines))
	if len(machines) != pools {
		t.Fatalf("wide burst provisioned %d machines for %d pools", len(machines), pools)
	}

	for i, id := range ids {
		if err := p.client.DestroyWorkspace(ctx, id); err != nil {
			t.Fatalf("drain destroy %d: %v", i, err)
		}
	}
	drained := planbScaleAwaitEmptyPool(t, ctx, p, zones, budget)
	t.Logf("wide drain of %d idle machines took %s", pools, drained.Round(time.Millisecond))
}

// planbScaleInventory is the provider's own answer for every named zone pool.
func planbScaleInventory(t *testing.T, ctx context.Context, provider *simProvisioner, zones []string) []provision.Machine {
	t.Helper()
	var out []provision.Machine
	for _, zone := range zones {
		machines, err := provider.List(ctx, provision.ListOptions{Tenant: "local", Pool: "iad-" + zone})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, machines...)
	}
	return out
}

// planbScaleAwaitEmptyPool waits until the provider reports no inventory for
// any of the named pools and returns how long that took. The provider is the
// authority: a durable row saying zero while a machine still runs would be the
// expensive kind of wrong.
func planbScaleAwaitEmptyPool(t *testing.T, ctx context.Context, p *planbScalePool, zones []string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	report := start.Add(10 * time.Second)
	for {
		machines := planbScaleInventory(t, ctx, p.provider, zones)
		if len(machines) == 0 {
			return time.Since(start)
		}
		now := time.Now()
		if now.After(report) {
			report = now.Add(10 * time.Second)
			t.Logf("drain after %s: %d of %d machines still provisioned: %s",
				now.Sub(start).Round(time.Second), len(machines), len(zones), planbScalePoolStatus(t, ctx, p, zones))
		}
		if now.After(deadline) {
			t.Fatalf("%d of %d idle provider machines were not destroyed within %s. Every one of them is "+
				"online, holds no workspace and is past its pool's idle threshold, so each is retained provider "+
				"inventory that no ceiling will ever release: %s",
				len(machines), len(zones), within, planbScalePoolStatus(t, ctx, p, zones))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// planbScalePoolStatus renders, for each pool that still owns inventory, the
// machine, its durable count and what the control plane believes about the
// node behind it. A stalled drain is either an unobserved machine, a node the
// control plane no longer recognizes, or a durable count that disagrees with
// the provider, and this says which.
func planbScalePoolStatus(t *testing.T, ctx context.Context, p *planbScalePool, zones []string) string {
	t.Helper()
	nodes, err := p.client.ListNodes(ctx)
	if err != nil {
		return "node list unavailable: " + err.Error()
	}
	online := map[string]proto.NodeStatus{}
	for _, n := range nodes {
		online[n.ID] = n
	}
	var b strings.Builder
	for _, zone := range zones {
		machines, err := p.provider.List(ctx, provision.ListOptions{Tenant: "local", Pool: "iad-" + zone})
		if err != nil {
			t.Fatal(err)
		}
		if len(machines) == 0 {
			continue
		}
		durable := -1
		if state, err := p.client.GetPool(ctx, "iad-"+zone); err == nil {
			durable = state.Current
		}
		for _, machine := range machines {
			nodeID := machine.Labels[provision.NodeLabel]
			status, known := online[nodeID]
			fmt.Fprintf(&b, "[%s durable=%d node=%s known=%v online=%v workspaces=%d] ",
				zone, durable, nodeID, known, status.Online, len(status.Workspaces))
		}
	}
	return b.String()
}
