package control

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
)

// retireDriver is a provider whose only inventory is one machine. Destroy can
// be parked, made to fail, or made to time out, and it reports what control
// state looked like at the moment the irreversible call was made.
type retireDriver struct {
	mu         sync.Mutex
	machine    provision.Machine
	destroyed  []string
	destroyErr error
	entered    chan struct{}
	release    chan struct{}
	onDestroy  func(id string)
	gone       bool
}

func (d *retireDriver) Name() string { return "audit" }

func (d *retireDriver) Create(context.Context, provision.Request) (provision.Machine, error) {
	return provision.Machine{}, errors.New("audit provider never creates")
}

func (d *retireDriver) Destroy(_ context.Context, id string) error {
	if d.entered != nil {
		select {
		case d.entered <- struct{}{}:
		default:
		}
	}
	if d.release != nil {
		<-d.release
	}
	if d.onDestroy != nil {
		d.onDestroy(id)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.destroyed = append(d.destroyed, id)
	if d.destroyErr != nil {
		return d.destroyErr
	}
	d.gone = true
	return nil
}

func (d *retireDriver) List(context.Context, provision.ListOptions) ([]provision.Machine, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.gone {
		return nil, nil
	}
	return []provision.Machine{provision.CloneMachine(d.machine)}, nil
}

func (d *retireDriver) destroys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.destroyed...)
}

type retireTokens struct{}

func (retireTokens) Issue(context.Context, string, string, time.Duration) (string, error) {
	return "", errors.New("audit provider never enrolls")
}

// parkedReconciler hands the enriched inventory snapshot to the test and then
// waits inside Reconcile, before the real reconciler has selected a victim.
// That is exactly the window RMR-001 describes: the snapshot said idle, and
// the world moves on while the reconciler still believes it.
type parkedReconciler struct {
	*nodepool.Reconciler
	entered chan []nodepool.Node
	release chan struct{}
}

func (p *parkedReconciler) Reconcile(ctx context.Context, spec nodepool.Spec, nodes []nodepool.Node, demand int) ([]nodepool.Action, error) {
	select {
	case p.entered <- append([]nodepool.Node(nil), nodes...):
	default:
	}
	<-p.release
	return p.Reconciler.Reconcile(ctx, spec, nodes, demand)
}

const retirePoolKey = "tenant-a\x00audit"

type retireFixture struct {
	f       *controlFixture
	driver  *retireDriver
	parked  *parkedReconciler
	nodeKey ed25519.PrivateKey
}

func retireMachine() provision.Machine {
	return provision.Machine{
		ID: "machine-audit", Provider: "audit", Tenant: "tenant-a",
		Labels: map[string]string{provision.PoolLabel: "audit", provision.NodeLabel: "n_audit"},
	}
}

// newRetireFixture builds a control plane with one pool whose only machine is
// an online, assignment-free node that has been idle for an hour.
func newRetireFixture(t *testing.T, path string, driver *retireDriver, poolOpts nodepool.Options) *retireFixture {
	t.Helper()
	return newRetireFixtureWithKey(t, path, driver, poolOpts, nil)
}

// newRetireFixtureWithKey reopens path with the node key a prior fixture
// enrolled, so the same node reconnects to a restarted controller.
func newRetireFixtureWithKey(t *testing.T, path string, driver *retireDriver, poolOpts nodepool.Options, nodeKey ed25519.PrivateKey) *retireFixture {
	t.Helper()
	r, err := nodepool.New([]provision.Driver{driver}, retireTokens{}, poolOpts)
	if err != nil {
		t.Fatal(err)
	}
	parked := &parkedReconciler{Reconciler: r, entered: make(chan []nodepool.Node, 1), release: make(chan struct{})}
	f := newControlFixture(t, path, func(opts *Options) {
		opts.PoolReconciler = parked
		opts.PoolBootstrap = PoolBootstrap{ServerURL: "https://control.example", BinaryURL: "https://control.example/remount", DataDir: "/var/lib/remount"}
	})
	nodeKey = connectNodeWithKey(t, f.c, "n_audit", processNodeInfo(4096), nodeKey)
	f.c.mu.Lock()
	f.c.nodes["n_audit"].Status.Labels = map[string]string{provision.PoolLabel: "audit", "tenant": "tenant-a"}
	f.c.mu.Unlock()
	if _, err := f.c.poolGet(context.Background(), localSubject(), "audit"); err != nil {
		if _, err := f.c.poolCreate(context.Background(), localSubject(), &proto.PoolCreateReq{
			Spec:           proto.PoolSpec{Name: "audit", Vendor: "audit", Max: 1, Backend: "process", IdleScaleDownMilli: 1},
			IdempotencyKey: "audit-pool",
		}); err != nil {
			t.Fatal(err)
		}
	}
	f.c.mu.Lock()
	f.c.poolIdle[poolMachine{pool: retirePoolKey, machine: "machine-audit"}] = time.Now().Add(-time.Hour)
	f.c.mu.Unlock()
	return &retireFixture{f: f, driver: driver, parked: parked, nodeKey: nodeKey}
}

// reconcileParked starts one reconcile and waits until it is parked inside
// Reconcile with an idle snapshot. It fails the test if the snapshot is not
// the single idle node the fixture promised.
func (rf *retireFixture) reconcileParked(t *testing.T) {
	t.Helper()
	rf.f.c.reconcilePoolsAsync()
	select {
	case nodes := <-rf.parked.entered:
		if len(nodes) != 1 || nodes[0].Workspaces != 0 || nodes[0].IdleSince.IsZero() {
			close(rf.parked.release)
			rf.f.c.poolWG.Wait()
			t.Fatalf("snapshot is not one idle node: %+v", nodes)
		}
	case <-time.After(10 * time.Second):
		close(rf.parked.release)
		t.Fatal("reconcile never reached the pool reconciler")
	}
}

func (rf *retireFixture) claimAndReady(t *testing.T) error {
	t.Helper()
	created := createWorkspace(t, rf.f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := rf.f.c.wsClaim(context.Background(), "n_audit", created.ID)
	if err != nil {
		return err
	}
	return rf.f.c.wsReady(context.Background(), "n_audit", &proto.WSReadyReq{ID: created.ID, Gen: claim.Workspace.Generation})
}

func (rf *retireFixture) fenced(t *testing.T) (poolRetirement, bool) {
	t.Helper()
	rf.f.c.mu.Lock()
	defer rf.f.c.mu.Unlock()
	fence, ok := rf.f.c.poolRetiring["n_audit"]
	return fence, ok
}

func (rf *retireFixture) durableFences(t *testing.T) int {
	t.Helper()
	var n int
	if err := rf.f.sq.DB().QueryRow(`SELECT COUNT(*) FROM pool_retirements WHERE node='n_audit'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func poolEvents(t *testing.T, f *controlFixture, typ string) []proto.Event {
	t.Helper()
	events, err := f.log.Read(context.Background(), 1, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []proto.Event
	for _, event := range events {
		if event.Type == typ && event.Stream == "audit" {
			out = append(out, event)
		}
	}
	return out
}

// TestPoolScaleDownNeverDestroysANodeClaimedAfterTheIdleSnapshot is RMR-001.
// The reconciler is parked after it received an idle snapshot but before it
// picked a victim. A workspace then claims and becomes ready on that node.
// When the reconciler resumes it must not destroy the machine: the snapshot
// is an observation, and the only authority that may make a node destroyable
// is a fence committed against current assignment state.
func TestPoolScaleDownNeverDestroysANodeClaimedAfterTheIdleSnapshot(t *testing.T) {
	driver := &retireDriver{machine: retireMachine()}
	rf := newRetireFixture(t, "", driver, nodepool.Options{})
	activeAtDestroy := make(chan bool, 1)
	driver.onDestroy = func(string) {
		rf.f.c.mu.Lock()
		active := false
		for _, ws := range rf.f.c.workspaces {
			if ws.Node == "n_audit" && held(ws.State) {
				active = true
			}
		}
		rf.f.c.mu.Unlock()
		activeAtDestroy <- active
	}

	rf.reconcileParked(t)
	if err := rf.claimAndReady(t); err != nil {
		close(rf.parked.release)
		rf.f.c.poolWG.Wait()
		t.Fatalf("claim on an unfenced idle node must succeed: %v", err)
	}
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()

	select {
	case active := <-activeAtDestroy:
		if active {
			t.Fatal("provider destroyed a node after a new workspace claim and ws.ready both committed")
		}
		t.Fatal("provider destroyed a node the control plane no longer considered idle")
	default:
	}
	if destroys := driver.destroys(); len(destroys) != 0 {
		t.Fatalf("destroys = %v, want none", destroys)
	}
	if _, ok := rf.fenced(t); ok {
		t.Fatal("a lost retirement race left a fence behind")
	}
	if n := rf.durableFences(t); n != 0 {
		t.Fatalf("durable fences = %d, want 0", n)
	}
	if scaled := poolScaledReasons(t, rf.f); len(scaled) != 1 || scaled[0] != "inventory_reconciled" {
		t.Fatalf("pool.scaled reasons = %v; a destroy that must not happen was recorded", scaled)
	}
	got, err := rf.f.c.poolGet(context.Background(), localSubject(), "audit")
	if err != nil || got.Current != 1 {
		t.Fatalf("pool current = %+v, %v; want the machine still counted", got, err)
	}
}

func poolScaledReasons(t *testing.T, f *controlFixture) []string {
	t.Helper()
	var out []string
	for _, event := range poolEvents(t, f, proto.EvPoolScaled) {
		reason, _ := payloadOf(t, event)["reason"].(string)
		out = append(out, reason)
	}
	return out
}

// TestPoolRetirementFenceBlocksClaimsAcrossTheProviderDestroy parks inside
// the provider destroy itself. By then the fence is durable and in memory:
// a claim on the node is denied, ordinary control-plane work still completes
// (no mutex is held across the provider call), and after the destroy the
// fence stays until inventory confirms the machine is gone.
func TestPoolRetirementFenceBlocksClaimsAcrossTheProviderDestroy(t *testing.T) {
	driver := &retireDriver{machine: retireMachine(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	rf := newRetireFixture(t, "", driver, nodepool.Options{})
	rf.reconcileParked(t)
	close(rf.parked.release)
	select {
	case <-driver.entered:
	case <-time.After(10 * time.Second):
		close(driver.release)
		t.Fatal("reconcile never reached the provider destroy")
	}

	if fence, ok := rf.fenced(t); !ok || fence.Pool != retirePoolKey || fence.Machine != "machine-audit" {
		close(driver.release)
		t.Fatalf("fence before destroy = %+v, %v", fence, ok)
	}
	if n := rf.durableFences(t); n != 1 {
		close(driver.release)
		t.Fatalf("durable fences before destroy = %d, want 1", n)
	}
	if retiring := poolEvents(t, rf.f, proto.EvPoolRetiring); len(retiring) != 1 {
		close(driver.release)
		t.Fatalf("pool.retiring events = %+v, want exactly one", retiring)
	}

	// The claim must be refused while the destroy is in flight, and the
	// refusal must come back promptly: c.mu is not held across the provider.
	done := make(chan error, 1)
	go func() {
		created := createWorkspace(t, rf.f.c, localSubject(), proto.WorkspaceSpec{})
		_, err := rf.f.c.wsClaim(context.Background(), "n_audit", created.ID)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			close(driver.release)
			t.Fatal("a fenced node accepted a new claim while its destroy was in flight")
		}
		if codeOf(err) != proto.CodeDenied {
			close(driver.release)
			t.Fatalf("claim error = %v, want %s", err, proto.CodeDenied)
		}
	case <-time.After(20 * time.Second):
		close(driver.release)
		t.Fatal("control authority was held across the provider destroy")
	}
	if pending := createWorkspace(t, rf.f.c, localSubject(), proto.WorkspaceSpec{}); pending.State != proto.WSPending {
		close(driver.release)
		t.Fatalf("workspace created during destroy = %s, want pending", pending.State)
	}

	close(driver.release)
	rf.f.c.poolWG.Wait()
	if destroys := driver.destroys(); len(destroys) != 1 || destroys[0] != "machine-audit" {
		t.Fatalf("destroys = %v, want machine-audit", destroys)
	}
	if _, ok := rf.fenced(t); !ok {
		t.Fatal("fence lifted before inventory confirmed the machine was gone")
	}
	if scaled := poolEvents(t, rf.f, proto.EvPoolScaled); len(scaled) != 1 {
		t.Fatalf("pool.scaled events = %+v, want exactly one", scaled)
	}
	got, err := rf.f.c.poolGet(context.Background(), localSubject(), "audit")
	if err != nil || got.Current != 0 {
		t.Fatalf("pool current after destroy = %+v, %v", got, err)
	}

	// The pool counts zero machines, but the node is still connected and its
	// fence unresolved. Removing the pool now would remove the only
	// reconciler able to resolve it, so removal is refused and the node stays
	// unclaimable.
	removeErr := rf.f.c.poolRemove(context.Background(), localSubject(), &proto.PoolRemoveReq{Name: "audit", IdempotencyKey: "remove-early"})
	if codeOf(removeErr) != proto.CodeConflict {
		t.Fatalf("pool remove with a retirement in flight = %v, want %s", removeErr, proto.CodeConflict)
	}
	if _, ok := rf.fenced(t); !ok {
		t.Fatal("refused pool removal lifted the fence")
	}
	if err := rf.claimAndReady(t); err == nil || codeOf(err) != proto.CodeDenied {
		t.Fatalf("claim after refused pool removal = %v, want %s", err, proto.CodeDenied)
	}

	// The next inventory no longer lists the machine, which releases the
	// fence durably and in memory, with one pool.retired event.
	rf.parked.release = make(chan struct{})
	rf.f.c.reconcilePoolsAsync()
	select {
	case <-rf.parked.entered:
	case <-time.After(10 * time.Second):
		close(rf.parked.release)
		t.Fatal("second reconcile never ran")
	}
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()
	if _, ok := rf.fenced(t); ok {
		t.Fatal("fence survived the machine's disappearance from inventory")
	}
	if n := rf.durableFences(t); n != 0 {
		t.Fatalf("durable fences after prune = %d, want 0", n)
	}
	if retired := poolEvents(t, rf.f, proto.EvPoolRetired); len(retired) != 1 {
		t.Fatalf("pool.retired events = %d, want exactly one", len(retired))
	}
	if aborted := poolEvents(t, rf.f, proto.EvPoolRetireAborted); len(aborted) != 0 {
		t.Fatalf("pool.retire_aborted events = %d after a successful destroy, want none", len(aborted))
	}
	if err := rf.f.c.poolRemove(context.Background(), localSubject(), &proto.PoolRemoveReq{Name: "audit", IdempotencyKey: "remove-late"}); err != nil {
		t.Fatalf("pool remove after the fence resolved = %v", err)
	}
}

// TestPoolRetirementFenceIsReleasedWhenTheProviderDefinitelyFails: a provider
// that returns a definite error has not destroyed anything, so the node goes
// back into service and a claim on it succeeds.
func TestPoolRetirementFenceIsReleasedWhenTheProviderDefinitelyFails(t *testing.T) {
	driver := &retireDriver{machine: retireMachine(), destroyErr: errors.New("provider: quota exceeded")}
	rf := newRetireFixture(t, "", driver, nodepool.Options{})
	rf.reconcileParked(t)
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()

	if destroys := driver.destroys(); len(destroys) != 1 {
		t.Fatalf("destroys = %v, want one attempt", destroys)
	}
	if fence, ok := rf.fenced(t); ok {
		t.Fatalf("fence survived a definite provider failure: %+v", fence)
	}
	if n := rf.durableFences(t); n != 0 {
		t.Fatalf("durable fences after failure = %d, want 0", n)
	}
	if aborted := poolEvents(t, rf.f, proto.EvPoolRetireAborted); len(aborted) != 1 {
		t.Fatalf("pool.retire_aborted events = %+v, want exactly one", aborted)
	}
	if failed := poolEvents(t, rf.f, proto.EvPoolFailed); len(failed) != 1 {
		t.Fatalf("pool.provision_failed events = %+v, want exactly one", failed)
	}
	if err := rf.claimAndReady(t); err != nil {
		t.Fatalf("claim after unfence = %v, want success", err)
	}
	if scaled := poolScaledReasons(t, rf.f); len(scaled) != 0 {
		t.Fatalf("pool.scaled reasons = %v after a failed destroy, want none", scaled)
	}
}

// TestPoolRetirementFenceSurvivesAnAmbiguousProviderResult: a timeout is not
// evidence the machine survived. The fence stays, so no claim can land on a
// machine that may already be half destroyed, and the retry converges.
func TestPoolRetirementFenceSurvivesAnAmbiguousProviderResult(t *testing.T) {
	driver := &retireDriver{machine: retireMachine(), destroyErr: errors.New("provider: " + context.DeadlineExceeded.Error())}
	driver.destroyErr = errors.Join(driver.destroyErr, context.DeadlineExceeded)
	rf := newRetireFixture(t, "", driver, nodepool.Options{MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	rf.reconcileParked(t)
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()

	if _, ok := rf.fenced(t); !ok {
		t.Fatal("fence lifted after an ambiguous provider result")
	}
	if n := rf.durableFences(t); n != 1 {
		t.Fatalf("durable fences after ambiguous failure = %d, want 1", n)
	}
	if err := rf.claimAndReady(t); err == nil || codeOf(err) != proto.CodeDenied {
		t.Fatalf("claim on a fenced node = %v, want %s", err, proto.CodeDenied)
	}

	// The provider recovers; the retry re-fences idempotently and destroys.
	driver.mu.Lock()
	driver.destroyErr = nil
	driver.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	rf.parked.release = make(chan struct{})
	rf.reconcileParked(t)
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()
	if destroys := driver.destroys(); len(destroys) != 2 {
		t.Fatalf("destroys = %v, want a retry", destroys)
	}
	if retiring := poolEvents(t, rf.f, proto.EvPoolRetiring); len(retiring) != 1 {
		t.Fatalf("pool.retiring events = %d, want one: the retry re-used the fence", len(retiring))
	}
	if scaled := poolEvents(t, rf.f, proto.EvPoolScaled); len(scaled) != 1 {
		t.Fatalf("pool.scaled events = %+v, want exactly one", scaled)
	}
}

// TestPoolRetirementFenceSurvivesControllerRestart: the fence is a durable
// row, so a controller that restarts while a destroy is in flight still
// refuses claims on the node until inventory proves the machine is gone.
func TestPoolRetirementFenceSurvivesControllerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	driver := &retireDriver{machine: retireMachine(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	rf := newRetireFixture(t, path, driver, nodepool.Options{})
	rf.reconcileParked(t)
	close(rf.parked.release)
	select {
	case <-driver.entered:
	case <-time.After(10 * time.Second):
		close(driver.release)
		t.Fatal("reconcile never reached the provider destroy")
	}
	// The destroy completes at the provider, but this controller dies before
	// observing it: release the driver and stop.
	close(driver.release)
	rf.f.c.poolWG.Wait()
	rf.f.c.Stop()
	if err := rf.f.log.Close(); err != nil {
		t.Fatal(err)
	}

	// The provider still lists the machine (an eventually consistent
	// inventory), and the node is still connected.
	driver.mu.Lock()
	driver.gone = false
	driver.mu.Unlock()
	restarted := newRetireFixtureWithKey(t, path, driver, nodepool.Options{}, rf.nodeKey)
	if fence, ok := restarted.fenced(t); !ok || fence.Pool != retirePoolKey {
		t.Fatalf("fence after restart = %+v, %v", fence, ok)
	}
	if err := restarted.claimAndReady(t); err == nil || codeOf(err) != proto.CodeDenied {
		t.Fatalf("claim on a fenced node after restart = %v, want %s", err, proto.CodeDenied)
	}

	// Inventory finally drops the machine; the fence is released.
	driver.mu.Lock()
	driver.gone = true
	driver.mu.Unlock()
	restarted.f.c.reconcilePoolsAsync()
	select {
	case <-restarted.parked.entered:
	case <-time.After(10 * time.Second):
		close(restarted.parked.release)
		t.Fatal("reconcile after restart never ran")
	}
	close(restarted.parked.release)
	restarted.f.c.poolWG.Wait()
	if _, ok := restarted.fenced(t); ok {
		t.Fatal("fence survived the machine's disappearance after restart")
	}
	if n := restarted.durableFences(t); n != 0 {
		t.Fatalf("durable fences after restart prune = %d, want 0", n)
	}
	if retired := poolEvents(t, restarted.f, proto.EvPoolRetired); len(retired) != 1 {
		t.Fatalf("pool.retired events after restart = %d, want exactly one", len(retired))
	}
	if err := restarted.claimAndReady(t); err != nil {
		t.Fatalf("claim after fence release = %v", err)
	}
}

// TestPoolRetirementRefusesAnIdentityMismatch: a fence is granted only for
// the exact provider machine whose remount.node label names an online node
// in this pool and tenant. A relabelled node is never destroyable.
func TestPoolRetirementRefusesAnIdentityMismatch(t *testing.T) {
	driver := &retireDriver{machine: retireMachine()}
	rf := newRetireFixture(t, "", driver, nodepool.Options{})
	rf.reconcileParked(t)
	rf.f.c.mu.Lock()
	rf.f.c.nodes["n_audit"].Status.Labels["tenant"] = "tenant-b"
	rf.f.c.mu.Unlock()
	close(rf.parked.release)
	rf.f.c.poolWG.Wait()
	if destroys := driver.destroys(); len(destroys) != 0 {
		t.Fatalf("destroys = %v, want none for a tenant mismatch", destroys)
	}
	if _, ok := rf.fenced(t); ok {
		t.Fatal("fence granted despite a tenant mismatch")
	}
}
