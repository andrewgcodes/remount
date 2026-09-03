package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/provision"
)

type fakeTokens struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeTokens) Issue(context.Context, string, string, time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return "one-time-token", nil
}

type fakeDriver struct {
	mu          sync.Mutex
	creates     []provision.Request
	destroys    []string
	createErr   error
	destroyErr  error
	createBlock chan struct{}
	inventory   []provision.Machine
}

func (f *fakeDriver) Name() string { return "fake" }
func (f *fakeDriver) Create(_ context.Context, req provision.Request) (provision.Machine, error) {
	if f.createBlock != nil {
		<-f.createBlock
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, provision.CloneRequest(req))
	if f.createErr != nil {
		return provision.Machine{}, f.createErr
	}
	return provision.Machine{ID: req.Name, Name: req.Name, Provider: "fake", Tenant: req.Tenant, Labels: req.Labels}, nil
}
func (f *fakeDriver) Destroy(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroys = append(f.destroys, id)
	return f.destroyErr
}
func (f *fakeDriver) List(context.Context, provision.ListOptions) ([]provision.Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]provision.Machine(nil), f.inventory...), nil
}

func testSpec() Spec {
	return Spec{
		Name: "p", Tenant: "t", Vendor: "fake", Min: 0, Max: 2,
		Backend: "gvisor", IdleScaleDown: time.Minute,
		Bootstrap: provision.Bootstrap{
			ServerURL: "https://remount.example", BinaryURL: "https://remount.example/remount",
			DataDir: "/var/lib/remount",
		},
	}
}

func TestDemandScalesFromZeroAndEnrollmentNeverEntersMachineState(t *testing.T) {
	driver, tokens := &fakeDriver{}, &fakeTokens{}
	r, err := New([]provision.Driver{driver}, tokens, Options{})
	if err != nil {
		t.Fatal(err)
	}
	actions, err := r.Reconcile(context.Background(), testSpec(), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 1 || actions[0].Kind != ActionCreated || actions[0].From != 0 || actions[0].To != 1 {
		t.Fatalf("actions = %#v", actions)
	}
	if actions[0].Machine.ID == "" || actions[0].Machine.Tenant != "t" {
		t.Fatalf("machine = %#v", actions[0].Machine)
	}
	driver.mu.Lock()
	req := driver.creates[0]
	driver.mu.Unlock()
	if req.Bootstrap.EnrollmentToken != "one-time-token" {
		t.Fatal("driver did not receive the issued enrollment token")
	}
	if req.Bootstrap.NodeID == "" || req.Labels[provision.NodeLabel] != req.Bootstrap.NodeID {
		t.Fatalf("provider/node identity contract = bootstrap %q labels %#v", req.Bootstrap.NodeID, req.Labels)
	}
}

func TestConcurrentDemandCannotOvercommitOnePool(t *testing.T) {
	driver, tokens := &fakeDriver{createBlock: make(chan struct{})}, &fakeTokens{}
	r, err := New([]provision.Driver{driver}, tokens, Options{})
	if err != nil {
		t.Fatal(err)
	}
	spec := testSpec()
	spec.Max = 1
	var wg sync.WaitGroup
	results := make(chan []Action, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			actions, _ := r.Reconcile(context.Background(), spec, nil, 1)
			results <- actions
		}()
	}
	close(driver.createBlock)
	wg.Wait()
	close(results)

	driver.mu.Lock()
	creates := len(driver.creates)
	driver.mu.Unlock()
	if creates != 1 {
		t.Fatalf("creates = %d, want one pending capacity reservation", creates)
	}
}

func TestVisibleBootingMachineSatisfiesRepeatedDemand(t *testing.T) {
	driver, tokens := &fakeDriver{}, &fakeTokens{}
	r, err := New([]provision.Driver{driver}, tokens, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), testSpec(), nil, 1); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	machine := provision.Machine{ID: driver.creates[0].Name, Name: driver.creates[0].Name, Provider: "fake", Tenant: "t", Labels: driver.creates[0].Labels}
	driver.mu.Unlock()
	actions, err := r.Reconcile(context.Background(), testSpec(), []Node{{Machine: machine, Workspaces: -1}}, 1)
	if err != nil || len(actions) != 0 {
		t.Fatalf("repeated demand over-scaled: actions=%#v err=%v", actions, err)
	}
	driver.mu.Lock()
	creates := len(driver.creates)
	driver.mu.Unlock()
	if creates != 1 {
		t.Fatalf("creates = %d, want 1", creates)
	}
}

func TestIdleScaleDownNeverDestroysActiveNode(t *testing.T) {
	now := time.Unix(1000, 0)
	driver, tokens := &fakeDriver{}, &fakeTokens{}
	r, _ := New([]provision.Driver{driver}, tokens, Options{Now: func() time.Time { return now }})
	spec := testSpec()
	nodes := []Node{
		{Machine: ownedMachine("active"), Workspaces: 1, IdleSince: now.Add(-time.Hour)},
		{Machine: ownedMachine("idle"), IdleSince: now.Add(-time.Hour)},
	}
	actions, err := r.Reconcile(context.Background(), spec, nodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 1 || actions[0].Kind != ActionDestroyed || actions[0].Machine.ID != "idle" {
		t.Fatalf("actions = %#v", actions)
	}
}

func TestProviderFailureBacksOffObservably(t *testing.T) {
	now := time.Unix(1000, 0)
	driver, tokens := &fakeDriver{createErr: errors.New("provider unavailable")}, &fakeTokens{}
	r, _ := New([]provision.Driver{driver}, tokens, Options{
		Now: func() time.Time { return now }, MinBackoff: 2 * time.Second, MaxBackoff: 8 * time.Second,
	})
	actions, err := r.Reconcile(context.Background(), testSpec(), nil, 1)
	if err == nil || len(actions) != 1 || actions[0].Kind != ActionCreateFailed || actions[0].Error == "" {
		t.Fatalf("first reconcile: actions=%#v err=%v", actions, err)
	}
	actions, err = r.Reconcile(context.Background(), testSpec(), nil, 1)
	if err != nil || len(actions) != 1 || actions[0].Kind != ActionBackoff || !actions[0].RetryAt.Equal(now.Add(2*time.Second)) {
		t.Fatalf("backoff reconcile: actions=%#v err=%v", actions, err)
	}
}

func ownedMachine(id string) provision.Machine {
	return provision.Machine{ID: id, Provider: "fake", Tenant: "t", Labels: map[string]string{provision.PoolLabel: "p", provision.NodeLabel: "n_" + id}}
}

func TestInventoryRejectsDriverScopeEscape(t *testing.T) {
	driver := &fakeDriver{inventory: []provision.Machine{{ID: "foreign", Provider: "fake", Tenant: "other", Labels: map[string]string{provision.PoolLabel: "p"}}}}
	r, err := New([]provision.Driver{driver}, &fakeTokens{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Inventory(context.Background(), testSpec()); err == nil {
		t.Fatal("cross-tenant provider inventory was accepted")
	}
}
