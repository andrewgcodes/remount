package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
)

type fakePoolController struct {
	mu           sync.Mutex
	inventory    []provision.Machine
	inventoryErr error
	actions      []nodepool.Action
	demand       int
	nodes        []nodepool.Node
	done         chan struct{}
}

func (f *fakePoolController) Inventory(context.Context, nodepool.Spec) ([]provision.Machine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]provision.Machine(nil), f.inventory...), f.inventoryErr
}

func (f *fakePoolController) Reconcile(_ context.Context, _ nodepool.Spec, nodes []nodepool.Node, demand int) ([]nodepool.Action, error) {
	f.mu.Lock()
	f.demand = demand
	f.nodes = append([]nodepool.Node(nil), nodes...)
	actions := append([]nodepool.Action(nil), f.actions...)
	f.mu.Unlock()
	select {
	case f.done <- struct{}{}:
	default:
	}
	return actions, nil
}

func (*fakePoolController) Forget(nodepool.Spec) {}

func TestPoolDemandCommitsProviderActionAndEvent(t *testing.T) {
	provider := &fakePoolController{done: make(chan struct{}, 1), actions: []nodepool.Action{{
		Kind: nodepool.ActionCreated, From: 0, To: 1, Reason: "demand",
		Machine: provision.Machine{ID: "machine-1"},
	}}}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.PoolReconciler = provider
		opts.PoolBootstrap = PoolBootstrap{ServerURL: "https://control.example", BinaryURL: "https://control.example/remount", DataDir: "/var/lib/remount"}
	})
	subject := Subject{ID: "owner", Tenant: "tenant-a", Roles: []string{"tenant_admin"}}
	pool, err := f.c.poolCreate(context.Background(), subject, &proto.PoolCreateReq{Spec: proto.PoolSpec{
		Name: "iad", Vendor: "fake", Max: 5, Backend: "gvisor", Labels: map[string]string{"region": "iad"},
	}, IdempotencyKey: "pool-create"})
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	f.c.workspaces["ws_pending"] = &proto.Workspace{ID: "ws_pending", Tenant: "tenant-a", State: proto.WSPending,
		Spec: proto.WorkspaceSpec{Requires: proto.Requires{Backend: "gvisor"}, Placement: proto.Placement{Allow: map[string]string{"region": "iad"}}}}
	f.c.mu.Unlock()

	f.c.reconcilePoolsAsync()
	select {
	case <-provider.done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool reconcile did not run")
	}
	f.c.poolWG.Wait()
	provider.mu.Lock()
	demand := provider.demand
	provider.mu.Unlock()
	if demand != 1 {
		t.Fatalf("pool demand = %d, want 1", demand)
	}
	got, err := f.c.poolGet(context.Background(), subject, pool.Spec.Name)
	if err != nil || got.Current != 1 {
		t.Fatalf("durable pool current = %+v, %v", got, err)
	}
	events, err := f.log.Read(context.Background(), 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var scaled bool
	for _, event := range events {
		if event.Type == proto.EvPoolScaled && event.Stream == "iad" {
			scaled = true
		}
	}
	if !scaled {
		t.Fatalf("pool.scaled absent from %#v", events)
	}
}

func TestPoolInventoryFailureIsObservableAndDoesNotChangeCapacity(t *testing.T) {
	provider := &fakePoolController{done: make(chan struct{}, 1), inventoryErr: errors.New("provider unavailable")}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.PoolReconciler = provider
		opts.PoolBootstrap = PoolBootstrap{ServerURL: "https://control.example", BinaryURL: "https://control.example/remount", DataDir: "/var/lib/remount"}
	})
	subject := Subject{ID: "owner", Tenant: "tenant-a", Roles: []string{"tenant_admin"}}
	if _, err := f.c.poolCreate(context.Background(), subject, &proto.PoolCreateReq{Spec: proto.PoolSpec{
		Name: "iad", Vendor: "fake", Max: 1, Backend: "gvisor",
	}, IdempotencyKey: "pool-create"}); err != nil {
		t.Fatal(err)
	}
	f.c.reconcilePoolsAsync()
	f.c.poolWG.Wait()
	events, err := f.log.Read(context.Background(), 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == proto.EvPoolFailed {
			return
		}
	}
	t.Fatalf("pool.provision_failed absent from %#v", events)
}
