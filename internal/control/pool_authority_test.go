package control

import (
	"context"
	"sync"
	"testing"
	"time"

	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
)

// blockingPoolController stalls inside a provider call until it is released,
// and records whether the control plane stayed responsive while it stalled.
type blockingPoolController struct {
	entered chan string
	release chan struct{}

	mu    sync.Mutex
	calls []string
}

func (b *blockingPoolController) block(call string) {
	b.mu.Lock()
	b.calls = append(b.calls, call)
	b.mu.Unlock()
	select {
	case b.entered <- call:
	default:
	}
	<-b.release
}

func (b *blockingPoolController) Inventory(context.Context, nodepool.Spec) ([]provision.Machine, error) {
	b.block("inventory")
	return nil, nil
}

func (b *blockingPoolController) Reconcile(context.Context, nodepool.Spec, []nodepool.Node, int) ([]nodepool.Action, error) {
	b.block("reconcile")
	return nil, nil
}

func (*blockingPoolController) Forget(nodepool.Spec) {}

// TestPoolReconcileNeverHoldsControlAuthorityAcrossAProviderCall is Plan B's
// B10. A provider call is a network call to somebody else's service: it can be
// slow, it can hang until a three-minute timeout, and it can fail in ways this
// process does not control. Holding the control-plane mutex across it would
// convert one unreachable vendor into a wholly unresponsive control plane, and
// the symptom would be an unexplained global stall rather than a pool that
// cannot scale.
//
// The forbidden result is made visible by stalling inside the provider and
// requiring ordinary control-plane work to complete anyway. If the lock were
// ever held across the call, this test blocks until its own deadline instead
// of failing on an assertion.
func TestPoolReconcileNeverHoldsControlAuthorityAcrossAProviderCall(t *testing.T) {
	provider := &blockingPoolController{entered: make(chan string, 4), release: make(chan struct{})}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.PoolReconciler = provider
		opts.PoolBootstrap = PoolBootstrap{ServerURL: "https://control.example", BinaryURL: "https://control.example/remount", DataDir: "/var/lib/remount"}
	})
	subject := Subject{ID: "owner", Tenant: "tenant-a", Roles: []string{"tenant_admin"}}
	if _, err := f.c.poolCreate(context.Background(), subject, &proto.PoolCreateReq{Spec: proto.PoolSpec{
		Name: "iad", Vendor: "fake", Min: 1, Max: 5, Backend: "process", Labels: map[string]string{"region": "iad"},
	}, IdempotencyKey: "pool-create"}); err != nil {
		t.Fatal(err)
	}

	// A pool create schedules reconciliation, which enters the provider and
	// stalls there.
	f.c.reconcilePoolsAsync()
	select {
	case <-provider.entered:
	case <-time.After(10 * time.Second):
		close(provider.release)
		t.Fatal("pool reconciliation never reached the provider")
	}

	// The provider is now stalled mid-call. Ordinary control-plane work must
	// still complete. Each of these takes c.mu, so any one of them would hang
	// if reconciliation held it across the provider call.
	done := make(chan error, 1)
	go func() {
		if _, err := f.c.wsCreate(context.Background(), subject, &proto.WSCreateReq{
			Spec: proto.WorkspaceSpec{Name: "served-while-provider-stalls"}, IdempotencyKey: "ws-during-stall",
		}); err != nil {
			done <- err
			return
		}
		if _, err := f.c.poolList(context.Background(), subject); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			close(provider.release)
			t.Fatalf("control-plane work failed while the provider was stalled: %v", err)
		}
	case <-time.After(20 * time.Second):
		close(provider.release)
		t.Fatal("control authority was held across a provider call: ordinary work could not complete while the pool provider stalled")
	}

	close(provider.release)
	f.c.poolWG.Wait()

	provider.mu.Lock()
	calls := append([]string(nil), provider.calls...)
	provider.mu.Unlock()
	if len(calls) == 0 {
		t.Fatal("the provider was never called")
	}
}
