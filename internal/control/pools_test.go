package control

import (
	"context"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/proto"
)

// TestPoolsAreDurableTenantScopedAndIdempotent makes the forbidden outcomes
// observable: a replay cannot create two pools, another tenant cannot read
// one, returned label maps cannot mutate control state, and a non-empty pool
// cannot be forgotten while machines still exist.
func TestPoolsAreDurableTenantScopedAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pools.db")
	f := newControlFixture(t, path, nil)
	ctx := context.Background()
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"tenant_admin"}}
	bob := Subject{ID: "bob", Tenant: "tenant-b", Roles: []string{"tenant_admin"}}
	req := &proto.PoolCreateReq{Spec: proto.PoolSpec{
		Name: "iad", Vendor: "fly", Min: 0, Max: 5, Backend: "gvisor",
		IdleScaleDownMilli: 15 * 60 * 1000, Labels: map[string]string{"region": "iad"},
	}, IdempotencyKey: "create-iad"}
	pool, err := f.c.poolCreate(ctx, alice, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.c.poolCreate(ctx, alice, req)
	if err != nil || again.CreatedAt != pool.CreatedAt {
		t.Fatalf("idempotent replay = %+v, %v", again, err)
	}
	pool.Spec.Labels["region"] = "mutated"
	got, err := f.c.poolGet(ctx, alice, "iad")
	if err != nil || got.Spec.Labels["region"] != "iad" {
		t.Fatalf("caller mutated stored pool: %+v, %v", got, err)
	}
	if _, err := f.c.poolGet(ctx, bob, "iad"); !isCode(err, proto.CodeNotFound) {
		t.Fatalf("cross-tenant get = %v, want not_found without enumeration", err)
	}

	f.c.mu.Lock()
	f.c.pools[poolKey("tenant-a", "iad")].Current = 1
	f.c.mu.Unlock()
	if err := f.c.poolRemove(ctx, alice, &proto.PoolRemoveReq{Name: "iad", IdempotencyKey: "remove"}); !isCode(err, proto.CodeConflict) {
		t.Fatalf("remove non-empty pool = %v, want conflict", err)
	}
	f.c.mu.Lock()
	f.c.pools[poolKey("tenant-a", "iad")].Current = 0
	f.c.mu.Unlock()

	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	f = newControlFixture(t, path, nil)
	got, err = f.c.poolGet(ctx, alice, "iad")
	if err != nil || got.Spec.Vendor != "fly" || got.Spec.Max != 5 {
		t.Fatalf("pool after restart = %+v, %v", got, err)
	}
	if err := f.c.poolRemove(ctx, alice, &proto.PoolRemoveReq{Name: "iad", IdempotencyKey: "remove"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.poolGet(ctx, alice, "iad"); !isCode(err, proto.CodeNotFound) {
		t.Fatalf("removed pool get = %v", err)
	}
}

func TestPoolSpecValidation(t *testing.T) {
	valid := proto.PoolSpec{Name: "workers", Vendor: "ssh", Max: 1, Backend: "gvisor"}
	if err := proto.ValidatePoolSpec(valid); err != nil {
		t.Fatal(err)
	}
	invalid := []proto.PoolSpec{
		{},
		{Name: "bad/name", Vendor: "ssh", Max: 1, Backend: "gvisor"},
		{Name: "workers", Vendor: "ssh", Min: 2, Max: 1, Backend: "gvisor"},
		{Name: "workers", Vendor: "ssh", Max: 1},
		{Name: "workers", Vendor: "ssh", Max: 1, Backend: "gvisor", Labels: map[string]string{"remount.pool": "escape"}},
	}
	for _, spec := range invalid {
		if err := proto.ValidatePoolSpec(spec); err == nil {
			t.Fatalf("accepted invalid spec: %+v", spec)
		}
	}
}
