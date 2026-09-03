package encrypted

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/metrics"
)

// inventoriedObjects is memoryObjects plus the tenant inventory a GC pass
// needs: without it the sweep has no namespace list to iterate.
type inventoriedObjects struct{ *memoryObjects }

func (s inventoriedObjects) Tenants(ctx context.Context) ([]string, error) {
	keys, err := s.List(ctx, "tenants/")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(keys))
	var out []string
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) < 4 {
			return nil, errors.New("malformed physical object key")
		}
		if _, ok := seen[parts[1]]; ok {
			continue
		}
		seen[parts[1]] = struct{}{}
		out = append(out, parts[1])
	}
	sort.Strings(out)
	return out, nil
}

func newTestResolver(t *testing.T) *Resolver {
	t.Helper()
	keys := newRotatingKeys()
	objects := inventoriedObjects{memoryObjects: newMemoryObjects()}
	store, err := NewStore(objects, keys, t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(store, keys, ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

// putTenantObject stores one object and returns its plaintext id.
func putTenantObject(t *testing.T, resolver *Resolver, tenant, body string) string {
	t.Helper()
	store, err := resolver.ResolveTenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustHead(t *testing.T, resolver *Resolver, tenant, id string) bool {
	t.Helper()
	store, err := resolver.ResolveTenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Head(id)
	return err == nil
}

func TestTenantRetentionCollectsOnlyItsOwnNamespace(t *testing.T) {
	// Both directions, because a boundary that only holds one way is an
	// accident of map iteration order rather than a namespace check.
	for _, policy := range []struct{ collected, retained string }{
		{collected: "tenant-a", retained: "tenant-b"},
		{collected: "tenant-b", retained: "tenant-a"},
	} {
		t.Run(policy.collected, func(t *testing.T) {
			ctx := context.Background()
			resolver := newTestResolver(t)
			collectedID := putTenantObject(t, resolver, policy.collected, "unreferenced "+policy.collected)
			retainedID := putTenantObject(t, resolver, policy.retained, "unreferenced "+policy.retained)
			now := time.Unix(1_800_000_000, 0)
			shared := now.Add(-time.Hour)
			first, err := resolver.CollectTenantRetention(ctx, nil, now, shared, nil)
			if err != nil || first.Scanned != 2 || first.GraceRetained != 2 || first.Removed != 0 {
				t.Fatalf("first pass = %+v, %v", first, err)
			}
			before := metrics.TenantArtifactsCollected.Value()
			cutoffs := map[string]time.Time{policy.collected: now.Add(time.Hour)}
			second, err := resolver.CollectTenantRetention(ctx, nil, now.Add(time.Minute), shared, cutoffs)
			if err != nil {
				t.Fatal(err)
			}
			if second.Scanned != 2 || second.Referenced != 0 || second.GraceRetained != 1 || second.Removed != 1 {
				t.Fatalf("retention pass = %+v", second)
			}
			if second.RemovedBytes <= 0 {
				t.Fatalf("retention pass removed no bytes: %+v", second)
			}
			if mustHead(t, resolver, policy.collected, collectedID) {
				t.Fatalf("%s object survived its own retention cutoff", policy.collected)
			}
			if !mustHead(t, resolver, policy.retained, retainedID) {
				t.Fatalf("%s object was collected by %s's retention policy", policy.retained, policy.collected)
			}
			if got := metrics.TenantArtifactsCollected.Value() - before; got != 1 {
				t.Fatalf("tenant retention counter delta = %d, want 1", got)
			}
		})
	}
}

func TestTenantRetentionNeverCollectsAReferencedObject(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver(t)
	policyID := putTenantObject(t, resolver, "tenant-a", "referenced under a policy")
	sharedID := putTenantObject(t, resolver, "tenant-b", "referenced without a policy")
	now := time.Unix(1_800_000_000, 0)
	// Age both objects first: a live closure must win even over an orphan the
	// grace window has already been observing.
	if first, err := resolver.CollectTenantRetention(ctx, nil, now, now.Add(-time.Hour), nil); err != nil || first.GraceRetained != 2 {
		t.Fatalf("first pass = %+v, %v", first, err)
	}
	before := metrics.TenantArtifactsCollected.Value()
	references := []artifact.TenantReference{{Tenant: "tenant-a", ID: policyID}, {Tenant: "tenant-b", ID: sharedID}}
	cutoffs := map[string]time.Time{"tenant-a": now.Add(time.Hour)}
	result, err := resolver.CollectTenantRetention(ctx, references, now.Add(time.Minute), now.Add(time.Hour), cutoffs)
	if err != nil {
		t.Fatal(err)
	}
	if result.Referenced != 2 || result.Removed != 0 || result.RemovedBytes != 0 {
		t.Fatalf("referenced objects were not exempt: %+v", result)
	}
	if !mustHead(t, resolver, "tenant-a", policyID) {
		t.Fatal("referenced tenant-a object was collected by its retention policy")
	}
	if !mustHead(t, resolver, "tenant-b", sharedID) {
		t.Fatal("referenced tenant-b object was collected by the shared cutoff")
	}
	if got := metrics.TenantArtifactsCollected.Value() - before; got != 0 {
		t.Fatalf("tenant retention counter delta = %d, want 0", got)
	}
}

func TestTenantRetentionCutoffForUnknownTenantIsInert(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver(t)
	idA := putTenantObject(t, resolver, "tenant-a", "unreferenced a")
	idB := putTenantObject(t, resolver, "tenant-b", "unreferenced b")
	now := time.Unix(1_800_000_000, 0)
	shared := now.Add(-time.Hour)
	if first, err := resolver.CollectTenantRetention(ctx, nil, now, shared, nil); err != nil || first.GraceRetained != 2 {
		t.Fatalf("first pass = %+v, %v", first, err)
	}
	before := metrics.TenantArtifactsCollected.Value()
	ghost := map[string]time.Time{"tenant-ghost": now.Add(time.Hour)}
	inert, err := resolver.CollectTenantRetention(ctx, nil, now.Add(time.Minute), shared, ghost)
	if err != nil || inert.Scanned != 2 || inert.GraceRetained != 2 || inert.Removed != 0 {
		t.Fatalf("unknown-tenant cutoff was not inert: %+v, %v", inert, err)
	}
	// The nil-map path must still be exactly the shared grace window.
	collecting := now.Add(time.Hour)
	shared2, err := resolver.CollectTenants(ctx, nil, now.Add(2*time.Minute), collecting)
	if err != nil || shared2.Scanned != 2 || shared2.Removed != 2 || shared2.GraceRetained != 0 {
		t.Fatalf("shared collection pass = %+v, %v", shared2, err)
	}
	if mustHead(t, resolver, "tenant-a", idA) || mustHead(t, resolver, "tenant-b", idB) {
		t.Fatal("shared cutoff left an aged unreferenced object behind")
	}
	if got := metrics.TenantArtifactsCollected.Value() - before; got != 0 {
		t.Fatalf("tenant retention counter delta = %d, want 0", got)
	}
}
