package encrypted

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
)

// ReadinessCheck verifies the configured physical store can be used before a
// production server advertises readiness. Implementations must return an
// error when the check is unavailable; absence of objects is healthy.
type ReadinessCheck func(context.Context) error

// ResolverOptions bound each explicit rotation pass. Rotation is serialized
// because key activation and physical migration share one tenant authority.
type ResolverOptions struct {
	RewrapBatch int
	Ready       ReadinessCheck
}

// Resolver exposes tenant-bound BlobStore views and bounded key rotation over
// one encrypted store.
type Resolver struct {
	store *Store
	keys  KeyProvider
	opts  ResolverOptions

	rotation sync.Mutex
	gc       sync.Mutex
	orphans  map[string]time.Time
}

var _ artifact.TenantResolver = (*Resolver)(nil)

// NewResolver constructs the integration boundary used by the HTTP server,
// chunked snapshots, and future node/control wiring.
func NewResolver(store *Store, keys KeyProvider, opts ResolverOptions) (*Resolver, error) {
	if store == nil || keys == nil {
		return nil, errors.New("encrypted artifact: resolver requires store and key provider")
	}
	if opts.RewrapBatch < 0 {
		return nil, errors.New("encrypted artifact: rewrap batch must not be negative")
	}
	if opts.RewrapBatch == 0 {
		opts.RewrapBatch = 100
	}
	return &Resolver{store: store, keys: keys, opts: opts, orphans: make(map[string]time.Time)}, nil
}

// ResolveTenant returns a view that permanently binds every operation to the
// supplied authenticated tenant.
func (r *Resolver) ResolveTenant(tenant string) (artifact.TenantBlobStore, error) {
	return r.store.ForTenant(tenant)
}

// EncryptedAtRest is the capability assertion enforced by production modes.
func (*Resolver) EncryptedAtRest() bool { return true }

// Ready executes the configured physical-store probe. Local stores are fully
// inventoried by their constructors and therefore need no additional probe.
func (r *Resolver) Ready(ctx context.Context) error {
	if r.opts.Ready == nil {
		return nil
	}
	return r.opts.Ready(ctx)
}

// RotationResult describes one bounded rotation invocation. Remaining may be
// non-zero; callers schedule another pass rather than monopolizing the store.
type RotationResult struct {
	Version   string
	Migrated  int
	Remaining int
}

// Rotate activates a fresh tenant key and migrates at most RewrapBatch
// existing logical artifacts. Old keys and ciphertext remain readable until
// a later verified pass reports Remaining == 0.
func (r *Resolver) Rotate(ctx context.Context, tenant string) (RotationResult, error) {
	rotator, ok := r.keys.(interface {
		Rotate(context.Context, string) (string, error)
	})
	if !ok {
		return RotationResult{}, errors.New("encrypted artifact: key provider does not support rotation")
	}
	r.rotation.Lock()
	defer r.rotation.Unlock()
	version, err := rotator.Rotate(ctx, tenant)
	if err != nil {
		return RotationResult{}, err
	}
	result, err := r.store.RewrapRetired(ctx, tenant, r.opts.RewrapBatch)
	return RotationResult{Version: version, Migrated: result.Migrated, Remaining: result.Remaining}, err
}

// ContinueRotation performs one additional bounded migration pass without
// activating another key version.
func (r *Resolver) ContinueRotation(ctx context.Context, tenant string) (RotationResult, error) {
	r.rotation.Lock()
	defer r.rotation.Unlock()
	version, key, err := r.keys.Current(ctx, tenant)
	if err != nil {
		return RotationResult{}, err
	}
	erase32(&key)
	result, err := r.store.RewrapRetired(ctx, tenant, r.opts.RewrapBatch)
	return RotationResult{Version: version, Migrated: result.Migrated, Remaining: result.Remaining}, err
}

// Tenants returns every physical tenant namespace.
func (r *Resolver) Tenants(ctx context.Context) ([]string, error) {
	return r.store.Tenants(ctx)
}

// VerifyTenant decrypts and hashes every physical representation, including
// retired versions that normal logical reads no longer select.
func (r *Resolver) VerifyTenant(ctx context.Context, tenant string) ([]artifact.TenantVerification, error) {
	objects, err := r.store.Inventory(ctx, tenant)
	if err != nil {
		return nil, err
	}
	out := make([]artifact.TenantVerification, 0, len(objects))
	for _, object := range objects {
		verification := artifact.TenantVerification{
			Tenant: tenant, ID: object.ID, Version: object.KeyVersion, Retired: object.Retired,
		}
		if !object.KeyAvailable {
			verification.Err = ErrKeyUnavailable
		} else {
			verification.Err = r.store.VerifyVersion(ctx, tenant, object.KeyVersion, object.ID)
		}
		out = append(out, verification)
	}
	return out, nil
}

// CollectTenants performs a tenant-qualified mark-and-sweep pass. Orphan age
// is observed conservatively in memory: restart resets the grace window and
// can delay reclamation, but can never accelerate deletion.
func (r *Resolver) CollectTenants(ctx context.Context, referenced []artifact.TenantReference, now, cutoff time.Time) (artifact.GCResult, error) {
	r.gc.Lock()
	defer r.gc.Unlock()
	keep := make(map[string]struct{}, len(referenced))
	tenantSet := make(map[string]struct{})
	for _, reference := range referenced {
		if _, err := artifact.Digest(reference.ID); err != nil || reference.Tenant == "" {
			return artifact.GCResult{}, fmt.Errorf("encrypted artifact: invalid tenant GC reference")
		}
		keep[reference.Tenant+"\x00"+reference.ID] = struct{}{}
		tenantSet[reference.Tenant] = struct{}{}
	}
	tenants, err := r.store.Tenants(ctx)
	if err != nil {
		return artifact.GCResult{}, err
	}
	for _, tenant := range tenants {
		tenantSet[tenant] = struct{}{}
	}
	var result artifact.GCResult
	for tenant := range tenantSet {
		ids, err := r.store.List(ctx, tenant)
		if err != nil {
			return result, err
		}
		for _, id := range ids {
			result.Scanned++
			key := tenant + "\x00" + id
			if _, retained := keep[key]; retained {
				result.Referenced++
				delete(r.orphans, key)
				continue
			}
			first, observed := r.orphans[key]
			if !observed {
				r.orphans[key] = now
				result.GraceRetained++
				continue
			}
			if cutoff.IsZero() || !first.Before(cutoff) {
				result.GraceRetained++
				continue
			}
			size, err := r.store.Head(ctx, tenant, id)
			if err != nil {
				return result, err
			}
			if err := r.store.Delete(ctx, tenant, id); err != nil {
				return result, err
			}
			result.Removed++
			result.RemovedBytes += size
			delete(r.orphans, key)
		}
	}
	return result, nil
}
