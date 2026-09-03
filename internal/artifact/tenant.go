package artifact

import (
	"context"
	"errors"
	"io"
	"time"
)

// TenantBlobStore is a BlobStore permanently bound to one authenticated
// tenant. PutExpected must not publish any object when the body exceeds
// maxBytes or hashes to a different id.
type TenantBlobStore interface {
	BlobStore
	PutExpected(expected string, body io.Reader, maxBytes int64) (int64, error)
	Verify(id string) error
}

// TenantResolver returns a store whose namespace cannot address a different
// tenant. EncryptedAtRest is a capability assertion checked before a
// production server becomes ready; a plain compatibility store must return
// false.
type TenantResolver interface {
	ResolveTenant(tenant string) (TenantBlobStore, error)
	EncryptedAtRest() bool
	Ready(context.Context) error
}

// TenantReference is one tenant-qualified logical artifact GC root.
type TenantReference struct {
	Tenant string
	ID     string
}

// TenantCollector removes only tenant-qualified unreferenced objects after a
// grace window. Implementations must serialize each pass internally.
type TenantCollector interface {
	CollectTenants(ctx context.Context, referenced []TenantReference, now, cutoff time.Time) (GCResult, error)
}

// TenantRetentionCollector applies a per-tenant age cutoff on top of the
// shared grace window. One tenant's retention policy must never be able to
// select another tenant's object, so the cutoff is indexed by the namespace
// the pass is about to sweep and a tenant with no policy keeps the shared
// cutoff. A store that cannot separate tenants must not implement this;
// callers report an unenforceable policy rather than sweeping globally.
type TenantRetentionCollector interface {
	TenantCollector
	CollectTenantRetention(ctx context.Context, referenced []TenantReference, now, cutoff time.Time, tenantCutoffs map[string]time.Time) (GCResult, error)
}

// TenantInventory enumerates physical tenant namespaces for diagnostics and
// garbage collection. It must fail on malformed or unaccounted namespaces.
type TenantInventory interface {
	Tenants(ctx context.Context) ([]string, error)
}

// TenantVerification describes one exact physical encrypted representation.
type TenantVerification struct {
	Tenant  string
	ID      string
	Version string
	Retired bool
	Err     error
}

// TenantDeepVerifier checks every retained representation, including retired
// key versions that a successful logical read could otherwise hide.
type TenantDeepVerifier interface {
	VerifyTenant(ctx context.Context, tenant string) ([]TenantVerification, error)
}

// SharedTenantResolver explicitly adapts the legacy global directory store.
// It is suitable only for standalone mode, where "local" is the sole trust
// domain. It deliberately does not implement TenantRetentionCollector,
// because it cannot separate tenants and a caller must report a per-tenant
// retention policy as unenforceable rather than sweep every tenant with it.
type SharedTenantResolver struct {
	store *Store
}

// NewSharedTenantResolver creates the standalone compatibility resolver.
func NewSharedTenantResolver(store *Store) (*SharedTenantResolver, error) {
	if store == nil {
		return nil, errors.New("artifact: shared tenant resolver requires a store")
	}
	return &SharedTenantResolver{store: store}, nil
}

// ResolveTenant returns the one shared standalone store. The tenant argument
// is intentionally ignored because this resolver cannot provide isolation.
func (r *SharedTenantResolver) ResolveTenant(string) (TenantBlobStore, error) {
	return sharedTenantStore{Store: r.store}, nil
}

// EncryptedAtRest reports that the compatibility store is plaintext.
func (*SharedTenantResolver) EncryptedAtRest() bool { return false }

// Ready reports the store constructed and inventoried successfully.
func (*SharedTenantResolver) Ready(context.Context) error { return nil }

type sharedTenantStore struct {
	*Store
}

func (s sharedTenantStore) PutExpected(expected string, body io.Reader, maxBytes int64) (int64, error) {
	return s.Store.PutExpected(expected, body, maxBytes)
}
