package s3

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"

	"remount.dev/remount/internal/artifact/encrypted"
)

// KeyedStore adapts named S3 objects to encrypted.KeyedStore. The encrypted
// layer owns the tenants/<tenant>/<key-version>/<id> namespace.
type KeyedStore struct {
	store *Store
}

// Tenants inventories every physical tenant namespace and rejects malformed
// keys rather than leaving them outside diagnostics and GC.
func (s *KeyedStore) Tenants(ctx context.Context) ([]string, error) {
	objects, err := s.store.ListObjects(ctx, "tenants/")
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{})
	for _, object := range objects {
		parts := strings.Split(object.Key, "/")
		if len(parts) != 4 || parts[0] != "tenants" || parts[1] == "" {
			return nil, encrypted.ErrMalformed
		}
		set[parts[1]] = struct{}{}
	}
	tenants := make([]string, 0, len(set))
	for tenant := range set {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	return tenants, nil
}

var _ encrypted.KeyedStore = (*KeyedStore)(nil)

// NewKeyedStore returns a named-object adapter beneath tenant encryption.
func NewKeyedStore(store *Store) (*KeyedStore, error) {
	if store == nil {
		return nil, errors.New("s3: keyed store requires a store")
	}
	return &KeyedStore{store: store}, nil
}

// Create conditionally publishes one complete immutable ciphertext object.
func (s *KeyedStore) Create(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := s.store.PutObject(ctx, key, body, size, PutOptions{IfNoneMatch: "*"})
	if errors.Is(err, ErrPreconditionFailed) {
		return encrypted.ErrObjectExists
	}
	return err
}

// Open returns one pinned ciphertext object.
func (s *KeyedStore) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	reader, info, err := s.store.OpenObject(ctx, key)
	return reader, info.Size, err
}

// Head returns the ciphertext size.
func (s *KeyedStore) Head(ctx context.Context, key string) (int64, error) {
	info, err := s.store.HeadObject(ctx, key)
	return info.Size, err
}

// Delete removes one unpinned ciphertext object.
func (s *KeyedStore) Delete(ctx context.Context, key string) error {
	return s.store.DeleteObject(ctx, key)
}

// List returns every relative physical key below prefix.
func (s *KeyedStore) List(ctx context.Context, prefix string) ([]string, error) {
	objects, err := s.store.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(objects))
	for i := range objects {
		keys[i] = objects[i].Key
	}
	return keys, nil
}

// Ready proves listing works and conditional creation is enforced. The probe
// is private and removed before success is returned.
func (s *KeyedStore) Ready(ctx context.Context) error {
	if _, err := s.store.Inventory(ctx); err != nil {
		return err
	}
	return s.store.CheckConditionalWrites(ctx)
}
