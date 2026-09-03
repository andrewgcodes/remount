package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/proto"
)

// resolveSnapshotArtifact accepts the pre-format wire shape only when the
// object itself has an unambiguous canonical representation. This lets an old
// client pin a snapshot returned by a new node without ever guessing that a
// JSON manifest is a tar stream (the failure would otherwise surface much
// later as gzip corruption during materialization).
func (c *Control) resolveSnapshotArtifact(ctx context.Context, tenant, id, requested string) (string, []string, error) {
	format := requested
	if format == "" {
		store, err := c.artifactStoreForTenant(tenant)
		if err != nil {
			return "", nil, err
		}
		if err := store.Verify(id); err != nil {
			return "", nil, err
		}
		r, _, err := store.Open(id)
		if err != nil {
			return "", nil, err
		}
		reader := bufio.NewReader(r)
		prefix, readErr := reader.Peek(2)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil {
			return "", nil, errors.Join(readErr, closeErr)
		}
		switch {
		case len(prefix) == 2 && prefix[0] == 0x1f && prefix[1] == 0x8b:
			format = proto.ArtifactFormatTar
		case len(prefix) > 0 && prefix[0] == '{':
			format = proto.ArtifactFormatChunkedV1
		default:
			return "", nil, errors.New("snapshot representation is neither gzip tar nor a canonical chunk manifest")
		}
	}
	format, err := proto.NormalizeArtifactFormat(format)
	if err != nil {
		return "", nil, err
	}
	objects, err := c.snapshotArtifactObjects(ctx, tenant, id, format)
	return format, objects, err
}

type snapshotReadStore struct{ artifactReader }

func (s snapshotReadStore) Put(io.Reader) (string, int64, error) {
	return "", 0, errors.New("control: snapshot verification store is read-only")
}

func (s snapshotReadStore) Head(id string) (int64, error) {
	r, size, err := s.Open(id)
	if err != nil {
		return 0, err
	}
	return size, r.Close()
}

func (snapshotReadStore) Delete(string) error {
	return errors.New("control: snapshot verification store is read-only")
}
func (snapshotReadStore) List() ([]string, error) {
	return nil, errors.New("control: snapshot verification store cannot enumerate")
}

// snapshotArtifactObjects returns the bounded tenant-local GC closure after
// proving every object exists. Callers persist this list in the same resource
// transaction that makes the snapshot authoritative.
func (c *Control) snapshotArtifactObjects(ctx context.Context, tenant, id, format string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	format, err := proto.NormalizeArtifactFormat(format)
	if err != nil {
		return nil, err
	}
	store, err := c.artifactStoreForTenant(tenant)
	if err != nil {
		return nil, err
	}
	if err := store.Verify(id); err != nil {
		return nil, err
	}
	if format == proto.ArtifactFormatTar || format == proto.ArtifactFormatFirecrackerFullV1 {
		return []string{id}, nil
	}
	readStore := snapshotReadStore{artifactReader: store}
	manifest, err := chunked.LoadManifest(readStore, id, chunked.Limits{})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]int)
	for _, entry := range manifest.Entries {
		for _, reference := range entry.Chunks {
			if expected, ok := seen[reference.ID]; ok {
				if expected != reference.Size {
					return nil, fmt.Errorf("chunk %s has inconsistent declared sizes", reference.ID)
				}
				continue
			}
			seen[reference.ID] = reference.Size
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			size, err := readStore.Head(reference.ID)
			if err != nil {
				return nil, fmt.Errorf("chunk %s is unavailable: %w", reference.ID, err)
			}
			if size != int64(reference.Size) {
				return nil, fmt.Errorf("chunk %s size %d, want %d", reference.ID, size, reference.Size)
			}
		}
	}
	objects := make([]string, 0, len(seen)+1)
	objects = append(objects, id)
	for object := range seen {
		objects = append(objects, object)
	}
	sort.Strings(objects)
	return objects, nil
}
