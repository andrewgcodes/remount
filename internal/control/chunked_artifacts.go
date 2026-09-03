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

// Closure verification names two properties that a content-hash rehash cannot
// see. A manifest blob can hash correctly and still fail to decode, and a
// manifest that decodes perfectly can reference chunks that are gone. Damage
// is only actionable if the diagnostic says which one it was.
const (
	closureCheckManifest = "artifact.manifest_canonical"
	closureCheckChunks   = "artifact.closure_complete"
)

// snapshotClosureError carries which property failed and on which object, so
// the closure walk can be reused unchanged by both snapshot commit, which only
// needs to fail, and doctor, which needs to explain.
type snapshotClosureError struct {
	Check  string
	Object string
	Err    error
}

func (e *snapshotClosureError) Error() string {
	return fmt.Sprintf("%s: %s: %v", e.Check, e.Object, e.Err)
}

func (e *snapshotClosureError) Unwrap() error { return e.Err }

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
		return nil, &snapshotClosureError{Check: closureCheckManifest, Object: id, Err: err}
	}
	seen := make(map[string]int)
	for _, entry := range manifest.Entries {
		for _, reference := range entry.Chunks {
			if expected, ok := seen[reference.ID]; ok {
				if expected != reference.Size {
					return nil, &snapshotClosureError{Check: closureCheckManifest, Object: reference.ID,
						Err: errors.New("the manifest declares two different sizes for one chunk")}
				}
				continue
			}
			seen[reference.ID] = reference.Size
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			size, err := readStore.Head(reference.ID)
			if err != nil {
				return nil, &snapshotClosureError{Check: closureCheckChunks, Object: reference.ID,
					Err: fmt.Errorf("referenced chunk is unavailable: %w", err)}
			}
			if size != int64(reference.Size) {
				return nil, &snapshotClosureError{Check: closureCheckChunks, Object: reference.ID,
					Err: fmt.Errorf("referenced chunk is %d bytes, want %d", size, reference.Size)}
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

// snapshotRoot is one durable reference the control plane still names, plus
// the representation it was committed as.
type snapshotRoot struct {
	tenant  string
	subject string
	id      string
	format  string
}

// snapshotRootsLocked collects every authoritative snapshot the control plane
// still depends on: a live workspace's checkpoint and every pinned base. A
// destroyed workspace with no node is excluded for the same reason garbage
// collection excludes it — nothing will ever restore from it again.
func (c *Control) snapshotRootsLocked() []snapshotRoot {
	c.mu.Lock()
	defer c.mu.Unlock()
	roots := make([]snapshotRoot, 0, len(c.workspaces)+len(c.bases))
	for id, ws := range c.workspaces {
		if ws.LastSnapshot == "" || (ws.State == proto.WSDestroyed && ws.Node == "") {
			continue
		}
		roots = append(roots, snapshotRoot{tenant: ws.Tenant, subject: id, id: ws.LastSnapshot, format: ws.LastSnapshotFormat})
	}
	for name, base := range c.bases {
		if base.Artifact == "" {
			continue
		}
		roots = append(roots, snapshotRoot{tenant: base.Tenant, subject: "base/" + name, id: base.Artifact, format: base.Format})
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].subject < roots[j].subject })
	return roots
}

// verifySnapshotClosures re-walks every authoritative snapshot and reports the
// two properties re-hashing a blob cannot see: that a chunk manifest still
// decodes canonically, and that every chunk it references is present at its
// declared size.
//
// Both are separate from artifact.digest on purpose. A manifest whose bytes
// hash correctly can still be undecodable, and a manifest that decodes
// perfectly can point at chunks that were collected away; either one makes a
// restore fail, and an operator needs to know which happened. A root whose
// store cannot be opened is reported as unavailable, never as verified.
func (c *Control) verifySnapshotClosures(ctx context.Context, tenant string) []proto.Finding {
	roots := c.snapshotRootsLocked()
	if len(roots) == 0 {
		return nil
	}
	if !c.artifactVerificationConfigured() {
		return []proto.Finding{{
			Severity: "warn", Check: "artifact.closure_unavailable",
			Detail: fmt.Sprintf("%d authoritative snapshots are named, but this control plane has no artifact store to walk them", len(roots)),
			Hint:   "snapshot closures were not checked; do not read this as a pass",
		}}
	}
	if c.opts.TenantArtifacts != nil && tenant == "" {
		// Tenant-qualified storage cannot be addressed without a tenant, and
		// guessing one would walk another tenant's namespace.
		return []proto.Finding{{
			Severity: "warn", Check: "artifact.closure_unavailable",
			Detail: "authenticated tenant is unavailable",
			Hint:   "snapshot closures were not checked; do not read this as a pass",
		}}
	}
	var out []proto.Finding
	walked, manifests := 0, 0
	for _, root := range roots {
		if tenant != "" && tenant != "*" && root.tenant != tenant {
			continue
		}
		if err := ctx.Err(); err != nil {
			return append(out, proto.Finding{
				Severity: "warn", Check: "artifact.closure_unavailable", Subject: root.subject,
				Detail: "the closure walk was cancelled before it finished",
				Hint:   "the remaining snapshots were not checked; do not read this as a pass",
			})
		}
		format := root.format
		if format == "" {
			// An unrecorded format is exactly the case resolveSnapshotArtifact
			// sniffs. Guessing here would risk reading a manifest as a tar, so
			// the honest answer is that this root went unchecked.
			resolved, _, err := c.resolveSnapshotArtifact(ctx, root.tenant, root.id, "")
			if err != nil {
				out = append(out, closureFinding(root, err))
				continue
			}
			format = resolved
		}
		if format != proto.ArtifactFormatChunkedV1 {
			continue
		}
		manifests++
		if _, err := c.snapshotArtifactObjects(ctx, root.tenant, root.id, format); err != nil {
			out = append(out, closureFinding(root, err))
			continue
		}
		walked++
	}
	return append(out, proto.Finding{
		Severity: "info", Check: "artifact.closure_verified",
		Detail: fmt.Sprintf("walked %d of %d chunked snapshot closures across %d authoritative roots", walked, manifests, len(roots)),
	})
}

// closureFinding names the failed property when the walk could tell which one
// it was, and reports anything else as unavailable rather than as a pass.
func closureFinding(root snapshotRoot, err error) proto.Finding {
	var closure *snapshotClosureError
	if errors.As(err, &closure) {
		hint := "the manifest no longer decodes canonically; a restore from this snapshot would fail"
		if closure.Check == closureCheckChunks {
			hint = "a referenced chunk is missing or the wrong size; the closure is incomplete and cannot be restored"
		}
		return proto.Finding{
			Severity: "error", Check: closure.Check, Subject: root.subject,
			Detail: fmt.Sprintf("%s object %s: %v", root.id, closure.Object, closure.Err), Hint: hint,
		}
	}
	return proto.Finding{
		Severity: "warn", Check: "artifact.closure_unavailable", Subject: root.subject,
		Detail: fmt.Sprintf("%s could not be walked: %v", root.id, err),
		Hint:   "this snapshot's closure was not checked; do not read this as a pass",
	}
}
