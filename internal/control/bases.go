package control

import (
	"context"
	"regexp"
	"sort"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// baseNamePattern keeps names usable as CLI arguments and file-system labels:
// no path separators, no leading dot, bounded length.
var baseNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func baseKey(tenant, name string) string { return tenant + "\x00" + name }

func baseResource(base *proto.Base) Resource {
	return Resource{Kind: "base", ID: base.Name, Tenant: base.Tenant, Owner: base.Owner}
}

type mutationBaseResult struct {
	Name string `cbor:"name"`
}

// baseEvent attributes an event to a tenant-scoped base rather than a
// workspace; the base's originating workspace, if any, is only payload.
func (c *Control) baseEvent(typ string, base *proto.Base, principal string) *proto.Event {
	e := c.newEvent(typ, base.Name, principal, "", map[string]any{
		"name": base.Name, "artifact": base.Artifact, "format": base.Format, "workspace": base.Workspace, "bytes": base.Bytes,
	})
	e.Tenant = base.Tenant
	return e
}

// baseCreate pins an already-uploaded, digest-verified artifact under a
// tenant-unique name. Pinning is what makes a base different from a snapshot:
// WithArtifactReferences counts it, so GC cannot unlink it until baseRemove.
func (c *Control) baseCreate(ctx context.Context, subject Subject, req *proto.BaseCreateReq) (*proto.Base, error) {
	if !baseNamePattern.MatchString(req.Name) {
		return nil, proto.Err(proto.CodeBadRequest, "base name %q: use letters, digits, '.', '_' or '-' (max 64, no leading '.')", req.Name)
	}
	if req.Artifact == "" {
		return nil, proto.Err(proto.CodeBadRequest, "artifact id is required")
	}
	if err := c.check(ctx, subject, ActionWrite, Resource{Kind: "base", ID: req.Name, Tenant: subject.Tenant, Owner: subject.ID}); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|base.create"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationBaseResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBaseCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.baseGet(subject.Tenant, prior.Name)
	}
	store, err := c.artifactStoreForTenant(subject.Tenant)
	if err != nil {
		return nil, proto.Err(proto.CodeUnsupported, "artifact store is unavailable")
	}
	// Verify before taking c.mu: this re-hashes the blob and must not stall
	// every other control operation.
	format, objects, err := c.resolveSnapshotArtifact(ctx, subject.Tenant, req.Artifact, req.Format)
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "base artifact %q is unavailable: %v", req.Artifact, err)
	}
	r, size, err := store.Open(req.Artifact)
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "base artifact %q is unavailable: %v", req.Artifact, err)
	}
	if err := r.Close(); err != nil {
		return nil, proto.Err(proto.CodeInternal, "close base artifact %q: %v", req.Artifact, err)
	}
	key := baseKey(subject.Tenant, req.Name)
	c.mu.Lock()
	if existing := c.bases[key]; existing != nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "base %q already exists", req.Name)
	}
	tenantBases := 0
	for _, base := range c.bases {
		if base.Tenant == subject.Tenant {
			tenantBases++
		}
	}
	if tenantBases >= c.opts.MaxBasesPerTenant {
		limit := c.opts.MaxBasesPerTenant
		c.mu.Unlock()
		metrics.BaseQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant base limit %d reached", limit)
	}
	base := &proto.Base{
		Name: req.Name, Tenant: subject.Tenant, Owner: subject.ID, Artifact: req.Artifact, Format: format, Objects: objects,
		Workspace: req.Workspace, Bytes: size, CreatedAt: c.now().UnixMilli(),
	}
	err = c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO bases(tenant, name, data) VALUES(?,?,?)`,
			base.Tenant, base.Name, proto.MustMarshal(base)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBaseCreate, req, mutationBaseResult{Name: base.Name})
	}, []*proto.Event{c.baseEvent(proto.EvBaseCreated, base, subject.ID)})
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.bases[key] = base
	cp := *base
	c.mu.Unlock()
	return &cp, nil
}

func (c *Control) baseGet(tenant, name string) (*proto.Base, error) {
	c.mu.Lock()
	base := c.bases[baseKey(tenant, name)]
	var cp proto.Base
	if base != nil {
		cp = *base
	}
	c.mu.Unlock()
	if base == nil {
		return nil, proto.Err(proto.CodeNotFound, "base %q is not defined", name)
	}
	return &cp, nil
}

// baseList returns every base the subject may read. Bases are tenant-shared
// by design: any member of the tenant may start a workspace from one, so the
// owner only gates removal.
func (c *Control) baseList(ctx context.Context, subject Subject) (*proto.BaseListRes, error) {
	c.mu.Lock()
	bases := make([]proto.Base, 0, len(c.bases))
	for _, base := range c.bases {
		bases = append(bases, *base)
	}
	c.mu.Unlock()
	out := &proto.BaseListRes{Bases: []proto.Base{}}
	for i := range bases {
		if c.baseReadable(ctx, subject, &bases[i]) {
			out.Bases = append(out.Bases, bases[i])
		}
	}
	sort.Slice(out.Bases, func(i, j int) bool {
		if out.Bases[i].Tenant != out.Bases[j].Tenant {
			return out.Bases[i].Tenant < out.Bases[j].Tenant
		}
		return out.Bases[i].Name < out.Bases[j].Name
	})
	return out, nil
}

func (c *Control) baseReadable(ctx context.Context, subject Subject, base *proto.Base) bool {
	if c.opts.Authorizer != nil {
		return c.check(ctx, subject, ActionRead, baseResource(base)) == nil
	}
	return hasRole(subject, "admin") || subject.Tenant == base.Tenant
}

// baseRemove unpins a base. Workspaces created from it already carry the
// artifact in their own RestoreFrom, so nothing they depend on is lost; the
// artifact merely becomes eligible for GC once no workspace references it.
func (c *Control) baseRemove(ctx context.Context, subject Subject, req *proto.BaseRemoveReq) error {
	if req.Name == "" {
		return proto.Err(proto.CodeBadRequest, "base name is required")
	}
	scope := subject.Tenant + "|" + subject.ID + "|base.remove"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBaseRemove, req, nil); err != nil {
		return err
	} else if hit {
		return nil
	}
	key := baseKey(subject.Tenant, req.Name)
	c.mu.Lock()
	base := c.bases[key]
	var cp proto.Base
	if base != nil {
		cp = *base
	}
	c.mu.Unlock()
	if base == nil {
		return proto.Err(proto.CodeNotFound, "base %q is not defined", req.Name)
	}
	if err := c.check(ctx, subject, ActionWrite, baseResource(&cp)); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.bases[key]
	if current == nil || current.Artifact != cp.Artifact || current.CreatedAt != cp.CreatedAt {
		// Removed or replaced while the authorization check ran; the
		// caller's decision was about a base that no longer exists.
		return proto.Err(proto.CodeNotFound, "base %q is not defined", req.Name)
	}
	err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`DELETE FROM bases WHERE tenant=? AND name=?`, cp.Tenant, cp.Name); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBaseRemove, req, struct{}{})
	}, []*proto.Event{c.baseEvent(proto.EvBaseRemoved, &cp, subject.ID)})
	if err != nil {
		return err
	}
	delete(c.bases, key)
	return nil
}
