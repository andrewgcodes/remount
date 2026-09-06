package control

import (
	"context"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// maxBindingsPerTenant bounds the durable binding table the same way every
// other tenant-scoped collection in the control plane is bounded: an
// unbounded collection is an availability defect, not a feature.
const maxBindingsPerTenant = 256

// bindingKey scopes a binding to its tenant. A file-configured binding has no
// tenant and is global; binding.create always writes an exact tenant, so the
// two namespaces cannot collide.
func bindingKey(tenant, id string) string { return tenant + "\x00" + id }

func cloneBinding(b Binding) Binding {
	b.Destinations = append([]string(nil), b.Destinations...)
	b.Principals = append([]string(nil), b.Principals...)
	b.Workspaces = append([]string(nil), b.Workspaces...)
	b.Methods = append([]string(nil), b.Methods...)
	b.PathPrefixes = append([]string(nil), b.PathPrefixes...)
	return b
}

// publicBinding is the only conversion that leaves this package. It never
// copies Secret: the credential is write-only from the moment it is accepted.
func publicBinding(b Binding) proto.BindingSpec {
	return proto.BindingSpec{
		ID: b.ID, Tenant: b.Tenant, Kind: b.Kind, Source: b.Source,
		Destinations:  append([]string(nil), b.Destinations...),
		Principals:    append([]string(nil), b.Principals...),
		Workspaces:    append([]string(nil), b.Workspaces...),
		Placeholder:   b.Placeholder,
		TTLSec:        b.TTLSec,
		Methods:       append([]string(nil), b.Methods...),
		PathPrefixes:  append([]string(nil), b.PathPrefixes...),
		Retention:     b.Retention,
		Revision:      b.Revision,
		CreatedAt:     b.CreatedAt,
		RotatedAt:     b.RotatedAt,
		RevokedAt:     b.RevokedAt,
		RevokedReason: b.RevokedReason,
	}
}

// lookupBindingLocked resolves a binding id for a workspace's tenant. A
// tenant-scoped binding wins over a global one with the same id so a tenant
// can shadow a deployment default without the two ever being confused.
func (c *Control) lookupBindingLocked(tenant, id string) (Binding, bool) {
	if tenant != "" {
		if b, ok := c.bindings[bindingKey(tenant, id)]; ok {
			return b, true
		}
	}
	b, ok := c.bindings[bindingKey("", id)]
	return b, ok
}

// bindingSetRevisionLocked fingerprints the binding set a workspace declares.
// Any create, rotate or revoke that touches one of those ids changes the
// fingerprint, and the node re-leases on the next renew. It is a fingerprint
// rather than a counter because a workspace names a set, and a set has no
// single monotonic revision to report.
func (c *Control) bindingSetRevisionLocked(tenant string, requested []string) uint64 {
	if len(requested) == 0 {
		return 0
	}
	h := fnv.New64a()
	for _, id := range requested {
		b, ok := c.lookupBindingLocked(tenant, id)
		_, _ = h.Write([]byte(id))
		if !ok {
			_, _ = h.Write([]byte("\x00missing"))
			continue
		}
		_, _ = h.Write([]byte("\x00" + strconv.FormatUint(b.Revision, 10) + "\x00" + strconv.FormatInt(b.RevokedAt, 10)))
	}
	sum := h.Sum64()
	if sum == 0 {
		// Zero is reserved for "this workspace declares no bindings", so a
		// hash collision with zero must not read as "nothing to compare".
		sum = 1
	}
	return sum
}

func (c *Control) bindingEvent(typ string, b Binding, principal string, payload map[string]any) *proto.Event {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["binding"] = b.ID
	payload["kind"] = b.Kind
	payload["revision"] = b.Revision
	payload["destinations"] = append([]string(nil), b.Destinations...)
	e := c.newEvent(typ, b.ID, principal, "", payload)
	e.Tenant = b.Tenant
	return e
}

// loadBindings restores the durable binding table. It runs before seeding so
// a seeded row that was later rotated or revoked keeps the durable state
// rather than being reset by the file it originally came from.
func (c *Control) loadBindings() error {
	rows, err := c.db.Query(`SELECT data FROM bindings`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return err
		}
		var b Binding
		if err := proto.Unmarshal(blob, &b); err != nil {
			return err
		}
		c.bindings[bindingKey(b.Tenant, b.ID)] = b
	}
	return rows.Err()
}

// seedBindings writes the --bindings entries that the store does not already
// have. A row that exists wins: rotation and revocation are durable decisions
// and must not be undone by restarting with the original file.
func (c *Control) seedBindings(configured []Binding) error {
	var missing []Binding
	for _, b := range configured {
		if _, exists := c.bindings[bindingKey(b.Tenant, b.ID)]; exists {
			continue
		}
		seeded := cloneBinding(b)
		seeded.Revision = 1
		seeded.CreatedAt = c.now().UnixMilli()
		missing = append(missing, seeded)
	}
	if len(missing) == 0 {
		return nil
	}
	events := make([]*proto.Event, 0, len(missing))
	for _, b := range missing {
		events = append(events, c.bindingEvent(proto.EvBindingCreated, b, "", map[string]any{"seeded": true}))
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		for _, b := range missing {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO bindings(tenant,id,revision,revoked_at,data) VALUES(?,?,?,?,?)`,
				b.Tenant, b.ID, int64(b.Revision), b.RevokedAt, proto.MustMarshal(&b)); err != nil {
				return err
			}
		}
		return nil
	}, events); err != nil {
		return err
	}
	for _, b := range missing {
		c.bindings[bindingKey(b.Tenant, b.ID)] = b
	}
	return nil
}

// validateBindingSpec checks everything that does not need the lock. It is
// deliberately strict about the credential: exactly one of an inline secret
// and an external source, because "both" hides which one is authoritative.
func (c *Control) validateBindingSpec(spec *proto.BindingSpec) error {
	if spec.ID == "" {
		return proto.Err(proto.CodeBadRequest, "binding id is required")
	}
	if strings.ContainsAny(spec.ID, "\x00\n\r ") || len(spec.ID) > 128 {
		return proto.Err(proto.CodeBadRequest, "binding id %q is malformed", spec.ID)
	}
	if (spec.Secret == "") == (spec.Source == "") {
		return proto.Err(proto.CodeBadRequest, "binding %s needs exactly one of a secret and a source", spec.ID)
	}
	if spec.Source != "" && c.opts.SecretResolver == nil {
		return proto.Err(proto.CodeUnsupported, "binding %s has an external source but no resolver is configured", spec.ID)
	}
	if len(spec.Destinations) == 0 {
		return proto.Err(proto.CodeBadRequest, "binding %s needs at least one destination", spec.ID)
	}
	for _, d := range spec.Destinations {
		if strings.TrimSpace(d) == "" {
			return proto.Err(proto.CodeBadRequest, "binding %s has an empty destination", spec.ID)
		}
	}
	switch spec.Kind {
	case "", proto.BindingKindAPIKey, proto.BindingKindBearer, proto.BindingKindCookie, proto.BindingKindHeader:
	default:
		return proto.Err(proto.CodeBadRequest, "binding kind %q is not one of api_key, bearer, cookie, header", spec.Kind)
	}
	if spec.TTLSec < 0 || spec.TTLSec > int64((24*time.Hour).Seconds()) {
		return proto.Err(proto.CodeBadRequest, "binding ttl must be between 0 and 86400 seconds")
	}
	for _, m := range spec.Methods {
		if m != strings.ToUpper(m) || strings.TrimSpace(m) == "" {
			return proto.Err(proto.CodeBadRequest, "binding method %q must be a non-empty upper-case verb", m)
		}
	}
	for _, p := range spec.PathPrefixes {
		if !strings.HasPrefix(p, "/") {
			return proto.Err(proto.CodeBadRequest, "binding path prefix %q must start with /", p)
		}
	}
	return nil
}

func bindingFromSpec(spec proto.BindingSpec, tenant, owner string, now int64) Binding {
	return Binding{
		ID: spec.ID, Tenant: tenant, Kind: spec.Kind, Secret: spec.Secret, Source: spec.Source,
		Destinations: append([]string(nil), spec.Destinations...),
		Principals:   append([]string(nil), spec.Principals...),
		Workspaces:   append([]string(nil), spec.Workspaces...),
		Placeholder:  spec.Placeholder, TTLSec: spec.TTLSec,
		Methods:      append([]string(nil), spec.Methods...),
		PathPrefixes: append([]string(nil), spec.PathPrefixes...),
		Retention:    spec.Retention, Owner: owner, Revision: 1, CreatedAt: now,
	}
}

func (c *Control) bindingCreate(ctx context.Context, actor Subject, req *proto.BindingCreateReq) (*proto.BindingSpec, error) {
	tenantID, err := exactPrincipalTenant(actor, req.Binding.Tenant)
	if err != nil {
		return nil, err
	}
	if req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "binding mutation requires an idempotency key")
	}
	if err := c.validateBindingSpec(&req.Binding); err != nil {
		return nil, err
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "binding", ID: req.Binding.ID, Tenant: tenantID, Owner: actor.ID}); err != nil {
		return nil, err
	}
	scope := actor.Tenant + "|" + actor.ID + "|binding.create|" + tenantID
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.BindingSpec
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBindingCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	b := bindingFromSpec(req.Binding, tenantID, actor.ID, c.now().UnixMilli())
	key := bindingKey(tenantID, b.ID)
	public := publicBinding(b)

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.bindings[key]; exists {
		return nil, proto.Err(proto.CodeConflict, "binding %q already exists", b.ID)
	}
	count := 0
	for _, existing := range c.bindings {
		if existing.Tenant == tenantID {
			count++
		}
	}
	if count >= maxBindingsPerTenant {
		return nil, proto.ErrReason(proto.CodeResourceExhausted, proto.ReasonQuotaExceeded,
			"tenant binding limit %d reached", maxBindingsPerTenant)
	}
	event := c.bindingEvent(proto.EvBindingCreated, b, actor.ID, nil)
	event.OperationID = req.IdempotencyKey
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO bindings(tenant,id,revision,revoked_at,data) VALUES(?,?,?,?,?)`,
			b.Tenant, b.ID, int64(b.Revision), b.RevokedAt, proto.MustMarshal(&b)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBindingCreate, req, public)
	}, []*proto.Event{event}); err != nil {
		return nil, err
	}
	c.bindings[key] = b
	return &public, nil
}

func (c *Control) bindingGet(ctx context.Context, actor Subject, req *proto.BindingGetReq) (*proto.BindingSpec, error) {
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, actor, ActionRead, Resource{Kind: "binding", ID: req.ID, Tenant: tenantID}); err != nil {
		return nil, err
	}
	c.mu.Lock()
	b, ok := c.lookupBindingLocked(tenantID, req.ID)
	c.mu.Unlock()
	if !ok {
		return nil, proto.ErrReason(proto.CodeNotFound, proto.ReasonBindingMissing, "binding %q is not defined", req.ID)
	}
	spec := publicBinding(b)
	return &spec, nil
}

func (c *Control) bindingList(ctx context.Context, actor Subject, req *proto.BindingListReq) (*proto.BindingListRes, error) {
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, actor, ActionRead, Resource{Kind: "binding-directory", Tenant: tenantID}); err != nil {
		return nil, err
	}
	c.mu.Lock()
	specs := make([]proto.BindingSpec, 0, len(c.bindings))
	for _, b := range c.bindings {
		// A global binding is visible to every tenant that can lease it; a
		// tenant-scoped one only to its own tenant.
		if b.Tenant != "" && b.Tenant != tenantID {
			continue
		}
		if b.RevokedAt != 0 && !req.IncludeRevoked {
			continue
		}
		specs = append(specs, publicBinding(b))
	}
	c.mu.Unlock()
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Tenant != specs[j].Tenant {
			return specs[i].Tenant < specs[j].Tenant
		}
		return specs[i].ID < specs[j].ID
	})
	return &proto.BindingListRes{Bindings: specs}, nil
}

func (c *Control) bindingRotate(ctx context.Context, actor Subject, req *proto.BindingRotateReq) (*proto.BindingSpec, error) {
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if req.ID == "" || req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "binding id and idempotency key are required")
	}
	if (req.Secret == "") == (req.Source == "") {
		return nil, proto.Err(proto.CodeBadRequest, "rotation needs exactly one of a secret and a source")
	}
	if req.Source != "" && c.opts.SecretResolver == nil {
		return nil, proto.Err(proto.CodeUnsupported, "binding %s has an external source but no resolver is configured", req.ID)
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "binding", ID: req.ID, Tenant: tenantID}); err != nil {
		return nil, err
	}
	scope := actor.Tenant + "|" + actor.ID + "|binding.rotate|" + tenantID
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.BindingSpec
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBindingRotate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.bindings[bindingKey(tenantID, req.ID)]
	if !ok {
		return nil, proto.ErrReason(proto.CodeNotFound, proto.ReasonBindingMissing, "binding %q is not defined", req.ID)
	}
	if current.RevokedAt != 0 {
		return nil, proto.ErrReason(proto.CodeUnauthorized, proto.ReasonRevoked, "binding %q was revoked", req.ID)
	}
	next := cloneBinding(current)
	next.Secret, next.Source = req.Secret, req.Source
	next.Revision++
	next.RotatedAt = c.now().UnixMilli()
	public := publicBinding(next)
	event := c.bindingEvent(proto.EvBindingRotated, next, actor.ID, map[string]any{"previous_revision": current.Revision})
	event.OperationID = req.IdempotencyKey
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO bindings(tenant,id,revision,revoked_at,data) VALUES(?,?,?,?,?)`,
			next.Tenant, next.ID, int64(next.Revision), next.RevokedAt, proto.MustMarshal(&next)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBindingRotate, req, public)
	}, []*proto.Event{event}); err != nil {
		return nil, err
	}
	c.bindings[bindingKey(next.Tenant, next.ID)] = next
	return &public, nil
}

func (c *Control) bindingRevoke(ctx context.Context, actor Subject, req *proto.BindingRevokeReq) (*proto.BindingSpec, error) {
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if req.ID == "" || req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "binding id and idempotency key are required")
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "binding", ID: req.ID, Tenant: tenantID}); err != nil {
		return nil, err
	}
	scope := actor.Tenant + "|" + actor.ID + "|binding.revoke|" + tenantID
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.BindingSpec
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBindingRevoke, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.bindings[bindingKey(tenantID, req.ID)]
	if !ok {
		return nil, proto.ErrReason(proto.CodeNotFound, proto.ReasonBindingMissing, "binding %q is not defined", req.ID)
	}
	next := cloneBinding(current)
	if next.RevokedAt == 0 {
		next.RevokedAt = c.now().UnixMilli()
		next.RevokedReason = req.Reason
		// The credential itself stops being leasable the moment the row is
		// durable. Dropping it here also keeps a revoked secret out of every
		// later snapshot of control state.
		next.Secret, next.Source = "", ""
		next.Revision++
	}
	public := publicBinding(next)
	event := c.bindingEvent(proto.EvBindingRevoked, next, actor.ID, map[string]any{"reason": req.Reason})
	event.OperationID = req.IdempotencyKey
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO bindings(tenant,id,revision,revoked_at,data) VALUES(?,?,?,?,?)`,
			next.Tenant, next.ID, int64(next.Revision), next.RevokedAt, proto.MustMarshal(&next)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBindingRevoke, req, public)
	}, []*proto.Event{event}); err != nil {
		return nil, err
	}
	c.bindings[bindingKey(next.Tenant, next.ID)] = next
	return &public, nil
}

// principalSessionCreate is the one-call session-scoped principal. It creates
// an ephemeral principal and immediately issues the workspace- and
// generation-bound capability it acts with, so an adopter never has to
// assemble the two halves correctly themselves. The capability is refused
// after the workspace moves; the principal is revocable with principal.revoke.
func (c *Control) principalSessionCreate(ctx context.Context, actor Subject, req *proto.PrincipalSessionCreateReq) (*proto.PrincipalSessionCreateRes, error) {
	if c.opts.Principals == nil {
		return nil, proto.Err(proto.CodeUnsupported, "principal authority is not configured")
	}
	if c.opts.SessionCapabilities == nil {
		return nil, proto.Err(proto.CodeUnsupported, "session capability authority is not configured")
	}
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if req.Workspace == "" || req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "workspace and idempotency key are required")
	}
	ttl := c.opts.SessionCapabilityTTL
	if req.TTLSec != 0 {
		ttl = time.Duration(req.TTLSec) * time.Second
		if ttl < time.Second || ttl > time.Hour {
			return nil, proto.Err(proto.CodeBadRequest, "session principal ttl must be between 1 and 3600 seconds")
		}
	}
	roles := append([]string(nil), req.Roles...)
	if len(roles) == 0 {
		roles = []string{"agent"}
	}
	subject := req.Subject
	if subject == "" {
		subject = ids.New("sp")
	}

	c.mu.Lock()
	ws := c.workspaces[req.Workspace]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.Workspace)
	}
	if ws.Tenant != tenantID {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeDenied, proto.ReasonPermissionDenied, "resource belongs to another tenant")
	}
	if ws.State != proto.WSClaimed {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonWorkspaceNotReady,
			"workspace %s is %s, not claimed", req.Workspace, state)
	}
	generation := ws.Generation
	resource := workspaceResource(ws)
	c.mu.Unlock()

	if err := c.check(ctx, actor, ActionAdmin, resource); err != nil {
		return nil, err
	}

	scope := actor.Tenant + "|" + actor.ID + "|principal.session.create|" + tenantID
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	// The bearer is deliberately absent from the replay record: an
	// idempotent replay re-mints the capability rather than handing back a
	// stored one, because a credential must never enter durable state.
	var prior proto.Principal
	replay := false
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpPrincipalSessionCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		replay = true
		subject = prior.ID
	}

	principal := prior
	if !replay {
		created, err := c.opts.Principals.CreatePrincipal(ctx, tenantID, subject, roles, actor.ID)
		if err != nil {
			return nil, mapIdentityError(err)
		}
		principal = created
		event := c.newEvent(proto.EvPrincipalSessionCreated, req.Workspace, actor.ID, "", map[string]any{
			"principal": principal.ID, "roles": roles, "ws": req.Workspace,
			"generation": generation, "ttl_ms": ttl.Milliseconds(),
		})
		event.Tenant = tenantID
		event.Workspace = req.Workspace
		event.Generation = generation
		event.OperationID = req.IdempotencyKey
		if err := c.transact(func(tx *eventlog.Tx) error {
			return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpPrincipalSessionCreate, req, principal)
		}, []*proto.Event{event}); err != nil {
			return nil, err
		}
	}

	token, err := c.opts.SessionCapabilities.IssueSessionCapability(ctx, principal.ID, tenantID, roles, req.Workspace, generation, ttl)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	// Resolution and signing happen off the lock. Revalidate the authority
	// boundary so a capability minted for a generation that has since moved
	// is never handed out.
	c.mu.Lock()
	current := c.workspaces[req.Workspace]
	valid := current != nil && current.State == proto.WSClaimed && current.Generation == generation && current.Tenant == tenantID
	c.mu.Unlock()
	if !valid {
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonGenerationMismatch,
			"workspace %s authority changed while issuing a session principal", req.Workspace)
	}
	return &proto.PrincipalSessionCreateRes{
		Principal: principal, Token: token, Workspace: req.Workspace, Generation: generation,
		ExpiresAt: c.now().Add(ttl).UnixMilli(),
	}, nil
}
