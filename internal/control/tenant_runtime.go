package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/tenant"
)

func tenantOperationID(kind, key string) string {
	sum := sha256.Sum256(proto.MustMarshal(struct{ Kind, Key string }{kind, key}))
	return kind + "-" + hex.EncodeToString(sum[:])
}

func tenantPolicyFromProto(value proto.TenantPolicy) tenant.Policy {
	return tenant.Policy{
		Quotas: tenant.Quotas{
			MaxWorkspaces: value.Quotas.MaxWorkspaces, MaxNodes: value.Quotas.MaxNodes,
			MaxArtifactBytes: value.Quotas.MaxArtifactBytes, MaxActiveSessions: value.Quotas.MaxActiveSessions,
		},
		Retention: tenant.Retention{
			Events:      time.Duration(value.Retention.EventsMS) * time.Millisecond,
			SessionLogs: time.Duration(value.Retention.SessionLogsMS) * time.Millisecond,
			Artifacts:   time.Duration(value.Retention.ArtifactsMS) * time.Millisecond,
			MeterEvents: time.Duration(value.Retention.MeterEventsMS) * time.Millisecond,
		},
		Residency: tenant.Residency{
			AllowedRegions: append([]string(nil), value.Residency.AllowedRegions...),
			RequiredLabels: cloneMap(value.Residency.RequiredLabels),
		},
		Billing: tenant.Billing{StripeCustomerID: value.StripeCustomerID},
		OIDC: tenant.OIDC{Issuer: value.OIDC.Issuer, ClientID: value.OIDC.ClientID, Audience: value.OIDC.Audience,
			Scopes: append([]string(nil), value.OIDC.Scopes...), GroupRoles: cloneRoleMap(value.OIDC.GroupRoles),
			TenantClaim: value.OIDC.TenantClaim, GroupsClaim: value.OIDC.GroupsClaim},
	}
}

func validTenantPolicyWire(value proto.TenantPolicy) bool {
	maximum := (10 * 365 * 24 * time.Hour).Milliseconds()
	minimum := time.Hour.Milliseconds()
	for _, milliseconds := range []int64{value.Retention.EventsMS, value.Retention.SessionLogsMS, value.Retention.ArtifactsMS, value.Retention.MeterEventsMS} {
		if milliseconds < 0 || milliseconds > maximum || (milliseconds != 0 && milliseconds < minimum) {
			return false
		}
	}
	return true
}

func tenantToProto(value tenant.Tenant) proto.Tenant {
	return proto.Tenant{
		ID: value.ID, State: string(value.State), Revision: value.Revision,
		Policy: proto.TenantPolicy{
			Quotas: proto.TenantQuotas{
				MaxWorkspaces: value.Policy.Quotas.MaxWorkspaces, MaxNodes: value.Policy.Quotas.MaxNodes,
				MaxArtifactBytes: value.Policy.Quotas.MaxArtifactBytes, MaxActiveSessions: value.Policy.Quotas.MaxActiveSessions,
			},
			Retention: proto.TenantRetention{
				EventsMS: value.Policy.Retention.Events.Milliseconds(), SessionLogsMS: value.Policy.Retention.SessionLogs.Milliseconds(),
				ArtifactsMS: value.Policy.Retention.Artifacts.Milliseconds(), MeterEventsMS: value.Policy.Retention.MeterEvents.Milliseconds(),
			},
			Residency: proto.TenantResidency{
				AllowedRegions: append([]string(nil), value.Policy.Residency.AllowedRegions...),
				RequiredLabels: cloneMap(value.Policy.Residency.RequiredLabels),
			},
			StripeCustomerID: value.Policy.Billing.StripeCustomerID,
			OIDC: proto.TenantOIDC{Issuer: value.Policy.OIDC.Issuer, ClientID: value.Policy.OIDC.ClientID, Audience: value.Policy.OIDC.Audience,
				Scopes: append([]string(nil), value.Policy.OIDC.Scopes...), GroupRoles: cloneRoleMap(value.Policy.OIDC.GroupRoles),
				TenantClaim: value.Policy.OIDC.TenantClaim, GroupsClaim: value.Policy.OIDC.GroupsClaim},
		},
		CreatedAt: value.CreatedAt.UnixMilli(), UpdatedAt: value.UpdatedAt.UnixMilli(),
	}
}

func cloneRoleMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, roles := range in {
		out[key] = append([]string(nil), roles...)
	}
	return out
}

func mapTenantError(err error) error {
	if err == nil {
		return nil
	}
	var typed *tenant.Error
	if !errors.As(err, &typed) {
		return proto.Err(proto.CodeUnreachable, "tenant authority unavailable")
	}
	code := proto.CodeInternal
	switch typed.Code {
	case tenant.CodeBadRequest:
		code = proto.CodeBadRequest
	case tenant.CodeNotFound:
		code = proto.CodeNotFound
	case tenant.CodeConflict:
		code = proto.CodeConflict
	case tenant.CodeResourceExhausted:
		code = proto.CodeResourceExhausted
	case tenant.CodeUnavailable:
		code = proto.CodeUnreachable
	}
	if code == proto.CodeResourceExhausted {
		// A tenant quota is the one resource_exhausted an SDK must be able to
		// tell apart from control-plane capacity, so it carries a reason.
		return proto.ErrReason(code, proto.ReasonQuotaExceeded, "%s", typed.Error())
	}
	return proto.Err(code, "%s", typed.Error())
}

func (c *Control) requireTenantOperator(ctx context.Context, actor Subject, tenantID string) error {
	if tenantID == "" || tenantID == "*" {
		return proto.Err(proto.CodeBadRequest, "an exact tenant is required")
	}
	if actor.Tenant != tenantID && actor.Tenant != "*" {
		return proto.Err(proto.CodeDenied, "resource belongs to another tenant")
	}
	return c.check(ctx, actor, ActionAdmin, Resource{Kind: "tenant", ID: tenantID, Tenant: tenantID})
}

func (c *Control) tenantCreate(ctx context.Context, actor Subject, req *proto.TenantCreateReq) (*proto.Tenant, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	if actor.Tenant != "*" {
		return nil, proto.Err(proto.CodeDenied, "only a global operator may create tenants")
	}
	if !validTenantPolicyWire(req.Policy) {
		return nil, proto.Err(proto.CodeBadRequest, "invalid tenant retention")
	}
	if err := c.requireTenantOperator(ctx, actor, req.ID); err != nil {
		return nil, err
	}
	created, err := c.opts.Tenants.Create(ctx, req.ID, tenantPolicyFromProto(req.Policy), tenant.Mutation{
		OperationID: req.IdempotencyKey, Actor: actor.ID,
	})
	if err != nil {
		return nil, mapTenantError(err)
	}
	result := tenantToProto(created)
	return &result, nil
}

func (c *Control) tenantGet(ctx context.Context, actor Subject, id string) (*proto.Tenant, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	if err := c.requireTenantOperator(ctx, actor, id); err != nil {
		return nil, err
	}
	value, err := c.opts.Tenants.Get(ctx, id)
	if err != nil {
		return nil, mapTenantError(err)
	}
	result := tenantToProto(value)
	return &result, nil
}

func (c *Control) tenantList(ctx context.Context, actor Subject) (*proto.TenantListRes, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	if actor.Tenant != "*" {
		return nil, proto.Err(proto.CodeDenied, "only a global operator may list tenants")
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "tenant-directory", Tenant: "*"}); err != nil {
		return nil, err
	}
	values, err := c.opts.Tenants.List(ctx)
	if err != nil {
		return nil, mapTenantError(err)
	}
	result := &proto.TenantListRes{Tenants: make([]proto.Tenant, len(values))}
	for index := range values {
		result.Tenants[index] = tenantToProto(values[index])
	}
	return result, nil
}

func (c *Control) tenantUpdate(ctx context.Context, actor Subject, req *proto.TenantUpdateReq) (*proto.Tenant, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	if err := c.requireTenantOperator(ctx, actor, req.ID); err != nil {
		return nil, err
	}
	if !validTenantPolicyWire(req.Policy) {
		return nil, proto.Err(proto.CodeBadRequest, "invalid tenant retention")
	}
	value, err := c.opts.Tenants.UpdatePolicy(ctx, req.ID, tenantPolicyFromProto(req.Policy), tenant.Mutation{
		OperationID: req.IdempotencyKey, Actor: actor.ID, ExpectedRevision: req.ExpectedRevision,
	})
	if err != nil {
		return nil, mapTenantError(err)
	}
	result := tenantToProto(value)
	return &result, nil
}

func (c *Control) tenantSetState(ctx context.Context, actor Subject, req *proto.TenantStateReq) (*proto.Tenant, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	if err := c.requireTenantOperator(ctx, actor, req.ID); err != nil {
		return nil, err
	}
	value, err := c.opts.Tenants.SetState(ctx, req.ID, tenant.State(req.State), tenant.Mutation{
		OperationID: req.IdempotencyKey, Actor: actor.ID, ExpectedRevision: req.ExpectedRevision,
	})
	if err != nil {
		return nil, mapTenantError(err)
	}
	result := tenantToProto(value)
	return &result, nil
}

func (c *Control) tenantUsage(ctx context.Context, actor Subject, req *proto.TenantUsageReq) (*proto.TenantUsage, error) {
	if c.opts.Tenants == nil {
		return nil, proto.Err(proto.CodeUnsupported, "tenant authority is not configured")
	}
	tenantID := req.Tenant
	if tenantID == "" {
		tenantID = actor.Tenant
	}
	if actor.Tenant != tenantID {
		if actor.Tenant != "*" {
			return nil, proto.Err(proto.CodeDenied, "resource belongs to another tenant")
		}
		if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "tenant-usage", Tenant: tenantID}); err != nil {
			return nil, err
		}
	}
	usage, err := c.opts.Tenants.CurrentUsage(ctx, tenantID)
	if err != nil {
		return nil, mapTenantError(err)
	}
	return &proto.TenantUsage{Workspaces: usage.Workspaces, Nodes: usage.Nodes, ArtifactBytes: usage.ArtifactBytes, ActiveSessions: usage.ActiveSessions}, nil
}

// persistWorkspaceCreateWithTenantQuota commits quota, workspace, mutation
// result, and canonical events under the tenant store's one transaction.
// Caller holds c.mu and updates the in-memory workspace only after success.
func (c *Control) persistWorkspaceCreateWithTenantQuota(ctx context.Context, subject Subject, ws *proto.Workspace, scope string, request any, idempotencyKey string) error {
	if idempotencyKey == "" {
		return proto.Err(proto.CodeBadRequest, "workspace create requires an idempotency key under tenant quotas")
	}
	ws.UpdatedAt = c.now().UnixMilli()
	_, err := c.opts.Tenants.Admit(ctx, tenant.Admission{
		Tenant: subject.Tenant, OperationID: tenantOperationID("wsc", idempotencyKey), Actor: subject.ID,
		Resource: tenant.ResourceWorkspace, ResourceID: ws.ID, Amount: 1,
	}, func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO workspaces(id,data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws)); err != nil {
			return err
		}
		if err := c.insertMutationTx(tx.Tx, scope, idempotencyKey, proto.OpWSCreate, request, mutationWorkspaceResult{ID: ws.ID}); err != nil {
			return err
		}
		events := []*proto.Event{c.wsEvent(ws, proto.EvWSCreated, subject.ID, "", ws.Spec)}
		events = append(events, c.volumeAttachEvents(ws, ws.Spec.Volumes, subject.ID, idempotencyKey, "workspace.create")...)
		return stageAll(tx, events)
	})
	return mapTenantError(err)
}

func (c *Control) tenantResidencyAllowsLocked(workspace *proto.Workspace, labels map[string]string) bool {
	if c.opts.Tenants == nil {
		return true
	}
	current, err := c.opts.Tenants.Get(context.Background(), workspace.Tenant)
	allowed := err == nil && current.State != tenant.StateDeleted && tenant.MatchResidency(current.Policy.Residency, labels)
	if !allowed {
		// A counter, not an event: this runs per candidate node inside the
		// placement loop under c.mu, where a durable write would be both a
		// lock violation and an amplifier. The paired durable record is the
		// residency.denied event that EnforceTenantResidency emits for a
		// workspace that is actually held outside its policy.
		metrics.TenantResidencyDenied.Inc()
	}
	return allowed
}
