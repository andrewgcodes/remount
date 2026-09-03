package control

import (
	"context"
	"math"
	"slices"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

func exactPrincipalTenant(actor Subject, requested string) (string, error) {
	if requested == "" {
		requested = actor.Tenant
	}
	if requested == "" || requested == "*" {
		return "", proto.Err(proto.CodeBadRequest, "an exact tenant is required")
	}
	if actor.Tenant != requested && actor.Tenant != "*" {
		return "", proto.Err(proto.CodeDenied, "resource belongs to another tenant")
	}
	return requested, nil
}

func (c *Control) principalCreate(ctx context.Context, actor Subject, req *proto.PrincipalCreateReq) (*proto.Principal, error) {
	if c.opts.Principals == nil {
		return nil, proto.Err(proto.CodeUnsupported, "principal authority is not configured")
	}
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if req.Principal == "" || req.IdempotencyKey == "" || len(req.Roles) == 0 {
		return nil, proto.Err(proto.CodeBadRequest, "principal, roles and idempotency key are required")
	}
	if c.opts.Tenants != nil {
		if _, err := c.opts.Tenants.Get(ctx, tenantID); err != nil {
			return nil, mapTenantError(err)
		}
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "principal", ID: req.Principal, Tenant: tenantID}); err != nil {
		return nil, err
	}
	result, err := c.opts.Principals.CreatePrincipal(ctx, tenantID, req.Principal, req.Roles, actor.ID)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	return &result, nil
}

func (c *Control) principalList(ctx context.Context, actor Subject, req *proto.PrincipalListReq) (*proto.PrincipalListRes, error) {
	if c.opts.Principals == nil {
		return nil, proto.Err(proto.CodeUnsupported, "principal authority is not configured")
	}
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "principal-directory", Tenant: tenantID}); err != nil {
		return nil, err
	}
	principals, err := c.opts.Principals.ListPrincipals(ctx, tenantID)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	return &proto.PrincipalListRes{Principals: principals}, nil
}

func (c *Control) principalTokenIssue(ctx context.Context, actor Subject, req *proto.PrincipalTokenIssueReq) (*proto.PrincipalTokenIssueRes, error) {
	if c.opts.Principals == nil {
		return nil, proto.Err(proto.CodeUnsupported, "principal authority is not configured")
	}
	tenantID, err := exactPrincipalTenant(actor, req.Tenant)
	if err != nil {
		return nil, err
	}
	if req.Principal == "" || req.Role == "" || req.IdempotencyKey == "" || req.TTLMS < 1000 || req.TTLMS > (24*time.Hour).Milliseconds() {
		return nil, proto.Err(proto.CodeBadRequest, "principal, role, ttl and idempotency key are required")
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "principal-token", ID: req.Principal, Tenant: tenantID}); err != nil {
		return nil, err
	}
	token, expires, err := c.opts.Principals.IssueAccessToken(ctx, tenantID, req.Principal, req.Role, time.Duration(req.TTLMS)*time.Millisecond)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	return &proto.PrincipalTokenIssueRes{AccessToken: token, ExpiresAt: expires.UnixMilli()}, nil
}

func (c *Control) principalInvite(ctx context.Context, actor Subject, req *proto.PrincipalInviteReq) (*proto.PrincipalTokenIssueRes, error) {
	if req.Tenant == "" || req.TTLMS < 1000 || req.TTLMS > (24*time.Hour).Milliseconds() {
		return nil, proto.Err(proto.CodeBadRequest, "invite requires an exact tenant")
	}
	created, err := c.principalCreate(ctx, actor, &proto.PrincipalCreateReq{Tenant: req.Tenant, Principal: req.Principal, Roles: []string{"operator"}, IdempotencyKey: req.IdempotencyKey})
	if err != nil {
		return nil, err
	}
	_ = created
	return c.principalTokenIssue(ctx, actor, &proto.PrincipalTokenIssueReq{Tenant: req.Tenant, Principal: req.Principal, Role: "operator", TTLMS: req.TTLMS, IdempotencyKey: req.IdempotencyKey})
}

func (c *Control) reauthenticateClient(ctx context.Context, peer string) error {
	if c.opts.Authenticator == nil {
		return nil
	}
	c.mu.Lock()
	hello := c.clients[peer]
	c.mu.Unlock()
	if hello == nil {
		return nil // node or an unauthenticated/unknown peer; dispatch handles it.
	}
	subject, err := c.opts.Authenticator.Authenticate(ctx, Credential{Token: hello.Token, Role: proto.RoleClient, Peer: peer})
	if err != nil || subject.ID == "" || subject.Tenant == "" {
		return proto.Err(proto.CodeDenied, "credential expired or was revoked")
	}
	c.mu.Lock()
	current := c.clients[peer]
	if current != hello {
		c.mu.Unlock()
		return proto.Err(proto.CodeUnauthorized, "client identity changed while authenticating")
	}
	c.subjects[peer] = subject
	c.mu.Unlock()
	return nil
}

func (c *Control) principalRevoke(ctx context.Context, actor Subject, req *proto.PrincipalRevokeReq) (*proto.PrincipalRevokeRes, error) {
	if req.Principal == "" || req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "principal and idempotency key are required")
	}
	tenantID := req.Tenant
	if tenantID == "" {
		tenantID = actor.Tenant
	}
	if tenantID == "" || tenantID == "*" {
		return nil, proto.Err(proto.CodeBadRequest, "an exact tenant is required")
	}
	if err := c.check(ctx, actor, ActionAdmin, Resource{Kind: "principal", ID: req.Principal, Tenant: tenantID}); err != nil {
		return nil, err
	}
	if c.opts.PrincipalRevocations == nil {
		return nil, proto.Err(proto.CodeUnreachable, "principal revocation authority is unavailable")
	}
	scope := actor.Tenant + "|" + actor.ID + "|principal.revoke|" + tenantID
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.PrincipalRevokeRes
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpPrincipalRevoke, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}

	c.mu.Lock()
	updates := make(map[string]proto.Workspace)
	for id, workspace := range c.workspaces {
		// Role- or policy-based access need not put the principal in the ACL.
		// Advance every live workspace in the tenant; the pushed revocation is
		// principal-specific, so other principals and the workspace stay live.
		if workspace.Tenant != tenantID || workspace.State == proto.WSDestroyed {
			continue
		}
		if workspace.AuthzRevision == math.MaxUint64 {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeResourceExhausted, "workspace %s authorization revision space exhausted", id)
		}
		next := *workspace
		next.AuthzRevision++
		next.Revocations = append([]proto.AuthzRevocation(nil), workspace.Revocations...)
		next.Revocations = append(next.Revocations, proto.AuthzRevocation{Revision: next.AuthzRevision, Principal: req.Principal})
		for len(next.Revocations) > proto.MaxRetainedRevocations {
			next.RevocationFloor = next.Revocations[0].Revision
			next.Revocations = next.Revocations[1:]
		}
		next.UpdatedAt = c.now().UnixMilli()
		updates[id] = next
	}
	result := proto.PrincipalRevokeRes{}
	revision, err := c.opts.PrincipalRevocations.RevokePrincipalWith(ctx, tenantID, req.Principal,
		func(tx *eventlog.Tx, identityRevision uint64) error {
			result.Revision = identityRevision
			for id := range updates {
				next := updates[id]
				if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id,data) VALUES(?,?)`, id, proto.MustMarshal(&next)); err != nil {
					return err
				}
				if err := tx.Emit(c.wsEvent(&next, proto.EvIdentityWorkspaceRevoked, actor.ID, next.Node, map[string]any{
					"principal": req.Principal, "identity_revision": identityRevision, "authz_revision": next.AuthzRevision,
				})); err != nil {
					return err
				}
			}
			return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpPrincipalRevoke, req, result)
		})
	if err != nil {
		c.mu.Unlock()
		return nil, mapIdentityError(err)
	}
	result.Revision = revision
	for id, next := range updates {
		*c.workspaces[id] = next
	}
	c.mu.Unlock()
	return &result, nil
}

func mapIdentityError(err error) error {
	if err == nil {
		return nil
	}
	if protocol, ok := err.(*proto.Error); ok {
		return protocol
	}
	return proto.Err(proto.CodeUnreachable, "identity authority unavailable")
}

func (c *Control) sessionCapabilityIssue(ctx context.Context, node string, req *proto.SessionCapabilityIssueReq) (*proto.SessionCapabilityIssueRes, error) {
	if c.opts.SessionCapabilities == nil {
		return nil, proto.Err(proto.CodeUnsupported, "session capability authority is not configured")
	}
	c.mu.Lock()
	workspace := c.workspaces[req.Workspace]
	subject, hasSubject := c.subjects[req.Client]
	valid := workspace != nil && hasSubject && workspace.State == proto.WSClaimed && workspace.Node == node &&
		workspace.Generation == req.Generation && workspace.AuthzRevision == req.AuthzRevision &&
		subject.ID == req.Principal && subject.Tenant == req.Tenant && slices.Equal(subject.Roles, req.Roles)
	var resource Resource
	if workspace != nil {
		resource = workspaceResource(workspace)
	}
	c.mu.Unlock()
	if !valid {
		return nil, proto.Err(proto.CodeDenied, "session capability request has stale authority")
	}
	if err := c.check(ctx, subject, ActionExecute, resource); err != nil {
		return nil, err
	}
	ttl := c.opts.SessionCapabilityTTL
	capability, err := c.opts.SessionCapabilities.IssueSessionCapability(ctx, subject.ID, subject.Tenant, subject.Roles, req.Workspace, req.Generation, ttl)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	c.mu.Lock()
	current := c.workspaces[req.Workspace]
	currentSubject, stillConnected := c.subjects[req.Client]
	valid = current == workspace && stillConnected && current.State == proto.WSClaimed && current.Node == node &&
		current.Generation == req.Generation && current.AuthzRevision == req.AuthzRevision &&
		currentSubject.ID == subject.ID && currentSubject.Tenant == subject.Tenant && slices.Equal(currentSubject.Roles, subject.Roles)
	c.mu.Unlock()
	if !valid {
		return nil, proto.Err(proto.CodeDenied, "session capability authority changed while issuing")
	}
	return &proto.SessionCapabilityIssueRes{Capability: capability, ExpiresAt: c.now().Add(ttl).UnixMilli()}, nil
}

func (c *Control) sessionCapabilityCheck(ctx context.Context, node string, req *proto.SessionCapabilityCheckReq) (*proto.SessionCapabilityCheckRes, error) {
	capability, _, err := c.verifySessionCapabilityForNode(ctx, node, req.Workspace, req.Generation, req.Capability)
	if err != nil {
		return nil, err
	}
	return &proto.SessionCapabilityCheckRes{Principal: capability.Subject.ID, Tenant: capability.Subject.Tenant}, nil
}

func (c *Control) sessionCapabilityRenew(ctx context.Context, node string, req *proto.SessionCapabilityRenewReq) (*proto.SessionCapabilityIssueRes, error) {
	capability, authzRevision, err := c.verifySessionCapabilityForNode(ctx, node, req.Workspace, req.Generation, req.Capability)
	if err != nil {
		return nil, err
	}
	ttl := c.opts.SessionCapabilityTTL
	rotated, err := c.opts.SessionCapabilities.IssueSessionCapability(ctx, capability.Subject.ID, capability.Subject.Tenant,
		capability.Subject.Roles, req.Workspace, req.Generation, ttl)
	if err != nil {
		return nil, mapIdentityError(err)
	}
	c.mu.Lock()
	current := c.workspaces[req.Workspace]
	valid := current != nil && current.State == proto.WSClaimed && current.Node == node &&
		current.Generation == req.Generation && current.AuthzRevision == authzRevision
	c.mu.Unlock()
	if !valid {
		return nil, proto.Err(proto.CodeDenied, "workspace authority changed while renewing session capability")
	}
	return &proto.SessionCapabilityIssueRes{Capability: rotated, ExpiresAt: c.now().Add(ttl).UnixMilli()}, nil
}

func (c *Control) verifySessionCapabilityForNode(ctx context.Context, node, workspaceID string, generation uint64, token string) (SessionCapability, uint64, error) {
	if c.opts.SessionCapabilities == nil {
		return SessionCapability{}, 0, proto.Err(proto.CodeUnsupported, "session capability authority is not configured")
	}
	c.mu.Lock()
	workspace := c.workspaces[workspaceID]
	if workspace == nil || workspace.State != proto.WSClaimed || workspace.Node != node || workspace.Generation != generation {
		c.mu.Unlock()
		return SessionCapability{}, 0, proto.Err(proto.CodeDenied, "workspace assignment is not live")
	}
	tenantID := workspace.Tenant
	authzRevision := workspace.AuthzRevision
	resource := workspaceResource(workspace)
	c.mu.Unlock()
	capability, err := c.opts.SessionCapabilities.VerifyBrokerCapability(ctx, token, tenantID, workspaceID, generation)
	if err != nil {
		return SessionCapability{}, 0, proto.Err(proto.CodeDenied, "session capability is invalid or revoked")
	}
	if err := c.check(ctx, capability.Subject, ActionExecute, resource); err != nil {
		return SessionCapability{}, 0, err
	}
	c.mu.Lock()
	current := c.workspaces[workspaceID]
	valid := current == workspace && current.State == proto.WSClaimed && current.Node == node && current.Generation == generation &&
		current.AuthzRevision == authzRevision
	c.mu.Unlock()
	if !valid {
		return SessionCapability{}, 0, proto.Err(proto.CodeDenied, "workspace assignment changed while checking session capability")
	}
	return capability, authzRevision, nil
}
