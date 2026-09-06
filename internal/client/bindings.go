package client

import (
	"context"
	"strings"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// OperationKey resolves one logical mutation's idempotency key: the
// caller-supplied one when present, otherwise a fresh key. It is exported so
// the public SDK wrapper applies exactly the convention the methods that own
// their request struct already apply.
func OperationKey(options []OperationOption) string {
	if key, set := operationKey(options); set {
		return key
	}
	return ids.New("idem")
}

// CreateBinding defines a tenant-scoped brokered credential. The secret is
// write-only: it is accepted here and never returned by any later call.
func (c *Client) CreateBinding(ctx context.Context, spec proto.BindingSpec, options ...OperationOption) (*proto.BindingSpec, error) {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	var created proto.BindingSpec
	err := c.call(ctx, proto.PeerControl, proto.OpBindingCreate,
		proto.BindingCreateReq{Binding: spec, IdempotencyKey: idem}, &created)
	return &created, err
}

// ListBindings returns the bindings visible to the caller's tenant, without
// secrets. Revoked bindings are omitted unless includeRevoked is set; their
// rows are retained so an audit can resolve an id seen in an older event.
func (c *Client) ListBindings(ctx context.Context, tenant string, includeRevoked bool) ([]proto.BindingSpec, error) {
	var response proto.BindingListRes
	err := c.call(ctx, proto.PeerControl, proto.OpBindingList,
		proto.BindingListReq{Tenant: tenant, IncludeRevoked: includeRevoked}, &response)
	return response.Bindings, err
}

// GetBinding returns one binding definition without its secret.
func (c *Client) GetBinding(ctx context.Context, tenant, id string) (*proto.BindingSpec, error) {
	var spec proto.BindingSpec
	err := c.call(ctx, proto.PeerControl, proto.OpBindingGet, proto.BindingGetReq{ID: id, Tenant: tenant}, &spec)
	return &spec, err
}

// RotateBinding replaces the credential behind a binding and bumps its
// revision. Nodes holding a lease from the previous revision re-lease within
// one renew interval; the old secret stops being substituted at that point.
func (c *Client) RotateBinding(ctx context.Context, req proto.BindingRotateReq, options ...OperationOption) (*proto.BindingSpec, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var spec proto.BindingSpec
	err := c.call(ctx, proto.PeerControl, proto.OpBindingRotate, req, &spec)
	return &spec, err
}

// RevokeBinding permanently stops substitution for a binding. It does not
// revoke the credential at the provider: rotate or delete the key there too.
func (c *Client) RevokeBinding(ctx context.Context, tenant, id, reason string, options ...OperationOption) (*proto.BindingSpec, error) {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	var spec proto.BindingSpec
	err := c.call(ctx, proto.PeerControl, proto.OpBindingRevoke,
		proto.BindingRevokeReq{ID: id, Tenant: tenant, Reason: reason, IdempotencyKey: idem}, &spec)
	return &spec, err
}

// CreateSessionPrincipal mints an ephemeral principal and the workspace- and
// generation-bound capability it acts with, in one call. The returned token is
// delivered exactly once; it is refused after the workspace moves, and
// RevokePrincipal invalidates it immediately.
func (c *Client) CreateSessionPrincipal(ctx context.Context, req proto.PrincipalSessionCreateReq, options ...OperationOption) (*proto.PrincipalSessionCreateRes, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var res proto.PrincipalSessionCreateRes
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalSessionCreate, req, &res)
	return &res, err
}

// CredentialFilter selects credential-use and egress-decision events. Every
// field is optional; an empty filter returns every such event from Since.
type CredentialFilter struct {
	// WS limits the stream to one workspace. It is also the server-side event
	// stream selector, so setting it is materially cheaper than filtering.
	WS string
	// Binding, Host and Decision match the audit payload the node records.
	Binding  string
	Host     string
	Decision string
	// Since is the first event sequence to read; zero starts at the beginning.
	Since uint64
}

// CredentialEvent is one decision the broker recorded: who used which binding,
// for which workspace, against which host, with what outcome. It never carries
// a credential value.
type CredentialEvent struct {
	Seq        uint64
	At         time.Time
	Type       string
	Tenant     string
	WS         string
	Generation uint64
	Node       string
	Principal  string
	Binding    string
	Host       string
	Method     string
	Path       string
	Decision   string
	Reason     string
	Status     int64
	Event      proto.Event
}

// credentialEventTypes are the audit events the broker emits per request.
var credentialEventTypes = map[string]struct{}{
	proto.EvCredUsed:       {},
	proto.EvEgressAllowed:  {},
	proto.EvEgressDenied:   {},
	proto.EvEgressRedacted: {},
}

// CredentialEvents returns the credential-use audit trail matching filter.
//
// The filtering is client-side for now: the control plane streams a
// workspace's events and this selects the credential decisions from them. A
// server-side filter is a compatible later addition, so callers should keep
// using this rather than reimplementing the payload decoding.
func (c *Client) CredentialEvents(ctx context.Context, filter CredentialFilter) ([]CredentialEvent, error) {
	events, err := c.ReadEvents(ctx, filter.Since, filter.WS)
	if err != nil {
		return nil, err
	}
	out := make([]CredentialEvent, 0, len(events))
	for _, event := range events {
		decoded, ok := decodeCredentialEvent(event)
		if !ok {
			continue
		}
		if filter.Binding != "" && decoded.Binding != filter.Binding {
			continue
		}
		if filter.Host != "" && !strings.EqualFold(decoded.Host, filter.Host) {
			continue
		}
		if filter.Decision != "" && decoded.Decision != filter.Decision {
			continue
		}
		out = append(out, decoded)
	}
	return out, nil
}

// credentialAudit mirrors the payload the node writes for a broker decision.
// Only non-sensitive fields are named here; a credential value is never part
// of the payload in the first place.
type credentialAudit struct {
	Generation uint64 `cbor:"generation"`
	Decision   string `cbor:"decision"`
	Binding    string `cbor:"binding"`
	Host       string `cbor:"host"`
	Method     string `cbor:"method"`
	Path       string `cbor:"path"`
	Reason     string `cbor:"reason"`
	Status     int64  `cbor:"status"`
}

func decodeCredentialEvent(event proto.Event) (CredentialEvent, bool) {
	if _, ok := credentialEventTypes[event.Type]; !ok {
		return CredentialEvent{}, false
	}
	var audit credentialAudit
	if len(event.Payload) > 0 {
		// A payload this client cannot decode is still a real decision; report
		// what the envelope says rather than dropping the audit record.
		_ = proto.Unmarshal(event.Payload, &audit)
	}
	ws := event.Workspace
	if ws == "" {
		ws = event.Stream
	}
	generation := event.Generation
	if generation == 0 {
		generation = audit.Generation
	}
	return CredentialEvent{
		Seq: event.Seq, At: event.Time(), Type: event.Type, Tenant: event.Tenant,
		WS: ws, Generation: generation, Node: event.Node, Principal: event.Principal,
		Binding: audit.Binding, Host: audit.Host, Method: audit.Method, Path: audit.Path,
		Decision: audit.Decision, Reason: audit.Reason, Status: audit.Status, Event: event,
	}, true
}
