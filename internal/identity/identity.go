// Package identity issues and verifies short-lived principal credentials and
// one-time node enrollment credentials. Its OIDC device-flow adapter exchanges
// verified provider identity for the same signed claims.
package identity

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// Principal roles.
const (
	RoleOperator = "operator"
	RoleAgent    = "agent"
	RoleNode     = "node"
	RoleViewer   = "viewer"
	RoleService  = "service"
)

// ErrEnrollmentCapacity means live one-time credentials reached admission capacity.
var ErrEnrollmentCapacity = errors.New("identity: enrollment capacity reached")

const (
	kindAccess           = "access"
	kindRefresh          = "refresh"
	kindSession          = "session-cap"
	defaultAudience      = "remount"
	defaultMaxTokenBytes = 16 << 10
	maxRoles             = 8
)

// Claims are the signed, security-relevant token body.
type Claims struct {
	ID         string   `json:"jti"`
	Subject    string   `json:"sub"`
	Tenant     string   `json:"tenant"`
	Roles      []string `json:"roles"`
	Kind       string   `json:"kind"`
	Audience   string   `json:"aud"`
	Revision   uint64   `json:"rev,omitempty"`
	SPIFFEID   string   `json:"spiffe_id,omitempty"`
	Workspace  string   `json:"ws,omitempty"`
	Generation uint64   `json:"gen,omitempty"`
	IssuedAt   int64    `json:"iat"`
	Expires    int64    `json:"exp"`
}

// Enrollment is the non-secret authority attached to a one-time node token.
type Enrollment struct {
	ID        string
	Pool      string
	Tenant    string
	IssuedAt  time.Time
	ExpiresAt time.Time
	Labels    map[string]string
}

// NodeBinding is the durable identity created by consuming an enrollment.
// PubKey is copied at every Store boundary.
type NodeBinding struct {
	NodeID     string
	PubKey     []byte
	Pool       string
	Tenant     string
	Labels     map[string]string
	EnrolledAt time.Time
}

// Store owns revocation, enrollment and node-key durability. EnrollNode must
// atomically consume a live enrollment, bind the candidate key and append its
// event; concurrent hellos cannot both enroll with the same token.
type Store interface {
	Revoked(context.Context, string, time.Time) (bool, error)
	Revoke(context.Context, string, time.Time, Event) error
	PutEnrollment(context.Context, [32]byte, Enrollment, Event) error
	EnrollNode(context.Context, [32]byte, time.Time, NodeBinding, Event) (NodeBinding, bool, bool, error)
}

// PrincipalRevocationStore invalidates every credential minted before a
// principal's current revision. Implementations commit the revision and event
// atomically.
type PrincipalRevocationStore interface {
	PrincipalRevision(context.Context, string, string) (uint64, error)
	RevokePrincipal(context.Context, string, string, time.Time, Event) (uint64, error)
}

// AtomicPrincipalRevocationStore lets the control plane advance its affected
// workspace authorization revisions in the identity transaction.
type AtomicPrincipalRevocationStore interface {
	RevokePrincipalWith(context.Context, string, string, time.Time, Event, func(*eventlog.Tx, uint64) error) (uint64, error)
}

// BoundedEnrollmentStore atomically collects expired enrollments, checks
// admission capacity, and commits a new digest and event.
type BoundedEnrollmentStore interface {
	PutEnrollmentBounded(context.Context, [32]byte, Enrollment, int, Event) error
}

// MaintenanceResult reports bounded durable identity cleanup.
type MaintenanceResult struct {
	Revocations int
	Enrollments int
}

// Maintainer removes expired retained authority with observable events.
type Maintainer interface {
	CollectExpired(context.Context, time.Time, int) (MaintenanceResult, error)
}

// RefreshStore atomically consumes a refresh credential. Exactly one
// concurrent exchange may return consumed=true.
type RefreshStore interface {
	ConsumeRefresh(context.Context, string, time.Time, Event) (bool, error)
}

// Event records identity state changes without exposing bearer material.
type Event struct {
	Type     string
	ID       string
	Subject  string
	Tenant   string
	Pool     string
	Revision uint64
	Actor    string
	Roles    []string
}

// Options configure a Manager.
type Options struct {
	PrivateKey     ed25519.PrivateKey
	Issuer         string
	Audience       string
	Store          Store
	Now            func() time.Time
	AccessTTL      time.Duration
	RefreshTTL     time.Duration
	SessionTTL     time.Duration
	ClockSkew      time.Duration
	MaxTokenBytes  int
	MaxEnrollments int
	OnEvent        func(context.Context, Event) error
}

// Manager verifies signed principal tokens and issues one-time enrollment
// tokens. Its Store is the durable commit point for revocation and consume.
type Manager struct {
	key            ed25519.PrivateKey
	issuer         string
	audience       string
	store          Store
	now            func() time.Time
	accessTTL      time.Duration
	refreshTTL     time.Duration
	sessionTTL     time.Duration
	clockSkew      time.Duration
	maxTokenBytes  int
	maxEnrollments int
	onEvent        func(context.Context, Event) error
}

// New constructs a Manager.
func New(opts Options) (*Manager, error) {
	if len(opts.PrivateKey) != ed25519.PrivateKeySize || opts.Store == nil {
		return nil, errors.New("identity: private key and store are required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.AccessTTL == 0 {
		opts.AccessTTL = time.Hour
	}
	if opts.RefreshTTL == 0 {
		opts.RefreshTTL = 30 * 24 * time.Hour
	}
	if opts.SessionTTL == 0 {
		opts.SessionTTL = 5 * time.Minute
	}
	if opts.ClockSkew == 0 {
		opts.ClockSkew = 30 * time.Second
	}
	if opts.MaxTokenBytes == 0 {
		opts.MaxTokenBytes = defaultMaxTokenBytes
	}
	if opts.MaxEnrollments == 0 {
		opts.MaxEnrollments = 10_000
	}
	if opts.Issuer == "" {
		opts.Issuer = "remount"
	}
	if opts.Audience == "" {
		opts.Audience = defaultAudience
	}
	if opts.AccessTTL < time.Second || opts.AccessTTL > 24*time.Hour || opts.RefreshTTL < time.Second || opts.RefreshTTL > 90*24*time.Hour || opts.SessionTTL < time.Second || opts.SessionTTL > time.Hour || opts.ClockSkew < 0 || opts.ClockSkew > 5*time.Minute || opts.MaxTokenBytes < 512 || opts.MaxTokenBytes > 1<<20 || opts.MaxEnrollments < 1 || opts.MaxEnrollments > 1_000_000 || !validText(opts.Issuer, 512) || !validText(opts.Audience, 256) {
		return nil, errors.New("identity: invalid issuer, audience, token, ttl, or clock-skew bounds")
	}
	key := append(ed25519.PrivateKey(nil), opts.PrivateKey...)
	return &Manager{key: key, issuer: opts.Issuer, audience: opts.Audience, store: opts.Store, now: opts.Now,
		accessTTL: opts.AccessTTL, refreshTTL: opts.RefreshTTL, sessionTTL: opts.SessionTTL, clockSkew: opts.ClockSkew, maxTokenBytes: opts.MaxTokenBytes, maxEnrollments: opts.MaxEnrollments, onEvent: opts.OnEvent}, nil
}

// IssueTokens returns an access token and a longer-lived refresh token for the same
// principal. No token value is passed to OnEvent.
func (m *Manager) IssueTokens(ctx context.Context, subject, tenant string, roles []string) (string, string, error) {
	roles, err := normalizeRoles(roles)
	if err != nil || !validPrincipal(subject, tenant) {
		return "", "", errors.New("identity: subject, tenant and valid roles are required")
	}
	if hasRole(roles, RoleNode) {
		return "", "", errors.New("identity: node role requires one-time enrollment")
	}
	if err := m.ensurePrincipalRoles(ctx, subject, tenant, roles); err != nil {
		return "", "", err
	}
	now := m.now()
	revision, err := m.principalRevision(ctx, tenant, subject)
	if err != nil {
		return "", "", err
	}
	access, err := m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles,
		Kind: kindAccess, Audience: m.audience, Revision: revision, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
	if err != nil {
		return "", "", err
	}
	refresh, err := m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles,
		Kind: kindRefresh, Audience: m.audience, Revision: revision, IssuedAt: now.Unix(), Expires: now.Add(m.refreshTTL).Unix()})
	if err != nil {
		return "", "", err
	}
	if err := m.emit(ctx, Event{Type: "identity.issued", Subject: subject, Tenant: tenant}); err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

// Refresh exchanges a live, unrevoked refresh token for a new access token.
// Deprecated: hosted token endpoints must use RotateRefresh so the bearer is
// single-use. This method remains for local source compatibility.
func (m *Manager) Refresh(ctx context.Context, token string) (string, error) {
	claims, err := m.verify(ctx, token, kindRefresh)
	if err != nil {
		return "", err
	}
	now := m.now()
	return m.sign(Claims{ID: ids.New("tok"), Subject: claims.Subject, Tenant: claims.Tenant,
		Roles: claims.Roles, Kind: kindAccess, Audience: m.audience, Revision: claims.Revision, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
}

// RotateRefresh atomically consumes a refresh token and returns a fresh access
// and refresh pair. A crash after consume can require login again but cannot
// make one refresh bearer produce multiple live families.
func (m *Manager) RotateRefresh(ctx context.Context, token string) (string, string, error) {
	claims, err := m.verify(ctx, token, kindRefresh)
	if err != nil {
		return "", "", err
	}
	store, ok := m.store.(RefreshStore)
	if !ok {
		return "", "", errors.New("identity: refresh rotation is unavailable")
	}
	consumed, err := store.ConsumeRefresh(ctx, claims.ID, time.Unix(claims.Expires, 0), Event{Type: "identity.refresh_rotated", ID: claims.ID, Subject: claims.Subject, Tenant: claims.Tenant})
	if err != nil {
		return "", "", err
	}
	if !consumed {
		return "", "", errors.New("identity: refresh token already consumed")
	}
	return m.IssueTokens(ctx, claims.Subject, claims.Tenant, claims.Roles)
}

// SessionCapability is the verified authority for one principal's process in
// one workspace generation.
type SessionCapability struct {
	Principal  string
	Tenant     string
	Roles      []string
	Workspace  string
	Generation uint64
	SPIFFEID   string
	ExpiresAt  time.Time
	Revision   uint64
}

// SessionCapabilityVerifier is the node-broker admission seam.
type SessionCapabilityVerifier interface {
	VerifySessionCapabilityFor(context.Context, string, string, string, uint64) (SessionCapability, error)
}

// IssueSessionCapability mints a short-lived principal-bound broker token.
func (m *Manager) IssueSessionCapability(ctx context.Context, subject, tenant string, roles []string, workspace string, generation uint64, ttl time.Duration) (string, error) {
	roles, err := normalizeRoles(roles)
	if err != nil || !validPrincipal(subject, tenant) || !validText(workspace, 256) || generation == 0 {
		return "", errors.New("identity: invalid session capability scope")
	}
	if ttl == 0 {
		ttl = m.sessionTTL
	}
	if ttl <= 0 || ttl > m.sessionTTL {
		return "", errors.New("identity: session capability ttl exceeds configured maximum")
	}
	spiffe, err := principalSPIFFEID(tenant, workspace, generation, subject)
	if err != nil {
		return "", err
	}
	revision, err := m.principalRevision(ctx, tenant, subject)
	if err != nil {
		return "", err
	}
	now := m.now()
	return m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles, Kind: kindSession, Audience: m.audience, Revision: revision, SPIFFEID: spiffe, Workspace: workspace, Generation: generation, IssuedAt: now.Unix(), Expires: now.Add(ttl).Unix()})
}

// VerifySessionCapability verifies the principal, revision, workspace and
// generation encoded by a per-session broker capability.
func (m *Manager) VerifySessionCapability(ctx context.Context, token string) (SessionCapability, error) {
	claims, err := m.verify(ctx, token, kindSession)
	if err != nil {
		return SessionCapability{}, err
	}
	expected, err := principalSPIFFEID(claims.Tenant, claims.Workspace, claims.Generation, claims.Subject)
	if err != nil || claims.SPIFFEID != expected {
		return SessionCapability{}, errors.New("identity: invalid session capability")
	}
	return SessionCapability{Principal: claims.Subject, Tenant: claims.Tenant, Roles: append([]string(nil), claims.Roles...), Workspace: claims.Workspace, Generation: claims.Generation, SPIFFEID: claims.SPIFFEID, ExpiresAt: time.Unix(claims.Expires, 0), Revision: claims.Revision}, nil
}

// VerifySessionCapabilityFor additionally binds verification to the broker's
// local workspace generation. Callers must prefer this at request admission.
func (m *Manager) VerifySessionCapabilityFor(ctx context.Context, token, tenant, workspace string, generation uint64) (SessionCapability, error) {
	capability, err := m.VerifySessionCapability(ctx, token)
	if err != nil {
		return SessionCapability{}, err
	}
	if capability.Tenant != tenant || capability.Workspace != workspace || capability.Generation != generation {
		return SessionCapability{}, errors.New("identity: session capability scope mismatch")
	}
	return capability, nil
}

// VerifyBrokerCapability adapts identity's detailed result to the control
// plane's deliberately small node-broker interface.
func (m *Manager) VerifyBrokerCapability(ctx context.Context, token, tenant, workspace string, generation uint64) (control.SessionCapability, error) {
	capability, err := m.VerifySessionCapabilityFor(ctx, token, tenant, workspace, generation)
	if err != nil {
		return control.SessionCapability{}, err
	}
	return control.SessionCapability{
		Subject:   control.Subject{ID: capability.Principal, Tenant: capability.Tenant, Roles: append([]string(nil), capability.Roles...)},
		Workspace: capability.Workspace, Generation: capability.Generation,
	}, nil
}

// RevokePrincipal advances the principal's durable revision, invalidating all
// access, refresh, and session-capability tokens minted before it.
func (m *Manager) RevokePrincipal(ctx context.Context, tenant, subject string) (uint64, error) {
	if !validPrincipal(subject, tenant) {
		return 0, errors.New("identity: invalid principal")
	}
	store, ok := m.store.(PrincipalRevocationStore)
	if !ok {
		return 0, errors.New("identity: principal revocation is unavailable")
	}
	return store.RevokePrincipal(ctx, tenant, subject, m.now(), Event{Type: "identity.principal_revoked", Subject: subject, Tenant: tenant})
}

// RevokePrincipalWith advances the durable revision and commits related
// control-plane rows in the same transaction. Stores without this capability
// fail closed rather than returning a non-atomic success.
func (m *Manager) RevokePrincipalWith(ctx context.Context, tenant, subject string, commit func(*eventlog.Tx, uint64) error) (uint64, error) {
	if !validPrincipal(subject, tenant) || commit == nil {
		return 0, errors.New("identity: tenant, principal and commit callback are required")
	}
	store, ok := m.store.(AtomicPrincipalRevocationStore)
	if !ok {
		return 0, errors.New("identity: atomic principal revocation is unavailable")
	}
	return store.RevokePrincipalWith(ctx, tenant, subject, m.now(), Event{Type: "identity.principal_revoked", Subject: subject, Tenant: tenant}, commit)
}

// Authenticate implements control.Authenticator for client relay hellos.
func (m *Manager) Authenticate(ctx context.Context, credential control.Credential) (control.Subject, error) {
	claims, err := m.verify(ctx, credential.Token, kindAccess)
	if err != nil {
		return control.Subject{}, proto.Err(proto.CodeUnauthorized, "invalid or expired identity token")
	}
	if credential.Role == proto.RoleClient && hasRole(claims.Roles, RoleNode) {
		return control.Subject{}, proto.Err(proto.CodeUnauthorized, "node credential cannot authenticate a client peer")
	}
	if !hasRole(claims.Roles, RoleNode) {
		if store, ok := m.store.(PrincipalDirectoryStore); ok {
			principal, lookupErr := store.GetPrincipal(ctx, claims.Tenant, claims.Subject)
			rolesLive := lookupErr == nil
			for _, role := range claims.Roles {
				rolesLive = rolesLive && slices.Contains(principal.Roles, role)
			}
			if lookupErr != nil || principal.Revision != claims.Revision || !rolesLive {
				return control.Subject{}, proto.Err(proto.CodeUnauthorized, "principal role assignment changed or was removed")
			}
		}
	}
	return control.Subject{ID: claims.Subject, Tenant: claims.Tenant, Roles: append([]string(nil), claims.Roles...)}, nil
}

// Revoke makes a token unusable before returning, then emits an audit event.
func (m *Manager) Revoke(ctx context.Context, token string) error {
	claims, err := m.parseAndVerify(token)
	if err != nil {
		return err
	}
	return m.store.Revoke(ctx, claims.ID, time.Unix(claims.Expires, 0), Event{
		Type: "identity.revoked", ID: claims.ID, Subject: claims.Subject, Tenant: claims.Tenant,
	})
}

// Issue implements pool.EnrollmentSource. The random bearer is returned once;
// only its SHA-256 digest is committed to the Store.
func (m *Manager) IssueEnrollment(ctx context.Context, pool, tenant string, ttl time.Duration) (string, error) {
	return m.issueEnrollment(ctx, pool, tenant, nil, ttl)
}

// IssueWithLabels binds trusted scheduler labels to a one-time enrollment.
// A node cannot expand or replace them in its hello.
func (m *Manager) IssueWithLabels(ctx context.Context, pool, tenant string, labels map[string]string, ttl time.Duration) (string, error) {
	return m.issueEnrollment(ctx, pool, tenant, labels, ttl)
}

func (m *Manager) issueEnrollment(ctx context.Context, pool, tenant string, labels map[string]string, ttl time.Duration) (string, error) {
	if !validText(pool, 256) || !validText(tenant, 253) || ttl < time.Second || ttl > 10*time.Minute || !validLabels(labels) {
		return "", errors.New("identity: enrollment needs pool, tenant and ttl <= 10m")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := "enroll_" + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	now := m.now()
	enrollment := Enrollment{ID: ids.New("enr"), Pool: pool, Tenant: tenant, IssuedAt: now, ExpiresAt: now.Add(ttl), Labels: cloneLabels(labels)}
	event := Event{
		Type: "identity.enrollment_issued", ID: enrollment.ID, Pool: pool, Tenant: tenant,
	}
	var storeErr error
	if bounded, ok := m.store.(BoundedEnrollmentStore); ok {
		storeErr = bounded.PutEnrollmentBounded(ctx, hash, enrollment, m.maxEnrollments, event)
	} else {
		storeErr = m.store.PutEnrollment(ctx, hash, enrollment, event)
	}
	if storeErr != nil {
		return "", storeErr
	}
	return token, nil
}

// Issue implements the enrollment-source interface used by node pools.
func (m *Manager) Issue(ctx context.Context, pool, tenant string, ttl time.Duration) (string, error) {
	return m.IssueEnrollment(ctx, pool, tenant, ttl)
}

// AuthenticateNode implements control.NodeAuthenticator. An already-bound
// node reconnects by proving possession of the same key; its enrollment token
// may have been consumed and is never consulted again. A first connection
// consumes and binds the enrollment in one Store transaction.
func (m *Manager) AuthenticateNode(ctx context.Context, nodeID, token string, pubKey []byte) (control.NodeIdentity, error) {
	if !strings.HasPrefix(nodeID, "n_") || !validText(nodeID, 128) || len(pubKey) != ed25519.PublicKeySize {
		return control.NodeIdentity{}, proto.Err(proto.CodeUnauthorized, "invalid node identity")
	}
	var hash [32]byte
	if validEnrollmentToken(token) {
		hash = sha256.Sum256([]byte(token))
	}
	candidate := NodeBinding{NodeID: nodeID, PubKey: append([]byte(nil), pubKey...), EnrolledAt: m.now()}
	binding, fresh, ok, err := m.store.EnrollNode(ctx, hash, m.now(), candidate, Event{
		Type: proto.EvNodeEnrolled, ID: nodeID,
	})
	if err != nil {
		return control.NodeIdentity{}, err
	}
	if !ok || subtle.ConstantTimeCompare(binding.PubKey, pubKey) != 1 {
		return control.NodeIdentity{}, proto.Err(proto.CodeUnauthorized, "invalid or expired node enrollment")
	}
	now := m.now()
	accessToken, err := m.sign(Claims{ID: ids.New("tok"), Subject: nodeID, Tenant: binding.Tenant, Roles: []string{RoleNode},
		Kind: kindAccess, Audience: m.audience, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
	if err != nil {
		return control.NodeIdentity{}, err
	}
	return control.NodeIdentity{Tenant: binding.Tenant, Pool: binding.Pool, Labels: cloneLabels(binding.Labels), Fresh: fresh, Token: accessToken}, nil
}

// Check implements control.Authorizer with tenant isolation as the first
// predicate. Operators are tenant administrators; an operator on tenant "*"
// may administer all tenants.
func (m *Manager) Check(_ context.Context, subject control.Subject, action string, resource control.Resource) error {
	// Tenant isolation is deliberately evaluated before role or ACL. An
	// operator is cross-tenant only when its signed tenant is the explicit
	// wildcard; no same-role shortcut may leak resource existence first.
	if subject.Tenant == "" || subject.Tenant != resource.Tenant {
		if !(subject.Tenant == "*" && hasRole(subject.Roles, RoleOperator)) {
			return errors.New("cross-tenant access")
		}
	}
	if hasRole(subject.Roles, RoleOperator) {
		return nil
	}
	if hasRole(subject.Roles, RoleNode) && resource.Kind == "workspace" && (action == control.ActionWrite || action == control.ActionExecute) {
		return nil
	}
	if action == control.ActionRead && hasAnyRole(subject.Roles, RoleViewer, RoleAgent, RoleService) {
		if subject.ID == resource.Owner || slices.Contains(resource.Readers, subject.ID) || slices.Contains(resource.Writers, subject.ID) {
			return nil
		}
	}
	if (action == control.ActionWrite || action == control.ActionExecute) && hasAnyRole(subject.Roles, RoleAgent, RoleService) &&
		(subject.ID == resource.Owner || slices.Contains(resource.Writers, subject.ID)) {
		return nil
	}
	return errors.New("role does not permit action")
}

func (m *Manager) sign(claims Claims) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "RMT", "iss": m.issuer})
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	sig := ed25519.Sign(m.key, []byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (m *Manager) verify(ctx context.Context, token, kind string) (Claims, error) {
	claims, err := m.parseAndVerify(token)
	now := m.now()
	maxTTL := m.accessTTL
	if kind == kindRefresh {
		maxTTL = m.refreshTTL
	} else if kind == kindSession {
		maxTTL = m.sessionTTL
	}
	lifetimeSeconds := claims.Expires - claims.IssuedAt
	if err != nil || claims.Kind != kind || claims.Audience != m.audience || claims.Expires <= claims.IssuedAt || lifetimeSeconds <= 0 || lifetimeSeconds > int64(maxTTL/time.Second) || !now.Before(time.Unix(claims.Expires, 0).Add(m.clockSkew)) || time.Unix(claims.IssuedAt, 0).After(now.Add(m.clockSkew)) {
		return Claims{}, errors.New("identity: invalid token")
	}
	revoked, err := m.store.Revoked(ctx, claims.ID, m.now())
	if err != nil || revoked {
		return Claims{}, errors.New("identity: revoked token")
	}
	revision, err := m.principalRevision(ctx, claims.Tenant, claims.Subject)
	if err != nil || revision != claims.Revision {
		return Claims{}, errors.New("identity: principal revoked")
	}
	return claims, nil
}

func (m *Manager) parseAndVerify(token string) (Claims, error) {
	if len(token) < 16 || len(token) > m.maxTokenBytes || strings.Count(token, ".") != 2 {
		return Claims{}, errors.New("identity: malformed token")
	}
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return Claims{}, errors.New("identity: malformed token")
	}
	headerBytes, err := decodeSegment(parts[0], 2048)
	if err != nil {
		return Claims{}, errors.New("identity: malformed header")
	}
	var header struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		Issuer    string `json:"iss"`
	}
	if err := decodeStrictJSON(headerBytes, &header); err != nil || header.Algorithm != "EdDSA" || header.Type != "RMT" || header.Issuer != m.issuer {
		return Claims{}, errors.New("identity: invalid header")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(m.key.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature) {
		return Claims{}, errors.New("identity: bad signature")
	}
	payload, err := decodeSegment(parts[1], m.maxTokenBytes)
	if err != nil {
		return Claims{}, errors.New("identity: malformed payload")
	}
	var claims Claims
	if err := decodeStrictJSON(payload, &claims); err != nil || !validText(claims.ID, 128) || !validPrincipal(claims.Subject, claims.Tenant) || claims.Audience != m.audience || claims.Revision > math.MaxInt64 {
		return Claims{}, errors.New("identity: invalid claims")
	}
	normalized, err := normalizeRoles(claims.Roles)
	if err != nil || !slices.Equal(normalized, claims.Roles) {
		return Claims{}, errors.New("identity: invalid roles")
	}
	return claims, nil
}

func (m *Manager) principalRevision(ctx context.Context, tenant, subject string) (uint64, error) {
	store, ok := m.store.(PrincipalRevocationStore)
	if !ok {
		return 0, nil
	}
	return store.PrincipalRevision(ctx, tenant, subject)
}

func (m *Manager) emit(ctx context.Context, event Event) error {
	if m.onEvent == nil {
		return nil
	}
	return m.onEvent(ctx, event)
}

func normalizeRoles(roles []string) ([]string, error) {
	if len(roles) == 0 || len(roles) > maxRoles {
		return nil, errors.New("identity: role count outside configured bound")
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(roles))
	for _, role := range roles {
		role = strings.ToLower(strings.TrimSpace(role))
		switch role {
		case RoleOperator, RoleAgent, RoleNode, RoleViewer, RoleService:
		default:
			return nil, fmt.Errorf("identity: unknown role %q", role)
		}
		if !seen[role] {
			seen[role] = true
			out = append(out, role)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("identity: at least one role is required")
	}
	return out, nil
}

func decodeSegment(encoded string, max int) ([]byte, error) {
	if encoded == "" || len(encoded) > base64.RawURLEncoding.EncodedLen(max) {
		return nil, errors.New("identity: token segment exceeds bound")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) > max {
		return nil, errors.New("identity: malformed token segment")
	}
	return decoded, nil
}

func decodeStrictJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("identity: trailing JSON")
	}
	return nil
}

func validText(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f {
			return false
		}
	}
	return true
}

func validPrincipal(subject, tenant string) bool {
	return validText(subject, 256) && validText(tenant, 253)
}

func validEnrollmentToken(token string) bool {
	if !strings.HasPrefix(token, "enroll_") || len(token) != len("enroll_")+base64.RawURLEncoding.EncodedLen(32) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "enroll_"))
	return err == nil && len(raw) == 32
}

func validLabels(labels map[string]string) bool {
	if len(labels) > 64 {
		return false
	}
	for key, value := range labels {
		if !validText(key, 128) || !validText(value, 1024) {
			return false
		}
	}
	return true
}

func principalSPIFFEID(tenant, workspace string, generation uint64, principal string) (string, error) {
	if !validPrincipal(principal, tenant) || !validText(workspace, 256) || generation == 0 || !validTrustDomain(tenant) {
		return "", errors.New("identity: invalid SPIFFE capability scope")
	}
	return "spiffe://" + tenant + "/ws/" + url.PathEscape(workspace) + "/gen/" + fmt.Sprint(generation) + "/principal/" + url.PathEscape(principal), nil
}

func validTrustDomain(tenant string) bool {
	if strings.ToLower(tenant) != tenant || strings.ContainsAny(tenant, "/:@") {
		return false
	}
	for _, label := range strings.Split(tenant, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
				return false
			}
		}
	}
	return true
}

func hasRole(roles []string, want string) bool {
	for _, role := range roles {
		if subtle.ConstantTimeCompare([]byte(role), []byte(want)) == 1 {
			return true
		}
	}
	return false
}

func hasAnyRole(roles []string, candidates ...string) bool {
	for _, candidate := range candidates {
		if hasRole(roles, candidate) {
			return true
		}
	}
	return false
}

// MemoryStore is a concurrency-safe Store for standalone mode and tests.
type MemoryStore struct {
	mu                 sync.Mutex
	revoked            map[string]time.Time
	enrollments        map[[32]byte]Enrollment
	nodes              map[string]NodeBinding
	events             []Event
	principalRevisions map[string]uint64
	principals         map[string]proto.Principal
}

// NewMemoryStore returns an empty identity store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{revoked: map[string]time.Time{}, enrollments: map[[32]byte]Enrollment{}, nodes: map[string]NodeBinding{}, principalRevisions: map[string]uint64{}, principals: map[string]proto.Principal{}}
}

// CreatePrincipal commits a role assignment and its audit event.
func (s *MemoryStore) CreatePrincipal(_ context.Context, principal proto.Principal, event Event) (proto.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := principal.Tenant + "\x00" + principal.ID
	if _, exists := s.principals[key]; exists {
		return proto.Principal{}, ErrPrincipalExists
	}
	principal = clonePrincipal(principal)
	s.principals[key] = principal
	s.events = append(s.events, event)
	return clonePrincipal(principal), nil
}

// GetPrincipal returns one exact role assignment.
func (s *MemoryStore) GetPrincipal(_ context.Context, tenant, subject string) (proto.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	principal, ok := s.principals[tenant+"\x00"+subject]
	if !ok {
		return proto.Principal{}, ErrPrincipalNotFound
	}
	principal.Revision = s.principalRevisions[tenant+"\x00"+subject]
	return clonePrincipal(principal), nil
}

// ListPrincipals returns the tenant directory in stable subject order.
func (s *MemoryStore) ListPrincipals(_ context.Context, tenant string) ([]proto.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]proto.Principal, 0)
	for key, principal := range s.principals {
		if principal.Tenant != tenant {
			continue
		}
		principal.Revision = s.principalRevisions[key]
		result = append(result, clonePrincipal(principal))
	}
	slices.SortFunc(result, func(a, b proto.Principal) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}

// UpsertPrincipalRoles synchronizes federated assignments and revisions.
func (s *MemoryStore) UpsertPrincipalRoles(_ context.Context, principal proto.Principal, event Event) (proto.Principal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := principal.Tenant + "\x00" + principal.ID
	current, exists := s.principals[key]
	if exists && slices.Equal(current.Roles, principal.Roles) {
		current.Revision = s.principalRevisions[key]
		return clonePrincipal(current), false, nil
	}
	if exists {
		principal.CreatedAt = current.CreatedAt
		s.principalRevisions[key]++
	} else {
		event.Type = proto.EvIdentityPrincipalCreated
	}
	principal.Revision = s.principalRevisions[key]
	s.principals[key] = clonePrincipal(principal)
	event.Revision = principal.Revision
	s.events = append(s.events, event)
	return clonePrincipal(principal), true, nil
}

// PrincipalRevision returns the durable invalidation epoch for a principal.
func (s *MemoryStore) PrincipalRevision(_ context.Context, tenant, subject string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.principalRevisions[tenant+"\x00"+subject], nil
}

// RevokePrincipal atomically advances a principal revision and records it.
func (s *MemoryStore) RevokePrincipal(_ context.Context, tenant, subject string, _ time.Time, event Event) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenant + "\x00" + subject
	current := s.principalRevisions[key]
	if current == math.MaxInt64 {
		return 0, errors.New("identity: principal revision exhausted")
	}
	current++
	s.principalRevisions[key] = current
	event.Revision = current
	s.events = append(s.events, event)
	return current, nil
}

// Revoked reports whether a token id is currently revoked.
func (s *MemoryStore) Revoked(_ context.Context, id string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.revoked[id]
	if ok && !expires.After(now) {
		delete(s.revoked, id)
		ok = false
	}
	return ok, nil
}

// Revoke records a token id until its natural expiry.
func (s *MemoryStore) Revoke(_ context.Context, id string, expires time.Time, event Event) error {
	s.mu.Lock()
	if existing, ok := s.revoked[id]; ok && !expires.After(existing) {
		s.mu.Unlock()
		return nil
	}
	s.revoked[id] = expires
	s.events = append(s.events, event)
	s.mu.Unlock()
	return nil
}

// ConsumeRefresh atomically records the bearer as spent.
func (s *MemoryStore) ConsumeRefresh(_ context.Context, id string, expires time.Time, event Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.revoked[id]; exists {
		return false, nil
	}
	s.revoked[id] = expires
	s.events = append(s.events, event)
	return true, nil
}

// PutEnrollment records only the digest of a new enrollment bearer.
func (s *MemoryStore) PutEnrollment(_ context.Context, hash [32]byte, enrollment Enrollment, event Event) error {
	return s.PutEnrollmentBounded(context.Background(), hash, enrollment, math.MaxInt, event)
}

// PutEnrollmentBounded admits a live enrollment without exceeding max.
func (s *MemoryStore) PutEnrollmentBounded(_ context.Context, hash [32]byte, enrollment Enrollment, max int, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for digest, existing := range s.enrollments {
		if !existing.ExpiresAt.After(enrollment.IssuedAt) {
			delete(s.enrollments, digest)
			s.events = append(s.events, Event{Type: "identity.enrollment_expired", ID: existing.ID, Pool: existing.Pool, Tenant: existing.Tenant})
		}
	}
	if len(s.enrollments) >= max {
		return ErrEnrollmentCapacity
	}
	if _, exists := s.enrollments[hash]; exists {
		return errors.New("identity: enrollment digest collision")
	}
	s.enrollments[hash] = enrollment
	s.events = append(s.events, event)
	return nil
}

// EnrollNode atomically verifies an existing binding or consumes one live
// enrollment and creates the binding with its event.
func (s *MemoryStore) EnrollNode(_ context.Context, hash [32]byte, now time.Time, candidate NodeBinding, event Event) (NodeBinding, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.nodes[candidate.NodeID]; ok {
		return cloneBinding(existing), false, true, nil
	}
	if candidate.NodeID == "" || len(candidate.PubKey) != ed25519.PublicKeySize {
		return NodeBinding{}, false, false, nil
	}
	enrollment, ok := s.enrollments[hash]
	if !ok {
		return NodeBinding{}, false, false, nil
	}
	delete(s.enrollments, hash)
	if !enrollment.ExpiresAt.After(now) {
		s.events = append(s.events, Event{Type: "identity.enrollment_expired", ID: enrollment.ID, Pool: enrollment.Pool, Tenant: enrollment.Tenant})
		return NodeBinding{}, false, false, nil
	}
	candidate.Pool, candidate.Tenant = enrollment.Pool, enrollment.Tenant
	candidate.Labels = cloneLabels(enrollment.Labels)
	if candidate.Labels == nil {
		candidate.Labels = map[string]string{}
	}
	candidate.Labels["pool"], candidate.Labels["tenant"] = candidate.Pool, candidate.Tenant
	candidate.Labels["remount.pool"], candidate.Labels["remount.node"] = candidate.Pool, candidate.NodeID
	s.nodes[candidate.NodeID] = cloneBinding(candidate)
	event.Pool, event.Tenant = candidate.Pool, candidate.Tenant
	s.events = append(s.events, event)
	return cloneBinding(candidate), true, true, nil
}

// Events returns a copy of the state-change audit records committed by this
// store. Production stores append these to the canonical event log instead.
func (s *MemoryStore) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// CollectExpired removes at most limit expired revocations and enrollments.
func (s *MemoryStore) CollectExpired(_ context.Context, now time.Time, limit int) (MaintenanceResult, error) {
	if limit < 1 || limit > 10_000 {
		return MaintenanceResult{}, errors.New("identity: invalid maintenance limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result MaintenanceResult
	for id, expiry := range s.revoked {
		if result.Revocations+result.Enrollments >= limit {
			break
		}
		if !expiry.After(now) {
			delete(s.revoked, id)
			result.Revocations++
			s.events = append(s.events, Event{Type: "identity.revocation_expired", ID: id})
		}
	}
	for digest, enrollment := range s.enrollments {
		if result.Revocations+result.Enrollments >= limit {
			break
		}
		if !enrollment.ExpiresAt.After(now) {
			delete(s.enrollments, digest)
			result.Enrollments++
			s.events = append(s.events, Event{Type: "identity.enrollment_expired", ID: enrollment.ID, Pool: enrollment.Pool, Tenant: enrollment.Tenant})
		}
	}
	return result, nil
}

var _ control.Authenticator = (*Manager)(nil)
var _ control.Authorizer = (*Manager)(nil)
var _ control.NodeAuthenticator = (*Manager)(nil)
var _ SessionCapabilityVerifier = (*Manager)(nil)
var _ Maintainer = (*MemoryStore)(nil)

func cloneLabels(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneBinding(in NodeBinding) NodeBinding {
	in.PubKey = append([]byte(nil), in.PubKey...)
	in.Labels = cloneLabels(in.Labels)
	return in
}
