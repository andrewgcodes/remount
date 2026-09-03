// Package identity issues and verifies short-lived principal credentials and
// one-time node enrollment credentials. It contains no HTTP login flow; OIDC
// adapters exchange their result for these same signed claims.
package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/control"
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

const (
	kindAccess  = "access"
	kindRefresh = "refresh"
)

// Claims are the signed, security-relevant token body.
type Claims struct {
	ID       string   `json:"jti"`
	Subject  string   `json:"sub"`
	Tenant   string   `json:"tenant"`
	Roles    []string `json:"roles"`
	Kind     string   `json:"kind"`
	IssuedAt int64    `json:"iat"`
	Expires  int64    `json:"exp"`
}

// Enrollment is the non-secret authority attached to a one-time node token.
type Enrollment struct {
	ID        string
	Pool      string
	Tenant    string
	IssuedAt  time.Time
	ExpiresAt time.Time
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

// Event records identity state changes without exposing bearer material.
type Event struct {
	Type    string
	ID      string
	Subject string
	Tenant  string
	Pool    string
}

// Options configure a Manager.
type Options struct {
	PrivateKey ed25519.PrivateKey
	Issuer     string
	Store      Store
	Now        func() time.Time
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	OnEvent    func(context.Context, Event) error
}

// Manager verifies signed principal tokens and issues one-time enrollment
// tokens. Its Store is the durable commit point for revocation and consume.
type Manager struct {
	key        ed25519.PrivateKey
	issuer     string
	store      Store
	now        func() time.Time
	accessTTL  time.Duration
	refreshTTL time.Duration
	onEvent    func(context.Context, Event) error
}

// New constructs a Manager.
func New(opts Options) (*Manager, error) {
	if len(opts.PrivateKey) != ed25519.PrivateKeySize || opts.Store == nil {
		return nil, errors.New("identity: private key and store are required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.AccessTTL <= 0 {
		opts.AccessTTL = time.Hour
	}
	if opts.RefreshTTL <= 0 {
		opts.RefreshTTL = 30 * 24 * time.Hour
	}
	if opts.Issuer == "" {
		opts.Issuer = "remount"
	}
	return &Manager{key: opts.PrivateKey, issuer: opts.Issuer, store: opts.Store, now: opts.Now,
		accessTTL: opts.AccessTTL, refreshTTL: opts.RefreshTTL, onEvent: opts.OnEvent}, nil
}

// IssueTokens returns an access token and a longer-lived refresh token for the same
// principal. No token value is passed to OnEvent.
func (m *Manager) IssueTokens(ctx context.Context, subject, tenant string, roles []string) (string, string, error) {
	roles, err := normalizeRoles(roles)
	if err != nil || strings.TrimSpace(subject) == "" || strings.TrimSpace(tenant) == "" {
		return "", "", errors.New("identity: subject, tenant and valid roles are required")
	}
	now := m.now()
	access, err := m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles,
		Kind: kindAccess, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
	if err != nil {
		return "", "", err
	}
	refresh, err := m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles,
		Kind: kindRefresh, IssuedAt: now.Unix(), Expires: now.Add(m.refreshTTL).Unix()})
	if err != nil {
		return "", "", err
	}
	if err := m.emit(ctx, Event{Type: "identity.issued", Subject: subject, Tenant: tenant}); err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

// Refresh exchanges a live, unrevoked refresh token for a new access token.
func (m *Manager) Refresh(ctx context.Context, token string) (string, error) {
	claims, err := m.verify(ctx, token, kindRefresh)
	if err != nil {
		return "", err
	}
	now := m.now()
	return m.sign(Claims{ID: ids.New("tok"), Subject: claims.Subject, Tenant: claims.Tenant,
		Roles: claims.Roles, Kind: kindAccess, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
}

// Authenticate implements control.Authenticator for client relay hellos.
func (m *Manager) Authenticate(ctx context.Context, credential control.Credential) (control.Subject, error) {
	claims, err := m.verify(ctx, credential.Token, kindAccess)
	if err != nil {
		return control.Subject{}, proto.Err(proto.CodeUnauthorized, "invalid or expired identity token")
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
	if pool == "" || tenant == "" || ttl <= 0 || ttl > 10*time.Minute {
		return "", errors.New("identity: enrollment needs pool, tenant and ttl <= 10m")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := "enroll_" + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	now := m.now()
	enrollment := Enrollment{ID: ids.New("enr"), Pool: pool, Tenant: tenant, IssuedAt: now, ExpiresAt: now.Add(ttl)}
	if err := m.store.PutEnrollment(ctx, hash, enrollment, Event{
		Type: "identity.enrollment_issued", ID: enrollment.ID, Pool: pool, Tenant: tenant,
	}); err != nil {
		return "", err
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
	if !strings.HasPrefix(nodeID, "n_") || len(pubKey) != ed25519.PublicKeySize {
		return control.NodeIdentity{}, proto.Err(proto.CodeUnauthorized, "invalid node identity")
	}
	var hash [32]byte
	if strings.HasPrefix(token, "enroll_") {
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
		Kind: kindAccess, IssuedAt: now.Unix(), Expires: now.Add(m.accessTTL).Unix()})
	if err != nil {
		return control.NodeIdentity{}, err
	}
	return control.NodeIdentity{Tenant: binding.Tenant, Pool: binding.Pool, Labels: cloneLabels(binding.Labels), Fresh: fresh, Token: accessToken}, nil
}

// Check implements control.Authorizer with tenant isolation as the first
// predicate. Operators are tenant administrators; an operator on tenant "*"
// may administer all tenants.
func (m *Manager) Check(_ context.Context, subject control.Subject, action string, resource control.Resource) error {
	if hasRole(subject.Roles, RoleOperator) && (subject.Tenant == "*" || subject.Tenant == resource.Tenant) {
		return nil
	}
	if subject.Tenant == "" || subject.Tenant != resource.Tenant {
		return errors.New("cross-tenant access")
	}
	if action == control.ActionRead && hasAnyRole(subject.Roles, RoleViewer, RoleAgent, RoleNode, RoleService) {
		return nil
	}
	if subject.ID == resource.Owner && (action == control.ActionRead || action == control.ActionWrite || action == control.ActionExecute) &&
		hasAnyRole(subject.Roles, RoleAgent, RoleService) {
		return nil
	}
	if action == control.ActionWrite || action == control.ActionExecute {
		if hasRole(subject.Roles, RoleNode) && resource.Kind == "workspace" {
			return nil
		}
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
	if err != nil || claims.Kind != kind || claims.Expires <= m.now().Unix() || claims.IssuedAt > m.now().Add(time.Minute).Unix() {
		return Claims{}, errors.New("identity: invalid token")
	}
	revoked, err := m.store.Revoked(ctx, claims.ID, m.now())
	if err != nil || revoked {
		return Claims{}, errors.New("identity: revoked token")
	}
	return claims, nil
}

func (m *Manager) parseAndVerify(token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, errors.New("identity: malformed token")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, errors.New("identity: malformed header")
	}
	var header struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		Issuer    string `json:"iss"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil || header.Algorithm != "EdDSA" || header.Type != "RMT" || header.Issuer != m.issuer {
		return Claims{}, errors.New("identity: invalid header")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(m.key.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature) {
		return Claims{}, errors.New("identity: bad signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, errors.New("identity: malformed payload")
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil || claims.ID == "" || claims.Subject == "" || claims.Tenant == "" {
		return Claims{}, errors.New("identity: invalid claims")
	}
	return claims, nil
}

func (m *Manager) emit(ctx context.Context, event Event) error {
	if m.onEvent == nil {
		return nil
	}
	return m.onEvent(ctx, event)
}

func normalizeRoles(roles []string) ([]string, error) {
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
	mu          sync.Mutex
	revoked     map[string]time.Time
	enrollments map[[32]byte]Enrollment
	nodes       map[string]NodeBinding
	events      []Event
}

// NewMemoryStore returns an empty identity store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{revoked: map[string]time.Time{}, enrollments: map[[32]byte]Enrollment{}, nodes: map[string]NodeBinding{}}
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

// PutEnrollment records only the digest of a new enrollment bearer.
func (s *MemoryStore) PutEnrollment(_ context.Context, hash [32]byte, enrollment Enrollment, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	candidate.Labels = cloneLabels(candidate.Labels)
	if candidate.Labels == nil {
		candidate.Labels = map[string]string{}
	}
	candidate.Labels["pool"], candidate.Labels["tenant"] = candidate.Pool, candidate.Tenant
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

var _ control.Authenticator = (*Manager)(nil)
var _ control.Authorizer = (*Manager)(nil)
var _ control.NodeAuthenticator = (*Manager)(nil)

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
