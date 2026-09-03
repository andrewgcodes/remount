package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"
)

var (
	// ErrAuthorizationPending means the user has not completed device login.
	ErrAuthorizationPending = errors.New("identity: OIDC authorization pending")
	// ErrSlowDown means the issuer asked the client to increase its poll interval.
	ErrSlowDown = errors.New("identity: OIDC polling must slow down")
	// ErrAccessDenied means the user or issuer denied the device request.
	ErrAccessDenied = errors.New("identity: OIDC device access denied")
	// ErrExpiredDeviceCode means the one-time device code is no longer live.
	ErrExpiredDeviceCode = errors.New("identity: OIDC device code expired")
)

// OIDCDiscovery is the bounded subset of provider metadata used by device flow.
type OIDCDiscovery struct {
	Issuer                      string `json:"issuer"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	JWKSURI                     string `json:"jwks_uri"`
}

// DeviceAuthorization is safe-to-display device-flow state. DeviceCode is a
// bearer and must never be logged or included in an event.
type DeviceAuthorization struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete,omitempty"`
	ExpiresIn               int64  `json:"expires_in"`
	Interval                int64  `json:"interval,omitempty"`
}

// DeviceTokenResponse is returned by one successful token poll.
type DeviceTokenResponse struct {
	AccessToken  string `json:"access_token,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	IDToken      string `json:"id_token"`
}

// OIDCClaims are cryptographically verified claims returned by an
// IDTokenVerifier. The OIDC client independently rechecks semantic bindings.
type OIDCClaims struct {
	Issuer          string
	Audience        []string
	AuthorizedParty string
	Subject         string
	Tenant          string
	Groups          []string
	Nonce           string
	IssuedAt        time.Time
	ExpiresAt       time.Time
}

// IDTokenVerifier verifies the ID-token signature against issuer keys and
// returns claims. Implementations must reject unsupported algorithms and key
// IDs; semantic issuer/audience/time checks are repeated by Exchange.
type IDTokenVerifier interface {
	VerifyIDToken(context.Context, string, string, string) (OIDCClaims, error)
}

// OIDCOptions configure a public-client device flow.
type OIDCOptions struct {
	Issuer            string
	ClientID          string
	Audience          string
	Scopes            []string
	GroupRoles        map[string][]string
	HTTPClient        *http.Client
	Now               func() time.Time
	ClockSkew         time.Duration
	MaxResponseBytes  int64
	AllowInsecureHTTP bool // tests and explicitly local issuers only
}

// OIDCClient implements discovery, device authorization, token polling and
// exchange into Remount tokens without retaining provider bearer credentials.
type OIDCClient struct {
	opts   OIDCOptions
	client *http.Client
	issuer *url.URL
}

// NewOIDCClient validates an issuer and bounds all remote responses.
func NewOIDCClient(opts OIDCOptions) (*OIDCClient, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.ClockSkew == 0 {
		opts.ClockSkew = 30 * time.Second
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = 1 << 20
	}
	if opts.Audience == "" {
		opts.Audience = opts.ClientID
	}
	if len(opts.Scopes) == 0 {
		opts.Scopes = []string{"openid", "profile"}
	}
	issuer, err := parseIssuerURL(opts.Issuer, opts.AllowInsecureHTTP)
	if err != nil || !validText(opts.ClientID, 256) || !validText(opts.Audience, 256) || opts.ClockSkew < 0 || opts.ClockSkew > 5*time.Minute || opts.MaxResponseBytes < 1024 || opts.MaxResponseBytes > 8<<20 || len(opts.Scopes) > 16 {
		return nil, errors.New("identity: invalid OIDC issuer, client, audience, clock, or response bounds")
	}
	for _, scope := range opts.Scopes {
		if !validText(scope, 128) || strings.ContainsAny(scope, " \t\r\n") {
			return nil, errors.New("identity: invalid OIDC scope")
		}
	}
	if !slices.Contains(opts.Scopes, "openid") {
		return nil, errors.New("identity: OIDC scopes must include openid")
	}
	for group, roles := range opts.GroupRoles {
		if !validText(group, 256) {
			return nil, errors.New("identity: invalid OIDC group mapping")
		}
		if _, err := normalizeRoles(roles); err != nil {
			return nil, err
		}
		if hasRole(roles, RoleNode) {
			return nil, errors.New("identity: OIDC groups cannot grant node role")
		}
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if opts.HTTPClient != nil {
		*client = *opts.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	opts.Scopes = append([]string(nil), opts.Scopes...)
	opts.GroupRoles = cloneRoleMap(opts.GroupRoles)
	return &OIDCClient{opts: opts, client: client, issuer: issuer}, nil
}

// Discover fetches provider metadata and requires an exact issuer match.
func (c *OIDCClient) Discover(ctx context.Context) (OIDCDiscovery, error) {
	discoveryURL := strings.TrimSuffix(c.issuer.String(), "/") + "/.well-known/openid-configuration"
	var metadata OIDCDiscovery
	if err := c.getJSON(ctx, discoveryURL, &metadata); err != nil {
		return OIDCDiscovery{}, err
	}
	if err := c.validateDiscovery(metadata); err != nil {
		return OIDCDiscovery{}, err
	}
	return metadata, nil
}

// StartDeviceAuthorization requests a one-time device and user code.
func (c *OIDCClient) StartDeviceAuthorization(ctx context.Context, discovery OIDCDiscovery) (DeviceAuthorization, error) {
	return c.startDeviceAuthorization(ctx, discovery, "")
}

// StartDeviceAuthorizationWithNonce binds the eventual ID token to this CLI
// login attempt.
func (c *OIDCClient) StartDeviceAuthorizationWithNonce(ctx context.Context, discovery OIDCDiscovery, nonce string) (DeviceAuthorization, error) {
	if !validText(nonce, 256) {
		return DeviceAuthorization{}, errors.New("identity: invalid OIDC nonce")
	}
	return c.startDeviceAuthorization(ctx, discovery, nonce)
}

func (c *OIDCClient) startDeviceAuthorization(ctx context.Context, discovery OIDCDiscovery, nonce string) (DeviceAuthorization, error) {
	if err := c.validateDiscovery(discovery); err != nil {
		return DeviceAuthorization{}, err
	}
	form := url.Values{"client_id": {c.opts.ClientID}, "scope": {strings.Join(c.opts.Scopes, " ")}}
	if nonce != "" {
		form.Set("nonce", nonce)
	}
	var response DeviceAuthorization
	if err := c.postForm(ctx, discovery.DeviceAuthorizationEndpoint, form, &response); err != nil {
		return DeviceAuthorization{}, err
	}
	if !validText(response.DeviceCode, 4096) || !validText(response.UserCode, 256) || response.ExpiresIn < 1 || response.ExpiresIn > 3600 || response.Interval < 0 || response.Interval > 300 {
		return DeviceAuthorization{}, errors.New("identity: invalid OIDC device authorization response")
	}
	if _, err := parseIssuerURL(response.VerificationURI, c.opts.AllowInsecureHTTP); err != nil {
		return DeviceAuthorization{}, errors.New("identity: invalid OIDC verification URI")
	}
	if response.VerificationURIComplete != "" {
		if _, err := parseOIDCURL(response.VerificationURIComplete, c.opts.AllowInsecureHTTP, true); err != nil {
			return DeviceAuthorization{}, errors.New("identity: invalid complete OIDC verification URI")
		}
	}
	if response.Interval == 0 {
		response.Interval = 5
	}
	return response, nil
}

// PollDeviceToken performs one poll. Callers wait at least the advertised
// interval and increase it by five seconds after ErrSlowDown, per RFC 8628.
func (c *OIDCClient) PollDeviceToken(ctx context.Context, discovery OIDCDiscovery, deviceCode string) (DeviceTokenResponse, error) {
	if err := c.validateDiscovery(discovery); err != nil {
		return DeviceTokenResponse{}, err
	}
	if !validText(deviceCode, 4096) {
		return DeviceTokenResponse{}, errors.New("identity: invalid OIDC device code")
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:device_code"}, "device_code": {deviceCode}, "client_id": {c.opts.ClientID}}
	var response DeviceTokenResponse
	status, body, err := c.postFormRaw(ctx, discovery.TokenEndpoint, form)
	if err != nil {
		return DeviceTokenResponse{}, err
	}
	if status < 200 || status >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		if decodeRemoteJSON(body, &failure) != nil {
			return DeviceTokenResponse{}, fmt.Errorf("identity: OIDC token endpoint returned HTTP %d", status)
		}
		switch failure.Error {
		case "authorization_pending":
			return DeviceTokenResponse{}, ErrAuthorizationPending
		case "slow_down":
			return DeviceTokenResponse{}, ErrSlowDown
		case "access_denied":
			return DeviceTokenResponse{}, ErrAccessDenied
		case "expired_token":
			return DeviceTokenResponse{}, ErrExpiredDeviceCode
		default:
			return DeviceTokenResponse{}, fmt.Errorf("identity: OIDC token endpoint returned HTTP %d", status)
		}
	}
	if err := decodeRemoteJSON(body, &response); err != nil || !validText(response.IDToken, 1<<20) {
		return DeviceTokenResponse{}, errors.New("identity: invalid OIDC token response")
	}
	return response, nil
}

// Exchange verifies an OIDC ID token, maps groups to roles, and issues
// Remount access/refresh credentials. Provider tokens are not retained.
func (c *OIDCClient) Exchange(ctx context.Context, manager *Manager, verifier IDTokenVerifier, rawIDToken, nonce string) (string, string, error) {
	return c.exchange(ctx, manager, verifier, rawIDToken, nonce, "")
}

// ExchangeForTenant additionally binds the provider tenant claim to the
// tenant whose server-side configuration selected this issuer.
func (c *OIDCClient) ExchangeForTenant(ctx context.Context, manager *Manager, verifier IDTokenVerifier, rawIDToken, nonce, tenant string) (string, string, error) {
	if !validText(tenant, 253) {
		return "", "", errors.New("identity: expected tenant is required")
	}
	return c.exchange(ctx, manager, verifier, rawIDToken, nonce, tenant)
}

func (c *OIDCClient) exchange(ctx context.Context, manager *Manager, verifier IDTokenVerifier, rawIDToken, nonce, expectedTenant string) (string, string, error) {
	if manager == nil || verifier == nil || !validText(rawIDToken, 1<<20) {
		return "", "", errors.New("identity: OIDC verifier, manager and ID token are required")
	}
	claims, err := verifier.VerifyIDToken(ctx, rawIDToken, c.issuer.String(), c.opts.Audience)
	if err != nil {
		return "", "", errors.New("identity: OIDC ID token verification failed")
	}
	now := c.opts.Now()
	partyInvalid := claims.AuthorizedParty != "" && claims.AuthorizedParty != c.opts.ClientID
	if len(claims.Audience) > 1 && claims.AuthorizedParty != c.opts.ClientID {
		partyInvalid = true
	}
	if claims.Issuer != c.issuer.String() || !slices.Contains(claims.Audience, c.opts.Audience) || partyInvalid || !validPrincipal(claims.Subject, claims.Tenant) || (expectedTenant != "" && claims.Tenant != expectedTenant) || claims.ExpiresAt.IsZero() || claims.IssuedAt.IsZero() || !claims.ExpiresAt.After(claims.IssuedAt) || claims.ExpiresAt.Sub(claims.IssuedAt) > 24*time.Hour || !now.Before(claims.ExpiresAt.Add(c.opts.ClockSkew)) || claims.IssuedAt.After(now.Add(c.opts.ClockSkew)) || (nonce != "" && claims.Nonce != nonce) {
		return "", "", errors.New("identity: OIDC claims failed semantic validation")
	}
	roles, err := c.mapRoles(claims.Groups)
	if err != nil {
		return "", "", err
	}
	// The provider mapping is authoritative. Persist and audit a changed role
	// set before minting credentials so older grants are revision-fenced.
	if _, err := manager.SyncOIDCPrincipal(ctx, claims.Tenant, claims.Subject, roles); err != nil {
		return "", "", err
	}
	return manager.IssueTokens(ctx, claims.Subject, claims.Tenant, roles)
}

func (c *OIDCClient) mapRoles(groups []string) ([]string, error) {
	if len(groups) > 64 {
		return nil, errors.New("identity: too many OIDC groups")
	}
	roles := make([]string, 0)
	for _, group := range groups {
		if !validText(group, 256) {
			return nil, errors.New("identity: invalid OIDC group")
		}
		roles = append(roles, c.opts.GroupRoles[group]...)
	}
	roles, err := normalizeRoles(roles)
	if err != nil {
		return nil, errors.New("identity: OIDC groups grant no configured role")
	}
	sort.Strings(roles)
	return roles, nil
}

func (c *OIDCClient) validateDiscovery(metadata OIDCDiscovery) error {
	if metadata.Issuer != c.issuer.String() {
		return errors.New("identity: OIDC discovery issuer mismatch")
	}
	for _, endpoint := range []string{metadata.DeviceAuthorizationEndpoint, metadata.TokenEndpoint, metadata.JWKSURI} {
		if _, err := parseOIDCURL(endpoint, c.opts.AllowInsecureHTTP, true); err != nil {
			return errors.New("identity: invalid OIDC discovery endpoint")
		}
	}
	return nil
}

func (c *OIDCClient) getJSON(ctx context.Context, endpoint string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	status, body, err := c.do(req)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("identity: OIDC endpoint returned HTTP %d", status)
	}
	if err := decodeRemoteJSON(body, target); err != nil {
		return errors.New("identity: invalid OIDC JSON response")
	}
	return nil
}

func (c *OIDCClient) postForm(ctx context.Context, endpoint string, form url.Values, target any) error {
	status, body, err := c.postFormRaw(ctx, endpoint, form)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("identity: OIDC endpoint returned HTTP %d", status)
	}
	if err := decodeRemoteJSON(body, target); err != nil {
		return errors.New("identity: invalid OIDC JSON response")
	}
	return nil
}

func (c *OIDCClient) postFormRaw(ctx context.Context, endpoint string, form url.Values) (int, []byte, error) {
	if _, err := parseOIDCURL(endpoint, c.opts.AllowInsecureHTTP, true); err != nil {
		return 0, nil, errors.New("identity: invalid OIDC endpoint")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	return c.do(req)
}

func (c *OIDCClient) do(req *http.Request) (int, []byte, error) {
	response, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, c.opts.MaxResponseBytes+1))
	if err != nil {
		return 0, nil, err
	}
	if int64(len(body)) > c.opts.MaxResponseBytes {
		return 0, nil, errors.New("identity: OIDC response exceeds configured bound")
	}
	return response.StatusCode, body, nil
}

func parseIssuerURL(raw string, allowHTTP bool) (*url.URL, error) {
	return parseOIDCURL(raw, allowHTTP, false)
}

func parseOIDCURL(raw string, allowHTTP, allowQuery bool) (*url.URL, error) {
	if !validText(raw, 4096) {
		return nil, errors.New("identity: invalid OIDC URL")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || (!allowQuery && parsed.RawQuery != "") || parsed.Fragment != "" {
		return nil, errors.New("identity: invalid OIDC URL")
	}
	if parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http") {
		return nil, errors.New("identity: OIDC URL must use HTTPS")
	}
	return parsed, nil
}

func decodeRemoteJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("identity: trailing JSON")
	}
	return nil
}

func cloneRoleMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, roles := range in {
		out[key] = append([]string(nil), roles...)
	}
	return out
}
