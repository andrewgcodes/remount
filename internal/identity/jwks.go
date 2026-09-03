package identity

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// JWKSOptions configure the dependency-free RS256 ID-token verifier. RS256 is
// the mandatory-to-implement OIDC signing algorithm; every other algorithm is
// rejected rather than inferred from key type.
type JWKSOptions struct {
	JWKSURI           string
	TenantClaim       string
	GroupsClaim       string
	HTTPClient        *http.Client
	Now               func() time.Time
	CacheTTL          time.Duration
	MaxResponseBytes  int64
	MaxTokenBytes     int
	AllowInsecureHTTP bool
}

// JWKSVerifier verifies OIDC RS256 signatures against a bounded key set.
type JWKSVerifier struct {
	opts     JWKSOptions
	client   *http.Client
	mu       sync.Mutex
	keys     map[string]*rsa.PublicKey
	loadedAt time.Time
}

// NewJWKSVerifier validates bounds without fetching remote keys.
func NewJWKSVerifier(opts JWKSOptions) (*JWKSVerifier, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.CacheTTL == 0 {
		opts.CacheTTL = 5 * time.Minute
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = 1 << 20
	}
	if opts.MaxTokenBytes == 0 {
		opts.MaxTokenBytes = 64 << 10
	}
	if opts.TenantClaim == "" {
		opts.TenantClaim = "tenant"
	}
	if opts.GroupsClaim == "" {
		opts.GroupsClaim = "groups"
	}
	if _, err := parseOIDCURL(opts.JWKSURI, opts.AllowInsecureHTTP, true); err != nil || !validOIDCCustomClaim(opts.TenantClaim) || !validOIDCCustomClaim(opts.GroupsClaim) || opts.TenantClaim == opts.GroupsClaim || opts.CacheTTL < time.Second || opts.CacheTTL > 24*time.Hour || opts.MaxResponseBytes < 1024 || opts.MaxResponseBytes > 8<<20 || opts.MaxTokenBytes < 1024 || opts.MaxTokenBytes > 1<<20 {
		return nil, errors.New("identity: invalid JWKS verifier options")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if opts.HTTPClient != nil {
		*client = *opts.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &JWKSVerifier{opts: opts, client: client}, nil
}

func validOIDCCustomClaim(name string) bool {
	if !validText(name, 128) {
		return false
	}
	switch name {
	case "iss", "sub", "aud", "exp", "iat", "azp", "nonce":
		return false
	}
	return true
}

// VerifyIDToken implements IDTokenVerifier.
func (v *JWKSVerifier) VerifyIDToken(ctx context.Context, token, issuer, audience string) (OIDCClaims, error) {
	if len(token) < 16 || len(token) > v.opts.MaxTokenBytes {
		return OIDCClaims{}, errors.New("identity: invalid ID token size")
	}
	parts := splitCompactToken(token)
	if len(parts) != 3 {
		return OIDCClaims{}, errors.New("identity: malformed ID token")
	}
	headerBytes, err := decodeSegment(parts[0], 4096)
	if err != nil {
		return OIDCClaims{}, err
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Type      string `json:"typ"`
	}
	if decodeStrictJSON(headerBytes, &header) != nil || header.Algorithm != "RS256" || !validText(header.KeyID, 256) || (header.Type != "" && header.Type != "JWT") {
		return OIDCClaims{}, errors.New("identity: unsupported ID token header")
	}
	key, err := v.key(ctx, header.KeyID)
	if err != nil {
		return OIDCClaims{}, err
	}
	signature, err := decodeSegment(parts[2], 1024)
	if err != nil {
		return OIDCClaims{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		return OIDCClaims{}, errors.New("identity: invalid ID token signature")
	}
	payload, err := decodeSegment(parts[1], v.opts.MaxTokenBytes)
	if err != nil {
		return OIDCClaims{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil || len(fields) > 64 {
		return OIDCClaims{}, errors.New("identity: invalid ID token claims")
	}
	claims, err := v.claims(fields)
	if err != nil {
		return OIDCClaims{}, err
	}
	if claims.Issuer != issuer || !containsString(claims.Audience, audience) {
		return OIDCClaims{}, errors.New("identity: ID token issuer or audience mismatch")
	}
	return claims, nil
}

func (v *JWKSVerifier) key(ctx context.Context, keyID string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.keys != nil && v.opts.Now().Sub(v.loadedAt) >= 0 && v.opts.Now().Sub(v.loadedAt) < v.opts.CacheTTL {
		if key := v.keys[keyID]; key != nil {
			return key, nil
		}
		return nil, errors.New("identity: ID token key id not found")
	}
	keys, err := v.fetchKeys(ctx)
	if err != nil {
		return nil, err
	}
	v.keys, v.loadedAt = keys, v.opts.Now()
	key := keys[keyID]
	if key == nil {
		return nil, errors.New("identity: ID token key id not found")
	}
	return key, nil
}

func (v *JWKSVerifier) fetchKeys(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.opts.JWKSURI, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	response, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("identity: JWKS endpoint unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, v.opts.MaxResponseBytes+1))
	if err != nil || int64(len(body)) > v.opts.MaxResponseBytes {
		return nil, errors.New("identity: invalid JWKS response size")
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := decodeRemoteJSON(body, &set); err != nil || len(set.Keys) == 0 || len(set.Keys) > 32 {
		return nil, errors.New("identity: invalid JWKS key set")
	}
	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, jwk := range set.Keys {
		if jwk.Kty != "RSA" || !validText(jwk.Kid, 256) || (jwk.Use != "" && jwk.Use != "sig") || (jwk.Alg != "" && jwk.Alg != "RS256") {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(jwk.N)
		if err != nil || len(modulus) < 256 || len(modulus) > 1024 {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			continue
		}
		exponent := new(big.Int).SetBytes(exponentBytes)
		if !exponent.IsInt64() || exponent.Int64() < 3 || exponent.Int64() > 1<<31-1 || exponent.Int64()%2 == 0 {
			continue
		}
		if _, duplicate := keys[jwk.Kid]; duplicate {
			return nil, errors.New("identity: duplicate JWKS key id")
		}
		keys[jwk.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(exponent.Int64())}
	}
	if len(keys) == 0 {
		return nil, errors.New("identity: JWKS has no usable RS256 keys")
	}
	return keys, nil
}

func (v *JWKSVerifier) claims(fields map[string]json.RawMessage) (OIDCClaims, error) {
	var issuer, subject, authorizedParty, nonce, tenant string
	var expires, issued int64
	var audiences, groups []string
	if decodeClaim(fields, "iss", &issuer) != nil || decodeClaim(fields, "sub", &subject) != nil || decodeClaim(fields, "exp", &expires) != nil || decodeClaim(fields, "iat", &issued) != nil || decodeClaim(fields, v.opts.TenantClaim, &tenant) != nil {
		return OIDCClaims{}, errors.New("identity: missing ID token claims")
	}
	_ = decodeClaim(fields, "azp", &authorizedParty)
	_ = decodeClaim(fields, "nonce", &nonce)
	if raw := fields["aud"]; len(raw) == 0 {
		return OIDCClaims{}, errors.New("identity: missing ID token audience")
	} else if json.Unmarshal(raw, &audiences) != nil {
		var one string
		if json.Unmarshal(raw, &one) != nil {
			return OIDCClaims{}, errors.New("identity: invalid ID token audience")
		}
		audiences = []string{one}
	}
	if raw := fields[v.opts.GroupsClaim]; len(raw) > 0 && json.Unmarshal(raw, &groups) != nil {
		return OIDCClaims{}, errors.New("identity: invalid ID token groups")
	}
	if len(audiences) == 0 || len(audiences) > 8 || len(groups) > 64 {
		return OIDCClaims{}, errors.New("identity: ID token claim collection exceeds bound")
	}
	return OIDCClaims{Issuer: issuer, Audience: audiences, AuthorizedParty: authorizedParty, Subject: subject, Tenant: tenant, Groups: groups, Nonce: nonce, IssuedAt: time.Unix(issued, 0), ExpiresAt: time.Unix(expires, 0)}, nil
}

func decodeClaim(fields map[string]json.RawMessage, name string, target any) error {
	raw := fields[name]
	if len(raw) == 0 {
		return errors.New("missing claim")
	}
	return json.Unmarshal(raw, target)
}
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func splitCompactToken(token string) []string {
	first := -1
	second := -1
	for index := 0; index < len(token); index++ {
		if token[index] == '.' {
			if first < 0 {
				first = index
			} else if second < 0 {
				second = index
			} else {
				return nil
			}
		}
	}
	if first <= 0 || second <= first+1 || second == len(token)-1 {
		return nil
	}
	return []string{token[:first], token[first+1 : second], token[second+1:]}
}
