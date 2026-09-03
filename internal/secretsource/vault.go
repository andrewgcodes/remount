package secretsource

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type vaultSource struct {
	address string
	mount   string
	path    string
	key     string
}

func parseVaultSource(source string) (vaultSource, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "vault" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment == "" {
		return vaultSource{}, errors.New("secretsource: malformed Vault source")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || !safeSegments(parts) {
		return vaultSource{}, errors.New("secretsource: Vault source needs mount, path, and key")
	}
	return vaultSource{address: u.Host, mount: parts[0], path: strings.Join(parts[1:], "/"), key: u.Fragment}, nil
}

func (r *CachedResolver) resolveVault(ctx context.Context, source string) ([]byte, time.Time, error) {
	parsed, err := parseVaultSource(source)
	if err != nil {
		return nil, time.Time{}, err
	}
	credentials, err := r.vaultCredentials(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	token := credentials.Token
	if token == "" {
		token, err = r.loginVaultAppRole(ctx, parsed.address, credentials)
		if err != nil {
			return nil, time.Time{}, err
		}
	}
	secretPath := parsed.mount + "/" + parsed.path
	if r.cfg.VaultKVVersion == 2 {
		secretPath = parsed.mount + "/data/" + parsed.path
	}
	endpoint := r.cfg.VaultScheme + "://" + parsed.address + "/v1/" + escapePath(secretPath)
	headers := make(http.Header)
	headers.Set("X-Vault-Token", token)
	response, err := r.request(ctx, http.MethodGet, endpoint, headers, nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	var result struct {
		Data          map[string]json.RawMessage `json:"data"`
		LeaseDuration int64                      `json:"lease_duration"`
	}
	if err := decodeJSON(response, r.cfg.MaxBytes, &result); err != nil {
		return nil, time.Time{}, err
	}
	data := result.Data
	if r.cfg.VaultKVVersion == 2 {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(data["data"], &nested); err != nil {
			return nil, time.Time{}, errors.New("secretsource: Vault response omitted secret data")
		}
		data = nested
	}
	var value string
	if err := json.Unmarshal(data[parsed.key], &value); err != nil || value == "" {
		return nil, time.Time{}, errors.New("secretsource: Vault response omitted requested key")
	}
	var expiry time.Time
	if result.LeaseDuration > 0 {
		expiry = r.cfg.Now().Add(time.Duration(result.LeaseDuration) * time.Second)
	}
	return []byte(value), expiry, nil
}

func (r *CachedResolver) vaultCredentials(ctx context.Context) (VaultCredentials, error) {
	if r.cfg.VaultCredentials != nil {
		credentials, err := r.cfg.VaultCredentials(ctx)
		if err != nil {
			return VaultCredentials{}, errors.New("secretsource: Vault credentials unavailable")
		}
		return credentials, nil
	}
	return VaultCredentials{Token: os.Getenv("VAULT_TOKEN"), RoleID: os.Getenv("VAULT_ROLE_ID"), SecretID: os.Getenv("VAULT_SECRET_ID")}, nil
}

func (r *CachedResolver) loginVaultAppRole(ctx context.Context, address string, credentials VaultCredentials) (string, error) {
	if credentials.RoleID == "" || credentials.SecretID == "" {
		return "", errors.New("secretsource: Vault authentication unavailable")
	}
	payload, _ := json.Marshal(map[string]string{"role_id": credentials.RoleID, "secret_id": credentials.SecretID})
	defer zero(payload)
	endpoint := r.cfg.VaultScheme + "://" + address + "/v1/" + escapePath(strings.Trim(r.cfg.VaultAppRolePath, "/"))
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	response, err := r.request(ctx, http.MethodPost, endpoint, headers, payload)
	if err != nil {
		return "", err
	}
	var result struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err := decodeJSON(response, r.cfg.MaxBytes, &result); err != nil {
		return "", err
	}
	if result.Auth.ClientToken == "" {
		return "", errors.New("secretsource: Vault login returned no token")
	}
	return result.Auth.ClientToken, nil
}

func escapePath(value string) string {
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func safeSegments(parts []string) bool {
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
