package secretsource

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type githubSource struct {
	appID          string
	installationID string
}

func parseGitHubSource(source string) (githubSource, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "github-app" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return githubSource{}, errors.New("secretsource: malformed GitHub App source")
	}
	installation := strings.Trim(u.Path, "/")
	appID, err := strconv.ParseInt(u.Host, 10, 64)
	if err != nil || appID <= 0 {
		return githubSource{}, errors.New("secretsource: GitHub App id must be numeric")
	}
	installationID, err := strconv.ParseInt(installation, 10, 64)
	if err != nil || installationID <= 0 || strings.Contains(installation, "/") {
		return githubSource{}, errors.New("secretsource: GitHub installation id must be numeric")
	}
	return githubSource{appID: u.Host, installationID: installation}, nil
}

func (r *CachedResolver) resolveGitHub(ctx context.Context, source string) ([]byte, time.Time, error) {
	parsed, err := parseGitHubSource(source)
	if err != nil {
		return nil, time.Time{}, err
	}
	keyPEM, err := r.githubKey(ctx, parsed.appID)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer zero(keyPEM)
	jwt, err := githubJWT(parsed.appID, keyPEM, r.cfg.Now())
	if err != nil {
		return nil, time.Time{}, err
	}
	endpoint := strings.TrimSuffix(r.cfg.GitHubEndpoint, "/") + "/app/installations/" + parsed.installationID + "/access_tokens"
	headers := make(http.Header)
	headers.Set("Accept", "application/vnd.github+json")
	headers.Set("Authorization", "Bearer "+jwt)
	headers.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := r.request(ctx, http.MethodPost, endpoint, headers, nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	var result struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := decodeJSON(response, r.cfg.MaxBytes, &result); err != nil {
		return nil, time.Time{}, err
	}
	if result.Token == "" || result.ExpiresAt.IsZero() {
		return nil, time.Time{}, errors.New("secretsource: GitHub response omitted token metadata")
	}
	expires := result.ExpiresAt.Add(-30 * time.Second)
	if !expires.After(r.cfg.Now()) {
		return nil, time.Time{}, errors.New("secretsource: GitHub returned an expired token")
	}
	return []byte(result.Token), expires, nil
}

func (r *CachedResolver) githubKey(ctx context.Context, appID string) ([]byte, error) {
	if r.cfg.GitHubPrivateKey != nil {
		key, err := r.cfg.GitHubPrivateKey(ctx, appID)
		if err != nil || len(key) == 0 {
			return nil, errors.New("secretsource: GitHub App private key unavailable")
		}
		return append([]byte(nil), key...), nil
	}
	key := os.Getenv("GITHUB_APP_PRIVATE_KEY")
	if key == "" {
		return nil, errors.New("secretsource: GitHub App private key unavailable")
	}
	return []byte(key), nil
}

func githubJWT(appID string, keyPEM []byte, now time.Time) (string, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return "", errors.New("secretsource: GitHub App private key is invalid")
	}
	var privateKey *rsa.PrivateKey
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		privateKey = parsed
	} else if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		privateKey, _ = parsed.(*rsa.PrivateKey)
	}
	if privateKey == nil {
		return "", errors.New("secretsource: GitHub App private key is not RSA")
	}
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": appID,
	})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", errors.New("secretsource: GitHub App JWT signing failed")
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
