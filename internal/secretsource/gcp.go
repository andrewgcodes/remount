package secretsource

import (
	"context"
	"encoding/base64"
	"errors"
	"hash/crc32"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func parseGCPSource(source string) (string, error) {
	u, err := url.Parse(source)
	if err != nil || u.Scheme != "gcpsm" || u.Host != "projects" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("secretsource: malformed GCP Secret Manager source")
	}
	resource := "projects/" + strings.Trim(u.Path, "/")
	parts := strings.Split(resource, "/")
	if len(parts) != 6 || !safeSegments(parts) || parts[0] != "projects" || parts[2] != "secrets" || parts[4] != "versions" {
		return "", errors.New("secretsource: GCP source must name a project, secret, and version")
	}
	return resource, nil
}

func (r *CachedResolver) resolveGCP(ctx context.Context, source string) ([]byte, time.Time, error) {
	resource, err := parseGCPSource(source)
	if err != nil {
		return nil, time.Time{}, err
	}
	token, err := r.gcpToken(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if token == "" {
		return nil, time.Time{}, errors.New("secretsource: GCP access token unavailable")
	}
	endpoint := strings.TrimSuffix(r.cfg.GCPEndpoint, "/") + "/v1/" + escapePath(resource) + ":access"
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+token)
	response, err := r.request(ctx, http.MethodGet, endpoint, headers, nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	var result struct {
		Payload struct {
			Data       string `json:"data"`
			DataCRC32C string `json:"dataCrc32c"`
		} `json:"payload"`
	}
	if err := decodeJSON(response, r.cfg.MaxBytes, &result); err != nil {
		return nil, time.Time{}, err
	}
	value, err := base64.StdEncoding.DecodeString(result.Payload.Data)
	if err != nil || len(value) == 0 {
		return nil, time.Time{}, errors.New("secretsource: GCP response omitted a valid payload")
	}
	if result.Payload.DataCRC32C != "" {
		expected, err := strconv.ParseUint(result.Payload.DataCRC32C, 10, 32)
		if err != nil || uint64(crc32.Checksum(value, crc32.MakeTable(crc32.Castagnoli))) != expected {
			zero(value)
			return nil, time.Time{}, errors.New("secretsource: GCP payload checksum mismatch")
		}
	}
	return value, time.Time{}, nil
}

func (r *CachedResolver) gcpToken(ctx context.Context) (string, error) {
	if r.cfg.GCPAccessToken != nil {
		token, err := r.cfg.GCPAccessToken(ctx)
		if err != nil {
			return "", errors.New("secretsource: GCP access token unavailable")
		}
		return token, nil
	}
	return os.Getenv("GOOGLE_OAUTH_ACCESS_TOKEN"), nil
}
