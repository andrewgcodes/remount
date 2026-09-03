package secretsource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
)

func (r *CachedResolver) request(ctx context.Context, method, rawURL string, headers http.Header, payload []byte) (*http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("secretsource: provider endpoint is invalid")
	}
	if u.Scheme != "https" && !r.cfg.AllowInsecureHTTP {
		return nil, errors.New("secretsource: plaintext provider transport is disabled")
	}
	request, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, errors.New("secretsource: provider request is invalid")
	}
	request.Header = headers.Clone()
	return r.send(request)
}

func (r *CachedResolver) send(request *http.Request) (*http.Response, error) {
	client := &http.Client{Transport: http.DefaultTransport}
	if r.cfg.HTTPClient != nil {
		*client = *r.cfg.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("secretsource: provider is unreachable")
	}
	return response, nil
}

func decodeJSON(response *http.Response, maxBytes int64, destination any) error {
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return errors.New("secretsource: provider rejected request")
	}
	limited := io.LimitReader(response.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil || int64(len(data)) > maxBytes {
		zero(data)
		return errors.New("secretsource: provider response unavailable or too large")
	}
	defer zero(data)
	if err := json.Unmarshal(data, destination); err != nil {
		return errors.New("secretsource: provider returned an invalid response")
	}
	return nil
}
