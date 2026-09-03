// Package providerutil contains secret-safe bounded transports shared only by
// infrastructure provisioner implementations.
package providerutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	defaultBodyLimit = 1 << 20
	maximumBodyLimit = 16 << 20
)

// HTTPOptions configure a provider API transport.
type HTTPOptions struct {
	Provider  string
	Endpoint  string
	Headers   http.Header
	Client    *http.Client
	BodyLimit int64
	Retries   int
}

// HTTP is a bounded JSON provider API client.
type HTTP struct {
	provider string
	base     *url.URL
	headers  http.Header
	client   *http.Client
	limit    int64
	retries  int
}

// StatusError reports only provider and status. Response bodies and request
// URLs are intentionally excluded because providers frequently echo secrets.
type StatusError struct {
	Provider string
	Status   int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: provider returned HTTP %d", e.Provider, e.Status)
}

// StatusCode extracts an HTTP status from err.
func StatusCode(err error) (int, bool) {
	var status *StatusError
	if !errors.As(err, &status) {
		return 0, false
	}
	return status.Status, true
}

// NewHTTP validates and copies a provider transport configuration.
func NewHTTP(options HTTPOptions) (*HTTP, error) {
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("provision: provider endpoint must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	if strings.TrimSpace(options.Provider) == "" {
		return nil, errors.New("provision: provider name is required")
	}
	limit := options.BodyLimit
	if limit == 0 {
		limit = defaultBodyLimit
	}
	if limit < 1 || limit > maximumBodyLimit {
		return nil, errors.New("provision: provider body limit is invalid")
	}
	retries := options.Retries
	if retries == 0 {
		retries = 3
	}
	if retries < 1 || retries > 6 {
		return nil, errors.New("provision: provider retry count is invalid")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if options.Client != nil {
		*client = *options.Client
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	headers := make(http.Header, len(options.Headers))
	for key, values := range options.Headers {
		if strings.ContainsAny(key, "\r\n") {
			return nil, errors.New("provision: provider header name is invalid")
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n") {
				return nil, errors.New("provision: provider header value is invalid")
			}
			headers.Add(key, value)
		}
	}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/")
	endpoint.RawPath = ""
	return &HTTP{provider: options.Provider, base: endpoint, headers: headers, client: client, limit: limit, retries: retries}, nil
}

// JSON performs one bounded JSON operation. Safe idempotent methods retry
// transient failures; callers reconcile ambiguous creates before another POST.
func (c *HTTP) JSON(ctx context.Context, method, relative string, query url.Values, input, output any, accepted ...int) error {
	_, err := c.JSONWithHeaders(ctx, method, relative, query, input, output, accepted...)
	return err
}

// JSONWithHeaders performs JSON and returns a copy of the response headers.
// It exists for provider pagination cursors; callers must not retain secret
// response headers or include them in errors.
func (c *HTTP) JSONWithHeaders(ctx context.Context, method, relative string, query url.Values, input, output any, accepted ...int) (http.Header, error) {
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("%s: encode provider request", c.provider)
		}
		if int64(len(payload)) > c.limit {
			return nil, fmt.Errorf("%s: provider request exceeds %d bytes", c.provider, c.limit)
		}
	}
	attempts := 1
	if method == http.MethodGet || method == http.MethodDelete {
		attempts = c.retries
	}
	for attempt := 0; attempt < attempts; attempt++ {
		var headers http.Header
		headers, err = c.jsonOnce(ctx, method, relative, query, payload, output, accepted)
		if err == nil || !retryable(err) || attempt+1 == attempts {
			return headers, err
		}
		timer := time.NewTimer(min(25*time.Millisecond<<attempt, time.Second))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, err
}

func (c *HTTP) jsonOnce(ctx context.Context, method, relative string, query url.Values, payload []byte, output any, accepted []int) (http.Header, error) {
	endpoint := *c.base
	endpoint.Path = path.Join(c.base.Path+"/", relative)
	if strings.HasSuffix(relative, "/") {
		endpoint.Path += "/"
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%s: construct provider request", c.provider)
	}
	for key, values := range c.headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	if inputPresent := len(payload) != 0; inputPresent {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, &transportError{provider: c.provider}
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, c.limit+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return nil, &transportError{provider: c.provider}
	}
	if int64(len(body)) > c.limit {
		return nil, fmt.Errorf("%s: provider response exceeds %d bytes", c.provider, c.limit)
	}
	if !containsStatus(accepted, response.StatusCode) {
		return response.Header.Clone(), &StatusError{Provider: c.provider, Status: response.StatusCode}
	}
	if output != nil && len(body) != 0 {
		if err := json.Unmarshal(body, output); err != nil {
			return nil, fmt.Errorf("%s: decode provider response", c.provider)
		}
	}
	return response.Header.Clone(), nil
}

type transportError struct{ provider string }

func (e *transportError) Error() string { return e.provider + ": provider transport failed" }

func containsStatus(accepted []int, status int) bool {
	for _, candidate := range accepted {
		if candidate == status {
			return true
		}
	}
	return false
}

func retryable(err error) bool {
	var transport *transportError
	if errors.As(err, &transport) {
		return true
	}
	status, ok := StatusCode(err)
	return ok && (status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500)
}
