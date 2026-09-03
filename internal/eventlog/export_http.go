package eventlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maximumExportResponseBytes = 1 << 20

type httpExportOptions struct {
	client       *http.Client
	headers      http.Header
	retries      int
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	maxBodyBytes int
}

func normalizeHTTPOptions(client *http.Client, headers http.Header, retries int, baseBackoff, maxBackoff time.Duration, maxBodyBytes int) (httpExportOptions, error) {
	if retries == 0 {
		retries = 3
	}
	if baseBackoff == 0 {
		baseBackoff = 100 * time.Millisecond
	}
	if maxBackoff == 0 {
		maxBackoff = 5 * time.Second
	}
	if maxBodyBytes == 0 {
		maxBodyBytes = maximumExportBatchBytes
	}
	if retries < 1 || retries > 8 || baseBackoff < time.Millisecond || maxBackoff < baseBackoff || maxBackoff > 30*time.Second {
		return httpExportOptions{}, errors.New("eventlog: invalid HTTP retry bounds")
	}
	if maxBodyBytes < 1 || maxBodyBytes > maximumExportBatchBytes {
		return httpExportOptions{}, errors.New("eventlog: invalid HTTP request body bound")
	}
	copyClient := &http.Client{Timeout: 30 * time.Second}
	if client != nil {
		*copyClient = *client
		if copyClient.Timeout == 0 {
			copyClient.Timeout = 30 * time.Second
		}
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	copyHeaders := make(http.Header, len(headers))
	for key, values := range headers {
		if strings.ContainsAny(key, "\r\n") {
			return httpExportOptions{}, errors.New("eventlog: invalid HTTP header name")
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n") {
				return httpExportOptions{}, errors.New("eventlog: invalid HTTP header value")
			}
			copyHeaders.Add(key, value)
		}
	}
	return httpExportOptions{
		client: copyClient, headers: copyHeaders, retries: retries,
		baseBackoff: baseBackoff, maxBackoff: maxBackoff, maxBodyBytes: maxBodyBytes,
	}, nil
}

func validateExportURL(raw string, requireHTTPS bool, appendPath string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("eventlog: invalid export endpoint")
	}
	if endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("eventlog: export endpoint must contain no credentials, query, or fragment")
	}
	if requireHTTPS {
		if endpoint.Scheme != "https" {
			return nil, errors.New("eventlog: SIEM endpoint must use HTTPS")
		}
	} else if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("eventlog: export endpoint must use HTTP or HTTPS")
	}
	if appendPath != "" {
		endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + appendPath
	}
	endpoint.RawPath = ""
	return endpoint, nil
}

type httpStatusError struct {
	kind   string
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("eventlog: %s export returned HTTP %d", e.kind, e.status)
}

func postExport(ctx context.Context, endpoint *url.URL, options httpExportOptions, kind, contentType string, body []byte, validate func([]byte) error) error {
	if len(body) > options.maxBodyBytes {
		return fmt.Errorf("eventlog: %s export request exceeds %d bytes", kind, options.maxBodyBytes)
	}
	for attempt := 0; attempt < options.retries; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return errors.New("eventlog: construct export request")
		}
		request.Header.Set("Content-Type", contentType)
		for key, values := range options.headers {
			for _, value := range values {
				request.Header.Add(key, value)
			}
		}
		response, err := options.client.Do(request)
		if err == nil {
			responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maximumExportResponseBytes+1))
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				err = errors.Join(readErr, closeErr)
			} else if len(responseBody) > maximumExportResponseBytes {
				return fmt.Errorf("eventlog: %s export response exceeds %d bytes", kind, maximumExportResponseBytes)
			} else if response.StatusCode >= 200 && response.StatusCode < 300 {
				if validate != nil {
					return validate(responseBody)
				}
				return nil
			} else if !retryableStatus(response.StatusCode) {
				return &httpStatusError{kind: kind, status: response.StatusCode}
			}
			if err == nil {
				err = &httpStatusError{kind: kind, status: response.StatusCode}
			}
			if attempt+1 < options.retries {
				delay := retryDelay(response.Header.Get("Retry-After"), options, attempt)
				if waitErr := waitExportRetry(ctx, delay); waitErr != nil {
					return waitErr
				}
				continue
			}
		}
		if attempt+1 == options.retries {
			if statusErr := (*httpStatusError)(nil); errors.As(err, &statusErr) {
				return statusErr
			}
			return fmt.Errorf("eventlog: %s export transport failed after %d attempts", kind, options.retries)
		}
		if waitErr := waitExportRetry(ctx, retryDelay("", options, attempt)); waitErr != nil {
			return waitErr
		}
	}
	return errors.New("eventlog: export retry loop exhausted")
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryDelay(header string, options httpExportOptions, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		delay := time.Duration(seconds) * time.Second
		if delay <= options.maxBackoff {
			return delay
		}
	}
	if when, err := http.ParseTime(header); err == nil {
		delay := time.Until(when)
		if delay >= 0 && delay <= options.maxBackoff {
			return delay
		}
	}
	delay := options.baseBackoff
	for step := 0; step < attempt && delay < options.maxBackoff; step++ {
		if delay > options.maxBackoff/2 {
			delay = options.maxBackoff
			break
		}
		delay *= 2
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err == nil {
		// A bounded 0.75x..1.25x jitter prevents synchronized retry storms.
		fraction := float64(binary.BigEndian.Uint64(random[:])) / float64(mathMaxUint64)
		delay = time.Duration(float64(delay) * (0.75 + 0.5*fraction))
	}
	return delay
}

const mathMaxUint64 = ^uint64(0)

func waitExportRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
