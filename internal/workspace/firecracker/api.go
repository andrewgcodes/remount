package firecracker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const maxAPIErrorBytes = 64 << 10

// API is the subset of Firecracker's Unix-socket HTTP API used by a machine.
// Keeping it explicit makes request compatibility testable without KVM.
type API interface {
	Put(context.Context, string, any) error
	Patch(context.Context, string, any) error
	Get(context.Context, string, any) error
}

type unixAPI struct {
	client *http.Client
}

func newUnixAPI(socket string, timeout time.Duration) *unixAPI {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
	}
	return &unixAPI{client: &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("firecracker API redirect refused")
		},
	}}
}

func (a *unixAPI) Put(ctx context.Context, path string, body any) error {
	return a.do(ctx, http.MethodPut, path, body, nil)
}

func (a *unixAPI) Patch(ctx context.Context, path string, body any) error {
	return a.do(ctx, http.MethodPatch, path, body, nil)
}

func (a *unixAPI) Get(ctx context.Context, path string, out any) error {
	return a.do(ctx, http.MethodGet, path, nil, out)
}

func (a *unixAPI) do(ctx context.Context, method, path string, body, out any) error {
	if path == "" || path[0] != '/' {
		return fmt.Errorf("firecracker API path %q is not absolute", path)
	}
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Firecracker API request: %w", err)
		}
		payload = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("firecracker API %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxAPIErrorBytes+1)
	data, readErr := io.ReadAll(limited)
	if readErr != nil {
		return fmt.Errorf("read Firecracker API %s %s response: %w", method, path, readErr)
	}
	if len(data) > maxAPIErrorBytes {
		return fmt.Errorf("firecracker API %s %s response exceeded %d bytes", method, path, maxAPIErrorBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			FaultMessage string `json:"fault_message"`
		}
		_ = json.Unmarshal(data, &apiErr)
		if apiErr.FaultMessage == "" {
			apiErr.FaultMessage = string(bytes.TrimSpace(data))
		}
		if apiErr.FaultMessage == "" {
			apiErr.FaultMessage = http.StatusText(resp.StatusCode)
		}
		return fmt.Errorf("firecracker API %s %s returned %s: %s", method, path, resp.Status, apiErr.FaultMessage)
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return errors.Join(errors.New("decode firecracker API response"), err)
	}
	return nil
}
