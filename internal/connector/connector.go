// Package connector contains managed, policy-scoped integrations that expose
// narrower capabilities than generic network access.
package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"remount.dev/remount/internal/proto"
)

// Connector is the common contract for a managed external integration.
// Authorize is side-effect free. Execute repeats authorization so a caller
// cannot accidentally turn a prior decision into ambient authority.
type Connector interface {
	Name() string
	Capabilities() []string
	Authorize(context.Context, ConnectorRequest) (ConnectorDecision, error)
	Execute(context.Context, ConnectorRequest) (ConnectorResponse, error)
}

// ConnectorRequest carries server-authoritative workspace identity and the
// exact normalized rule selected by the broker.
type ConnectorRequest struct {
	Workspace      string
	Tenant         string
	Principal      string
	Generation     uint64
	Rule           proto.EgressRule
	Method         string
	URL            *url.URL
	Header         http.Header
	ExpectedDigest string
	// Body and ContentLength carry a request body for connectors that accept
	// one (git). ContentLength 0 with a non-nil Body means chunked.
	Body          io.Reader
	ContentLength int64
}

// ConnectorDecision records the stable authorization outcome.
type ConnectorDecision struct {
	Allowed bool
	Code    string
	Reason  string
	// Operation and Resource name what was authorized in connector terms
	// (git: fetch|push and owner/name) so audits can say more than a path.
	Operation string
	Resource  string
}

// ConnectorResponse is an upstream response plus node-owned provenance.
// Body ownership passes to the caller and must be closed.
type ConnectorResponse struct {
	StatusCode    int
	Header        http.Header
	Body          io.ReadCloser
	ContentLength int64
	Provenance    Provenance
}

// Provenance identifies bytes without exposing node-local cache paths.
type Provenance struct {
	Connector string
	Registry  string
	Source    string
	SHA256    string
	Bytes     int64
	Cached    bool
	Operation string
}

// Error is a stable connector failure. Detail is safe to return to the
// workspace; wrapped implementation errors remain available only via Unwrap.
type Error struct {
	Code       string
	HTTPStatus int
	Detail     string
	Err        error
}

func (e *Error) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return "connector request failed"
}

func (e *Error) Unwrap() error { return e.Err }

func deny(code string, status int, format string, args ...any) *Error {
	return &Error{Code: code, HTTPStatus: status, Detail: fmt.Sprintf(format, args...)}
}
