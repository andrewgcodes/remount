package connector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Git operations as recorded in decisions, provenance and events.
const (
	GitOpFetch = "fetch"
	GitOpPush  = "push"
)

// GitOptions configure the managed git connector.
type GitOptions struct {
	Transport http.RoundTripper
}

// Git exposes the git smart-HTTP protocol — and nothing else a hosting
// service serves on the same origin — for the repositories a rule names.
// Fetch is GET info/refs plus POST git-upload-pack; push additionally needs
// POST git-receive-pack and a rule that says push: true (ADR 0054).
type Git struct {
	transport http.RoundTripper
}

// NewGit returns a git connector that dials upstream with opts.Transport.
func NewGit(opts GitOptions) *Git {
	return &Git{transport: opts.Transport}
}

// Name is the connector name a rule selects.
func (g *Git) Name() string { return proto.EgressConnectorGit }

// Capabilities lists what this connector can be asked to do.
func (g *Git) Capabilities() []string {
	return []string{"git.fetch", "git.push", "git.smart-http"}
}

// gitTarget is a parsed smart-HTTP request path.
type gitTarget struct {
	ownerRepo string // owner/name without .git
	service   string // git-upload-pack | git-receive-pack
	endpoint  string // info/refs | git-upload-pack | git-receive-pack
}

// ParseGitPath splits /owner/name(.git)?/(info/refs|git-upload-pack|
// git-receive-pack) and, for info/refs, reads the service from the query.
// Anything else — the dumb-HTTP object paths, the hosting service's API,
// raw file downloads, LFS — is refused.
func parseGitPath(method, requestPath, rawQuery string) (gitTarget, string) {
	trimmed := strings.TrimPrefix(requestPath, "/")
	parts := strings.Split(trimmed, "/")
	if strings.Contains(requestPath, "//") || len(parts) < 3 || len(parts) > 4 {
		return gitTarget{}, "path is not owner/name/<smart-http endpoint>"
	}
	owner, name := parts[0], strings.TrimSuffix(parts[1], ".git")
	if !proto.ValidRepoSegment(owner) || !proto.ValidRepoSegment(name) {
		return gitTarget{}, "repository path segment is not a valid owner or name"
	}
	t := gitTarget{ownerRepo: owner + "/" + name}
	endpoint := strings.Join(parts[2:], "/")
	switch endpoint {
	case "info/refs":
		if method != http.MethodGet {
			return gitTarget{}, "info/refs is GET only"
		}
		q, err := url.ParseQuery(rawQuery)
		if err != nil {
			return gitTarget{}, "malformed info/refs query"
		}
		service := q.Get("service")
		if service != "git-upload-pack" && service != "git-receive-pack" {
			return gitTarget{}, "info/refs without a smart-HTTP service (dumb HTTP is not supported)"
		}
		for key := range q {
			if key != "service" {
				return gitTarget{}, "info/refs query carries unexpected parameters"
			}
		}
		t.service, t.endpoint = service, endpoint
	case "git-upload-pack", "git-receive-pack":
		if method != http.MethodPost {
			return gitTarget{}, endpoint + " is POST only"
		}
		if rawQuery != "" {
			return gitTarget{}, endpoint + " does not take a query"
		}
		t.service, t.endpoint = endpoint, endpoint
	default:
		return gitTarget{}, "not a git smart-HTTP endpoint"
	}
	return t, ""
}

// op maps the selected service to fetch or push.
func (t gitTarget) op() string {
	if t.service == "git-receive-pack" {
		return GitOpPush
	}
	return GitOpFetch
}

// Authorize checks identity, the rule shape, the path and the operation. It
// never touches the network.
func (g *Git) Authorize(_ context.Context, req ConnectorRequest) (ConnectorDecision, error) {
	denyDecision := func(code, reason string) (ConnectorDecision, error) {
		return ConnectorDecision{Code: code, Reason: reason}, nil
	}
	if req.Workspace == "" || req.Tenant == "" || req.Generation == 0 {
		return denyDecision("identity_required", "git connector requires authoritative workspace identity")
	}
	if req.Rule.Connector != proto.EgressConnectorGit || req.Rule.ID == "" || len(req.Rule.Repos) == 0 {
		return denyDecision("wrong_connector", "network rule does not authorize the git connector")
	}
	if req.Rule.Protocol != proto.EgressProtocolHTTPS {
		return denyDecision("mutable_policy", "git connector requires an HTTPS rule")
	}
	method := strings.ToUpper(req.Method)
	if method != http.MethodGet && method != http.MethodPost {
		return denyDecision("method_denied", "git connector permits only GET and POST")
	}
	if req.URL == nil || req.URL.Scheme != "https" || req.URL.Host == "" || req.URL.User != nil || req.URL.Fragment != "" {
		return denyDecision("target_denied", "git connector requires an absolute HTTPS URL without userinfo or fragment")
	}
	if !proto.HostMatchesAny(req.URL.Host, req.Rule.Hosts) {
		return denyDecision("rule_mismatch", "repository host is outside the selected rule")
	}
	target, reason := parseGitPath(method, req.URL.EscapedPath(), req.URL.RawQuery)
	if reason != "" {
		return denyDecision("target_denied", reason)
	}
	covered := false
	for _, pattern := range req.Rule.Repos {
		if proto.MatchRepo(pattern, target.ownerRepo) {
			covered = true
			break
		}
	}
	if !covered {
		return denyDecision("repo_denied", "repository "+target.ownerRepo+" is outside the selected rule")
	}
	op := target.op()
	if op == GitOpPush && !req.Rule.Push {
		return denyDecision("push_denied", "rule "+req.Rule.ID+" does not permit push to "+target.ownerRepo)
	}
	return ConnectorDecision{
		Allowed: true, Code: "allowed", Reason: "git " + op + " matched",
		Operation: op, Resource: target.ownerRepo,
	}, nil
}

// gitRequestHeaders is the allow list of what git sends that the upstream
// needs. Authorization arrives already substituted by the broker.
var gitRequestHeaders = []string{
	"Accept", "Accept-Encoding", "Accept-Language", "Authorization", "Cache-Control",
	"Content-Encoding", "Content-Type", "Git-Protocol", "Pragma", "User-Agent",
}

// gitResponseHeaders is what git needs back; everything else the hosting
// service adds (request ids, cookies, CSP) stays on the node.
var gitResponseHeaders = []string{
	"Cache-Control", "Content-Encoding", "Content-Length", "Content-Type",
	"Expires", "Pragma", "WWW-Authenticate",
}

func copyHeaders(dst, src http.Header, allowed []string) {
	for _, name := range allowed {
		if values, ok := src[http.CanonicalHeaderKey(name)]; ok {
			dst[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
}

var errGitRequestLimit = errors.New("request body exceeds rule limit")

// budgetReader fails a stream once it exceeds remaining bytes.
type budgetReader struct {
	r         io.Reader
	remaining int64
	err       error
}

func (b *budgetReader) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, b.err
	}
	n, err := b.r.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		// Hand back only the in-budget prefix, then the limit error.
		return n + int(b.remaining), b.err
	}
	return n, err
}

type budgetBody struct {
	*budgetReader
	closer io.Closer
}

func (b budgetBody) Close() error { return b.closer.Close() }

// Execute repeats Authorize, then streams the smart-HTTP exchange upstream
// without following redirects or retrying: a pack negotiation is stateful,
// and a redirect could steer the credential to another origin.
func (g *Git) Execute(ctx context.Context, in ConnectorRequest) (ConnectorResponse, error) {
	decision, err := g.Authorize(ctx, in)
	if err != nil {
		return ConnectorResponse{}, err
	}
	if !decision.Allowed {
		metrics.GitFailures.Inc()
		return ConnectorResponse{}, deny(decision.Code, http.StatusForbidden, "%s", decision.Reason)
	}
	if g.transport == nil {
		return ConnectorResponse{}, deny("unavailable", http.StatusServiceUnavailable, "git connector is unavailable")
	}
	method := strings.ToUpper(in.Method)
	var body io.Reader
	if method == http.MethodPost {
		body = in.Body
		if body == nil {
			body = http.NoBody
		}
		if limit := in.Rule.MaxRequestBytes; limit > 0 {
			if in.ContentLength > limit {
				metrics.GitFailures.Inc()
				return ConnectorResponse{}, deny("resource_exhausted", http.StatusRequestEntityTooLarge, "%s", errGitRequestLimit)
			}
			body = &budgetReader{r: body, remaining: limit, err: errGitRequestLimit}
		}
	}
	target := *in.URL
	target.User = nil
	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return ConnectorResponse{}, deny("bad_request", http.StatusBadRequest, "invalid git request")
	}
	if method == http.MethodPost {
		req.ContentLength = in.ContentLength
		if in.ContentLength == 0 && in.Body != nil {
			// Chunked upload: git streams negotiation rounds without a length.
			req.ContentLength = -1
		}
	}
	req.Header = http.Header{}
	copyHeaders(req.Header, in.Header, gitRequestHeaders)
	metrics.GitUpstreamRequests.Inc()
	upstream, err := g.transport.RoundTrip(req)
	if err != nil {
		metrics.GitFailures.Inc()
		if errors.Is(err, errGitRequestLimit) {
			return ConnectorResponse{}, deny("resource_exhausted", http.StatusRequestEntityTooLarge, "%s", errGitRequestLimit)
		}
		return ConnectorResponse{}, &Error{Code: "upstream_unavailable", HTTPStatus: http.StatusBadGateway, Detail: "git upstream request failed", Err: err}
	}
	if upstream.Body == nil {
		upstream.Body = http.NoBody
	}
	if upstream.StatusCode >= 300 && upstream.StatusCode < 400 {
		_ = upstream.Body.Close()
		metrics.GitFailures.Inc()
		return ConnectorResponse{}, deny("redirect_rejected", http.StatusBadGateway, "git upstream redirected; the connector never follows a redirect")
	}
	header := http.Header{}
	copyHeaders(header, upstream.Header, gitResponseHeaders)
	response := ConnectorResponse{
		StatusCode: upstream.StatusCode, Header: header, Body: upstream.Body,
		ContentLength: upstream.ContentLength,
		Provenance: Provenance{
			Connector: proto.EgressConnectorGit, Registry: in.URL.Host, Source: decision.Resource,
			Operation: decision.Operation,
		},
	}
	if limit := in.Rule.MaxResponseBytes; limit > 0 {
		if upstream.ContentLength > limit {
			_ = upstream.Body.Close()
			metrics.GitFailures.Inc()
			return ConnectorResponse{}, deny("response_too_large", http.StatusBadGateway, "response body exceeds rule limit")
		}
		response.Body = budgetBody{
			budgetReader: &budgetReader{r: upstream.Body, remaining: limit, err: errors.New("response body exceeds rule limit")},
			closer:       upstream.Body,
		}
	}
	return response, nil
}
