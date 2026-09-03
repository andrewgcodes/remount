package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"strconv"
	"strings"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	// ExpectedDigestHeader opts a request into workspace-local cache reuse. A
	// sibling's immutable blob is never sufficient: this workspace must already
	// have a private reference established by its own successful fetch.
	ExpectedDigestHeader  = "X-Remount-Expected-Digest"
	ContentDigestHeader   = "X-Remount-Content-Digest"
	maxPackageHeaderBytes = 16 << 10
)

// PackageOptions configure the managed package connector.
type PackageOptions struct {
	Transport http.RoundTripper
	Store     *Store
}

// Package provides read-only HTTPS retrieval from registries selected by a
// normalized connector rule.
type Package struct {
	transport http.RoundTripper
	store     *Store
}

func NewPackage(opts PackageOptions) *Package {
	return &Package{transport: opts.Transport, store: opts.Store}
}

func (p *Package) Name() string { return proto.EgressConnectorPackage }

func (p *Package) Capabilities() []string {
	return []string{"package.read", "package.provenance", "cache.immutable.sha256"}
}

func (p *Package) Authorize(_ context.Context, req ConnectorRequest) (ConnectorDecision, error) {
	denyDecision := func(code, reason string) (ConnectorDecision, error) {
		return ConnectorDecision{Code: code, Reason: reason}, nil
	}
	if req.Workspace == "" || req.Tenant == "" || req.Generation == 0 {
		return denyDecision("identity_required", "package connector requires authoritative workspace identity")
	}
	if req.Rule.Connector != proto.EgressConnectorPackage {
		return denyDecision("wrong_connector", "network rule does not authorize the package connector")
	}
	if req.Rule.Protocol != proto.EgressProtocolHTTPS || req.Rule.SharedState != proto.SharedStateImmutableRead {
		return denyDecision("mutable_policy", "package connector requires HTTPS immutable-read policy")
	}
	method := strings.ToUpper(req.Method)
	if method != http.MethodGet && method != http.MethodHead {
		return denyDecision("method_denied", "package connector permits only GET and HEAD")
	}
	if req.URL == nil || req.URL.Scheme != "https" || req.URL.Host == "" || req.URL.User != nil || req.URL.Fragment != "" {
		return denyDecision("target_denied", "package connector requires an absolute HTTPS registry URL without userinfo or fragment")
	}
	if !packageRuleMatches(req.Rule, req.URL, method) {
		return denyDecision("rule_mismatch", "package registry request is outside the selected rule")
	}
	if req.Header.Get("Range") != "" {
		return denyDecision("range_denied", "package connector does not permit partial-object requests")
	}
	if req.ExpectedDigest != "" {
		if _, err := normalizeDigest(req.ExpectedDigest); err != nil {
			return ConnectorDecision{Code: "invalid_digest", Reason: err.Error()}, nil
		}
	}
	return ConnectorDecision{Allowed: true, Code: "allowed", Reason: "read-only package capability matched"}, nil
}

func packageRuleMatches(rule proto.EgressRule, target *url.URL, method string) bool {
	if rule.ID == "" || len(rule.Hosts) == 0 || target == nil {
		return false
	}
	methodMatched := len(rule.Methods) == 0
	for _, allowed := range rule.Methods {
		if strings.EqualFold(allowed, method) {
			methodMatched = true
			break
		}
	}
	if !methodMatched {
		return false
	}
	port := target.Port()
	if port == "" {
		port = "443"
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return false
	}
	if len(rule.Ports) != 0 {
		portMatched := false
		for _, allowed := range rule.Ports {
			if uint16(parsedPort) == allowed {
				portMatched = true
				break
			}
		}
		if !portMatched {
			return false
		}
	}
	host := strings.ToLower(strings.TrimSuffix(target.Hostname(), "."))
	hostMatched := false
	for _, pattern := range rule.Hosts {
		pattern = strings.ToLower(pattern)
		patternHost, patternPort := pattern, ""
		if splitHost, splitPort, splitErr := net.SplitHostPort(pattern); splitErr == nil {
			patternHost, patternPort = splitHost, splitPort
		}
		patternHost = strings.TrimSuffix(strings.Trim(patternHost, "[]"), ".")
		if patternPort != "" && patternPort != port {
			continue
		}
		if patternHost == "*" || patternHost == host ||
			(strings.HasPrefix(patternHost, "*.") && host != strings.TrimPrefix(patternHost, "*.") && strings.HasSuffix(host, patternHost[1:])) {
			hostMatched = true
			break
		}
	}
	if !hostMatched {
		return false
	}
	escaped := strings.ToLower(target.EscapedPath())
	if strings.Contains(escaped, "\\") || strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") ||
		strings.Contains(escaped, "%2e") || strings.Contains(escaped, "%25") {
		return false
	}
	requestPath := pathpkg.Clean("/" + strings.TrimPrefix(target.Path, "/"))
	if len(rule.PathPrefixes) == 0 {
		return true
	}
	for _, prefix := range rule.PathPrefixes {
		if prefix == "/" || requestPath == prefix ||
			(strings.HasSuffix(prefix, "/") && strings.HasPrefix(requestPath, prefix)) ||
			strings.HasPrefix(requestPath, prefix+"/") {
			return true
		}
	}
	return false
}

func (p *Package) Execute(ctx context.Context, in ConnectorRequest) (ConnectorResponse, error) {
	decision, err := p.Authorize(ctx, in)
	if err != nil {
		return ConnectorResponse{}, err
	}
	if !decision.Allowed {
		metrics.PackageFailures.Inc()
		status := http.StatusForbidden
		if decision.Code == "invalid_digest" {
			status = http.StatusBadRequest
		}
		return ConnectorResponse{}, deny(decision.Code, status, "%s", decision.Reason)
	}
	if p.transport == nil || p.store == nil {
		return ConnectorResponse{}, deny("unavailable", http.StatusServiceUnavailable, "package connector is unavailable")
	}
	method := strings.ToUpper(in.Method)

	expected := ""
	if in.ExpectedDigest != "" {
		expected, _ = normalizeDigest(in.ExpectedDigest)
		cached, ok, err := p.store.lookup(in.Tenant, in.Workspace, expected)
		if err != nil {
			return ConnectorResponse{}, &Error{Code: "cache_integrity", HTTPStatus: http.StatusServiceUnavailable, Detail: err.Error(), Err: err}
		}
		if ok {
			metrics.PackageCacheHits.Inc()
			if method == http.MethodHead {
				_ = cached.Body.Close()
				cached.Body = http.NoBody
			}
			cached.Header.Set(ContentDigestHeader, "sha256:"+expected)
			cached.Header.Set("Content-Length", strconv.FormatInt(cached.ContentLength, 10))
			cached.Provenance = provenance(in.URL, expected, cached.ContentLength, true)
			return cached, nil
		}
	}

	target := *in.URL
	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return ConnectorResponse{}, deny("bad_request", http.StatusBadRequest, "invalid package request")
	}
	req.Header = packageRequestHeaders(in.Header)
	metrics.PackageUpstreamRequests.Inc()
	upstream, err := p.transport.RoundTrip(req)
	if err != nil {
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, &Error{Code: "upstream_unavailable", HTTPStatus: http.StatusBadGateway, Detail: "package registry request failed", Err: err}
	}
	if upstream.Body == nil {
		upstream.Body = http.NoBody
	}
	header, headerErr := packageResponseHeaders(upstream.Header)
	if headerErr != nil {
		_ = upstream.Body.Close()
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, deny("response_headers_too_large", http.StatusBadGateway, "%s", headerErr)
	}
	if method == http.MethodHead {
		_ = upstream.Body.Close()
		return ConnectorResponse{
			StatusCode: upstream.StatusCode, Header: header, Body: http.NoBody,
			ContentLength: upstream.ContentLength,
			Provenance:    provenance(in.URL, "", 0, false),
		}, nil
	}

	maximum := in.Rule.MaxResponseBytes
	if maximum <= 0 || maximum > p.store.maxObjectBytes {
		maximum = p.store.maxObjectBytes
	}
	if upstream.ContentLength > maximum {
		_ = upstream.Body.Close()
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, deny("response_too_large", http.StatusBadGateway, "package object exceeds the configured response limit")
	}
	reservation, staged, err := p.store.begin(in.Tenant, in.Workspace, maximum)
	if err != nil {
		_ = upstream.Body.Close()
		metrics.PackageFailures.Inc()
		metrics.PackageQuotaRejected.Inc()
		return ConnectorResponse{}, &Error{Code: "resource_exhausted", HTTPStatus: http.StatusInsufficientStorage, Detail: err.Error(), Err: err}
	}
	abort := true
	defer func() {
		if abort {
			reservation.abort()
		}
	}()
	hash := sha256.New()
	probeLimit := maximum
	if maximum < int64(^uint64(0)>>1) {
		probeLimit++
	}
	written, copyErr := io.Copy(io.MultiWriter(staged, hash), io.LimitReader(upstream.Body, probeLimit))
	closeUpstreamErr := upstream.Body.Close()
	if copyErr == nil && closeUpstreamErr != nil {
		copyErr = closeUpstreamErr
	}
	if syncErr := staged.Sync(); copyErr == nil && syncErr != nil {
		copyErr = syncErr
	}
	if closeErr := staged.Close(); copyErr == nil && closeErr != nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, &Error{Code: "upstream_read", HTTPStatus: http.StatusBadGateway, Detail: "package registry response could not be read", Err: copyErr}
	}
	if written > maximum {
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, deny("response_too_large", http.StatusBadGateway, "package object exceeds the configured response limit")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	metrics.PackageResponseBytes.Add(uint64(written))
	header.Set("Content-Length", strconv.FormatInt(written, 10))

	if upstream.StatusCode == http.StatusOK {
		if expected != "" && !strings.EqualFold(expected, digest) {
			metrics.PackageFailures.Inc()
			return ConnectorResponse{}, deny("digest_mismatch", http.StatusBadGateway, "package object does not match the expected SHA-256 digest")
		}
		header.Set(ContentDigestHeader, "sha256:"+digest)
		blob, err := reservation.commit(digest, written, upstream.StatusCode, httpHeader(header))
		abort = false
		if err != nil {
			metrics.PackageFailures.Inc()
			metrics.PackageQuotaRejected.Inc()
			return ConnectorResponse{}, &Error{Code: "cache_write", HTTPStatus: http.StatusInsufficientStorage, Detail: "package cache commit failed", Err: err}
		}
		body, err := openCommittedBlob(blob, written)
		if err != nil {
			metrics.PackageFailures.Inc()
			return ConnectorResponse{}, &Error{Code: "cache_integrity", HTTPStatus: http.StatusServiceUnavailable, Detail: "package cache content is unavailable", Err: err}
		}
		return ConnectorResponse{
			StatusCode: upstream.StatusCode, Header: header, Body: body,
			ContentLength: written, Provenance: provenance(in.URL, digest, written, false),
		}, nil
	}

	body, err := reservation.detachTemporary()
	abort = false
	if err != nil {
		metrics.PackageFailures.Inc()
		return ConnectorResponse{}, &Error{Code: "staging_read", HTTPStatus: http.StatusServiceUnavailable, Detail: "package response staging failed", Err: err}
	}
	return ConnectorResponse{
		StatusCode: upstream.StatusCode, Header: header, Body: body,
		ContentLength: written, Provenance: provenance(in.URL, "", written, false),
	}, nil
}

func provenance(target *url.URL, digest string, size int64, cached bool) Provenance {
	source := ""
	registry := ""
	if target != nil {
		registry = target.Host
		source = target.Scheme + "://" + target.Host + target.EscapedPath()
	}
	return Provenance{
		Connector: proto.EgressConnectorPackage, Registry: registry, Source: source,
		SHA256: digest, Bytes: size, Cached: cached,
	}
}

func openCommittedBlob(path string, expectedSize int64) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() != expectedSize {
		_ = f.Close()
		if err == nil {
			err = errors.New("committed package blob metadata mismatch")
		}
		return nil, err
	}
	return f, nil
}

func packageRequestHeaders(in http.Header) http.Header {
	out := make(http.Header)
	for _, name := range []string{
		"Accept", "Accept-Encoding", "Authorization", "If-Match", "If-Modified-Since",
		"If-None-Match", "If-Unmodified-Since", "User-Agent",
	} {
		for _, value := range in.Values(name) {
			out.Add(name, value)
		}
	}
	return out
}

func packageResponseHeaders(in http.Header) (http.Header, error) {
	out := make(http.Header)
	total := 0
	for _, name := range []string{
		"Accept-Ranges", "Cache-Control", "Content-Disposition", "Content-Encoding",
		"Content-Language", "Content-Type", "ETag", "Expires", "Last-Modified", "Location",
	} {
		for _, value := range in.Values(name) {
			total += len(name) + len(value)
			if total > maxPackageHeaderBytes {
				return nil, errors.New("package registry response headers exceed the connector limit")
			}
			out.Add(name, value)
		}
	}
	return out, nil
}

var _ Connector = (*Package)(nil)
