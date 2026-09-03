package tenant

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"
)

// SecretProvider resolves an outbound credential just in time. Providers are
// never serialized and their returned value is never included in errors.
type SecretProvider func(context.Context) (string, error)

// Resolver is the DNS seam used by the rebinding-resistant transport.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type netResolver struct{}

func (netResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, network, host)
}

// HTTPExporterOptions configure the generic HTTPS batch exporter.
type HTTPExporterOptions struct {
	Destination  string
	AllowedHosts []string
	Secret       SecretProvider
	Timeout      time.Duration
	Resolver     Resolver
	// RoundTripper exists for deterministic tests. Production callers leave it
	// nil so resolution is authorized again at connect time.
	RoundTripper http.RoundTripper
}

// HTTPExporter sends a JSON envelope to an exact allow-listed HTTPS endpoint.
type HTTPExporter struct {
	destination *url.URL
	secret      SecretProvider
	client      *http.Client
}

// NewHTTPExporter validates and constructs a generic exporter.
func NewHTTPExporter(options HTTPExporterOptions) (*HTTPExporter, error) {
	destination, err := validateDestination(options.Destination, options.AllowedHosts)
	if err != nil {
		return nil, err
	}
	if options.Timeout <= 0 {
		options.Timeout = 15 * time.Second
	}
	if options.Resolver == nil {
		options.Resolver = netResolver{}
	}
	transport := options.RoundTripper
	if transport == nil {
		transport = safeTransport(options.Resolver, options.Timeout)
	}
	client := &http.Client{Transport: transport, Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("tenant: exporter redirects are disabled")
	}}
	return &HTTPExporter{destination: destination, secret: options.Secret, client: client}, nil
}

// Export implements BillingExporter.
func (e *HTTPExporter) Export(ctx context.Context, events []MeterEvent) error {
	if e == nil || len(events) == 0 || len(events) > maxPage {
		return &Error{Code: CodeBadRequest}
	}
	for _, event := range events {
		if validateMeterEvent(event) != nil {
			return &Error{Code: CodeBadRequest}
		}
	}
	body, err := json.Marshal(struct {
		Version int          `json:"version"`
		Events  []MeterEvent `json:"events"`
	}{Version: 1, Events: events})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.destination.String(), bytes.NewReader(body))
	if err != nil {
		return &Error{Code: CodeBadRequest}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "remount-meter/1")
	if e.secret != nil {
		secret, secretErr := e.secret(ctx)
		if secretErr != nil {
			return &Error{Code: CodeUnavailable}
		}
		if secret == "" || len(secret) > 16<<10 || strings.ContainsAny(secret, "\r\n") {
			return &Error{Code: CodeBadRequest}
		}
		request.Header.Set("Authorization", "Bearer "+secret)
	}
	response, err := e.client.Do(request)
	if err != nil {
		return &Error{Code: CodeUnavailable, Retryable: true}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return outboundError(response.StatusCode)
	}
	return nil
}

func outboundError(status int) error {
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500
	return &Error{Code: CodeUnavailable, HTTPStatus: status, Retryable: retryable}
}

func validateDestination(raw string, allowedHosts []string) (*url.URL, error) {
	destination, err := url.Parse(raw)
	if len(raw) > 2048 || err != nil || destination.Scheme != "https" || destination.Hostname() == "" || destination.Opaque != "" || destination.User != nil ||
		destination.RawQuery != "" || destination.Fragment != "" || (destination.Port() != "" && destination.Port() != "443") ||
		len(allowedHosts) == 0 || len(allowedHosts) > 64 {
		return nil, &Error{Code: CodeBadRequest}
	}
	host, ok := normalizeDNSName(destination.Hostname())
	if !ok {
		return nil, &Error{Code: CodeBadRequest}
	}
	normalizedAllowed := make([]string, 0, len(allowedHosts))
	for _, allowed := range allowedHosts {
		normalized, valid := normalizeDNSName(allowed)
		if !valid {
			return nil, &Error{Code: CodeBadRequest}
		}
		normalizedAllowed = append(normalizedAllowed, normalized)
	}
	if !slices.Contains(normalizedAllowed, host) {
		return nil, &Error{Code: CodeBadRequest}
	}
	destination.Host = host
	if destination.Path == "" {
		destination.Path = "/"
	}
	return destination, nil
}

func normalizeDNSName(host string) (string, bool) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" || len(host) > 253 || net.ParseIP(host) != nil || strings.ContainsAny(host, "*/:@[]%") {
		return "", false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return "", false
			}
		}
	}
	return host, true
}

func safeTransport(resolver Resolver, timeout time.Duration) *http.Transport {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy:           nil,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, errors.New("tenant: invalid exporter address")
			}
			addresses, err := resolver.LookupNetIP(ctx, "ip", host)
			if err != nil || len(addresses) == 0 {
				return nil, errors.New("tenant: exporter DNS unavailable")
			}
			for _, candidate := range addresses {
				if !publicAddress(candidate.Unmap()) {
					return nil, errors.New("tenant: exporter DNS resolved to a non-public address")
				}
			}
			var lastErr error
			for _, candidate := range addresses {
				connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.String(), port))
				if dialErr == nil {
					return connection, nil
				}
				lastErr = dialErr
			}
			return nil, fmt.Errorf("tenant: exporter connection failed: %w", lastErr)
		},
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           16,
		MaxIdleConnsPerHost:    4,
		MaxConnsPerHost:        1,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    timeout,
		ResponseHeaderTimeout:  timeout,
		MaxResponseHeaderBytes: 64 << 10,
		DisableCompression:     true,
	}
}

func publicAddress(address netip.Addr) bool {
	if !address.IsValid() || address.Zone() != "" || !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsUnspecified() {
		return false
	}
	blocked := []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
		netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
		netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("64:ff9b::/96"),
		netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("2001::/32"),
		netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	}
	for _, prefix := range blocked {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var _ BillingExporter = (*HTTPExporter)(nil)
