package notifier

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const slackWebhookHost = "hooks.slack.com"

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001:db8::/32"),
}

// Resolver resolves destination names immediately before an outbound request.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type netResolver struct{}

func (netResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, network, host)
}

type destination struct {
	kind        Kind
	endpoint    string
	host        string
	bearerToken string
}

func normalizeDestination(configured Destination) (destination, error) {
	parsed, err := url.Parse(configured.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Opaque != "" {
		return destination{}, errors.New("notifier: destination must be an absolute HTTPS URL")
	}
	if parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return destination{}, errors.New("notifier: destination cannot contain userinfo, query, or fragment")
	}
	if parsed.Port() != "" && parsed.Port() != "443" {
		return destination{}, errors.New("notifier: destination must use HTTPS port 443")
	}
	host, ok := normalizeDNSName(parsed.Hostname())
	if !ok {
		return destination{}, errors.New("notifier: destination host must be a DNS name")
	}
	switch configured.Kind {
	case SlackWebhook:
		if host != slackWebhookHost || !strings.HasPrefix(parsed.EscapedPath(), "/services/") {
			return destination{}, errors.New("notifier: Slack destination is not an official incoming-webhook URL")
		}
		if configured.BearerToken != "" || len(configured.AllowedHosts) != 0 {
			return destination{}, errors.New("notifier: Slack destination has unsupported credentials or hosts")
		}
	case GenericWebhook:
		if len(configured.AllowedHosts) == 0 || len(configured.AllowedHosts) > 64 {
			return destination{}, errors.New("notifier: generic destination requires a bounded exact host allow-list")
		}
		allowed := false
		for _, entry := range configured.AllowedHosts {
			normalized, valid := normalizeDNSName(entry)
			if !valid {
				return destination{}, errors.New("notifier: generic allowed hosts must be exact DNS names")
			}
			if normalized == host {
				allowed = true
			}
		}
		if !allowed {
			return destination{}, errors.New("notifier: generic destination is outside its exact host allow-list")
		}
	default:
		return destination{}, errors.New("notifier: unsupported destination kind")
	}
	return destination{kind: configured.Kind, endpoint: parsed.String(), host: host, bearerToken: configured.BearerToken}, nil
}

func normalizeDNSName(host string) (string, bool) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" || len(host) > 253 || net.ParseIP(host) != nil || strings.ContainsAny(host, "*/:@[]%") {
		return "", false
	}
	for _, character := range host {
		if character > unicode.MaxASCII {
			return "", false
		}
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

func newSafeTransport(resolver Resolver, timeout time.Duration) *http.Transport {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, errors.New("notifier: invalid destination address")
			}
			addresses, err := resolvePublic(ctx, resolver, host)
			if err != nil {
				return nil, err
			}
			var attempted bool
			for _, address := range addresses {
				attempted = true
				connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(address.String(), port))
				if dialErr == nil {
					return connection, nil
				}
			}
			if !attempted {
				return nil, errors.New("notifier: destination has no public addresses")
			}
			return nil, errors.New("notifier: destination connection failed")
		},
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
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

func authorizeResolvedHost(ctx context.Context, resolver Resolver, host string) error {
	_, err := resolvePublic(ctx, resolver, host)
	return err
}

func resolvePublic(ctx context.Context, resolver Resolver, host string) ([]netip.Addr, error) {
	addresses, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, errors.New("notifier: destination resolution failed")
	}
	if len(addresses) == 0 {
		return nil, errors.New("notifier: destination has no addresses")
	}
	for index := range addresses {
		addresses[index] = addresses[index].Unmap()
		if isNonPublic(addresses[index]) {
			return nil, errors.New("notifier: destination resolved to a non-public address")
		}
	}
	return addresses, nil
}

func isNonPublic(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsLoopback() || address.IsPrivate() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() ||
		address.IsUnspecified() || address.IsInterfaceLocalMulticast() {
		return true
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}
