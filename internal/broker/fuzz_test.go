package broker

import (
	"testing"

	"remount.dev/remount/internal/proto"
)

func FuzzDestinationParsing(f *testing.F) {
	for _, host := range []string{"example.com", "example.com:443", "[::1]:443", "100.100.100.200", "a..b", "host%zone"} {
		f.Add(host, uint8(0))
	}
	f.Fuzz(func(t *testing.T, host string, selector uint8) {
		if len(host) > 4096 {
			t.Skip()
		}
		protocols := []string{proto.EgressProtocolHTTP, proto.EgressProtocolHTTPS, proto.EgressProtocolConnect}
		protocol := protocols[int(selector)%len(protocols)]
		authority, err := normalizeAuthority(host, protocol)
		if err == nil {
			if second, err := normalizeAuthority(authority, protocol); err != nil || second != authority {
				t.Fatalf("normalization is not idempotent: %q -> %q -> %q (%v)", host, authority, second, err)
			}
		}
		_ = hostMatches(host, []string{"example.com", "*.example.org", "[::1]:443"})
	})
}
