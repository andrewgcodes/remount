package broker

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func startTypedPolicy(t *testing.T, up *upstream, rules []proto.EgressRule, rec *recorder) *Broker {
	t.Helper()
	b := New(Options{
		WS: "ws_t", Generation: 1, Principal: "a_test",
		Network:      proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: rules},
		AllowPrivate: []string{"127.0.0.1", "localhost"}, Audit: rec.add, RootCAs: up.pool(),
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// The typed-policy ambiguity guard reads r.URL.EscapedPath(). Capability
// authentication used to rewrite r.URL.Path from the already-decoded path and
// clear RawPath, so by the time the guard ran, %2f had become a separator and
// %2e a dot: the two encodings the rule names first were the two it could not
// see. The request was admitted and the upstream received a path the caller
// never asked for.
func TestAmbiguousEncodedPathIsRefusedOnTheCapabilitySurface(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	b := startTypedPolicy(t, up, []proto.EgressRule{{
		ID: "r", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host},
		Methods: []string{"GET"}, PathPrefixes: []string{"/"},
	}}, rec)
	for _, suffix := range []string{"/v1%2fmodels", "/v1%2e%2e/admin", "/a%25b", "/a%5cb", "/a%2Fb"} {
		resp, body := get(t, b.BaseURL()+"/d/"+up.host+suffix, nil)
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "ambiguous encoded path") {
			t.Fatalf("GET %s -> %d %q; want 400 ambiguous encoded path", suffix, resp.StatusCode, strings.TrimSpace(body))
		}
	}
	up.mu.Lock()
	seen := append([]string(nil), up.paths...)
	up.mu.Unlock()
	if len(seen) != 0 {
		t.Fatalf("upstream was reached with rewritten paths: %v", seen)
	}
	// An unambiguously encoded path still works and still reaches the upstream
	// byte-for-byte.
	resp, _ := get(t, b.BaseURL()+"/d/"+up.host+"/v1/a%20b", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("legitimate encoded path -> %d", resp.StatusCode)
	}
	up.mu.Lock()
	seen = append([]string(nil), up.paths...)
	up.mu.Unlock()
	if len(seen) != 1 || seen[0] != "/v1/a%20b" {
		t.Fatalf("upstream saw %v; want [/v1/a%%20b]", seen)
	}
}

// Every broker listener carries a random 256-bit capability that authorizes
// this workspace's whole egress surface. The two decisions taken before
// authentication used the raw request path, so that bearer was written into an
// egress.denied audit -- and audits land in the node's durable, exportable
// event log.
func TestCapabilityNeverReachesAnAuditRecord(t *testing.T) {
	up := newUpstream(t)
	capabilityOf := func(b *Broker) string {
		base := b.BaseURL()
		return base[strings.Index(base, "/c/")+len("/c/"):]
	}
	assertClean := func(t *testing.T, rec *recorder, capability string) {
		t.Helper()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.ev) == 0 {
			t.Fatal("no audit was emitted")
		}
		for _, a := range rec.ev {
			if strings.Contains(fmt.Sprintf("%+v", a), capability) {
				t.Fatalf("capability leaked into audit: %+v", a)
			}
		}
	}

	t.Run("quiesced", func(t *testing.T) {
		rec := &recorder{}
		b := start(t, up, nil, []string{up.host}, rec)
		b.Suspend()
		if resp, _ := get(t, b.BaseURL()+"/d/"+up.host+"/x", nil); resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status %d", resp.StatusCode)
		}
		assertClean(t, rec, capabilityOf(b))
	})

	t.Run("concurrency exhausted", func(t *testing.T) {
		rec := &recorder{}
		b := New(Options{
			WS: "ws_t", Allow: []string{up.host}, AllowPrivate: []string{"127.0.0.1"},
			Audit: rec.add, RootCAs: up.pool(), MaxConcurrentRequests: 1,
		})
		if _, err := b.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		// Hold the only request slot so the next request is rejected before
		// authentication, which is where the raw path was recorded.
		b.requestSlots <- struct{}{}
		if resp, _ := get(t, b.BaseURL()+"/d/"+up.host+"/x", nil); resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status %d", resp.StatusCode)
		}
		<-b.requestSlots
		assertClean(t, rec, capabilityOf(b))
	})

	t.Run("bad capability", func(t *testing.T) {
		rec := &recorder{}
		b := start(t, up, nil, []string{up.host}, rec)
		root := b.BaseURL()[:strings.Index(b.BaseURL(), "/c/")]
		if resp, _ := get(t, root+"/c/not-the-token/d/"+up.host+"/x", nil); resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status %d", resp.StatusCode)
		}
		assertClean(t, rec, capabilityOf(b))
	})
}
