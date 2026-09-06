package broker

import (
	"net/http"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

// TestBindingMethodAndPathPrefixNarrowSubstitution proves the two policy
// fields a binding can declare beyond its destination hosts are enforced where
// the credential is placed, not merely carried on the lease.
//
// A binding scoped to POST /v1/chat/completions is exactly how an operator
// says "this key may only be spent on completions". Before this test, the node
// received `methods` and `path_prefixes` and ignored them, so the same
// placeholder bought DELETE /v1/files with the real key attached — the
// operator's stated policy and the broker's behaviour disagreed silently.
func TestBindingMethodAndPathPrefixNarrowSubstitution(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	// A synthetic canary, never a real credential.
	const canary = "sk-remount-scope-canary-0000"
	lease := proto.BindingLease{
		ID: "b_scoped", Secret: canary, Destinations: []string{up.host},
		Methods: []string{http.MethodPost}, PathPrefixes: []string{"/v1/chat/"},
	}
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{up.host}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})
	base := DestURL(b.BaseURL(), up.host)
	credential := map[string]string{"Authorization": "Bearer ref:b_scoped"}

	// In scope: the credential is substituted and the upstream sees it.
	resp, body := send(t, http.MethodPost, base+"/v1/chat/completions", "application/json", `{"m":1}`, credential)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("in-scope request = %d %q", resp.StatusCode, body)
	}
	if !strings.Contains(body, "auth=Bearer "+canary) {
		t.Fatalf("in-scope upstream credential = %q", body)
	}

	// Out of scope by path: refused before any upstream byte, with the typed
	// refusal contract every other broker denial carries.
	resp, body = send(t, http.MethodPost, base+"/v1/files", "application/json", `{"m":1}`, credential)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("out-of-path request = %d %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get(ReasonHeader); got != proto.ReasonEgressDenied {
		t.Fatalf("out-of-path reason header = %q", got)
	}
	if strings.Contains(body, canary) {
		t.Fatal("the refusal body carried the credential")
	}

	// Out of scope by method: same refusal.
	resp, body = send(t, http.MethodGet, base+"/v1/chat/completions", "", "", credential)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("out-of-method request = %d %q", resp.StatusCode, body)
	}

	// Exactly one request reached the upstream, and it carried the real key.
	if got := upstreamRequests(up); got != 1 {
		t.Fatalf("%d requests reached the upstream, want 1", got)
	}
	if rec.count(DecisionDenied) != 2 || rec.count(DecisionSubstituted) != 1 {
		t.Fatalf("denied=%d substituted=%d", rec.count(DecisionDenied), rec.count(DecisionSubstituted))
	}
	for _, audit := range recordedAudits(rec) {
		if strings.Contains(audit.Reason, canary) {
			t.Fatal("an audit reason carried the credential")
		}
	}
}

// upstreamRequests counts what actually reached the upstream. A scope test
// that only read status codes could not tell a refusal from an upstream 403.
func upstreamRequests(u *upstream) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.paths)
}

// recordedAudits copies the audit trail so it can be scanned without holding
// the recorder's lock.
func recordedAudits(r *recorder) []Audit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Audit(nil), r.ev...)
}

// TestWithdrawnPlaceholderIsRefusedRatherThanForwarded proves what happens to
// a workspace that is still holding a placeholder after its binding was
// revoked: the request is refused with `revoked` before any upstream byte.
//
// The alternative — which is what happened before the broker kept withdrawals
// — is that the placeholder travels to the provider as an ordinary string,
// earns the provider's own 401, and tells the workspace nothing about why,
// while handing an internal identifier to a third party.
func TestWithdrawnPlaceholderIsRefusedRatherThanForwarded(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	const canary = "sk-remount-withdrawn-canary-0000"
	lease := proto.BindingLease{ID: "b_gone", Secret: canary, Destinations: []string{up.host}}
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{up.host}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})
	base := DestURL(b.BaseURL(), up.host)
	credential := map[string]string{"Authorization": "Bearer ref:b_gone"}

	if resp, body := send(t, http.MethodGet, base+"/ok", "", "", credential); resp.StatusCode != http.StatusOK {
		t.Fatalf("before revocation = %d %q", resp.StatusCode, body)
	}

	// The node drops the lease and records the withdrawal, exactly as it does
	// when control refuses to re-lease a revoked binding.
	b.SetLeases(nil)
	b.SetWithdrawn([]Withdrawn{{ID: "b_gone"}})

	before := upstreamRequests(up)
	resp, body := send(t, http.MethodGet, base+"/ok", "", "", credential)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("after revocation = %d %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get(ReasonHeader); got != proto.ReasonRevoked {
		t.Fatalf("refusal reason = %q, want %q", got, proto.ReasonRevoked)
	}
	if !strings.Contains(body, proto.CodeUnauthorized) {
		t.Fatalf("refusal body does not carry the wire code: %q", body)
	}
	if after := upstreamRequests(up); after != before {
		t.Fatalf("%d requests reached the upstream after revocation", after-before)
	}
	if rec.last().Decision != DecisionRevoked {
		t.Fatalf("the refusal was audited as %q", rec.last().Decision)
	}
	for _, audit := range recordedAudits(rec) {
		if strings.Contains(audit.Reason, canary) {
			t.Fatal("an audit reason carried the credential")
		}
	}
}
