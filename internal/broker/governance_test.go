package broker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/budget"
	"remount.dev/remount/internal/connector"
	"remount.dev/remount/internal/proto"
)

// connectorSurfaces enumerates every HTTP egress surface with the request
// each one needs to reach the same upstream: the generic proxy, the managed
// package connector, and the managed git connector (a read and a write).
type connectorSurface struct {
	name      string
	connector string
	method    string
	body      string
	push      bool
	target    func(b *Broker, host string) string
}

func connectorSurfaces() []connectorSurface {
	return []connectorSurface{
		{name: "proxy", target: func(b *Broker, host string) string { return DestURL(b.BaseURL(), host) + "/artifact" }},
		{name: "package", connector: proto.EgressConnectorPackage, target: func(b *Broker, host string) string {
			return b.PackageURL() + "/" + host + "/artifact"
		}},
		{name: "git-fetch", connector: proto.EgressConnectorGit, target: func(b *Broker, host string) string {
			return b.GitURL() + "/" + host + "/acme/repo.git/info/refs?service=git-upload-pack"
		}},
		{name: "git-push", connector: proto.EgressConnectorGit, method: http.MethodPost, body: "0000PACK", push: true, target: func(b *Broker, host string) string {
			return b.GitURL() + "/" + host + "/acme/repo.git/git-receive-pack"
		}},
	}
}

func (s connectorSurface) rule(host string) proto.EgressRule {
	rule := proto.EgressRule{
		ID: "governed", Connector: s.connector, Protocol: proto.EgressProtocolHTTPS,
		Hosts: []string{host}, Methods: []string{http.MethodGet, http.MethodPost}, MaxRequests: 1,
	}
	switch s.connector {
	case proto.EgressConnectorGit:
		rule.Repos = []string{"acme/repo"}
		rule.Push = s.push
	case proto.EgressConnectorPackage:
		rule.Methods = []string{http.MethodGet}
	}
	return rule
}

func (s connectorSurface) do(t *testing.T, b *Broker, host string, header map[string]string) *http.Response {
	t.Helper()
	method := s.method
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequest(method, s.target(b, host), strings.NewReader(s.body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func (u *upstream) hits() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.seen)
}

func (u *upstream) sawAuthorization(value string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, h := range u.seen {
		if h.Get("Authorization") == value {
			return true
		}
	}
	return false
}

func newConnectorStore(t *testing.T) *connector.Store {
	t.Helper()
	store, err := connector.NewStore(t.TempDir(), connector.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

const governanceLeaseSecret = "test-only-real-value"

func governanceLease(host string) proto.BindingLease {
	return proto.BindingLease{ID: "b_gov", Secret: governanceLeaseSecret, Destinations: []string{host}, Placeholder: "ref:b_gov"}
}

// RMR-003: an approve-mode rule is an approval requirement on every surface.
// Pending, denied, and unavailable authority release no upstream byte and no
// credential; an allowed decision releases exactly one attempt and consumes
// the rule's request count.
func TestApproveModeGovernsEveryEgressSurfaceBeforeSubstitution(t *testing.T) {
	for _, surface := range connectorSurfaces() {
		t.Run(surface.name, func(t *testing.T) {
			up := newUpstream(t)
			rec := &recorder{}
			rule := surface.rule(up.host)
			rule.Mode = proto.EgressModeApprove
			var approvals atomic.Int64
			var decision atomic.Value
			decision.Store(proto.ApprovalPending)
			var authorityDown atomic.Bool
			b := New(Options{
				WS: "ws_gov", Tenant: "t", Generation: 3, Principal: "alice", RootCAs: up.pool(),
				AllowPrivate: []string{"127.0.0.1"}, Audit: rec.add, ConnectorStore: newConnectorStore(t),
				Leases:       []proto.BindingLease{governanceLease(up.host)},
				Network:      proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}},
				ApprovalWait: time.Millisecond,
				Approval: func(_ context.Context, req proto.EgressApprovalReq) (*proto.EgressApprovalRes, error) {
					approvals.Add(1)
					if req.WS != "ws_gov" || req.Gen != 3 || req.Rule != rule.ID || req.Fingerprint == "" || req.BodyHash == "" {
						t.Errorf("approval request lacks identity: %+v", req)
					}
					if authorityDown.Load() {
						return nil, errors.New("control unreachable")
					}
					switch decision.Load().(string) {
					case proto.ApprovalPending:
						return &proto.EgressApprovalRes{ID: "ap_gov", Status: proto.ApprovalPending}, nil
					case "denied":
						return &proto.EgressApprovalRes{ID: "ap_gov", Status: proto.ApprovalDecided}, nil
					}
					return &proto.EgressApprovalRes{ID: "ap_gov", Status: proto.ApprovalDecided, Allowed: true}, nil
				},
			})
			if _, err := b.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			header := map[string]string{"Authorization": "Bearer ref:b_gov"}

			resp := surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusForbidden || resp.Header.Get("X-Remount-Approval") != "ap_gov" || resp.Header.Get("Retry-After") == "" {
				t.Fatalf("pending approval: status=%d headers=%v", resp.StatusCode, resp.Header)
			}
			if approvals.Load() != 1 || up.hits() != 0 {
				t.Fatalf("pending approval: approval_calls=%d upstream_hits=%d", approvals.Load(), up.hits())
			}

			decision.Store("denied")
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusForbidden || up.hits() != 0 {
				t.Fatalf("denied approval: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
			if a, ok := rec.lastDecision(DecisionDenied); !ok || a.Reason != "approval denied" || a.Rule != rule.ID {
				t.Fatalf("denied approval not audited: %+v ok=%v", a, ok)
			}

			authorityDown.Store(true)
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusServiceUnavailable || up.hits() != 0 {
				t.Fatalf("unavailable authority: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
			authorityDown.Store(false)
			if rec.count(DecisionSubstituted) != 0 {
				t.Fatalf("credential substituted before an approval decision: %d", rec.count(DecisionSubstituted))
			}

			decision.Store("allowed")
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusOK || up.hits() != 1 {
				t.Fatalf("allowed approval: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
			if !up.sawAuthorization("Bearer "+governanceLeaseSecret) || up.sawAuthorization("Bearer ref:b_gov") {
				t.Fatal("allowed request did not carry the substituted credential")
			}
			if rec.count(DecisionSubstituted) != 1 {
				t.Fatalf("substituted audits = %d, want 1", rec.count(DecisionSubstituted))
			}
			// MaxRequests: 1 is consumed by the approved request, not by the
			// pending or denied attempts.
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusTooManyRequests || up.hits() != 1 {
				t.Fatalf("approved rule request count: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
		})
	}
}

// RMR-004: the central budget authority admits every surface before
// substitution or upstream I/O. Denied admission returns 429 with zero
// upstream requests; unavailable authority fails closed; admitted requests
// carry the bindings they will use and settle request-only once.
func TestHardBudgetAdmissionGovernsEveryEgressSurfaceBeforeSubstitution(t *testing.T) {
	for _, surface := range connectorSurfaces() {
		t.Run(surface.name, func(t *testing.T) {
			up := newUpstream(t)
			rec := &recorder{}
			rule := surface.rule(up.host)
			rule.MaxRequests = 0
			var reserves atomic.Int64
			var verdict atomic.Value
			verdict.Store("denied")
			settled := make(chan proto.BudgetSettleReq, 4)
			b := New(Options{
				WS: "ws_gov", Tenant: "t", Generation: 3, Principal: "alice", RootCAs: up.pool(),
				AllowPrivate: []string{"127.0.0.1"}, Audit: rec.add, ConnectorStore: newConnectorStore(t),
				Leases:  []proto.BindingLease{governanceLease(up.host)},
				Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}},
				BudgetReserve: func(_ context.Context, req proto.BudgetReserveReq) (*proto.BudgetReservation, error) {
					reserves.Add(1)
					if req.WS != "ws_gov" || req.Gen != 3 || req.Principal != "alice" || len(req.Bindings) != 1 || req.Bindings[0] != "b_gov" {
						t.Errorf("reservation lacks subject identity: %+v", req)
					}
					switch verdict.Load().(string) {
					case "denied":
						return &proto.BudgetReservation{Denied: true, BudgetIDs: []string{"hard"}, Limit: "requests"}, nil
					case "unavailable":
						return nil, errors.New("control unreachable")
					}
					return &proto.BudgetReservation{ID: "bres_gov", Tracked: true, BudgetIDs: []string{"hard"}}, nil
				},
				BudgetSettle: func(_ context.Context, req proto.BudgetSettleReq) (*proto.BudgetSettlement, error) {
					settled <- req
					return &proto.BudgetSettlement{Reservation: req.Reservation, Mode: req.Mode}, nil
				},
			})
			if _, err := b.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			header := map[string]string{"Authorization": "Bearer ref:b_gov"}

			resp := surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusTooManyRequests || reserves.Load() != 1 || up.hits() != 0 {
				t.Fatalf("exhausted budget: status=%d reservation_calls=%d upstream_hits=%d", resp.StatusCode, reserves.Load(), up.hits())
			}
			if rec.count(DecisionLimitExceeded) != 0 {
				t.Fatal("broker duplicated the authority's canonical budget denial event")
			}

			verdict.Store("unavailable")
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusServiceUnavailable || up.hits() != 0 {
				t.Fatalf("unavailable authority: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
			if rec.count(DecisionSubstituted) != 0 {
				t.Fatalf("credential substituted before budget admission: %d", rec.count(DecisionSubstituted))
			}

			verdict.Store("admitted")
			resp = surface.do(t, b, up.host, header)
			if resp.StatusCode != http.StatusOK || up.hits() != 1 {
				t.Fatalf("admitted request: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
			}
			if !up.sawAuthorization("Bearer " + governanceLeaseSecret) {
				t.Fatal("admitted request did not carry the substituted credential")
			}
			select {
			case req := <-settled:
				if req.Reservation != "bres_gov" || req.Mode != string(budget.SettlementRequestOnly) {
					t.Fatalf("settlement = %+v", req)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("admitted request never settled its reservation")
			}
			select {
			case req := <-settled:
				t.Fatalf("reservation settled twice: %+v", req)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// A package cache hit is still a governed request: admission precedes
// execution, so the reservation is taken whether or not the registry is
// contacted. The cache saves registry bytes, never budget units.
func TestPackageCacheHitStillConsumesBudgetAdmission(t *testing.T) {
	payload := "immutable wheel bytes"
	up := newUpstream(t)
	digest := "sha256:" + digestString(payload)
	up.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up.mu.Lock()
		up.seen = append(up.seen, r.Header.Clone())
		up.mu.Unlock()
		_, _ = io.WriteString(w, payload)
	})
	rule := proto.EgressRule{
		ID: "packages", Connector: proto.EgressConnectorPackage, Protocol: proto.EgressProtocolHTTPS,
		Hosts: []string{up.host}, Methods: []string{http.MethodGet}, SharedState: proto.SharedStateImmutableRead,
	}
	var reserves atomic.Int64
	b := New(Options{
		WS: "ws_gov", Tenant: "t", Generation: 3, Principal: "alice", RootCAs: up.pool(),
		AllowPrivate: []string{"127.0.0.1"}, ConnectorStore: newConnectorStore(t),
		Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}},
		BudgetReserve: func(context.Context, proto.BudgetReserveReq) (*proto.BudgetReservation, error) {
			if reserves.Add(1) > 2 {
				return &proto.BudgetReservation{Denied: true, BudgetIDs: []string{"hard"}, Limit: "requests"}, nil
			}
			return &proto.BudgetReservation{}, nil
		},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	target := b.PackageURL() + "/" + up.host + "/artifact.whl"
	for attempt := 0; attempt < 2; attempt++ {
		resp, body := get(t, target, map[string]string{connector.ExpectedDigestHeader: digest})
		if resp.StatusCode != http.StatusOK || body != payload {
			t.Fatalf("attempt %d: status=%d body=%q", attempt, resp.StatusCode, body)
		}
	}
	if up.hits() != 1 || reserves.Load() != 2 {
		t.Fatalf("cache hit accounting: upstream_hits=%d reservation_calls=%d", up.hits(), reserves.Load())
	}
	resp, _ := get(t, target, map[string]string{connector.ExpectedDigestHeader: digest})
	if resp.StatusCode != http.StatusTooManyRequests || up.hits() != 1 {
		t.Fatalf("exhausted budget served from cache: status=%d upstream_hits=%d", resp.StatusCode, up.hits())
	}
}

// RMR-005: a workspace-controlled Idempotency-Key names an upstream contract
// the broker cannot verify. Every admitted upstream attempt consumes one
// request unit from the real ledger; the header itself still reaches the
// upstream unchanged.
func TestRepeatedIdempotencyKeyChargesEveryUpstreamAttempt(t *testing.T) {
	ctx := context.Background()
	up := newUpstream(t)
	manager, err := budget.NewManager(budget.Config{Budgets: []budget.Budget{{
		ID: "one-request", Tenant: "tenant-a", AttachTo: budget.AttachTenant, AttachID: "tenant-a",
		Window: budget.WindowHour, MaxRequests: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	var reserveErrors atomic.Int64
	b := New(Options{
		WS: "ws_idem", Tenant: "tenant-a", Generation: 1, Principal: "alice", RootCAs: up.pool(),
		AllowPrivate: []string{"127.0.0.1"}, Allow: []string{up.host},
		BudgetReserve: func(ctx context.Context, req proto.BudgetReserveReq) (*proto.BudgetReservation, error) {
			r, err := manager.Reserve(ctx, budget.ReserveRequest{
				Key: req.Key, Node: "n_idem",
				Subject:  budget.Subject{Tenant: "tenant-a", Workspace: req.WS, Generation: req.Gen, Principal: req.Principal, Bindings: req.Bindings},
				Provider: req.Provider, Model: req.Model, InputTokens: req.InputTokens, MaxOutputTokens: req.MaxOutputTokens, Metered: req.Metered,
			})
			if err != nil {
				var denied *budget.DeniedError
				if errors.As(err, &denied) {
					return &proto.BudgetReservation{Denied: true, BudgetIDs: denied.BudgetIDs, Limit: string(denied.Limit)}, nil
				}
				reserveErrors.Add(1)
				return nil, err
			}
			return &proto.BudgetReservation{ID: r.ID, Tracked: r.Tracked}, nil
		},
		BudgetSettle: func(ctx context.Context, req proto.BudgetSettleReq) (*proto.BudgetSettlement, error) {
			_, err := manager.Settle(ctx, budget.SettleRequest{ReservationID: req.Reservation, Mode: budget.SettlementMode(req.Mode), InputTokens: req.InputTokens, OutputTokens: req.OutputTokens})
			return &proto.BudgetSettlement{}, err
		},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	statuses := make([]int, 0, 3)
	for _, body := range []string{"repeat", "repeat", "REPEAT"} {
		req, err := http.NewRequest(http.MethodPost, DestURL(b.BaseURL(), up.host)+"/charged", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Idempotency-Key", "workspace-controlled")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		statuses = append(statuses, resp.StatusCode)
	}
	usage, err := manager.Usage(ctx, budget.UsageQuery{Tenant: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if up.hits() != 1 || statuses[0] != http.StatusOK || statuses[1] != http.StatusTooManyRequests || statuses[2] != http.StatusTooManyRequests {
		t.Fatalf("hard request budget bypass: upstream_hits=%d statuses=%v usage=%+v", up.hits(), statuses, usage)
	}
	if reserveErrors.Load() != 0 {
		t.Fatalf("repeated attempts produced %d reservation conflicts instead of admissions", reserveErrors.Load())
	}
	if len(usage) != 1 || usage[0].Requests != 1 || usage[0].ActiveReservations != 0 {
		t.Fatalf("ledger after one admitted attempt: %+v", usage)
	}
	up.mu.Lock()
	forwarded := up.seen[0].Get("Idempotency-Key")
	up.mu.Unlock()
	if forwarded != "workspace-controlled" {
		t.Fatalf("upstream idempotency header rewritten: %q", forwarded)
	}
}
