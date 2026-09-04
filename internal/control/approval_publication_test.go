package control

import (
	"context"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/proto"
)

// failApprovalWrites makes every approvals row write fail until the returned
// function is called. The failing statement rolls the whole decision
// transaction back, which is the durable boundary these tests observe.
func failApprovalWrites(t *testing.T, f *controlFixture) func() {
	t.Helper()
	if _, err := f.c.db.Exec(`CREATE TRIGGER fail_approval_write BEFORE INSERT ON approvals BEGIN SELECT RAISE(FAIL, 'injected durable write failure'); END`); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if _, err := f.c.db.Exec(`DROP TRIGGER IF EXISTS fail_approval_write`); err != nil {
			t.Fatal(err)
		}
	}
}

func durableApproval(t *testing.T, f *controlFixture, id string) proto.Approval {
	t.Helper()
	var raw []byte
	if err := f.c.db.QueryRow(`SELECT data FROM approvals WHERE id=?`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var ap proto.Approval
	if err := proto.Unmarshal(raw, &ap); err != nil {
		t.Fatal(err)
	}
	return ap
}

// dirtyApprovalStatus reports the status of the approval the next flush would
// write, or "" when the approval is not marked dirty.
func dirtyApprovalStatus(f *controlFixture, id string) string {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if ap := f.c.dirtyApprovals[id]; ap != nil {
		return ap.Status
	}
	return ""
}

func TestApprovalDecideFailedCommitPublishesNoDecision(t *testing.T) {
	cases := []struct {
		name      string
		decide    proto.ApprovalDecideReq
		allowed   bool
		eventType string
	}{
		{name: "allow", decide: proto.ApprovalDecideReq{Option: "allow"}, allowed: true, eventType: proto.EvEgressAllowed},
		{name: "deny", decide: proto.ApprovalDecideReq{Option: "deny"}, allowed: false, eventType: proto.EvEgressDenied},
		{name: "remember-host", decide: proto.ApprovalDecideReq{Remember: proto.ApprovalRememberHost}, allowed: true, eventType: proto.EvEgressAllowed},
		{name: "remember-rule", decide: proto.ApprovalDecideReq{Remember: proto.ApprovalRememberRule}, allowed: true, eventType: proto.EvEgressAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "control.db")
			f := newControlFixture(t, path, nil)
			ws := claimedApprovalWorkspace(t, f)
			rulesBefore := len(ws.Spec.Security.Network.Rules)
			req := &proto.EgressApprovalReq{
				WS: ws.ID, Gen: ws.Generation, Principal: "alice", Rule: "github", Host: "api.github.com", Method: "POST",
				PathHash: approvalHash('a'), BodyHash: approvalHash('b'), Fingerprint: approvalHash('c'),
			}
			pending, err := f.c.egressApproval(ctx, "n_approve", req)
			if err != nil || pending.Status != proto.ApprovalPending {
				t.Fatalf("pending approval = %+v, %v", pending, err)
			}
			decide := tc.decide
			decide.ID, decide.IdempotencyKey = pending.ID, "decide-once"

			restore := failApprovalWrites(t, f)
			_, decideErr := f.c.approvalDecide(ctx, localSubject(), &decide)
			if decideErr == nil || !strings.Contains(decideErr.Error(), "injected durable write failure") {
				t.Fatalf("decide with failing approval write = %v, want the injected failure", decideErr)
			}

			// Forbidden result: the broker path sees a decision that never committed.
			observed, err := f.c.egressApproval(ctx, "n_approve", req)
			if err != nil {
				t.Fatal(err)
			}
			if observed.ID != pending.ID || observed.Status != proto.ApprovalPending || observed.Allowed {
				t.Fatalf("broker observed %+v after the decision commit failed; want pending %s", observed, pending.ID)
			}
			live, err := f.c.approvalGet(ctx, localSubject(), pending.ID)
			if err != nil {
				t.Fatal(err)
			}
			if live.Status != proto.ApprovalPending || live.Decision != nil || live.ExpiresAt != pending.ExpiresAt {
				t.Fatalf("live approval after failed commit = %+v; want the pending row unchanged", live)
			}
			if durable := durableApproval(t, f, pending.ID); durable.Status != proto.ApprovalPending || durable.Decision != nil {
				t.Fatalf("durable approval after failed commit = %+v", durable)
			}
			if status := dirtyApprovalStatus(f, pending.ID); status != "" && status != proto.ApprovalPending {
				t.Fatalf("failed decision is queued for a later flush as %q", status)
			}
			if n := len(eventsOfType(t, f.log, tc.eventType)); n != 0 {
				t.Fatalf("%s events after failed commit = %d", tc.eventType, n)
			}
			if n := len(eventsOfType(t, f.log, proto.EvPolicyUpdated)); n != 0 {
				t.Fatalf("policy.updated events after failed commit = %d", n)
			}
			if got := f.c.snapshotWS(ws.ID); len(got.Spec.Security.Network.Rules) != rulesBefore {
				t.Fatalf("workspace rules after failed commit = %+v", got.Spec.Security.Network.Rules)
			}
			restore()

			// A later unrelated approval flush must not publish the failed
			// decision, and a remembered scope must not cover a new fingerprint.
			other := *req
			other.Fingerprint, other.PathHash = approvalHash('d'), approvalHash('e')
			second, err := f.c.egressApproval(ctx, "n_approve", &other)
			if err != nil || second.ID == pending.ID || second.Status != proto.ApprovalPending || second.Allowed {
				t.Fatalf("new fingerprint after failed remember = %+v, %v", second, err)
			}
			if durable := durableApproval(t, f, pending.ID); durable.Status != proto.ApprovalPending {
				t.Fatalf("unrelated flush published the failed decision: %+v", durable)
			}
			if n := len(eventsOfType(t, f.log, proto.EvEgressPending)); n != 2 {
				t.Fatalf("egress.pending events = %d, want 2", n)
			}

			// The same idempotency key retries the whole decision, because
			// nothing about the failed attempt was recorded.
			decided, err := f.c.approvalDecide(ctx, localSubject(), &decide)
			if err != nil || decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Denied == tc.allowed {
				t.Fatalf("retry = %+v, %v", decided, err)
			}
			observed, err = f.c.egressApproval(ctx, "n_approve", req)
			if err != nil || observed.ID != pending.ID || observed.Status != proto.ApprovalDecided || observed.Allowed != tc.allowed {
				t.Fatalf("broker after durable decision = %+v, %v; want allowed=%v", observed, err, tc.allowed)
			}
			if n := len(eventsOfType(t, f.log, tc.eventType)); n != 1 {
				t.Fatalf("%s events after retry = %d, want 1", tc.eventType, n)
			}
			if durable := durableApproval(t, f, pending.ID); durable.Status != proto.ApprovalDecided || durable.Decision == nil {
				t.Fatalf("durable approval after retry = %+v", durable)
			}

			f.c.Stop()
			_ = f.log.Close()
			again := newControlFixture(t, path, nil)
			after, err := again.c.approvalGet(ctx, localSubject(), pending.ID)
			if err != nil || after.Status != proto.ApprovalDecided || after.Decision == nil || after.Decision.Denied == tc.allowed {
				t.Fatalf("approval after restart = %+v, %v", after, err)
			}
		})
	}
}

func TestApprovalDecideFailedCommitSurvivesRestartAsPending(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.db")
	f := newControlFixture(t, path, nil)
	ws := claimedApprovalWorkspace(t, f)
	req := &proto.EgressApprovalReq{
		WS: ws.ID, Gen: ws.Generation, Principal: "alice", Rule: "github", Host: "api.github.com", Method: "POST",
		PathHash: approvalHash('a'), BodyHash: approvalHash('b'), Fingerprint: approvalHash('c'),
	}
	pending, err := f.c.egressApproval(ctx, "n_approve", req)
	if err != nil {
		t.Fatal(err)
	}
	restore := failApprovalWrites(t, f)
	decide := &proto.ApprovalDecideReq{ID: pending.ID, Option: "allow", Remember: proto.ApprovalRememberHost, IdempotencyKey: "decide-once"}
	if _, err := f.c.approvalDecide(ctx, localSubject(), decide); err == nil {
		t.Fatal("injected approval write failure was not reached")
	}
	restore()
	f.c.Stop()
	_ = f.log.Close()

	again := newControlFixture(t, path, nil)
	after, err := again.c.approvalGet(ctx, localSubject(), pending.ID)
	if err != nil || after.Status != proto.ApprovalPending || after.Decision != nil {
		t.Fatalf("approval after restart = %+v, %v; want pending", after, err)
	}
	if n := len(eventsOfType(t, again.log, proto.EvEgressAllowed)) + len(eventsOfType(t, again.log, proto.EvPolicyUpdated)); n != 0 {
		t.Fatalf("decision events after restart = %d", n)
	}
	if got := again.c.snapshotWS(ws.ID); len(got.Spec.Security.Network.Rules) != len(ws.Spec.Security.Network.Rules) {
		t.Fatalf("remembered rule survived a failed commit: %+v", got.Spec.Security.Network.Rules)
	}
	decided, err := again.c.approvalDecide(ctx, localSubject(), decide)
	if err != nil || decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Denied {
		t.Fatalf("retry after restart = %+v, %v", decided, err)
	}
	if got := again.c.snapshotWS(ws.ID); len(got.Spec.Security.Network.Rules) != len(ws.Spec.Security.Network.Rules)+1 {
		t.Fatalf("remembered rule missing after durable decision: %+v", got.Spec.Security.Network.Rules)
	}
	if n := len(eventsOfType(t, again.log, proto.EvEgressAllowed)); n != 1 {
		t.Fatalf("egress.allowed events after retry = %d", n)
	}
}

func TestApprovalDecideFailedCommitRestoresAgent(t *testing.T) {
	ctx := context.Background()
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportPermission, Approval: &proto.Approval{
		ID: "ap_fail", Kind: proto.ApprovalToolCall, Title: "write file", ToolCall: "tc-1",
		Options: []proto.ApprovalOption{{ID: "once", Kind: "allow_once", Name: "Allow once"}},
	}})
	before := af.agent(t, a.ID)
	if before.Status != proto.AgentWaitingApproval || before.PendingApprovals != 1 {
		t.Fatalf("agent before decision = %+v", before)
	}

	restore := failApprovalWrites(t, af.controlFixture)
	decide := &proto.ApprovalDecideReq{ID: "ap_fail", IdempotencyKey: "d1"}
	if _, err := af.c.approvalDecide(ctx, localSubject(), decide); err == nil || !strings.Contains(err.Error(), "injected durable write failure") {
		t.Fatalf("decide with failing approval write = %v", err)
	}
	ap, err := af.c.approvalCopy("ap_fail")
	if err != nil || ap.Status != proto.ApprovalPending || ap.Decision != nil {
		t.Fatalf("approval after failed commit = %+v, %v", ap, err)
	}
	if got := af.agent(t, a.ID); got.Status != proto.AgentWaitingApproval || got.PendingApprovals != 1 || got.UpdatedAt != before.UpdatedAt {
		t.Fatalf("agent after failed commit = %+v; want %+v", got, before)
	}
	if n := af.opCount(proto.OpAgentApprovalDecided); n != 0 {
		t.Fatalf("decision sent to the node %d times before it was durable", n)
	}
	if n := len(eventsOfType(t, af.log, proto.EvApprovalDecided)); n != 0 {
		t.Fatalf("approval.decided events after failed commit = %d", n)
	}
	if status := dirtyApprovalStatus(af.controlFixture, "ap_fail"); status != "" && status != proto.ApprovalPending {
		t.Fatalf("failed decision is queued for a later flush as %q", status)
	}
	restore()

	decided, err := af.c.approvalDecide(ctx, localSubject(), decide)
	if err != nil || decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Option != "once" {
		t.Fatalf("retry = %+v, %v", decided, err)
	}
	af.waitOps(proto.OpAgentApprovalDecided, 1)
	if got := af.agent(t, a.ID); got.PendingApprovals != 0 || got.Status == proto.AgentWaitingApproval {
		t.Fatalf("agent after durable decision = %+v", got)
	}
	if n := len(eventsOfType(t, af.log, proto.EvApprovalDecided)); n != 1 {
		t.Fatalf("approval.decided events after retry = %d", n)
	}
}

// TestFailedApprovalDecisionReleasesNoUpstreamBytes composes the broker with
// the control plane's approval authority: a decision whose commit failed
// must leave the request parked, and only the durable retry releases it.
func TestFailedApprovalDecisionReleasesNoUpstreamBytes(t *testing.T) {
	ctx := context.Background()
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	var reached atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	parsed, _ := url.Parse(srv.URL)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	b := broker.New(broker.Options{
		WS: ws.ID, Generation: ws.Generation, Principal: "alice", AllowPrivate: []string{"127.0.0.1"}, RootCAs: roots,
		ApprovalWait: time.Millisecond, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "github", Mode: proto.EgressModeApprove, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{parsed.Host}, Methods: []string{"POST"},
		}}},
		Approval: func(ctx context.Context, req proto.EgressApprovalReq) (*proto.EgressApprovalRes, error) {
			return f.c.egressApproval(ctx, "n_approve", &req)
		},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	destination := broker.DestURL(b.BaseURL(), parsed.Host) + "/repos?a=1"
	post := func() (*http.Response, string) {
		t.Helper()
		resp, err := http.Post(destination, "application/json", strings.NewReader(`{"x":1}`))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(body)
	}

	resp, _ := post()
	approvalID := resp.Header.Get("X-Remount-Approval")
	if resp.StatusCode != http.StatusForbidden || approvalID == "" || reached.Load() != 0 {
		t.Fatalf("pending status=%d approval=%q reached=%d", resp.StatusCode, approvalID, reached.Load())
	}

	restore := failApprovalWrites(t, f)
	decide := &proto.ApprovalDecideReq{ID: approvalID, Option: "allow", IdempotencyKey: "decide-once"}
	if _, err := f.c.approvalDecide(ctx, localSubject(), decide); err == nil {
		t.Fatal("injected approval write failure was not reached")
	}
	resp, _ = post()
	if resp.StatusCode != http.StatusForbidden || resp.Header.Get("X-Remount-Approval") != approvalID || reached.Load() != 0 {
		t.Fatalf("after failed commit status=%d approval=%q reached=%d; upstream must see nothing", resp.StatusCode, resp.Header.Get("X-Remount-Approval"), reached.Load())
	}
	restore()

	if _, err := f.c.approvalDecide(ctx, localSubject(), decide); err != nil {
		t.Fatal(err)
	}
	resp, body := post()
	if resp.StatusCode != http.StatusOK || body != "ok" || reached.Load() != 1 {
		t.Fatalf("after durable decision status=%d body=%q reached=%d", resp.StatusCode, body, reached.Load())
	}
}
