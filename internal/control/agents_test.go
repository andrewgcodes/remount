package control

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

// agentFixture is a control plane with one online process node and a fake
// sender that records what the control plane asked the node to do.
type agentFixture struct {
	*controlFixture
	sender *fakeSender
	store  *artifact.Store
	mu     sync.Mutex
	runs   []proto.AgentRunReq
	ops    []string
	fail   atomic.Bool
	refuse map[string]bool // ops the node refuses while fail is off
	clock  atomic.Int64
	// nodeKey is n_one's identity; a restarted fixture reuses it so the node
	// is the same peer to the control plane, not an impostor with a new key.
	nodeKey ed25519.PrivateKey
}

func newAgentFixture(t *testing.T, path string, configure func(*Options)) *agentFixture {
	t.Helper()
	return newAgentFixtureWithNode(t, path, nil, configure)
}

func newAgentFixtureWithNode(t *testing.T, path string, nodeKey ed25519.PrivateKey, configure func(*Options)) *agentFixture {
	t.Helper()
	af := &agentFixture{}
	af.clock.Store(time.Now().UnixMilli())
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	af.store = store
	af.controlFixture = newControlFixture(t, path, func(o *Options) {
		o.PublicURL = "https://remount.example"
		o.Artifacts = store
		o.Now = func() time.Time { return time.UnixMilli(af.clock.Load()) }
		if configure != nil {
			configure(o)
		}
	})
	af.sender = &fakeSender{online: map[string]bool{"n_one": true}}
	af.sender.request = func(_ context.Context, to, op string, body, out any) error {
		af.mu.Lock()
		af.ops = append(af.ops, op)
		refused := af.refuse[op]
		af.mu.Unlock()
		if af.fail.Load() || refused {
			return proto.Err(proto.CodeUnreachable, "node refused %s", op)
		}
		switch op {
		case proto.OpAgentRun:
			req := body.(proto.AgentRunReq)
			af.mu.Lock()
			af.runs = append(af.runs, req)
			af.mu.Unlock()
			if res, ok := out.(*proto.AgentRunRes); ok {
				res.Transcript = "s_transcript_" + req.Run
			}
		case proto.OpWSSnapshot:
			req := body.(proto.WSSnapshotReq)
			if res, ok := out.(*proto.WSSnapshotRes); ok {
				res.Artifact = af.put("fork:" + req.WS)
			}
		case proto.OpWSRelease:
			req := body.(proto.WSReleaseReq)
			if res, ok := out.(*proto.WSReleasedReq); ok {
				*res = proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, OperationID: req.OperationID, Reason: req.Reason, Snapshot: af.put("sleep:" + req.WS)}
			}
		}
		return nil
	}
	af.c.Attach(af.sender)
	af.nodeKey = connectNodeWithKey(t, af.c, "n_one", processNodeInfo(4096), nodeKey)
	return af
}

func (af *agentFixture) put(content string) string {
	id, _, err := af.store.Put(strings.NewReader(content))
	if err != nil {
		panic(err)
	}
	return id
}

// waitOps blocks until op has been sent n times or the deadline passes;
// deliveries and cancels leave the request handler on their own goroutine.
func (af *agentFixture) waitOps(op string, n int) {
	deadline := time.Now().Add(5 * time.Second)
	for af.opCount(op) < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

func (af *agentFixture) advance(d time.Duration) {
	af.clock.Add(d.Milliseconds())
}

func (af *agentFixture) opCount(op string) int {
	af.mu.Lock()
	defer af.mu.Unlock()
	n := 0
	for _, o := range af.ops {
		if o == op {
			n++
		}
	}
	return n
}

func (af *agentFixture) lastRun(t *testing.T) proto.AgentRunReq {
	t.Helper()
	af.mu.Lock()
	defer af.mu.Unlock()
	if len(af.runs) == 0 {
		t.Fatal("no agent.run was sent to the node")
	}
	return af.runs[len(af.runs)-1]
}

// claim drives the agent's workspace to claimed on n_one the way a node would.
func (af *agentFixture) claim(t *testing.T, wsID string) uint64 {
	t.Helper()
	ctx := context.Background()
	claim, err := af.c.wsClaim(ctx, "n_one", wsID)
	if err != nil {
		t.Fatal(err)
	}
	if err := af.c.wsReady(ctx, "n_one", &proto.WSReadyReq{ID: wsID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	return claim.Workspace.Generation
}

func agentSpec(task string) proto.AgentSpec {
	return proto.AgentSpec{Recipe: "pi", Task: task}
}

func (af *agentFixture) create(t *testing.T, subject Subject, req proto.AgentCreateReq) *proto.Agent {
	t.Helper()
	a, err := af.c.agentCreate(context.Background(), subject, &req)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// start creates an agent with a task, claims its workspace and reconciles
// until the run is dispatched and reported started.
func (af *agentFixture) start(t *testing.T, subject Subject, task string) (*proto.Agent, proto.AgentRunReq) {
	t.Helper()
	a := af.create(t, subject, proto.AgentCreateReq{Name: "worker", Spec: agentSpec(task)})
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp"})
	return a, run
}

// startNamed is start for a second agent in the same fixture; the fixture's
// single node runs both.
func (af *agentFixture) startNamed(t *testing.T, subject Subject, name, task string) (*proto.Agent, proto.AgentRunReq) {
	t.Helper()
	a := af.create(t, subject, proto.AgentCreateReq{Name: name, Spec: agentSpec(task)})
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	if run.Agent != a.ID {
		t.Fatalf("last run belongs to %s, want %s", run.Agent, a.ID)
	}
	af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp_" + name})
	return a, run
}

func (af *agentFixture) report(t *testing.T, run proto.AgentRunReq, seq uint64, rep proto.AgentReport) {
	t.Helper()
	rep.Agent, rep.Run, rep.WS, rep.Gen, rep.Seq = run.Agent, run.Run, run.WS, run.Gen, seq
	if err := af.c.agentReport(context.Background(), "n_one", &rep); err != nil {
		t.Fatalf("report %s seq %d: %v", rep.Kind, seq, err)
	}
}

func (af *agentFixture) agent(t *testing.T, id string) *proto.Agent {
	t.Helper()
	a, err := af.c.agentCopy(id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func payloadOf(t *testing.T, e proto.Event) map[string]any {
	t.Helper()
	out := map[string]any{}
	if len(e.Payload) == 0 {
		return out
	}
	if err := proto.Unmarshal(e.Payload, &out); err != nil {
		t.Fatalf("decode %s payload: %v", e.Type, err)
	}
	return out
}

func payloadInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}

func codeOf(err error) string {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestAgentCreateOwnsWorkspaceAndCommitsAtomically(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{
		Name: "worker", Spec: agentSpec("fix the tests"),
		Workspace: &proto.WorkspaceSpec{Repo: proto.RepoSpec{URL: "https://github.com/acme/repo.git"}},
	})
	if a.Status != proto.AgentCreating || !a.OwnsWS || a.Mode != proto.AgentModeACP {
		t.Fatalf("agent = %+v", a)
	}
	if a.Policy.Approve != proto.ApproveOnRequest {
		t.Fatalf("default approve policy = %q", a.Policy.Approve)
	}
	if a.URL != "https://remount.example/a/"+a.ID {
		t.Fatalf("agent url = %q", a.URL)
	}
	ws, err := af.c.wsGet(a.WS)
	if err != nil {
		t.Fatal(err)
	}
	if ws.Spec.Repo.Branch != "agent/"+a.ID {
		t.Fatalf("repo branch = %q, want agent/%s", ws.Spec.Repo.Branch, a.ID)
	}
	if ws.Spec.Labels["remount.agent"] != a.ID {
		t.Fatalf("workspace labels = %v", ws.Spec.Labels)
	}
	if len(a.Inbox) != 1 || a.Inbox[0].Text != "fix the tests" {
		t.Fatalf("inbox = %+v", a.Inbox)
	}
	created := eventsOfType(t, af.log, proto.EvAgentCreated)
	if len(created) != 1 {
		t.Fatalf("agent.created events = %d", len(created))
	}
	if strings.Contains(string(proto.MustMarshal(created[0])), "fix the tests") {
		t.Fatal("agent.created carries the prompt body; it must carry the hash only")
	}
	if p := payloadOf(t, created[0]); p["task_hash"] != proto.AgentTaskHash("fix the tests") {
		t.Fatalf("task_hash = %v", p["task_hash"])
	}
	if created[0].Workspace != a.WS || created[0].Tenant != "tenant-a" {
		t.Fatalf("event stamped %q/%q", created[0].Workspace, created[0].Tenant)
	}
	row := af.c.db.QueryRow(`SELECT count(*) FROM agents WHERE id = ?`, a.ID)
	var n int
	if err := row.Scan(&n); err != nil || n != 1 {
		t.Fatalf("agent rows = %d, %v", n, err)
	}
}

func TestAgentCreateIsIdempotentAndValidates(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	req := proto.AgentCreateReq{Name: "worker", Spec: agentSpec("hello"), IdempotencyKey: "k1"}
	first := af.create(t, localSubject(), req)
	second := af.create(t, localSubject(), req)
	if first.ID != second.ID || first.WS != second.WS {
		t.Fatalf("replay made a new agent: %s/%s vs %s/%s", first.ID, first.WS, second.ID, second.WS)
	}
	if first.Spec.Sandbox != proto.AgentSandboxWorkspaceWrite {
		t.Fatalf("default sandbox = %q, want workspace-write", first.Spec.Sandbox)
	}
	list, err := af.c.agentList(context.Background(), localSubject(), &proto.AgentListReq{})
	if err != nil || len(list.Agents) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{Spec: proto.AgentSpec{}}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("missing recipe = %v", err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: agentSpec(""), Policy: proto.AgentPolicy{Approve: "sometimes"},
	}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("bad approve policy = %v", err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: proto.AgentSpec{Recipe: "pi", Sandbox: "unconfined"},
	}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("bad sandbox = %v", err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: agentSpec(""), Policy: proto.AgentPolicy{Approve: proto.ApproveAuto},
	}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("auto approve on a local process workspace = %v, want denied", err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: agentSpec(""), WS: first.WS,
	}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("second agent on one workspace = %v, want conflict", err)
	}
	if got := af.c.snapshotWS(first.WS); got == nil || got.State == proto.WSDestroyed {
		t.Fatal("a rejected create must not destroy the adopted workspace")
	}
}

func TestAgentBindingSpecsAreValidatedAndCopied(t *testing.T) {
	af := newAgentFixture(t, "", func(opts *Options) {
		opts.Bindings = []Binding{{ID: "b_team", Secret: "test-only", Destinations: []string{"api.openai.com"}}}
	})
	spec := agentSpec("hello")
	spec.Providers = []string{"openai"}
	spec.Primary = "openai"
	spec.BindingSpecs = []string{"b_team:openai"}
	a := af.create(t, localSubject(), proto.AgentCreateReq{
		Spec:      spec,
		Workspace: &proto.WorkspaceSpec{Bindings: []string{"b_team"}},
	})
	a.Spec.BindingSpecs[0] = "b_other:anthropic"
	got := af.agent(t, a.ID)
	if len(got.Spec.BindingSpecs) != 1 || got.Spec.BindingSpecs[0] != "b_team:openai" {
		t.Fatalf("stored binding specs = %v", got.Spec.BindingSpecs)
	}

	bad := agentSpec("")
	bad.BindingSpecs = []string{"b_other:openai"}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: bad, Workspace: &proto.WorkspaceSpec{Bindings: []string{"b_team"}},
	}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("unattached binding = %v, want bad_request", err)
	}
}

func TestAgentAutoApproveRejectedBeforeWorkspaceExists(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	count := func() int {
		af.c.mu.Lock()
		defer af.c.mu.Unlock()
		return len(af.c.workspaces)
	}
	before := count()
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{
		Spec: agentSpec(""), Policy: proto.AgentPolicy{Approve: proto.ApproveAuto},
	}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("auto approve on a local process workspace = %v, want denied", err)
	}
	if after := count(); after != before {
		t.Fatalf("a rejected create left %d workspaces, had %d", after, before)
	}

	// A deployment floor raises every workspace to isolated, so the same
	// request is legitimate there: the early check judges the effective spec.
	floored := newAgentFixture(t, "", func(o *Options) { o.SecurityProfileFloor = proto.SecurityIsolated })
	a := floored.create(t, localSubject(), proto.AgentCreateReq{
		Spec: agentSpec(""), Policy: proto.AgentPolicy{Approve: proto.ApproveAuto},
	})
	if ws := floored.c.snapshotWS(a.WS); ws == nil || ws.Spec.Security.Profile != proto.SecurityIsolated {
		t.Fatalf("floored workspace = %+v", ws)
	}
}

func TestAgentAuthorizationFollowsWorkspaceACL(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("")})
	stranger := Subject{ID: "mallory", Tenant: "tenant-b"}
	if _, err := af.c.agentGet(context.Background(), stranger, a.ID); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant get = %v, want denied", err)
	}
	if _, err := af.c.agentMessage(context.Background(), stranger, &proto.AgentMessageReq{ID: a.ID, Text: "hi"}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant message = %v, want denied", err)
	}
	if err := af.c.agentDestroy(context.Background(), stranger, &proto.AgentGetReq{ID: a.ID}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant destroy = %v, want denied", err)
	}
	list, err := af.c.agentList(context.Background(), stranger, &proto.AgentListReq{})
	if err != nil || len(list.Agents) != 0 {
		t.Fatalf("stranger sees %d agents", len(list.Agents))
	}
	if got, err := af.c.agentGet(context.Background(), localSubject(), a.ID); err != nil || got.ID != a.ID {
		t.Fatalf("owner get = %v, %v", got, err)
	}
}

func TestAgentDestroyKeepsAdoptedWorkspace(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{Name: "mine"})
	adopted := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec(""), WS: ws.ID})
	if adopted.OwnsWS {
		t.Fatal("adopting an existing workspace must not claim ownership")
	}
	owned := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("")})
	for _, a := range []*proto.Agent{adopted, owned} {
		if err := af.c.agentDestroy(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "d-" + a.ID}); err != nil {
			t.Fatal(err)
		}
		if err := af.c.agentDestroy(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "d-" + a.ID}); err != nil {
			t.Fatalf("destroy replay = %v", err)
		}
		if got := af.agent(t, a.ID); got.Status != proto.AgentDestroyed {
			t.Fatalf("status after destroy = %s", got.Status)
		}
	}
	if got := af.c.snapshotWS(ws.ID); got == nil || got.State == proto.WSDestroyed || got.State == proto.WSDestroying {
		t.Fatalf("adopted workspace = %+v; destroying the agent must leave it alone", got)
	}
	if got := af.c.snapshotWS(owned.WS); got != nil && got.State != proto.WSDestroyed && got.State != proto.WSDestroying {
		t.Fatalf("owned workspace = %s, want destroyed", got.State)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentDestroyed)); n != 2 {
		t.Fatalf("agent.destroyed events = %d", n)
	}
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: owned.ID, Text: "hi"}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("message to destroyed agent = %v", err)
	}
}

func TestAgentMessageIsFIFOHashOnlyAndDegradesSteer(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("first")})
	res, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "second", IdempotencyKey: "m2"})
	if err != nil || res.Degraded {
		t.Fatalf("follow_up = %+v, %v", res, err)
	}
	replay, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "second", IdempotencyKey: "m2"})
	if err != nil || replay.Message.ID != res.Message.ID {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	steer, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "third", Kind: proto.AgentMessageSteer})
	if err != nil || !steer.Degraded || steer.Message.Kind != proto.AgentMessageFollowUp {
		t.Fatalf("steer = %+v, %v; ACP has no mid-turn input so a steer degrades", steer, err)
	}
	got := af.agent(t, a.ID)
	var texts []string
	for _, m := range got.Inbox {
		texts = append(texts, m.Text)
	}
	if strings.Join(texts, ",") != "first,second,third" {
		t.Fatalf("inbox order = %v", texts)
	}
	for _, e := range eventsOfType(t, af.log, proto.EvAgentMessage) {
		raw := string(proto.MustMarshal(e))
		for _, body := range []string{"second", "third"} {
			if strings.Contains(raw, `"`+body+`"`) {
				t.Fatalf("agent.message payload carries the prompt body %q: %s", body, raw)
			}
		}
		if p := payloadOf(t, e); p["text_hash"] == "" || p["text_hash"] == nil {
			t.Fatalf("agent.message without text_hash: %v", p)
		}
	}
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "", Kind: "shout"}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("bad kind = %v", err)
	}
	for i := len(got.Inbox); i < proto.MaxAgentInbox; i++ {
		if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "fill"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "overflow"}); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("inbox overflow = %v, want resource_exhausted", err)
	}
}

func TestAgentRunLifecycleAndStatusDerivation(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "do the thing")
	if run.Agent != a.ID || run.WS != a.WS || len(run.Messages) != 1 || run.Messages[0].Text != "do the thing" {
		t.Fatalf("agent.run request = %+v", run)
	}
	if got := af.agent(t, a.ID); got.Status != proto.AgentRunning || got.TranscriptSession != "s_acp" {
		t.Fatalf("after start: status=%s transcript=%q", got.Status, got.TranscriptSession)
	}
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportSession, ACPSessionID: "sess-1", Loaded: false})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	if got := af.agent(t, a.ID); got.Status != proto.AgentRunning || got.ACPSessionID != "sess-1" || len(got.Inbox) != 1 {
		t.Fatalf("mid-turn: status=%s session=%q inbox=%d (the message stays queued until its turn ends)", got.Status, got.ACPSessionID, len(got.Inbox))
	}
	// A duplicate report is applied once.
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 4, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn", Usage: &proto.AgentUsage{Input: 10, Output: 5}})
	af.report(t, run, 4, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	got := af.agent(t, a.ID)
	if got.Status != proto.AgentWaitingInput || got.Turns != 1 || len(got.Inbox) != 0 || got.IdleSince == 0 {
		t.Fatalf("after turn: %+v", got)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentTurn)); n != 1 {
		t.Fatalf("agent.turn events = %d, want 1 (duplicate seq applied once)", n)
	}
	// A second message reaches the live run directly.
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "more"}); err != nil {
		t.Fatal(err)
	}
	af.waitOps(proto.OpAgentDeliver, 1)
	if af.opCount(proto.OpAgentDeliver) != 1 {
		t.Fatalf("agent.deliver sent %d times", af.opCount(proto.OpAgentDeliver))
	}
	if got := af.agent(t, a.ID); got.Status != proto.AgentRunning {
		t.Fatalf("queued message => %s, want running", got.Status)
	}
	// The harness exits cleanly with nothing queued: idle, not failed.
	af.report(t, run, 5, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: af.agent(t, a.ID).Inbox[0].ID})
	af.report(t, run, 6, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	af.report(t, run, 7, proto.AgentReport{Kind: proto.AgentReportFinished, StopReason: "exit"})
	got = af.agent(t, a.ID)
	if got.Status != proto.AgentIdle || got.Failures != 0 || liveRun(got) != nil {
		t.Fatalf("after clean exit: %+v", got)
	}
	// A duplicate of an applied report is ignored. A later report for the
	// finished run tells the node its copy is no longer authoritative:
	// conflict, not silence, so a harness the control plane gave up on stops.
	af.report(t, run, 7, proto.AgentReport{Kind: proto.AgentReportFinished, StopReason: "exit"})
	late := proto.AgentReport{Agent: run.Agent, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: 8, Kind: proto.AgentReportToolCall, ToolCall: "tc1"}
	if err := af.c.agentReport(context.Background(), "n_one", &late); codeOf(err) != proto.CodeConflict {
		t.Fatalf("late tool_call after finished: err = %v, want conflict", err)
	}
	if got := af.agent(t, a.ID); got.Status != proto.AgentIdle || got.Failures != 0 {
		t.Fatalf("late report changed the agent: %+v", got)
	}
	// A new message starts a fresh run.
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "again"}); err != nil {
		t.Fatal(err)
	}
	af.c.agentReconcile(context.Background())
	next := af.lastRun(t)
	if next.Run == run.Run || next.Attempt != 2 || next.ACPSessionID != "sess-1" {
		t.Fatalf("second run = %+v; it must load the same ACP session", next)
	}
	for _, e := range eventsOfType(t, af.log, proto.EvAgentRunStarted) {
		if e.Node != "n_one" {
			t.Fatalf("run.started event without node: %+v", e)
		}
	}
}

func TestAgentReportFencing(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	_, run := af.start(t, localSubject(), "task")
	bad := run
	bad.Gen++
	rep := proto.AgentReport{Agent: bad.Agent, Run: bad.Run, WS: bad.WS, Gen: bad.Gen, Seq: 2, Kind: proto.AgentReportToolCall}
	if err := af.c.agentReport(context.Background(), "n_one", &rep); codeOf(err) != proto.CodeConflict {
		t.Fatalf("stale generation report = %v, want conflict", err)
	}
	rep.Gen = run.Gen
	if err := af.c.agentReport(context.Background(), "n_two", &rep); codeOf(err) != proto.CodeUnauthorized {
		t.Fatalf("report from the wrong node = %v, want unauthorized", err)
	}
	rep.Kind = "gossip"
	if err := af.c.agentReport(context.Background(), "n_one", &rep); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("unknown report kind = %v", err)
	}
	rep.Run = "run_missing"
	if err := af.c.agentReport(context.Background(), "n_one", &rep); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("unknown run = %v", err)
	}
}

func TestAgentFailureRetriesOnceThenFails(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "crashy")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportFinished, ExitCode: 1})
	got := af.agent(t, a.ID)
	if got.Status != proto.AgentRunning || got.Failures != 1 || len(got.Inbox) != 1 {
		t.Fatalf("after first crash: status=%s failures=%d inbox=%d; the message must survive for the retry", got.Status, got.Failures, len(got.Inbox))
	}
	af.c.agentReconcile(context.Background())
	if len(af.runs) != 1 {
		t.Fatal("retry launched before the backoff elapsed")
	}
	af.advance(agentRetryBackoff + time.Second)
	af.c.agentReconcile(context.Background())
	retry := af.lastRun(t)
	if retry.Run == run.Run || retry.Attempt != 2 || len(retry.Messages) != 1 {
		t.Fatalf("retry = %+v", retry)
	}
	af.report(t, retry, 1, proto.AgentReport{Kind: proto.AgentReportStarted})
	af.report(t, retry, 2, proto.AgentReport{Kind: proto.AgentReportFinished, Error: "session/load: no such session"})
	got = af.agent(t, a.ID)
	if got.Status != proto.AgentFailed || got.Failures != 2 || len(got.Inbox) != 0 {
		t.Fatalf("after second failure: %+v", got)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentFailed)); n != 1 {
		t.Fatalf("agent.failed events = %d", n)
	}
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "hello?"}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("message to failed agent = %v", err)
	}
	// Launch refusal counts like a crash.
	b := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("refused")})
	af.claim(t, b.WS)
	af.fail.Store(true)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, b.ID); got.Failures != 1 || liveRun(got) != nil {
		t.Fatalf("after refused launch: %+v", got)
	}
	af.advance(agentRetryBackoff + time.Second)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, b.ID); got.Status != proto.AgentFailed {
		t.Fatalf("after second refused launch: %s", got.Status)
	}
}

func TestAgentCancelDropsInboxAndTellsNode(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "long task")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "and then"}); err != nil {
		t.Fatal(err)
	}
	got, err := af.c.agentCancel(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Inbox) != 0 || got.Status != proto.AgentWaitingInput {
		t.Fatalf("after cancel: %+v", got)
	}
	af.waitOps(proto.OpAgentRunCancel, 1)
	if af.opCount(proto.OpAgentRunCancel) != 1 {
		t.Fatalf("agent.run.cancel sent %d times", af.opCount(proto.OpAgentRunCancel))
	}
	if _, err := af.c.agentCancel(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "c1"}); err != nil {
		t.Fatalf("cancel replay = %v", err)
	}
	ev := eventsOfType(t, af.log, proto.EvAgentCancelled)
	if len(ev) != 1 || payloadInt(payloadOf(t, ev[0])["dropped"]) != 2 {
		t.Fatalf("agent.cancelled = %+v; the in-flight message and the queued one are both dropped", ev)
	}
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportFinished, Cancelled: true})
	if got := af.agent(t, a.ID); got.Status != proto.AgentIdle || got.Failures != 0 {
		t.Fatalf("after cancelled exit: %+v; a cancel is not a failure", got)
	}
}

func TestAgentSleepAndWakeOnMessage(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	slept, err := af.c.agentSleep(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if slept.Status != proto.AgentSleeping || slept.WakeTimer == "" || liveRun(slept) != nil {
		t.Fatalf("after sleep: %+v", slept)
	}
	if ws := af.c.snapshotWS(a.WS); ws.State != proto.WSPaused {
		t.Fatalf("workspace after agent sleep = %s, want paused", ws.State)
	}
	if af.opCount(proto.OpAgentRunCancel) != 1 {
		t.Fatalf("sleep must stop the run first; cancel sent %d times", af.opCount(proto.OpAgentRunCancel))
	}
	res, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "wake up"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Woken || res.Agent.Status != proto.AgentRunning || res.Agent.WakeTimer != "" {
		t.Fatalf("message to sleeping agent = %+v", res)
	}
	if ws := af.c.snapshotWS(a.WS); ws.State != proto.WSPending {
		t.Fatalf("workspace after wake = %s, want pending (claimable again)", ws.State)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentWoken)); n != 1 {
		t.Fatalf("agent.woken events = %d", n)
	}
	// Once a node claims it, the queued message starts a run that loads the session.
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	next := af.lastRun(t)
	if next.Run == run.Run || len(next.Messages) != 1 || next.Messages[0].Text != "wake up" {
		t.Fatalf("run after wake = %+v", next)
	}
	// A policy sleep fires after the idle window.
	af.report(t, next, 1, proto.AgentReport{Kind: proto.AgentReportStarted})
	af.report(t, next, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: next.Messages[0].ID})
	af.report(t, next, 3, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	af.c.mu.Lock()
	af.c.agents[a.ID].Policy.SleepAfterSec = 30
	af.c.mu.Unlock()
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, a.ID); got.Status != proto.AgentWaitingInput {
		t.Fatalf("slept before the idle window: %s", got.Status)
	}
	af.advance(31 * time.Second)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, a.ID); got.Status != proto.AgentSleeping {
		t.Fatalf("policy sleep did not fire: %s", got.Status)
	}
}

func TestAgentForkSnapshotsAndInheritsPolicy(t *testing.T) {
	af := newAgentFixture(t, "", func(opts *Options) {
		opts.Bindings = []Binding{{ID: "b_team", Secret: "test-only", Destinations: []string{"api.openai.com"}}}
	})
	a := af.create(t, localSubject(), proto.AgentCreateReq{
		Spec: proto.AgentSpec{
			Recipe: "pi", Task: "task", Providers: []string{"openai"},
			Primary: "openai", BindingSpecs: []string{"b_team:openai"},
		},
		Policy: proto.AgentPolicy{MaxTurns: 10},
		Workspace: &proto.WorkspaceSpec{
			Repo:     proto.RepoSpec{URL: "https://github.com/acme/repo.git"},
			Bindings: []string{"b_team"},
		},
	})
	if _, err := af.c.agentFork(context.Background(), localSubject(), &proto.AgentForkReq{ID: a.ID}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("fork of a pending workspace without a snapshot = %v, want conflict", err)
	}
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted})
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportSession, ACPSessionID: "sess-9"})
	wider := proto.AgentPolicy{MaxTurns: 0}
	if _, err := af.c.agentFork(context.Background(), localSubject(), &proto.AgentForkReq{ID: a.ID, Policy: &wider}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("fork widening max_turns = %v, want denied", err)
	}
	auto := proto.AgentPolicy{Approve: proto.ApproveAuto, MaxTurns: 5}
	if _, err := af.c.agentFork(context.Background(), localSubject(), &proto.AgentForkReq{ID: a.ID, Policy: &auto}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("fork widening approve = %v, want denied", err)
	}
	tighter := proto.AgentPolicy{MaxTurns: 3}
	child, err := af.c.agentFork(context.Background(), localSubject(), &proto.AgentForkReq{ID: a.ID, Name: "child", Task: "explore", Policy: &tighter, IdempotencyKey: "f1"})
	if err != nil {
		t.Fatal(err)
	}
	if child.ForkedFrom != a.ID || child.WS == a.WS || !child.OwnsWS || child.ACPSessionID != "sess-9" || child.Policy.MaxTurns != 3 {
		t.Fatalf("fork = %+v", child)
	}
	if child.Policy.Approve != proto.ApproveOnRequest {
		t.Fatalf("fork approve = %q", child.Policy.Approve)
	}
	if !slices.Equal(child.Spec.BindingSpecs, []string{"b_team:openai"}) {
		t.Fatalf("fork binding specs = %v", child.Spec.BindingSpecs)
	}
	ws, err := af.c.wsGet(child.WS)
	if err != nil {
		t.Fatal(err)
	}
	if ws.Spec.RestoreFrom == "" || ws.Spec.Repo.URL != "" {
		t.Fatalf("fork workspace spec = %+v; it restores the snapshot, not a fresh clone", ws.Spec)
	}
	if ws.Spec.Labels["remount.forked_from"] != a.ID || ws.Spec.Labels["remount.agent"] != child.ID {
		t.Fatalf("fork labels = %v", ws.Spec.Labels)
	}
	if af.opCount(proto.OpWSSnapshot) != 1 {
		t.Fatalf("snapshot requested %d times", af.opCount(proto.OpWSSnapshot))
	}
	again, err := af.c.agentFork(context.Background(), localSubject(), &proto.AgentForkReq{ID: a.ID, Name: "child", Task: "explore", Policy: &tighter, IdempotencyKey: "f1"})
	if err != nil || again.ID != child.ID {
		t.Fatalf("fork replay = %+v, %v", again, err)
	}
	if af.opCount(proto.OpWSSnapshot) != 1 {
		t.Fatal("fork replay took another snapshot")
	}
	forked := eventsOfType(t, af.log, proto.EvAgentForked)
	if len(forked) != 1 || strings.Contains(string(proto.MustMarshal(forked[0])), "explore") {
		t.Fatalf("agent.forked = %+v", forked)
	}
	// The child's workspace being destroyed by hand fails the child, not the parent.
	if err := af.c.wsDestroy(context.Background(), "alice", child.WS, ""); err != nil {
		t.Fatal(err)
	}
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, child.ID); got.Status != proto.AgentFailed {
		t.Fatalf("child after workspace destroy = %s", got.Status)
	}
	if got := af.agent(t, a.ID); agentTerminal(got.Status) {
		t.Fatalf("parent after child workspace destroy = %s", got.Status)
	}
}

func TestAgentMaxTurnsFinishes(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("one"), Policy: proto.AgentPolicy{MaxTurns: 1}})
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted})
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	if got := af.agent(t, a.ID); got.Status != proto.AgentFinished || !strings.Contains(got.StatusReason, "max_turns") {
		t.Fatalf("after max_turns: %+v", got)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentFinished)); n != 1 {
		t.Fatalf("agent.finished events = %d", n)
	}
	if _, err := af.c.agentMessage(context.Background(), localSubject(), &proto.AgentMessageReq{ID: a.ID, Text: "more"}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("message to finished agent = %v", err)
	}
	if err := af.c.agentDestroy(context.Background(), localSubject(), &proto.AgentGetReq{ID: a.ID}); err != nil {
		t.Fatalf("destroy finished agent = %v", err)
	}
}

func TestAgentTenantQuota(t *testing.T) {
	af := newAgentFixture(t, "", func(o *Options) { o.MaxAgentsPerTenant = 2 })
	af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("")})
	second := af.create(t, localSubject(), proto.AgentCreateReq{Spec: agentSpec("")})
	_, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{Spec: agentSpec("")})
	if codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("third agent = %v, want resource_exhausted", err)
	}
	live := 0
	af.c.mu.Lock()
	for _, ws := range af.c.workspaces {
		if ws.State != proto.WSDestroyed && ws.State != proto.WSDestroying {
			live++
		}
	}
	af.c.mu.Unlock()
	if live != 2 {
		t.Fatalf("live workspaces after rejected create = %d; the rejected create must undo its workspace", live)
	}
	if err := af.c.agentDestroy(context.Background(), localSubject(), &proto.AgentGetReq{ID: second.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := af.c.agentCreate(context.Background(), localSubject(), &proto.AgentCreateReq{Spec: agentSpec("")}); err != nil {
		t.Fatalf("create after destroy = %v; destroyed agents do not count", err)
	}
	other := Subject{ID: "bob", Tenant: "tenant-b", Roles: []string{"admin"}}
	if _, err := af.c.agentCreate(context.Background(), other, &proto.AgentCreateReq{Spec: agentSpec("")}); err != nil {
		t.Fatalf("other tenant = %v; quotas are per tenant", err)
	}
}

func TestAgentLostRunWhenWorkspaceMovesOrNodeDies(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	// Node vanishes past the lease: the run is lost and retried after backoff.
	af.sender.mu.Lock()
	af.sender.online["n_one"] = false
	af.sender.mu.Unlock()
	af.c.PeerGone(context.Background(), "n_one")
	af.advance(time.Duration(af.c.opts.LeaseSec+1) * time.Second)
	af.c.agentReconcile(context.Background())
	got := af.agent(t, a.ID)
	if liveRun(got) != nil || got.Runs[0].StopReason != "node gone" || len(got.Inbox) != 1 {
		t.Fatalf("after node loss: %+v", got)
	}
	finished := eventsOfType(t, af.log, proto.EvAgentRunFinished)
	if len(finished) != 1 || payloadOf(t, finished[0])["stop_reason"] != "node gone" {
		t.Fatalf("run.finished = %+v", finished)
	}
	// An offline node is told nothing: no cancel for the lost run and no
	// retry dispatched to it. The retry waits for a node that can hear it.
	af.advance(agentRetryBackoff * 2)
	af.c.agentReconcile(context.Background())
	if cancels, runs := af.opCount(proto.OpAgentRunCancel), af.opCount(proto.OpAgentRun); cancels != 0 || runs != 1 {
		t.Fatalf("offline node received cancel=%d run=%d, want 0 and the original 1", cancels, runs)
	}
	if got := af.agent(t, a.ID); liveRun(got) != nil {
		t.Fatalf("a run was recorded against an offline node: %+v", got)
	}
}

// A scheduled agent survives a control-plane restart still scheduled: the
// start time is policy, not a timer that has to be re-armed, so the restarted
// control plane neither launches early nor forgets to launch once the clock
// passes start_at.
func TestScheduledAgentSurvivesRestartAndStartsOnTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	af := newAgentFixture(t, path, nil)
	startAt := time.UnixMilli(af.clock.Load()).Add(time.Hour)
	a := af.create(t, localSubject(), proto.AgentCreateReq{
		Name: "later", Spec: agentSpec("task"),
		Policy: proto.AgentPolicy{Approve: proto.ApproveNever, StartAt: startAt.UnixMilli()},
	})
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, a.ID); got.Status != proto.AgentScheduled || len(got.Runs) != 0 || len(got.Inbox) != 1 {
		t.Fatalf("before start_at: %+v", got)
	}
	if n := af.opCount(proto.OpAgentRun); n != 0 {
		t.Fatalf("agent.run sent %d times before start_at", n)
	}
	af.c.Stop()
	_ = af.log.Close()

	clock := &af.clock
	again := newAgentFixtureWithNode(t, path, af.nodeKey, func(o *Options) {
		o.Now = func() time.Time { return time.UnixMilli(clock.Load()) }
	})
	// The node re-adopts the tree it still holds; that alone must not start
	// the agent.
	again.claim(t, a.WS)
	again.c.agentReconcile(context.Background())
	if got := again.agent(t, a.ID); got.Status != proto.AgentScheduled || len(got.Runs) != 0 {
		t.Fatalf("after restart, before start_at: %+v", got)
	}
	if n := again.opCount(proto.OpAgentRun); n != 0 {
		t.Fatalf("restart launched a scheduled agent %d times early", n)
	}
	clock.Add((time.Hour + time.Second).Milliseconds())
	again.c.agentReconcile(context.Background())
	run := again.lastRun(t)
	if run.Agent != a.ID || len(run.Messages) != 1 || run.Messages[0].Text != "task" {
		t.Fatalf("run after start_at = %+v", run)
	}
	if got := again.agent(t, a.ID); len(got.Runs) != 1 || got.Status == proto.AgentScheduled {
		t.Fatalf("after start_at: %+v", got)
	}
}

// A run the control plane gives up on while its node is still reachable is
// cancelled there: the durable run is over, so the harness must not keep
// working with brokered credentials no run accounts for. The launch timeout
// is the case where the node acknowledged but never reported started.
func TestAgentLostRunOnOnlineNodeIsCancelledThere(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{Name: "worker", Spec: agentSpec("task")})
	af.claim(t, a.WS)
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	// A long recipe install must not trip the launch timeout: started goes
	// out before the install, and after it the run is active for as long as
	// the install takes.
	af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp"})
	af.advance(agentLaunchTimeout * 4)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, a.ID); liveRun(got) == nil || liveRun(got).ID != run.Run {
		t.Fatalf("active run lost to the launch timeout: %+v", got)
	}
	if af.opCount(proto.OpAgentRunCancel) != 0 {
		t.Fatal("cancel sent for a run that is active")
	}
	// Second agent: acknowledged, never started, node still online.
	b := af.create(t, localSubject(), proto.AgentCreateReq{Name: "stuck", Spec: agentSpec("task")})
	af.claim(t, b.WS)
	af.c.agentReconcile(context.Background())
	stuck := af.lastRun(t)
	if stuck.Agent != b.ID {
		t.Fatalf("last run belongs to %s, want %s", stuck.Agent, b.ID)
	}
	af.advance(agentLaunchTimeout + time.Second)
	af.c.agentReconcile(context.Background())
	af.waitOps(proto.OpAgentRunCancel, 1)
	got := af.agent(t, b.ID)
	if liveRun(got) != nil || got.Runs[0].StopReason != "launch timed out" || got.Status == proto.AgentFailed {
		t.Fatalf("after launch timeout: %+v", got)
	}
	if af.opCount(proto.OpAgentRunCancel) != 1 {
		t.Fatalf("agent.run.cancel sent %d times, want 1 for the online node", af.opCount(proto.OpAgentRunCancel))
	}
	// The agent's own run was untouched by the other agent's decision.
	if got := af.agent(t, a.ID); liveRun(got) == nil {
		t.Fatalf("unrelated agent lost its run: %+v", got)
	}
}

// A reconcile decision commits before the node hears of it. When the commit
// fails the in-memory agent is rolled back and nothing is dispatched: the
// node never runs an attempt the database did not record, and the next tick
// decides again from durable truth.
func TestAgentReconcilePersistFailureDispatchesNothing(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a := af.create(t, localSubject(), proto.AgentCreateReq{Name: "worker", Spec: agentSpec("task")})
	af.claim(t, a.WS)
	if _, err := af.c.db.Exec(`ALTER TABLE agents RENAME TO agents_offline`); err != nil {
		t.Fatal(err)
	}
	af.c.agentReconcile(context.Background())
	if n := af.opCount(proto.OpAgentRun); n != 0 {
		t.Fatalf("agent.run sent %d times after a failed commit", n)
	}
	got := af.agent(t, a.ID)
	if len(got.Runs) != 0 || len(got.Inbox) != 1 {
		t.Fatalf("in-memory agent kept the uncommitted run: %+v", got)
	}
	af.c.mu.Lock()
	marks, busy := len(af.c.agentDelivered), len(af.c.agentBusy)
	af.c.mu.Unlock()
	if marks != 0 || busy != 0 {
		t.Fatalf("delivery marks = %d, busy = %d after rollback, want 0", marks, busy)
	}
	if _, err := af.c.db.Exec(`ALTER TABLE agents_offline RENAME TO agents`); err != nil {
		t.Fatal(err)
	}
	af.c.agentReconcile(context.Background())
	run := af.lastRun(t)
	if run.Agent != a.ID || run.Attempt != 1 || len(run.Messages) != 1 {
		t.Fatalf("run after recovery = %+v", run)
	}
	if got := af.agent(t, a.ID); len(got.Runs) != 1 || got.Runs[0].ID != run.Run {
		t.Fatalf("agent after recovery = %+v", got)
	}
	af.c.mu.Lock()
	busy = len(af.c.agentBusy)
	af.c.mu.Unlock()
	if busy != 0 {
		t.Fatalf("busy = %d after the work returned", busy)
	}
}

func TestAgentsAndApprovalsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	af := newAgentFixture(t, path, nil)
	a, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportPermission, Approval: &proto.Approval{
		ID: "ap_1", Kind: proto.ApprovalToolCall, Title: "run rm -rf build", ToolCall: "tc1",
		Options: []proto.ApprovalOption{{ID: "allow", Kind: "allow_once", Name: "Allow"}, {ID: "reject", Kind: "reject_once", Name: "Reject"}},
		Detail:  json.RawMessage(`{"command":"rm -rf build"}`),
	}})
	if got := af.agent(t, a.ID); got.Status != proto.AgentWaitingApproval || got.PendingApprovals != 1 {
		t.Fatalf("after permission: %+v", got)
	}
	af.c.Stop()
	_ = af.log.Close()

	again := newControlFixture(t, path, func(o *Options) { o.PublicURL = "https://remount.example" })
	got, err := again.c.agentGet(context.Background(), localSubject(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != proto.AgentWaitingApproval || got.ACPSessionID != a.ACPSessionID || len(got.Runs) != 1 || got.TranscriptSession != "s_acp" {
		t.Fatalf("agent after restart = %+v", got)
	}
	ap, err := again.c.approvalGet(context.Background(), localSubject(), "ap_1")
	if err != nil {
		t.Fatal(err)
	}
	if ap.Status != proto.ApprovalPending || ap.Agent != a.ID || string(ap.Detail) != `{"command":"rm -rf build"}` {
		t.Fatalf("approval after restart = %+v", ap)
	}
	list, err := again.c.approvalList(context.Background(), localSubject(), &proto.ApprovalListReq{Agent: a.ID})
	if err != nil || len(list.Approvals) != 1 {
		t.Fatalf("approval list = %+v, %v", list, err)
	}
}

func TestApprovalDecideDeliversAndExpires(t *testing.T) {
	af := newAgentFixture(t, "", func(o *Options) { o.MaxApprovalsPerAgent = 2 })
	a, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	park := func(seq uint64, id string) error {
		rep := proto.AgentReport{Agent: run.Agent, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: seq, Kind: proto.AgentReportPermission, Approval: &proto.Approval{
			ID: id, Kind: proto.ApprovalToolCall, Title: "write file", ToolCall: "tc-" + id,
			Options: []proto.ApprovalOption{{ID: "once", Kind: "allow_once", Name: "Allow once"}, {ID: "no", Kind: "reject_once", Name: "Reject"}},
		}}
		return af.c.agentReport(context.Background(), "n_one", &rep)
	}
	if err := park(3, "ap_a"); err != nil {
		t.Fatal(err)
	}
	if err := park(4, "ap_b"); err != nil {
		t.Fatal(err)
	}
	if err := park(5, "ap_c"); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("third pending approval = %v, want resource_exhausted", err)
	}
	if err := park(6, "not-an-id"); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("bad approval id = %v", err)
	}
	pending := eventsOfType(t, af.log, proto.EvApprovalPending)
	if len(pending) != 2 {
		t.Fatalf("approval.pending events = %d", len(pending))
	}
	stranger := Subject{ID: "mallory", Tenant: "tenant-b"}
	if _, err := af.c.approvalDecide(context.Background(), stranger, &proto.ApprovalDecideReq{ID: "ap_a", Option: "once"}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("stranger decides = %v", err)
	}
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_a", Option: "maybe"}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("unknown option = %v", err)
	}
	decided, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_a", IdempotencyKey: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	if decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Option != "once" || decided.Decision.By != "alice" {
		t.Fatalf("decided = %+v; an empty option picks the first allow option", decided)
	}
	replay, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_a", IdempotencyKey: "d1"})
	if err != nil || replay.Decision.At != decided.Decision.At {
		t.Fatalf("decide replay = %+v, %v", replay, err)
	}
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_a", Denied: true}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("second decision = %v, want conflict", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ap, _ := af.c.approvalCopy("ap_a"); ap != nil && ap.DeliveredAt > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ap, _ := af.c.approvalCopy("ap_a")
	if ap.DeliveredAt <= 0 || af.opCount(proto.OpAgentApprovalDecided) != 1 {
		t.Fatalf("decision delivery: delivered_at=%d sends=%d", ap.DeliveredAt, af.opCount(proto.OpAgentApprovalDecided))
	}
	if got := af.agent(t, a.ID); got.Status != proto.AgentWaitingApproval || got.PendingApprovals != 1 {
		t.Fatalf("one approval still pending: %+v", got)
	}
	// The run ends: the remaining pending approval expires and the agent leaves waiting_approval.
	af.report(t, run, 7, proto.AgentReport{Kind: proto.AgentReportFinished, Cancelled: true})
	ap, _ = af.c.approvalCopy("ap_b")
	if ap.Status != proto.ApprovalExpired {
		t.Fatalf("ap_b after run end = %s, want expired", ap.Status)
	}
	if got := af.agent(t, a.ID); got.PendingApprovals != 0 || got.Status == proto.AgentWaitingApproval {
		t.Fatalf("agent after run end: %+v", got)
	}
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_b", Denied: true}); codeOf(err) != proto.CodeConflict {
		t.Fatalf("deciding an expired approval = %v", err)
	}
	if n := len(eventsOfType(t, af.log, proto.EvApprovalExpired)); n != 1 {
		t.Fatalf("approval.expired events = %d", n)
	}
	for _, e := range append(pending, eventsOfType(t, af.log, proto.EvApprovalDecided)...) {
		if strings.Contains(string(proto.MustMarshal(e)), "rm -rf") {
			t.Fatalf("approval event carries the request detail: %+v", e)
		}
	}
	all, err := af.c.approvalList(context.Background(), localSubject(), &proto.ApprovalListReq{})
	if err != nil || len(all.Approvals) != 0 {
		t.Fatalf("default list shows non-pending approvals: %+v", all)
	}
}

func TestApprovalDecisionRedeliveredUntilAcknowledged(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	_, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportElicitation, Approval: &proto.Approval{
		ID: "ap_e", Kind: proto.ApprovalElicitation, Title: "which db?", Detail: json.RawMessage(`{"schema":{}}`),
	}})
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_e"}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("elicitation without content = %v", err)
	}
	af.fail.Store(true)
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_e", Content: json.RawMessage(`{"db":"pg"}`)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for af.opCount(proto.OpAgentApprovalDecided) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ap, _ := af.c.approvalCopy("ap_e")
	if ap.Status != proto.ApprovalDecided || ap.DeliveredAt != 0 {
		t.Fatalf("decision must be durable before delivery succeeds: %+v", ap)
	}
	af.fail.Store(false)
	af.c.expireStaleApprovals(context.Background())
	if af.opCount(proto.OpAgentApprovalDecided) != 1 {
		t.Fatal("re-sent before the redelivery interval")
	}
	af.advance(agentRedeliverAfter + time.Second)
	af.c.expireStaleApprovals(context.Background())
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ap, _ := af.c.approvalCopy("ap_e"); ap.DeliveredAt > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ap, _ = af.c.approvalCopy("ap_e")
	if ap.DeliveredAt <= 0 || af.opCount(proto.OpAgentApprovalDecided) != 2 {
		t.Fatalf("redelivery: delivered_at=%d sends=%d", ap.DeliveredAt, af.opCount(proto.OpAgentApprovalDecided))
	}
	if string(ap.Decision.Content) != `{"db":"pg"}` {
		t.Fatalf("decision content = %s", ap.Decision.Content)
	}
}

func TestAgentTransitionsRejectResurrection(t *testing.T) {
	for _, from := range []string{proto.AgentDestroyed, proto.AgentFailed, proto.AgentFinished} {
		for _, to := range []string{proto.AgentRunning, proto.AgentWaitingInput, proto.AgentSleeping, proto.AgentCreating, proto.AgentIdle} {
			if err := transitionAgent(from, to); err == nil {
				t.Fatalf("%s -> %s allowed", from, to)
			}
		}
	}
	if err := transitionAgent(proto.AgentDestroyed, proto.AgentFailed); err == nil {
		t.Fatal("destroyed -> failed allowed")
	}
	for _, from := range []string{proto.AgentFailed, proto.AgentFinished} {
		if err := transitionAgent(from, proto.AgentDestroyed); err != nil {
			t.Fatalf("%s -> destroyed: %v", from, err)
		}
	}
	if err := transitionAgent(proto.AgentIdle, proto.AgentWaitingApproval); err == nil {
		t.Fatal("idle -> waiting_approval allowed without a run")
	}
}

// parkRaw puts an approval row of any kind into the control plane, the way
// the broker will once it parks egress decisions; the node path refuses the
// egress kind.
func (af *agentFixture) parkRaw(t *testing.T, a *proto.Agent, run proto.AgentRunReq, ap proto.Approval) {
	t.Helper()
	af.c.mu.Lock()
	live := af.c.agents[a.ID]
	r := findRun(live, run.Run)
	ws := af.c.workspaces[live.WS]
	kind := ap.Kind
	ap.Kind = proto.ApprovalToolCall
	st := af.c.stageApprovals()
	events, err := st.park(live, r, ws, "n_one", &ap)
	if err == nil {
		st.publishDirtyLocked()
		af.c.approvals[ap.ID].Kind = kind
		af.c.markApprovalDirty(af.c.approvals[ap.ID])
		var more []*proto.Event
		if more, err = af.c.refreshAgentStatusLocked(live, ""); err == nil {
			err = af.c.persistAgentRows(live, "", "", "", nil, nil, append(events, more...)...)
		}
	}
	af.c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestApprovalEgressAndElicitationSemantics(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	// The node may only park the ACP kinds.
	rep := proto.AgentReport{Agent: run.Agent, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: 3, Kind: proto.AgentReportPermission,
		Approval: &proto.Approval{ID: "ap_x", Kind: proto.ApprovalEgress, Title: "api.example.com"}}
	if err := af.c.agentReport(context.Background(), "n_one", &rep); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("node parking egress = %v", err)
	}
	af.parkRaw(t, a, run, proto.Approval{ID: "ap_eg1", Kind: proto.ApprovalEgress, Title: "POST api.example.com"})
	af.parkRaw(t, a, run, proto.Approval{ID: "ap_eg2", Kind: proto.ApprovalEgress, Title: "POST evil.example.com"})
	af.parkRaw(t, a, run, proto.Approval{ID: "ap_eg3", Kind: proto.ApprovalEgress, Title: "POST other.example.com"})
	if got := af.agent(t, a.ID); got.Status != proto.AgentWaitingApproval || got.PendingApprovals != 3 {
		t.Fatalf("agent with egress pending: %+v", got)
	}
	ctx := context.Background()
	if _, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_eg1", Option: "maybe"}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("egress with a made-up option = %v", err)
	}
	if _, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_eg1", Option: "allow", Denied: true}); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("egress allow+denied = %v", err)
	}
	d1, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_eg1"})
	if err != nil || d1.Decision.Denied || d1.Decision.Option != "allow" {
		t.Fatalf("egress default = %+v, %v; want allow", d1, err)
	}
	d2, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_eg2", Option: "deny"})
	if err != nil || !d2.Decision.Denied || d2.Decision.Option != "deny" {
		t.Fatalf("egress deny = %+v, %v", d2, err)
	}
	d3, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_eg3", Denied: true})
	if err != nil || !d3.Decision.Denied || d3.Decision.Option != "deny" {
		t.Fatalf("egress denied = %+v, %v", d3, err)
	}
	if got := af.agent(t, a.ID); got.PendingApprovals != 0 || got.Status != proto.AgentRunning {
		t.Fatalf("agent after all decided: %+v", got)
	}

	// Elicitation content is a JSON object; anything else is refused before
	// the row changes.
	af.report(t, run, 4, proto.AgentReport{Kind: proto.AgentReportElicitation, Approval: &proto.Approval{
		ID: "ap_el", Kind: proto.ApprovalElicitation, Title: "which db?", Detail: json.RawMessage(`{"schema":{}}`),
	}})
	for _, bad := range []proto.ApprovalDecideReq{
		{ID: "ap_el", Content: json.RawMessage(`"pg"`)},
		{ID: "ap_el", Content: json.RawMessage(`["pg"]`)},
		{ID: "ap_el", Content: json.RawMessage(`{"db":`)},
		{ID: "ap_el", Option: "once"},
	} {
		if _, err := af.c.approvalDecide(ctx, localSubject(), &bad); codeOf(err) != proto.CodeBadRequest {
			t.Fatalf("elicitation %+v = %v", bad, err)
		}
	}
	if ap, _ := af.c.approvalCopy("ap_el"); ap.Status != proto.ApprovalPending {
		t.Fatalf("a refused decision changed the row: %+v", ap)
	}
	// A denied elicitation keeps no content, even if some was sent.
	den, err := af.c.approvalDecide(ctx, localSubject(), &proto.ApprovalDecideReq{ID: "ap_el", Denied: true, Content: json.RawMessage(`{"db":"pg"}`)})
	if err != nil || !den.Decision.Denied || len(den.Decision.Content) != 0 {
		t.Fatalf("denied elicitation = %+v, %v", den, err)
	}
	// Events carry the decision, never the elicitation content or egress detail.
	for _, e := range eventsOfType(t, af.log, proto.EvApprovalDecided) {
		if s := string(proto.MustMarshal(e)); strings.Contains(s, `"pg"`) || strings.Contains(s, "schema") {
			t.Fatalf("decided event leaks content: %s", s)
		}
	}

	// A list filtered by agent and status sees only that agent's rows.
	other, orun := af.startNamed(t, localSubject(), "other", "second agent")
	af.report(t, orun, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: orun.Messages[0].ID})
	af.report(t, orun, 3, proto.AgentReport{Kind: proto.AgentReportPermission, Approval: &proto.Approval{
		ID: "ap_o", Kind: proto.ApprovalToolCall, Title: "rm", Options: []proto.ApprovalOption{{ID: "ok", Kind: "allow_once"}},
	}})
	mine, err := af.c.approvalList(ctx, localSubject(), &proto.ApprovalListReq{Agent: a.ID, Status: proto.ApprovalDecided})
	if err != nil || len(mine.Approvals) != 4 {
		t.Fatalf("decided approvals of %s = %+v, %v", a.ID, mine, err)
	}
	theirs, err := af.c.approvalList(ctx, localSubject(), &proto.ApprovalListReq{Agent: other.ID})
	if err != nil || len(theirs.Approvals) != 1 || theirs.Approvals[0].ID != "ap_o" {
		t.Fatalf("pending approvals of %s = %+v, %v", other.ID, theirs, err)
	}
	if _, err := af.c.approvalGet(ctx, Subject{ID: "mallory", Tenant: "tenant-b"}, "ap_o"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("stranger get = %v", err)
	}
}

func TestApprovalUndeliveredDecisionStopsRetryingWhenRunEnds(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	_, run := af.start(t, localSubject(), "ask me")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportPermission, Approval: &proto.Approval{
		ID: "ap_u", Kind: proto.ApprovalToolCall, Title: "rm", Options: []proto.ApprovalOption{{ID: "ok", Kind: "allow_once"}},
	}})
	af.fail.Store(true)
	if _, err := af.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{ID: "ap_u"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for af.opCount(proto.OpAgentApprovalDecided) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	sends := af.opCount(proto.OpAgentApprovalDecided)
	// The run ends before the node ever acknowledged.
	af.report(t, run, 4, proto.AgentReport{Kind: proto.AgentReportFinished, Cancelled: true})
	af.fail.Store(false)
	af.advance(agentRedeliverAfter + time.Second)
	af.c.expireStaleApprovals(context.Background())
	af.advance(agentRedeliverAfter + time.Second)
	af.c.expireStaleApprovals(context.Background())
	ap, _ := af.c.approvalCopy("ap_u")
	if ap.Status != proto.ApprovalDecided || ap.DeliveredAt != -1 {
		t.Fatalf("undelivered decision after run end = %+v; want delivered_at -1", ap)
	}
	if af.opCount(proto.OpAgentApprovalDecided) != sends {
		t.Fatalf("decision re-sent after the run ended: %d -> %d", sends, af.opCount(proto.OpAgentApprovalDecided))
	}
}

func TestAgentTranscriptMirrorPagesEvictsAndWaits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	af := newAgentFixture(t, path, func(o *Options) { o.MaxTranscriptBytesPerAgent = 1000 })
	a, run := af.start(t, localSubject(), "talk")
	ctx := context.Background()
	chunk := func(seq uint64, size int) proto.TranscriptChunk {
		return proto.TranscriptChunk{Seq: seq, Stream: proto.StreamACPOut, At: 1, Data: bytes.Repeat([]byte{byte('a' + seq%26)}, size)}
	}
	// Nothing yet: an empty page, no gap, not done.
	page, err := af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID})
	if err != nil || len(page.Records) != 0 || page.Next != 0 || page.Gap != nil || page.Done {
		t.Fatalf("empty page = %+v err=%v", page, err)
	}
	wait := af.c.TranscriptWait(a.ID, 0)
	select {
	case <-wait:
		t.Fatal("wait released before any record")
	default:
	}
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{chunk(1, 300), chunk(2, 300), chunk(3, 300)}})
	select {
	case <-wait:
	case <-time.After(time.Second):
		t.Fatal("wait not released by a mirrored batch")
	}
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, Limit: 2})
	if err != nil || len(page.Records) != 2 || page.Next != 2 || page.Gap != nil {
		t.Fatalf("first page = %+v err=%v", page, err)
	}
	if page.Records[0].Index != 0 || page.Records[0].Seq != 1 || page.Records[0].Run != run.Run || page.Records[1].Index != 1 || len(page.Records[1].Data) != 300 {
		t.Fatalf("records = %+v", page.Records)
	}
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: page.Next})
	if err != nil || len(page.Records) != 1 || page.Next != 3 || page.Records[0].Index != 2 {
		t.Fatalf("second page = %+v err=%v", page, err)
	}
	// A stranger cannot read; a reader without a subject in the tenant is denied.
	if _, err := af.c.agentTranscript(ctx, Subject{ID: "mallory", Tenant: "tenant-b"}, &proto.AgentTranscriptReq{ID: a.ID}); codeOf(err) != proto.CodeDenied {
		t.Fatalf("stranger read = %v", err)
	}
	// Past the budget: the oldest records go, and a reader below the new
	// first index is told about the gap rather than handed a shorter log.
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{chunk(4, 300), chunk(5, 300)}})
	got := af.agent(t, a.ID)
	if got.TranscriptNext != 5 || got.TranscriptFirst != 2 || got.TranscriptBytes != 900 {
		t.Fatalf("bounds after eviction: next=%d first=%d bytes=%d", got.TranscriptNext, got.TranscriptFirst, got.TranscriptBytes)
	}
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: 0})
	if err != nil || page.Gap == nil || page.Gap.From != 0 || page.Gap.To != 2 || len(page.Records) != 3 || page.Records[0].Index != 2 || page.Next != 5 {
		t.Fatalf("page across the gap = %+v gap=%+v err=%v", page, page.Gap, err)
	}
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: 2})
	if err != nil || page.Gap != nil || len(page.Records) != 3 {
		t.Fatalf("page from first = %+v err=%v", page, err)
	}
	// The mirror survives a restart: the bounds live on the agent row and
	// the rows in their own table.
	af.c.Stop()
	_ = af.log.Close()
	again := newControlFixture(t, path, func(o *Options) { o.PublicURL = "https://remount.example"; o.MaxTranscriptBytesPerAgent = 1000 })
	page, err = again.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: 3})
	if err != nil || len(page.Records) != 2 || page.Records[0].Index != 3 || page.Next != 5 || page.Done {
		t.Fatalf("page after reopen = %+v err=%v", page, err)
	}
	af.controlFixture = again
	af.c.Attach(af.sender)
	// An oversized report is refused as malformed, not stored.
	big := proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{chunk(6, proto.MaxACPTranscriptFrame+8192)}}
	big.Agent, big.Run, big.WS, big.Gen, big.Seq = run.Agent, run.Run, run.WS, run.Gen, 4
	if err := af.c.agentReport(ctx, "n_one", &big); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("oversized chunk = %v", err)
	}
	// A tail parked past the end is released when the agent ends, and the
	// page it then reads says done.
	wait = af.c.TranscriptWait(a.ID, 5)
	if err := af.c.agentDestroy(ctx, localSubject(), &proto.AgentGetReq{ID: a.ID}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wait:
	case <-time.After(time.Second):
		t.Fatal("wait not released by destroy")
	}
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: 5})
	if err != nil || len(page.Records) != 0 || !page.Done {
		t.Fatalf("page after destroy = %+v err=%v", page, err)
	}
	// Records of a destroyed agent stay readable until the row is pruned.
	page, err = af.c.agentTranscript(ctx, localSubject(), &proto.AgentTranscriptReq{ID: a.ID, From: 2})
	if err != nil || len(page.Records) != 3 {
		t.Fatalf("page after destroy from 2 = %+v err=%v", page, err)
	}
}

// A child workspace's security contract is bounded above by its parent's on
// every axis; an unset one inherits the parent's whole (E22).
func TestChildSecurityNeverWeakerThanParent(t *testing.T) {
	rule := proto.EgressRule{ID: "docs", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"docs.example.com"}, Methods: []string{"GET"}}
	isolated := proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{rule}}}
	local := proto.SecuritySpec{}
	wider := rule
	wider.Methods = nil
	for name, tc := range map[string]struct {
		child, parent proto.SecuritySpec
		ok            bool
	}{
		"same":                   {isolated, isolated, true},
		"stronger profile":       {proto.SecuritySpec{Profile: proto.SecurityMultiTenant, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{rule}}}, isolated, true},
		"subset of rules":        {proto.SecuritySpec{Profile: proto.SecurityIsolated}, isolated, true},
		"local under local":      {local, local, true},
		"rules under open":       {proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{rule}}}, local, true},
		"weaker profile":         {local, isolated, false},
		"weaker isolation":       {proto.SecuritySpec{Profile: proto.SecurityIsolated, MinIsolation: "none"}, isolated, false},
		"secret mode none":       {proto.SecuritySpec{Profile: proto.SecurityIsolated, SecretMode: "none"}, isolated, false},
		"default allow":          {proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultAllow}}, isolated, false},
		"invented rule":          {proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{ID: "other", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"x.example.com"}}}}}, isolated, false},
		"widened rule":           {proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{wider}}}, isolated, false},
		"dropped audit":          {proto.SecuritySpec{Profile: proto.SecurityIsolated, Audit: proto.AuditPolicy{Required: false}}, proto.SecuritySpec{Profile: proto.SecurityLocal, Audit: proto.AuditPolicy{Required: true}}, true},
		"dropped audit on local": {proto.SecuritySpec{Profile: proto.SecurityLocal, MinIsolation: "none"}, proto.SecuritySpec{Audit: proto.AuditPolicy{Required: true}}, false},
	} {
		err := securityWithin(tc.child, tc.parent)
		if tc.ok && err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if !tc.ok && !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
			t.Errorf("%s: err = %v, want denied", name, err)
		}
	}

	parent := &proto.Agent{Spec: proto.AgentSpec{
		Providers: []string{"p"}, Primary: "p", BindingSpecs: []string{"b_team:openai"},
		Sandbox: proto.AgentSandboxReadOnly,
	}}
	pws := &proto.Workspace{Spec: proto.WorkspaceSpec{Security: isolated}}
	req := &proto.AgentCreateReq{Workspace: &proto.WorkspaceSpec{}}
	if err := inheritFromParent(req, parent, pws); err != nil {
		t.Fatal(err)
	}
	if req.Workspace.Security.Profile != proto.SecurityIsolated || len(req.Workspace.Security.Network.Rules) != 1 {
		t.Fatalf("unset child security = %+v, want the parent's", req.Workspace.Security)
	}
	if req.Spec.Sandbox != proto.AgentSandboxReadOnly {
		t.Fatalf("unset child sandbox = %q, want the parent's read-only", req.Spec.Sandbox)
	}
	if len(req.Spec.BindingSpecs) != 1 || req.Spec.BindingSpecs[0] != "b_team:openai" {
		t.Fatalf("unset child binding specs = %v, want the parent's", req.Spec.BindingSpecs)
	}
	expanded := &proto.AgentCreateReq{Spec: proto.AgentSpec{
		Providers: []string{"p"}, BindingSpecs: []string{"b_other:openai"},
	}, Workspace: &proto.WorkspaceSpec{}}
	if err := inheritFromParent(expanded, parent, pws); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("expanded child binding specs: err = %v, want denied", err)
	}
	req = &proto.AgentCreateReq{Workspace: &proto.WorkspaceSpec{Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Default: proto.NetworkDefaultAllow}}}}
	if err := inheritFromParent(req, parent, pws); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("partially set weaker child security: err = %v, want denied", err)
	}
}

// A policy sleep the node refuses is retried after the backoff, not on every
// tick: each attempt is a release round trip, and the kick that follows a
// finished action would otherwise turn the refusal into a hot loop.
func TestAgentPolicySleepFailureBacksOff(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportTurnFinished, StopReason: "end_turn"})
	af.c.mu.Lock()
	af.c.agents[a.ID].Policy.SleepAfterSec = 30
	af.c.mu.Unlock()
	af.advance(31 * time.Second)
	af.mu.Lock()
	af.refuse = map[string]bool{proto.OpWSRelease: true}
	af.mu.Unlock()
	af.c.agentReconcile(context.Background())
	first := af.opCount(proto.OpWSRelease)
	if first == 0 {
		t.Fatal("policy sleep was never attempted")
	}
	if got := af.agent(t, a.ID); got.Status == proto.AgentSleeping {
		t.Fatalf("agent slept although the node refused: %+v", got)
	}
	for i := 0; i < 5; i++ {
		af.c.agentReconcile(context.Background())
	}
	if again := af.opCount(proto.OpWSRelease); again != first {
		t.Fatalf("sleep retried %d times inside the backoff window", again-first)
	}
	af.mu.Lock()
	af.refuse = nil
	af.mu.Unlock()
	af.advance(agentRetryBackoff + time.Second)
	af.c.agentReconcile(context.Background())
	if got := af.agent(t, a.ID); got.Status != proto.AgentSleeping {
		t.Fatalf("policy sleep did not retry after the backoff: %s", got.Status)
	}
}
