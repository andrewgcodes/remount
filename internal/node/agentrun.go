package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// Agent runs on the node. One agentRun is one attempt of an Agent: the
// harness process speaking ACP over its stdio, the record-only transcript
// session that keeps every frame, the terminals the harness opened through
// us, and the permission requests parked with the control plane. The control
// plane owns every durable decision; the run only executes and reports what
// it observed, in order, fenced by workspace generation and report sequence.

const (
	// agentReportRetry is how long the reporter waits between attempts to
	// hand a report to the control plane while the uplink is down.
	agentReportRetry = 500 * time.Millisecond
	// agentReportTimeout bounds one report round trip.
	agentReportTimeout = 15 * time.Second
	// agentStopGrace is how long the harness gets after SIGTERM before KILL.
	agentStopGrace = 5 * time.Second
	// agentTerminalOutputLimit is the default cap for terminal/output when
	// the harness names none; it is the ACP-recommended shape of "truncate,
	// keep the tail".
	agentTerminalOutputLimit = 1 << 20
	// agentACPTimeout bounds the ACP handshake calls; a prompt has no bound.
	agentACPTimeout = 60 * time.Second
	// maxAgentTerminals bounds terminal/create per run; a harness that leaks
	// terminals gets an error, not the node's whole process table.
	maxAgentTerminals = 32
	// acpLauncherEnvScript sources the workspace's broker env before an
	// explicit ACP command so a harness that is not a recipe still finds the
	// broker at its current address.
	acpLauncherEnvScript = `[ -f .remount/env ] && . ./.remount/env; exec "$@"`
)

// agentRun is the node-side state of one run attempt.
type agentRun struct {
	n   *Node
	w   *ws
	req proto.AgentRunReq
	key string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	// softCtx ends on any cancel, hard or soft. Work that has no ACP-level
	// cancel of its own (the recipe install) waits on it, so a control-plane
	// agent.run.cancel is honoured before the harness ever starts.
	softCtx    context.Context
	softCancel context.CancelFunc

	transcript *session.Session
	redact     *redactor
	reporter   *agentReporter
	mount      string
	stdin      io.Closer // the harness's stdin; closed first when stopping

	mu sync.Mutex
	// lastStderr is the harness's final redacted stderr line, kept so a
	// non-zero exit can say why: a launcher that refuses to start prints one
	// line and exits, and "harness exited 78" alone sends the operator to
	// the node's debug log for it.
	lastStderr   string
	inbox        []proto.AgentMessage
	seen         map[string]bool
	wake         chan struct{}
	cancelled    bool
	cancelReason string
	turnCancel   context.CancelFunc
	approvals    map[string]chan proto.ApprovalDecision
	terminals    map[string]*agentTerminal
	replaying    bool
	client       *acp.Client
	cmd          *exec.Cmd
	hardStop     bool
	sessionID    acp.SessionId
	usage        *proto.AgentUsage
}

// agentTerminal is one terminal/create the harness made: a managed exec
// session plus the cursor and bounded buffer terminal/output reads from.
type agentTerminal struct {
	s      *session.Session
	limit  int
	mu     sync.Mutex
	cursor uint64
	buf    []byte
	cut    bool
}

// agentReporter delivers reports to the control plane in order. Each report
// gets the next sequence number when it is queued, so what the control plane
// sees is the order the run observed even when the uplink flapped. A report
// the control plane refuses as stale or unknown is dropped; one it could not
// receive at all is retried until the run's context ends.
//
// The queue is bounded. While the uplink is down a chatty harness could
// otherwise grow it without limit; past the bound the oldest transcript
// reports are dropped and the next transcript report opens with a gap chunk
// covering them, so the mirror shows the loss instead of hiding it.
// Lifecycle reports are never dropped: they are what the control plane's
// state machine runs on.
type agentReporter struct {
	n      *Node
	run    *agentRun
	mu     sync.Mutex
	seq    uint64
	q      []*proto.AgentReport
	qBytes int
	gap    *proto.Gap // transcript seqs dropped from the queue, unreported
	kick   chan struct{}
	done   chan struct{}
	end    bool
	// Transcript records wait here until the batch is big enough, old
	// enough, or another report needs to overtake them; they then ship as one
	// transcript report so the mirror keeps seq order relative to lifecycle
	// reports.
	chunks     []proto.TranscriptChunk
	chunkBytes int
	flushTimer *time.Timer
}

// agentTranscriptFlushAfter bounds how stale the control plane's copy of a
// quiet run gets; agentTranscriptFlushBytes bounds the batch for a busy one.
const (
	agentTranscriptFlushAfter = 250 * time.Millisecond
	agentTranscriptFlushBytes = 64 << 10
	// agentReportQueueMax and agentReportQueueBytes bound the reports one run
	// holds for a control plane it cannot reach.
	agentReportQueueMax   = 512
	agentReportQueueBytes = 16 << 20
)

// chunk queues one transcript record for mirroring.
func (r *agentReporter) chunk(ch proto.TranscriptChunk) {
	r.mu.Lock()
	if r.end {
		r.mu.Unlock()
		return
	}
	r.chunks = append(r.chunks, ch)
	r.chunkBytes += len(ch.Data)
	if r.chunkBytes >= agentTranscriptFlushBytes {
		r.flushChunksLocked()
		r.mu.Unlock()
		r.wake()
		return
	}
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(agentTranscriptFlushAfter, r.flushChunks)
	}
	r.mu.Unlock()
}

func (r *agentReporter) flushChunks() {
	r.mu.Lock()
	r.flushTimer = nil
	if len(r.chunks) == 0 || r.end {
		r.mu.Unlock()
		return
	}
	r.flushChunksLocked()
	r.mu.Unlock()
	r.wake()
}

// flushChunksLocked moves the pending chunks into a transcript report. It
// splits a batch that outgrew the report bound. A gap left over from the
// queue trim ships even with nothing else pending, so a run that ends right
// after the trim still reports what it lost. Caller holds r.mu.
func (r *agentReporter) flushChunksLocked() {
	if r.flushTimer != nil {
		r.flushTimer.Stop()
		r.flushTimer = nil
	}
	for len(r.chunks) > 0 || r.gap != nil {
		n, size := 0, 0
		for n < len(r.chunks) && (n == 0 || size+len(r.chunks[n].Data) <= proto.MaxTranscriptReportBytes) {
			size += len(r.chunks[n].Data)
			n++
		}
		var batch []proto.TranscriptChunk
		if r.gap != nil {
			batch = append(batch, proto.TranscriptChunk{Seq: r.gap.To, Stream: proto.StreamGap, At: time.Now().UnixMilli(), Data: proto.MustMarshal(*r.gap)})
			r.gap = nil
		}
		batch = append(batch, r.chunks[:n]...)
		r.chunks = r.chunks[n:]
		r.seq++
		r.enqueueLocked(&proto.AgentReport{
			Agent: r.run.req.Agent, Run: r.run.req.Run, WS: r.run.req.WS, Gen: r.run.req.Gen,
			Seq: r.seq, Kind: proto.AgentReportTranscript, At: time.Now().UnixMilli(), Chunks: batch,
		})
	}
	r.chunks = nil
	r.chunkBytes = 0
}

func reportBytes(rep *proto.AgentReport) int {
	n := 256
	for i := range rep.Chunks {
		n += len(rep.Chunks[i].Data) + 32
	}
	return n
}

// enqueueLocked appends a report and then trims the queue back under its
// bound by dropping the oldest transcript reports behind the head. The head
// may be in flight, so it is never touched. Caller holds r.mu.
func (r *agentReporter) enqueueLocked(rep *proto.AgentReport) {
	r.q = append(r.q, rep)
	r.qBytes += reportBytes(rep)
	for len(r.q) > agentReportQueueMax || r.qBytes > agentReportQueueBytes {
		victim := -1
		for i := 1; i < len(r.q); i++ {
			if r.q[i].Kind == proto.AgentReportTranscript {
				victim = i
				break
			}
		}
		if victim < 0 {
			return
		}
		dropped := r.q[victim]
		r.q = append(r.q[:victim], r.q[victim+1:]...)
		r.qBytes -= reportBytes(dropped)
		metrics.AgentTranscriptReportsDropped.Inc()
		// The gap belongs where the dropped records were: at the front of the
		// next transcript report still queued, or, when the victim was the
		// newest, in the next one flushed.
		var next *proto.AgentReport
		for i := victim; i < len(r.q); i++ {
			if r.q[i].Kind == proto.AgentReportTranscript {
				next = r.q[i]
				break
			}
		}
		if next == nil {
			r.gap = widenGap(r.gap, dropped.Chunks)
			continue
		}
		g := widenGap(nil, dropped.Chunks)
		if len(next.Chunks) > 0 && next.Chunks[0].Stream == proto.StreamGap {
			g = widenGap(g, next.Chunks[:1])
			r.qBytes -= len(next.Chunks[0].Data) + 32
			next.Chunks = next.Chunks[1:]
		}
		gapChunk := proto.TranscriptChunk{Seq: g.To, Stream: proto.StreamGap, At: time.Now().UnixMilli(), Data: proto.MustMarshal(*g)}
		next.Chunks = append([]proto.TranscriptChunk{gapChunk}, next.Chunks...)
		r.qBytes += len(gapChunk.Data) + 32
	}
}

// widenGap extends g (nil for none) to cover the session seqs of chunks,
// reading through any gap chunk among them.
func widenGap(g *proto.Gap, chunks []proto.TranscriptChunk) *proto.Gap {
	for i := range chunks {
		ch := &chunks[i]
		from, to := ch.Seq, ch.Seq
		if ch.Stream == proto.StreamGap {
			var inner proto.Gap
			if err := proto.Unmarshal(ch.Data, &inner); err == nil {
				from, to = inner.From, inner.To
			}
		}
		if g == nil {
			g = &proto.Gap{From: from, To: to}
			continue
		}
		g.From = min(g.From, from)
		g.To = max(g.To, to)
	}
	return g
}

// wake nudges the loop; call it after releasing r.mu.
func (r *agentReporter) wake() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

func (r *agentReporter) report(kind string, fill func(*proto.AgentReport)) {
	r.mu.Lock()
	if r.end {
		r.mu.Unlock()
		return
	}
	r.flushChunksLocked()
	r.seq++
	rep := &proto.AgentReport{
		Agent: r.run.req.Agent, Run: r.run.req.Run, WS: r.run.req.WS, Gen: r.run.req.Gen,
		Seq: r.seq, Kind: kind, At: time.Now().UnixMilli(),
	}
	if fill != nil {
		fill(rep)
	}
	if kind == proto.AgentReportFinished {
		r.end = true
	}
	r.enqueueLocked(rep)
	r.mu.Unlock()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// loop sends queued reports head-first. It returns after the finished
// report is delivered (or permanently refused) or ctx ends.
func (r *agentReporter) loop(ctx context.Context) {
	defer close(r.done)
	for {
		r.mu.Lock()
		var head *proto.AgentReport
		if len(r.q) > 0 {
			head = r.q[0]
		}
		end := r.end
		r.mu.Unlock()
		if head == nil {
			if end {
				return
			}
			select {
			case <-r.kick:
				continue
			case <-ctx.Done():
				return
			}
		}
		err := r.n.sendAgentReport(ctx, head)
		if err != nil && !agentReportPermanent(err) {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-time.After(agentReportRetry):
			case <-ctx.Done():
				return
			}
			continue
		}
		if err != nil {
			r.n.logger.Warn("agent report refused", "agent", head.Agent, "run", head.Run, "kind", head.Kind, "seq", head.Seq, "err", err)
			if agentReportSupersedes(err) && head.Kind != proto.AgentReportFinished {
				// The control plane no longer owns this run: it closed the
				// row, the workspace moved on, or the agent is gone. The
				// harness must not keep working on nobody's behalf.
				r.run.requestCancel("control plane refused " + head.Kind + " report: " + err.Error())
			}
		}
		r.mu.Lock()
		if len(r.q) > 0 && r.q[0] == head {
			r.q = r.q[1:]
			r.qBytes -= reportBytes(head)
		}
		r.mu.Unlock()
	}
}

// agentReportSupersedes tells a refusal that means the run is not the
// control plane's anymore from one about the report itself (too big,
// malformed), which the run survives.
func agentReportSupersedes(err error) bool {
	var pe *proto.Error
	if !errors.As(err, &pe) {
		return false
	}
	switch pe.Code {
	case proto.CodeNotFound, proto.CodeConflict, proto.CodeUnauthorized, proto.CodeDenied:
		return true
	}
	return false
}

// agentReportPermanent tells a control-plane refusal (the run is gone, the
// generation moved on, the report is malformed) from a delivery failure.
func agentReportPermanent(err error) bool {
	var pe *proto.Error
	if !errors.As(err, &pe) {
		return false
	}
	switch pe.Code {
	case proto.CodeNotFound, proto.CodeConflict, proto.CodeBadRequest, proto.CodeUnauthorized, proto.CodeDenied, proto.CodeUnsupported:
		return true
	}
	return false
}

// sendAgentReport is the one place a report crosses the uplink. Tests swap
// it for a sink through Node.agentReportSink.
func (n *Node) sendAgentReport(ctx context.Context, rep *proto.AgentReport) error {
	if n.agentReportSink != nil {
		return n.agentReportSink(ctx, rep)
	}
	n.mu.Lock()
	p := n.peer
	n.mu.Unlock()
	if p == nil {
		return errors.New("uplink down")
	}
	rctx, cancel := context.WithTimeout(ctx, agentReportTimeout)
	defer cancel()
	return p.Call(rctx, proto.PeerControl, proto.OpAgentReport, rep, nil)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

func agentRunKey(agent, run string) string { return agent + "|" + run }

// agentRunsDoneTTL and agentRunsDoneMax bound the finished-run memory. A
// replay of agent.run for an attempt arrives within the control plane's
// launch retry window; anything older is not a replay the node can still
// tell apart, and the control plane opens a new attempt id regardless.
const (
	agentRunsDoneTTL = time.Hour
	agentRunsDoneMax = 4096
)

// pruneAgentRunsDoneLocked ages out finished-run markers. Caller holds n.mu.
func (n *Node) pruneAgentRunsDoneLocked() {
	cutoff := time.Now().Add(-agentRunsDoneTTL)
	for key, at := range n.agentRunsDone {
		if at.Before(cutoff) {
			delete(n.agentRunsDone, key)
		}
	}
	for len(n.agentRunsDone) > agentRunsDoneMax {
		oldestKey, oldest := "", time.Time{}
		for key, at := range n.agentRunsDone {
			if oldestKey == "" || at.Before(oldest) {
				oldestKey, oldest = key, at
			}
		}
		delete(n.agentRunsDone, oldestKey)
	}
}

// agentRunStart handles agent.run. A repeat for a run that is already live
// returns its transcript; a repeat for one that already finished is refused
// with conflict, because the control plane must open a new attempt rather
// than believe a dead run is still going. A new run for an agent whose older
// run is still live here means the control plane closed that run without
// the node hearing it: the node cancels the older run and refuses with
// conflict so the launch is retried once the harness has stopped.
func (n *Node) agentRunStart(ctx context.Context, p *transport.Peer, req *proto.AgentRunReq) (any, error) {
	if req.Agent == "" || req.Run == "" || req.WS == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent, run and ws are required")
	}
	if req.Mode != "" && req.Mode != proto.AgentModeACP {
		return nil, proto.Err(proto.CodeUnsupported, "agent mode %q is not supported by this node", req.Mode)
	}
	key := agentRunKey(req.Agent, req.Run)
	n.mu.Lock()
	w := n.workspaces[req.WS]
	if w == nil {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s not here", req.WS)
	}
	if w.Generation != req.Gen {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "generation mismatch")
	}
	if w.checkpointing {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is checkpointing", req.WS)
	}
	if existing := n.agentRuns[key]; existing != nil {
		n.mu.Unlock()
		if existing.transcript == nil {
			return nil, proto.Err(proto.CodeConflict, "run %s is starting", req.Run)
		}
		return proto.AgentRunRes{Transcript: existing.transcript.ID}, nil
	}
	if _, done := n.agentRunsDone[key]; done {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "run %s already finished on this node", req.Run)
	}
	for _, other := range n.agentRuns {
		if other.req.Agent == req.Agent {
			n.mu.Unlock()
			other.requestCancel("superseded by run " + req.Run)
			return nil, proto.Err(proto.CodeConflict, "agent %s already has run %s live on this node; stopping it", req.Agent, other.req.Run)
		}
	}
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	sctx, scancel := context.WithCancel(rctx)
	r := &agentRun{
		n: n, w: w, req: *req, key: key, ctx: rctx, cancel: cancel, done: make(chan struct{}),
		softCtx: sctx, softCancel: scancel,
		seen: map[string]bool{}, wake: make(chan struct{}, 1),
		approvals: map[string]chan proto.ApprovalDecision{}, terminals: map[string]*agentTerminal{},
	}
	for _, m := range req.Messages {
		if !r.seen[m.ID] {
			r.seen[m.ID] = true
			r.inbox = append(r.inbox, m)
		}
	}
	r.reporter = &agentReporter{n: n, run: r, kick: make(chan struct{}, 1), done: make(chan struct{})}
	n.agentRuns[key] = r
	n.mu.Unlock()

	transcript, err := r.openTranscript()
	if err != nil {
		n.mu.Lock()
		delete(n.agentRuns, key)
		n.mu.Unlock()
		cancel()
		return nil, err
	}
	n.mu.Lock()
	r.transcript = transcript
	n.mu.Unlock()
	// Reports outlive the run: the finished report must still be retried
	// after the run's own context is cancelled, until the node stops.
	go r.reporter.loop(n.requestCtx)
	go r.main()
	return proto.AgentRunRes{Transcript: transcript.ID}, nil
}

// agentRunDeliver handles agent.deliver: a message for a live run's inbox.
// Delivery is idempotent on the message id.
func (n *Node) agentRunDeliver(req *proto.AgentDeliverReq) error {
	n.mu.Lock()
	r := n.agentRuns[agentRunKey(req.Agent, req.Run)]
	n.mu.Unlock()
	if r == nil {
		return proto.Err(proto.CodeNotFound, "run %s is not live on this node", req.Run)
	}
	return r.deliver(req.Message)
}

// agentRunCancel handles agent.run.cancel. Cancelling a run that already
// ended is a no-op, not an error: the control plane's cancel raced the
// finish report and either order leaves the same durable state.
func (n *Node) agentRunCancel(req *proto.AgentRunCancelReq) error {
	n.mu.Lock()
	r := n.agentRuns[agentRunKey(req.Agent, req.Run)]
	n.mu.Unlock()
	if r == nil {
		return nil
	}
	r.requestCancel(req.Reason)
	return nil
}

// agentApprovalDecided handles agent.approval.decided. A decision for an
// approval the run is no longer waiting on is refused with not_found so the
// control plane records that the harness will have to ask again.
func (n *Node) agentApprovalDecided(req *proto.AgentApprovalDecidedReq) error {
	n.mu.Lock()
	r := n.agentRuns[agentRunKey(req.Agent, req.Run)]
	n.mu.Unlock()
	if r == nil {
		return proto.Err(proto.CodeNotFound, "run %s is not live on this node", req.Run)
	}
	return r.decide(req.Approval, req.Decision)
}

// stopAgentRuns cancels and joins every run on workspace w. It is the
// lifecycle hook for release, quarantine and sleep: sessions are killed by
// KillWorkspace, but the run's own goroutines and reports must be joined
// too, or a finished report could land after the workspace moved.
func (n *Node) stopAgentRuns(wsID, reason string) {
	n.mu.Lock()
	var runs []*agentRun
	for _, r := range n.agentRuns {
		if r.req.WS == wsID {
			runs = append(runs, r)
		}
	}
	n.mu.Unlock()
	for _, r := range runs {
		r.hardStopNow(reason)
		<-r.done
	}
}

// agentRunsIdle reports whether every run's reports have been delivered;
// tests use it to wait for the control plane to have seen the finish.
func (n *Node) agentRunsIdle() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.agentRuns) == 0
}

// ---------------------------------------------------------------------------
// run lifecycle
// ---------------------------------------------------------------------------

func (r *agentRun) deliver(m proto.AgentMessage) error {
	if m.ID == "" {
		return proto.Err(proto.CodeBadRequest, "message id is required")
	}
	r.mu.Lock()
	if r.seen[m.ID] {
		r.mu.Unlock()
		return nil
	}
	if len(r.inbox) >= proto.MaxAgentInbox {
		r.mu.Unlock()
		return proto.Err(proto.CodeResourceExhausted, "run inbox is full")
	}
	r.seen[m.ID] = true
	r.inbox = append(r.inbox, m)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return nil
}

// requestCancel asks the run to stop: the in-flight prompt is cancelled the
// ACP way (session/cancel, wait for the agent's cancelled stop), the inbox
// is left alone because the control plane owns it, and the loop exits.
func (r *agentRun) requestCancel(reason string) {
	r.mu.Lock()
	if r.cancelled {
		r.mu.Unlock()
		return
	}
	r.cancelled = true
	r.cancelReason = reason
	tc := r.turnCancel
	r.mu.Unlock()
	r.softCancel()
	if tc != nil {
		tc()
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *agentRun) isCancelled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancelled
}

func (r *agentRun) decide(approval string, d proto.ApprovalDecision) error {
	r.mu.Lock()
	ch := r.approvals[approval]
	if ch != nil {
		delete(r.approvals, approval)
	}
	r.mu.Unlock()
	if ch == nil {
		return proto.Err(proto.CodeNotFound, "approval %s is not pending on run %s", approval, r.req.Run)
	}
	ch <- d
	return nil
}

// openTranscript starts the record-only session every frame is appended
// to. Killing it (KillWorkspace, `remount s kill`) stops the harness.
func (r *agentRun) openTranscript() (*session.Session, error) {
	spec := session.Spec{
		WS: r.w.ID, Generation: r.w.Generation, Kind: proto.SessionACP, Principal: r.req.Owner, Tenant: r.req.Tenant,
		IdempotencyKey: "acp|" + r.key,
		Run:            &proto.RunInfo{Recipe: r.req.Spec.Recipe, TaskHash: launch.TaskHash(r.req.Spec.Task), Sandbox: r.req.Spec.Sandbox, Auth: r.req.Spec.Auth},
	}
	s, _, err := r.n.sessions.OpenOrReplay(spec)
	if err != nil {
		return nil, err
	}
	s.OnSignal(func(name string) { r.hardStopNow("transcript signalled " + name) })
	return s, nil
}

// hardStopNow is the path a session kill takes (KillWorkspace before a
// release, quarantine or authoritative snapshot; `remount s kill`). The
// harness is killed outright rather than asked, because the caller is
// waiting on the session manager's deadline and a harness that ignores EOF
// would otherwise turn a fence into a timeout.
func (r *agentRun) hardStopNow(reason string) {
	r.requestCancel(reason)
	r.mu.Lock()
	r.hardStop = true
	cmd := r.cmd
	r.mu.Unlock()
	r.cancel()
	if cmd != nil && cmd.Process != nil {
		_ = session.SignalProcess(cmd, "KILL")
	}
}

// main is the run from spawn to the finished report. Every exit path passes
// through finish, which joins the process and the terminals before it
// reports, so "finished" means nothing of the run is still executing.
func (r *agentRun) main() {
	var (
		exitCode   int
		runErr     error
		stopReason string
	)
	// Started goes out before the install: the control plane's launch
	// timeout is for a node that died between the ack and here, not for a
	// recipe that takes minutes to install.
	r.reporter.report(proto.AgentReportStarted, func(rep *proto.AgentReport) { rep.Transcript = r.transcript.ID })
	if err := r.installHarness(); err != nil {
		r.finish(-1, err, "", nil)
		return
	}
	if r.isCancelled() {
		r.finish(-1, nil, "", nil)
		return
	}
	cmd, client, err := r.spawn()
	if err != nil {
		r.finish(-1, err, "", nil)
		return
	}
	r.mu.Lock()
	r.client = client
	r.cmd = cmd
	hard := r.hardStop
	r.mu.Unlock()
	if hard {
		// The kill arrived while spawning; the process must not outlive it.
		_ = session.SignalProcess(cmd, "KILL")
	}
	updatesDone := make(chan struct{})
	go func() {
		defer close(updatesDone)
		r.consumeUpdates(client)
	}()

	if err := r.handshake(client); err != nil {
		runErr = err
	} else {
		stopReason, runErr = r.turns(client)
	}

	// Stop the harness: fail its pending calls, close its stdin so a
	// well-behaved server exits, then escalate. The reader is joined before
	// the process is waited on, because Wait closes the stdout pipe and
	// would discard frames still in it; the update consumer is joined after
	// the reader so every tool call it saw is reported before finished.
	_ = client.Close()
	exitCode = r.stopProcess(cmd, client)
	<-updatesDone
	r.finish(exitCode, runErr, stopReason, client)
}

// finish joins everything the run owns and then reports. Order matters:
// terminals and the transcript end before the report so a reader of the
// finished event never finds a live session behind it.
func (r *agentRun) finish(exitCode int, runErr error, stopReason string, client *acp.Client) {
	cancelled := r.isCancelled()
	r.mu.Lock()
	terms := r.terminals
	r.terminals = map[string]*agentTerminal{}
	pending := r.approvals
	r.approvals = map[string]chan proto.ApprovalDecision{}
	r.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
	for id, t := range terms {
		r.n.sessions.Remove(t.s.ID, true)
		delete(terms, id)
	}
	if client != nil {
		if cerr := client.Err(); runErr == nil && cerr != nil && !cancelled && !errors.Is(cerr, io.EOF) && !errors.Is(cerr, acp.ErrClosed) {
			runErr = cerr
		}
	}
	text := ""
	if runErr != nil && !cancelled {
		text = runErr.Error()
		if exitCode != 0 && !strings.Contains(text, "exited") {
			text = fmt.Sprintf("%s (harness exited %d)", text, exitCode)
			r.mu.Lock()
			last := r.lastStderr
			r.mu.Unlock()
			if last != "" {
				text += ": " + last
			}
		}
	}
	exit := proto.ExitInfo{Code: exitCode, Error: text}
	if cancelled {
		exit.Reason = "cancelled"
	}
	r.transcript.End(exit)
	// The mirror carries the exit chunk too, so a durable reader sees the
	// run end the same way a live session subscriber does.
	if r.reporter != nil {
		r.reporter.chunk(proto.TranscriptChunk{Seq: r.transcript.Log.Next() - 1, Stream: proto.StreamExit, At: time.Now().UnixMilli(), Data: proto.MustMarshal(exit)})
	}

	r.n.mu.Lock()
	if r.n.agentRuns[r.key] == r {
		delete(r.n.agentRuns, r.key)
	}
	r.n.agentRunsDone[r.key] = time.Now()
	r.n.pruneAgentRunsDoneLocked()
	r.n.mu.Unlock()

	r.reporter.report(proto.AgentReportFinished, func(rep *proto.AgentReport) {
		rep.ExitCode = exitCode
		rep.Error = text
		rep.StopReason = stopReason
		rep.Cancelled = cancelled
	})
	r.cancel()
	close(r.done)
}

// spawn builds the harness command inside the workspace and starts it with
// an ACP client on its stdio. The environment is the same one every session
// gets: broker variables and placeholders, never a real secret.
func (r *agentRun) spawn() (*exec.Cmd, *acp.Client, error) {
	unlock, err := r.n.lockWorkspaceTree(r.w, false)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	program, extraEnv, err := r.program()
	if err != nil {
		return nil, nil, err
	}
	spec := session.Spec{
		WS: r.w.ID, Generation: r.w.Generation, Kind: proto.SessionExec, Program: program, Cwd: ".", Env: r.n.sessionEnv(r.w, extraEnv),
		Principal: r.req.Owner, Tenant: r.req.Tenant, Stdin: true,
	}
	// The redactor learns the workspace's environment as declared, before a
	// backend rewrites spec.Env into the host command it actually execs
	// (docker moves the container's variables into -e flags).
	r.redact = newRedactor(r.secretLiterals(spec.Env))
	if err := r.w.handle.Prepare(&spec); err != nil {
		return nil, nil, err
	}
	r.mount = workspace.MountPathOf(r.w.handle)

	cmd := exec.Command(spec.Program[0], spec.Program[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = spec.Env
	session.ConfigureProcessGroup(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, proto.Err(proto.CodeInternal, "stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, proto.Err(proto.CodeInternal, "stdout: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, proto.Err(proto.CodeInternal, "stderr: %v", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, proto.Err(proto.CodeInternal, "start harness: %v", err)
	}
	if err := session.RegisterProcessGroup(cmd); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, nil, proto.Err(proto.CodeInternal, "contain harness process tree: %v", err)
	}
	r.stdin = stdin
	go r.drainStderr(stderr)
	client := acp.NewClient(stdout, stdin, acp.Options{
		Handler: &agentHandler{r: r},
		Frames:  r.recordFrame,
		MaxLine: 16 << 20,
		Logger:  r.n.logger,
	})
	return cmd, client, nil
}

// program decides what to exec: the recipe's ACP launcher written into the
// tree (regenerated on every run, like `remount run`), or the Agent's
// explicit ACP command behind a one-line env preamble.
func (r *agentRun) program() ([]string, map[string]string, error) {
	spec := r.req.Spec
	env, err := r.bindingEnv()
	if err != nil {
		return nil, nil, err
	}
	if spec.Recipe == "" {
		return explicitACPProgram(r.w.handle.Backend(), spec.ACPCommand), env, nil
	}
	recipe, data, err := r.recipe()
	if err != nil {
		return nil, nil, err
	}
	if recipe.ACP == nil && len(spec.ACPCommand) == 0 {
		return nil, nil, proto.Err(proto.CodeUnsupported, "recipe %s has no acp command", recipe.Name)
	}
	if len(spec.ACPCommand) > 0 {
		return explicitACPProgram(r.w.handle.Backend(), spec.ACPCommand), env, nil
	}
	script, err := recipe.ACPLauncher(data)
	if err != nil {
		return nil, nil, proto.Err(proto.CodeBadRequest, "recipe %s: %v", recipe.Name, err)
	}
	lp := recipe.ACPLauncherPath()
	if err := r.w.handle.FS().Write(lp, []byte(script), 0o755, false, true); err != nil {
		return nil, nil, err
	}
	program, err := recipeACPProgram(r.w.handle.Backend(), lp)
	if err != nil {
		return nil, nil, err
	}
	return program, env, nil
}

// bindingEnv is the placeholder environment the workspace's bindings give
// every session: names and base URLs, never a value the broker holds.
func (r *agentRun) bindingEnv() (map[string]string, error) {
	spec := r.req.Spec
	if spec.Recipe == "" && len(spec.ACPCommand) == 0 {
		return nil, proto.Err(proto.CodeBadRequest, "agent has neither a recipe nor an acp command")
	}
	bindings, err := agentBindings(spec, r.w.Spec)
	if err != nil {
		return nil, err
	}
	env := map[string]string{}
	for _, b := range bindings {
		for k, v := range b.SessionEnv() {
			env[k] = v
		}
	}
	return env, nil
}

// recipe loads the Agent's recipe and the template data every rendering of
// it uses.
func (r *agentRun) recipe() (*launch.Recipe, launch.Data, error) {
	spec := r.req.Spec
	var (
		recipe *launch.Recipe
		err    error
	)
	if spec.RecipeYAML != "" {
		recipe, err = launch.Parse([]byte(spec.RecipeYAML))
	} else {
		recipe, err = launch.Load(spec.Recipe)
	}
	if err != nil {
		return nil, launch.Data{}, proto.Err(proto.CodeBadRequest, "recipe: %v", err)
	}
	data := launch.Data{
		Task: spec.Task, Recipe: recipe.Name, Workspace: r.w.ID, Sandbox: spec.Sandbox,
		Approve: r.req.Policy.Approve, Model: spec.Model, Providers: spec.Providers, Primary: spec.Primary,
		Broker: "${REMOUNT_BROKER}", Auth: spec.Auth,
	}
	return recipe, data, nil
}

// agentInstallTimeout bounds a recipe's install script. A harness that
// cannot be installed in this long is a failed run, not a hung one.
const agentInstallTimeout = 15 * time.Minute

// installHarness runs the recipe's install script once per materialization,
// the way `remount run` does from the client, so an Agent created over the
// API alone gets its harness. The marker carries the workspace generation:
// a move lands on a possibly different image and installs again; a retry on
// the same node does not. Output is kept in the transcript under the stderr
// stream so `agent watch` shows an install that fails.
func (r *agentRun) installHarness() error {
	if r.req.Spec.Recipe == "" {
		return nil
	}
	recipe, data, err := r.recipe()
	if err != nil {
		return err
	}
	script, err := recipe.InstallScript(data)
	if err != nil {
		return proto.Err(proto.CodeBadRequest, "recipe %s: %v", recipe.Name, err)
	}
	if script == "" {
		return nil
	}
	marker := recipe.InstallMarkerPath()
	want := fmt.Sprintf("gen %d\n", r.req.Gen)
	extraEnv, err := r.bindingEnv()
	if err != nil {
		return err
	}
	unlock, err := r.n.lockWorkspaceTree(r.w, false)
	if err != nil {
		return err
	}
	if res, rerr := r.w.handle.FS().Read(marker, 0, 256); rerr == nil && string(res.Data) == want {
		unlock()
		return nil
	}
	spec := session.Spec{
		WS: r.w.ID, Generation: r.w.Generation, Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", ". ./.remount/env 2>/dev/null; " + script}, Cwd: ".",
		Env: r.n.sessionEnv(r.w, extraEnv), Principal: r.req.Owner, Tenant: r.req.Tenant,
		Timeout: agentInstallTimeout,
		Run:     &proto.RunInfo{Recipe: recipe.Name, TaskHash: launch.TaskHash(r.req.Spec.Task), Sandbox: r.req.Spec.Sandbox, Auth: r.req.Spec.Auth},
	}
	redact := newRedactor(r.secretLiterals(spec.Env))
	if err := r.w.handle.Prepare(&spec); err != nil {
		unlock()
		return err
	}
	s, err := r.n.sessions.Open(spec)
	unlock()
	if err != nil {
		return proto.Err(proto.CodeInternal, "install %s: %v", recipe.Name, err)
	}
	defer r.n.sessions.Remove(s.ID, true)
	exit, err := s.Wait(r.softCtx)
	if err != nil {
		s.Kill()
		if r.isCancelled() {
			return err
		}
		return proto.Err(proto.CodeInternal, "install %s: %v", recipe.Name, err)
	}
	r.recordInstallOutput(s, redact)
	if exit.Code != 0 || exit.Signal != "" {
		return proto.Err(proto.CodeInternal, "install %s exited %d %s", recipe.Name, exit.Code, exit.Signal)
	}
	unlock, err = r.n.lockWorkspaceTree(r.w, false)
	if err != nil {
		return err
	}
	defer unlock()
	return r.w.handle.FS().Write(marker, []byte(want), 0o644, false, true)
}

// recordInstallOutput copies the tail of an install session's output into
// the transcript. It is best effort: a redacted, bounded log line, never a
// reason to fail the run.
func (r *agentRun) recordInstallOutput(s *session.Session, redact *redactor) {
	chunks, err := s.Log.Read(0, 0)
	if err != nil {
		return
	}
	var out []byte
	for _, c := range chunks {
		if c.Stream == proto.StreamStdout || c.Stream == proto.StreamStderr {
			out = append(out, c.Data...)
		}
	}
	if len(out) > agentTerminalOutputLimit {
		out = out[len(out)-agentTerminalOutputLimit:]
	}
	out, _ = redact.apply(out)
	if len(out) > 0 {
		r.record(proto.StreamStderr, out)
	}
}

// record appends one redacted, bounded chunk to the transcript session and
// queues the same bytes for the control plane's mirror.
func (r *agentRun) record(stream uint8, data []byte) {
	seq, err := r.transcript.Record(stream, data)
	if err != nil {
		r.n.logger.Warn("acp transcript record", "agent", r.req.Agent, "run", r.req.Run, "err", err)
		return
	}
	if r.reporter != nil {
		r.reporter.chunk(proto.TranscriptChunk{Seq: seq, Stream: stream, At: time.Now().UnixMilli(), Data: data})
	}
}

// workspaceBindings reads the bindings `remount run` recorded on the
// workspace so a run resolves the same placeholders a session would.
func agentBindings(spec proto.AgentSpec, wsSpec proto.WorkspaceSpec) ([]launch.Binding, error) {
	out, err := launch.BindingsForWorkspace(spec.BindingSpecs, wsSpec.Labels, wsSpec.Bindings)
	if err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "%v", err)
	}
	return out, nil
}

// secretLiterals is everything the redactor must remove verbatim: the
// broker capability token, each lease's secret and its placeholder (the
// placeholder is harmless alone but pairs with the token), and any env
// value the workspace declared under a secret-shaped name.
func (r *agentRun) secretLiterals(env []string) []string {
	var lits []string
	if r.w.broker != nil {
		if _, tok, ok := strings.Cut(r.w.broker.BaseURL(), "/c/"); ok {
			lits = append(lits, strings.TrimSuffix(tok, "/"))
		}
	}
	r.n.mu.Lock()
	for _, l := range r.w.leases {
		lits = append(lits, l.Secret, broker.Placeholder(l))
	}
	r.n.mu.Unlock()
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		up := strings.ToUpper(k)
		if strings.Contains(up, "TOKEN") || strings.Contains(up, "SECRET") || strings.Contains(up, "PASSWORD") || strings.HasSuffix(up, "_KEY") || strings.HasSuffix(up, "API_KEY") {
			lits = append(lits, v)
		}
	}
	return lits
}

func (r *agentRun) drainStderr(rd io.Reader) {
	// A harness's stderr is its own diagnostics; it goes to the node log at
	// debug level after redaction and is never part of the transcript.
	buf := make([]byte, 8<<10)
	for {
		n, err := rd.Read(buf)
		if n > 0 {
			line, _ := r.redact.apply(bytes.TrimRight(buf[:n], "\n"))
			r.n.logger.Debug("harness stderr", "agent", r.req.Agent, "run", r.req.Run, "text", string(line))
			if last := lastStderrLine(line); last != "" {
				r.mu.Lock()
				r.lastStderr = last
				r.mu.Unlock()
			}
		}
		if err != nil {
			return
		}
	}
}

// stopProcess ends the harness and returns its exit code. It closes stdin
// and waits for the reader to see the harness's stdout close, escalating
// TERM then KILL for a server that ignores EOF. Only then is the process
// waited on: exec.Cmd.Wait closes the stdout pipe, and a frame still in the
// pipe at that moment would never reach the transcript. A descendant that
// kept stdout open past the kill is bounded by one more grace period.
func (r *agentRun) stopProcess(cmd *exec.Cmd, client *acp.Client) int {
	if r.stdin != nil {
		_ = r.stdin.Close()
	}
	r.mu.Lock()
	hard := r.hardStop
	r.mu.Unlock()
	if hard {
		_ = session.SignalProcess(cmd, "KILL")
		r.awaitReader(client, agentStopGrace)
		return harnessExit(cmd, agentStopGrace)
	}
	if r.awaitReader(client, 2*time.Second) {
		return harnessExit(cmd, agentStopGrace)
	}
	_ = session.SignalProcess(cmd, "TERM")
	if r.awaitReader(client, agentStopGrace) {
		return harnessExit(cmd, agentStopGrace)
	}
	_ = session.SignalProcess(cmd, "KILL")
	r.awaitReader(client, agentStopGrace)
	return harnessExit(cmd, agentStopGrace)
}

// harnessExitCode is what a run records when the harness's process group
// outlived the stop ladder and could not be waited on.
const harnessExitCode = -1

// harnessExit joins the harness process with a bound. A harness that closes
// its stdout and keeps running (the reader is done, so the ladder above never
// escalates) would otherwise pin cmd.Wait, and with it the run slot, the
// transcript and the agent's capacity, for as long as its tree lives. Past
// the grace period the whole group is killed; a group that still does not
// exit is abandoned to the reaper and reported as harnessExitCode.
func harnessExit(cmd *exec.Cmd, grace time.Duration) int {
	done := make(chan int, 1)
	go func() {
		defer session.ReleaseProcessGroup(cmd)
		code, _ := session.ExitStatus(cmd.Wait())
		done <- code
	}()
	select {
	case code := <-done:
		return code
	case <-time.After(grace):
	}
	_ = session.SignalProcess(cmd, "KILL")
	select {
	case code := <-done:
		return code
	case <-time.After(grace):
		session.ReleaseProcessGroup(cmd)
		return harnessExitCode
	}
}

// awaitReader reports whether the ACP reader reached the end of the
// harness's stdout within d.
func (r *agentRun) awaitReader(client *acp.Client, d time.Duration) bool {
	select {
	case <-client.Done():
		return true
	case <-time.After(d):
		return false
	}
}

// handshake initializes the connection and opens or reopens the session.
// The capabilities the harness advertised go to the control plane so it can
// decide, next attempt, whether the session id is worth carrying forward.
func (r *agentRun) handshake(client *acp.Client) error {
	ctx, cancel := context.WithTimeout(r.ctx, agentACPTimeout)
	defer cancel()
	_, err := client.Initialize(ctx, acp.InitializeRequest{
		ClientCapabilities: &acp.ClientCapabilities{
			Fs:          &acp.FileSystemCapabilities{ReadTextFile: true, WriteTextFile: true},
			Terminal:    true,
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}, URL: &acp.ElicitationUrlCapabilities{}},
		},
		ClientInfo: &acp.Implementation{Name: "remount", Version: r.n.opts.Version},
	})
	if err != nil {
		return fmt.Errorf("acp initialize: %w", err)
	}
	caps := &proto.ACPCapabilities{
		LoadSession: client.CanLoadSession(), ResumeSession: client.CanResumeSession(),
		CloseSession: client.CanCloseSession(), AdditionalDirectories: client.SupportsAdditionalDirectories(),
	}
	loaded := false
	var modes *acp.SessionModeState
	sid := acp.SessionId(r.req.ACPSessionID)
	if sid != "" && (caps.ResumeSession || caps.LoadSession) {
		r.setReplaying(caps.LoadSession && !caps.ResumeSession)
		res, rerr := client.Reopen(ctx, sid, r.mount, nil, nil)
		r.setReplaying(false)
		if rerr == nil {
			loaded = true
			modes = res.Modes
		} else {
			// The harness forgot the session (a fresh tree, a pruned store).
			// A new one is the right answer; the control plane records the
			// change of id and the user sees Loaded=false.
			r.n.logger.Info("acp reopen failed; starting a new session", "agent", r.req.Agent, "session", string(sid), "err", rerr)
			sid = ""
			_ = res
		}
	} else {
		sid = ""
	}
	if sid == "" {
		res, nerr := client.NewSession(ctx, acp.NewSessionRequest{Cwd: r.mount, MCPServers: []acp.McpServer{}})
		if nerr != nil {
			return fmt.Errorf("acp session/new: %w", nerr)
		}
		sid = res.SessionID
		modes = res.Modes
	}
	mode, err := r.acpSandboxMode()
	if err != nil {
		return err
	}
	if mode != "" && (modes == nil || modes.CurrentModeID != acp.SessionModeId(mode)) {
		if _, err := client.SetMode(ctx, acp.SetSessionModeRequest{
			SessionID: sid,
			ModeID:    acp.SessionModeId(mode),
		}); err != nil {
			return fmt.Errorf("acp session/set_mode %s: %w", mode, err)
		}
	}
	r.mu.Lock()
	r.sessionID = sid
	r.mu.Unlock()
	r.reporter.report(proto.AgentReportSession, func(rep *proto.AgentReport) {
		rep.ACPSessionID = string(sid)
		rep.Capabilities = caps
		rep.Loaded = loaded
	})
	return nil
}

func (r *agentRun) acpSandboxMode() (string, error) {
	if len(r.req.Spec.ACPCommand) > 0 {
		return "", nil
	}
	recipe, _, err := r.recipe()
	if err != nil {
		return "", err
	}
	if recipe.ACP == nil {
		return "", nil
	}
	return recipe.ACP.SandboxModes[r.req.Spec.Sandbox], nil
}

func (r *agentRun) setReplaying(v bool) {
	r.mu.Lock()
	r.replaying = v
	r.mu.Unlock()
}

// turns drives the inbox one prompt at a time until the inbox is empty and
// the run was told to stop, the run is cancelled, or the connection dies. A
// message leaves the node's copy of the inbox only once its turn finished;
// the control plane does the same with the durable inbox on the report.
func (r *agentRun) turns(client *acp.Client) (string, error) {
	last := ""
	for {
		if r.isCancelled() {
			return last, nil
		}
		r.mu.Lock()
		var next *proto.AgentMessage
		if len(r.inbox) > 0 {
			m := r.inbox[0]
			next = &m
		}
		sid := r.sessionID
		r.mu.Unlock()
		if next == nil {
			// Idle: the harness stays up so a follow-up arrives on the same
			// session without a spawn. The control plane's sleep policy ends
			// the run when it wants the capacity back.
			select {
			case <-r.wake:
				continue
			case <-client.Done():
				return last, client.Err()
			case <-r.ctx.Done():
				return last, nil
			}
		}
		stop, err := r.turn(client, sid, *next)
		if err != nil {
			return last, err
		}
		last = string(stop)
		if stop == acp.StopReasonCancelled && r.isCancelled() {
			return last, nil
		}
		r.mu.Lock()
		if len(r.inbox) > 0 && r.inbox[0].ID == next.ID {
			r.inbox = r.inbox[1:]
		}
		r.mu.Unlock()
	}
}

// turn sends one prompt and waits for its stop. The prompt text itself is
// not put in any event; the control plane keys the turn on the message id.
func (r *agentRun) turn(client *acp.Client, sid acp.SessionId, m proto.AgentMessage) (acp.StopReason, error) {
	tctx, tcancel := context.WithCancel(r.ctx)
	r.mu.Lock()
	r.turnCancel = tcancel
	cancelled := r.cancelled
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.turnCancel = nil
		r.mu.Unlock()
		tcancel()
	}()
	if cancelled {
		return acp.StopReasonCancelled, nil
	}
	r.reporter.report(proto.AgentReportTurnStarted, func(rep *proto.AgentReport) { rep.Message = m.ID })
	block, err := acp.NewContentBlockText(acp.TextContent{Text: m.Text})
	if err != nil {
		return "", err
	}
	res, err := client.Prompt(tctx, acp.PromptRequest{SessionID: sid, Prompt: []acp.ContentBlock{block}})
	if err != nil {
		if r.isCancelled() || errors.Is(err, context.Canceled) {
			return acp.StopReasonCancelled, nil
		}
		return "", fmt.Errorf("acp session/prompt: %w", err)
	}
	if res.StopReason == acp.StopReasonCancelled {
		return res.StopReason, nil
	}
	r.mu.Lock()
	usage := r.usage
	r.usage = nil
	r.mu.Unlock()
	r.reporter.report(proto.AgentReportTurnFinished, func(rep *proto.AgentReport) {
		rep.Message = m.ID
		rep.StopReason = string(res.StopReason)
		rep.Usage = usage
	})
	return res.StopReason, nil
}

// consumeUpdates turns session/update notifications into the reports the
// control plane keeps as events: tool calls and usage. Message chunks are
// only in the transcript; they are the bulk of a run and belong to the
// session log, not the event log.
func (r *agentRun) consumeUpdates(client *acp.Client) {
	for u := range client.Updates() {
		r.mu.Lock()
		replaying := r.replaying
		r.mu.Unlock()
		if replaying {
			continue
		}
		switch u.Update.Kind {
		case acp.SessionUpdateKindToolCall:
			tc, err := u.Update.AsToolCall()
			if err != nil {
				continue
			}
			r.reporter.report(proto.AgentReportToolCall, func(rep *proto.AgentReport) {
				rep.ToolCall = string(tc.ToolCallID)
				rep.ToolKind = string(tc.Kind)
				rep.ToolTitle = truncateString(tc.Title, 256)
				rep.ToolStatus = string(tc.Status)
				rep.Locations = locations(tc.Locations)
			})
		case acp.SessionUpdateKindToolCallUpdate:
			tc, err := u.Update.AsToolCallUpdate()
			if err != nil || tc.Status == nil {
				continue
			}
			r.reporter.report(proto.AgentReportToolCall, func(rep *proto.AgentReport) {
				rep.ToolCall = string(tc.ToolCallID)
				if tc.Kind != nil {
					rep.ToolKind = string(*tc.Kind)
				}
				if tc.Title != nil {
					rep.ToolTitle = truncateString(*tc.Title, 256)
				}
				rep.ToolStatus = string(*tc.Status)
				rep.Locations = locations(tc.Locations)
			})
		case acp.SessionUpdateKindUsageUpdate:
			var uu acp.UsageUpdate
			if err := json.Unmarshal(u.Update.Raw, &uu); err != nil {
				continue
			}
			// ACP reports context occupancy (used of size), not per-turn
			// token counts; the turn report carries the latest reading.
			r.mu.Lock()
			r.usage = &proto.AgentUsage{Input: uu.Used, Output: uu.Size}
			r.mu.Unlock()
		}
	}
}

func locations(in []acp.ToolCallLocation) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, l := range in {
		if len(out) == 16 {
			break
		}
		out = append(out, l.Path)
	}
	return out
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// recordFrame is the acp.Options.Frames hook: every line in either direction
// is redacted, bounded and appended to the transcript on its own stream.
func (r *agentRun) recordFrame(f acp.Frame) {
	frame, redacted := r.redact.apply(f.Raw)
	frame, truncated := boundFrame(frame)
	r.mu.Lock()
	replaying := r.replaying
	r.mu.Unlock()
	rec := proto.ACPFrameRecord{At: f.At.UnixMilli(), Frame: frame, Redacted: redacted, Truncated: truncated}
	stream := uint8(proto.StreamACPOut)
	if f.Direction == acp.DirectionIn {
		stream = proto.StreamACPIn
		rec.Replayed = replaying && isSessionUpdate(f.Raw)
	}
	r.record(stream, proto.MustMarshal(rec))
}

func isSessionUpdate(raw []byte) bool {
	var env struct {
		Method string `json:"method"`
	}
	return json.Unmarshal(raw, &env) == nil && env.Method == acp.MethodSessionUpdate
}

// ---------------------------------------------------------------------------
// ACP client-side handler: what the harness may ask of the workspace
// ---------------------------------------------------------------------------

// agentHandler answers the harness's requests. Filesystem access goes
// through the jail after translating the harness's absolute path; terminals
// are managed sessions; permissions and elicitations become Approvals the
// control plane owns until a human answers.
type agentHandler struct{ r *agentRun }

// wsPath maps a harness path (absolute, in the workspace's own view of the
// tree) to a jail-relative path. A path outside the mount is refused before
// the jail ever sees it; a relative path is taken as relative to the root.
func (h *agentHandler) wsPath(p string) (string, error) {
	return workspaceRelativePath(h.r.mount, p)
}

func (h *agentHandler) RequestPermission(ctx context.Context, req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	r := h.r
	pick := func(kinds ...acp.PermissionOptionKind) *acp.PermissionOption {
		for _, k := range kinds {
			for i := range req.Options {
				if req.Options[i].Kind == k {
					return &req.Options[i]
				}
			}
		}
		return nil
	}
	selected := func(id acp.PermissionOptionId) (acp.RequestPermissionResponse, error) {
		out, err := acp.NewRequestPermissionOutcomeSelected(acp.SelectedPermissionOutcome{OptionID: id})
		if err != nil {
			return acp.RequestPermissionResponse{}, err
		}
		return acp.RequestPermissionResponse{Outcome: out}, nil
	}
	cancelledOutcome := acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}
	switch r.req.Policy.Approve {
	case proto.ApproveAuto:
		if o := pick(acp.PermissionOptionKindAllowOnce, acp.PermissionOptionKindAllowAlways); o != nil {
			return selected(o.OptionID)
		}
		return cancelledOutcome, nil
	case proto.ApproveNever:
		if o := pick(acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways); o != nil {
			return selected(o.OptionID)
		}
		return cancelledOutcome, nil
	}
	detail, _ := json.Marshal(req)
	ap := &proto.Approval{
		ID: ids.New("ap"), Kind: proto.ApprovalToolCall, ToolCall: string(req.ToolCall.ToolCallID),
		Locations: locations(req.ToolCall.Locations), Detail: boundDetail(detail),
	}
	if req.ToolCall.Title != nil {
		ap.Title = truncateString(*req.ToolCall.Title, 512)
	}
	if req.ToolCall.Kind != nil {
		ap.ToolKind = string(*req.ToolCall.Kind)
	}
	for _, o := range req.Options {
		ap.Options = append(ap.Options, proto.ApprovalOption{ID: string(o.OptionID), Name: o.Name, Kind: string(o.Kind)})
	}
	d, ok := r.park(ctx, proto.AgentReportPermission, ap)
	if !ok || d.Denied && d.Option == "" {
		if o := pick(acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways); ok && o != nil {
			return selected(o.OptionID)
		}
		return cancelledOutcome, nil
	}
	for _, o := range req.Options {
		if string(o.OptionID) == d.Option {
			return selected(o.OptionID)
		}
	}
	// The human chose an id the harness did not offer (a stale UI); a reject
	// is the safe reading, cancelled if the harness offers none.
	if o := pick(acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways); o != nil {
		return selected(o.OptionID)
	}
	return cancelledOutcome, nil
}

// park reports an approval and waits for its decision. ok is false when the
// run ended, the harness withdrew the request ($/cancel_request), or the
// connection died before a decision arrived; the approval then stays on the
// control plane as delivered=-1 and the harness asks again next attempt.
func (r *agentRun) park(ctx context.Context, kind string, ap *proto.Approval) (proto.ApprovalDecision, bool) {
	ch := make(chan proto.ApprovalDecision, 1)
	r.mu.Lock()
	r.approvals[ap.ID] = ch
	client := r.client
	r.mu.Unlock()
	r.reporter.report(kind, func(rep *proto.AgentReport) {
		cp := *ap
		rep.Approval = &cp
		rep.ToolCall = ap.ToolCall
		rep.ToolKind = ap.ToolKind
		rep.ToolTitle = ap.Title
		rep.Locations = ap.Locations
	})
	var done <-chan struct{}
	if client != nil {
		done = client.Done()
	}
	select {
	case d, ok := <-ch:
		return d, ok
	case <-ctx.Done():
	case <-r.ctx.Done():
	case <-done:
	}
	r.mu.Lock()
	delete(r.approvals, ap.ID)
	r.mu.Unlock()
	return proto.ApprovalDecision{}, false
}

func boundDetail(b []byte) json.RawMessage {
	if len(b) <= 64<<10 {
		return b
	}
	return nil
}

func (h *agentHandler) ReadTextFile(ctx context.Context, req acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	p, err := h.wsPath(req.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}
	unlock, err := h.r.n.lockWorkspaceTree(h.r.w, false)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}
	defer unlock()
	res, err := h.r.w.handle.FS().Read(p, 0, 0)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}
	if !res.EOF {
		// The jail's read limit is for chunked clients; a harness wants the
		// file. Read the remainder in bounded steps up to the transcript's
		// idea of "too big".
		data := append([]byte(nil), res.Data...)
		for !res.EOF && len(data) < 16<<20 {
			res, err = h.r.w.handle.FS().Read(p, int64(len(data)), 0)
			if err != nil {
				return acp.ReadTextFileResponse{}, err
			}
			data = append(data, res.Data...)
		}
		res.Data = data
	}
	content := string(res.Data)
	if req.Line != nil || req.Limit != nil {
		lines := strings.Split(content, "\n")
		start := 0
		if req.Line != nil && *req.Line > 1 {
			start = int(*req.Line) - 1
		}
		if start > len(lines) {
			start = len(lines)
		}
		end := len(lines)
		if req.Limit != nil && *req.Limit >= 0 && start+int(*req.Limit) < end {
			end = start + int(*req.Limit)
		}
		content = strings.Join(lines[start:end], "\n")
	}
	return acp.ReadTextFileResponse{Content: content}, nil
}

func (h *agentHandler) WriteTextFile(ctx context.Context, req acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	p, err := h.wsPath(req.Path)
	if err != nil {
		return acp.WriteTextFileResponse{}, err
	}
	unlock, err := h.r.n.lockWorkspaceTree(h.r.w, false)
	if err != nil {
		return acp.WriteTextFileResponse{}, err
	}
	defer unlock()
	if err := h.r.w.handle.FS().Write(p, []byte(req.Content), 0, false, true); err != nil {
		return acp.WriteTextFileResponse{}, err
	}
	return acp.WriteTextFileResponse{}, nil
}

func (h *agentHandler) CreateTerminal(ctx context.Context, req acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	r := h.r
	if req.Command == "" {
		return acp.CreateTerminalResponse{}, proto.Err(proto.CodeBadRequest, "command is required")
	}
	cwd := "."
	if req.Cwd != nil && *req.Cwd != "" {
		p, err := h.wsPath(*req.Cwd)
		if err != nil {
			return acp.CreateTerminalResponse{}, err
		}
		cwd = p
	}
	extra := map[string]string{}
	for _, e := range req.Env {
		extra[e.Name] = e.Value
	}
	unlock, err := r.n.lockWorkspaceTree(r.w, false)
	if err != nil {
		return acp.CreateTerminalResponse{}, err
	}
	defer unlock()
	r.mu.Lock()
	count := len(r.terminals)
	r.mu.Unlock()
	if count >= maxAgentTerminals {
		return acp.CreateTerminalResponse{}, proto.Err(proto.CodeResourceExhausted, "run has %d terminals open", count)
	}
	spec := session.Spec{
		WS: r.w.ID, Generation: r.w.Generation, Kind: proto.SessionExec, Program: append([]string{req.Command}, req.Args...), Cwd: cwd,
		Env: r.n.sessionEnv(r.w, extra), Principal: r.req.Owner, Tenant: r.req.Tenant,
	}
	if err := r.w.handle.Prepare(&spec); err != nil {
		return acp.CreateTerminalResponse{}, err
	}
	s, err := r.n.sessions.Open(spec)
	if err != nil {
		return acp.CreateTerminalResponse{}, err
	}
	limit := agentTerminalOutputLimit
	if req.OutputByteLimit != nil && *req.OutputByteLimit > 0 && *req.OutputByteLimit < int64(limit) {
		limit = int(*req.OutputByteLimit)
	}
	r.mu.Lock()
	r.terminals[s.ID] = &agentTerminal{s: s, limit: limit}
	r.mu.Unlock()
	return acp.CreateTerminalResponse{TerminalID: acp.TerminalId(s.ID)}, nil
}

func (r *agentRun) terminal(id acp.TerminalId) (*agentTerminal, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.terminals[string(id)]
	if t == nil {
		return nil, proto.Err(proto.CodeNotFound, "terminal %s", string(id))
	}
	return t, nil
}

// pull drains new session output into the bounded buffer, keeping the tail
// when the limit is exceeded, as ACP's terminal/output contract asks.
func (t *agentTerminal) pull() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	chunks, err := t.s.Log.Read(t.cursor, 0)
	if err != nil {
		var ev *session.ErrEvicted
		if errors.As(err, &ev) {
			t.cursor = ev.Oldest
			t.cut = true
			return nil
		}
		return err
	}
	for _, c := range chunks {
		t.cursor = c.Seq + 1
		if c.Stream != proto.StreamStdout && c.Stream != proto.StreamStderr {
			continue
		}
		t.buf = append(t.buf, c.Data...)
	}
	if len(t.buf) > t.limit {
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.limit:]...)
		t.cut = true
	}
	return nil
}

func (h *agentHandler) TerminalOutput(ctx context.Context, req acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	t, err := h.r.terminal(req.TerminalID)
	if err != nil {
		return acp.TerminalOutputResponse{}, err
	}
	if err := t.pull(); err != nil {
		return acp.TerminalOutputResponse{}, err
	}
	t.mu.Lock()
	res := acp.TerminalOutputResponse{Output: string(t.buf), Truncated: t.cut}
	t.mu.Unlock()
	if exit := t.s.ExitInfo(); exit != nil {
		res.ExitStatus = exitStatus(exit)
	}
	return res, nil
}

func exitStatus(exit *proto.ExitInfo) *acp.TerminalExitStatus {
	st := &acp.TerminalExitStatus{}
	code := int64(exit.Code)
	st.ExitCode = &code
	if exit.Signal != "" {
		sig := exit.Signal
		st.Signal = &sig
	}
	return st
}

func (h *agentHandler) WaitForTerminalExit(ctx context.Context, req acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	t, err := h.r.terminal(req.TerminalID)
	if err != nil {
		return acp.WaitForTerminalExitResponse{}, err
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-h.r.ctx.Done():
			cancel()
		case <-wctx.Done():
		}
	}()
	exit, err := t.s.Wait(wctx)
	if err != nil {
		return acp.WaitForTerminalExitResponse{}, err
	}
	st := exitStatus(exit)
	return acp.WaitForTerminalExitResponse{ExitCode: st.ExitCode, Signal: st.Signal}, nil
}

func (h *agentHandler) KillTerminal(ctx context.Context, req acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	t, err := h.r.terminal(req.TerminalID)
	if err != nil {
		return acp.KillTerminalResponse{}, err
	}
	// Kill leaves the terminal readable: the harness reads the tail and
	// releases it, which is the ACP contract.
	t.s.Terminate("acp terminal/kill")
	return acp.KillTerminalResponse{}, nil
}

func (h *agentHandler) ReleaseTerminal(ctx context.Context, req acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	r := h.r
	r.mu.Lock()
	t := r.terminals[string(req.TerminalID)]
	delete(r.terminals, string(req.TerminalID))
	r.mu.Unlock()
	if t == nil {
		return acp.ReleaseTerminalResponse{}, proto.Err(proto.CodeNotFound, "terminal %s", string(req.TerminalID))
	}
	r.n.sessions.Remove(t.s.ID, true)
	return acp.ReleaseTerminalResponse{}, nil
}

func (h *agentHandler) CreateElicitation(ctx context.Context, req acp.CreateElicitationRequest) (acp.CreateElicitationResponse, error) {
	r := h.r
	if r.req.Policy.Approve != proto.ApproveOnRequest && r.req.Policy.Approve != "" {
		// Nobody is there to fill a form in: decline rather than block.
		return acp.NewCreateElicitationResponseDecline(), nil
	}
	var meta struct {
		Message       string `json:"message"`
		ElicitationID string `json:"elicitationId"`
		URL           string `json:"url"`
	}
	_ = json.Unmarshal(req.Raw, &meta)
	title := meta.Message
	if meta.URL != "" {
		title = strings.TrimSpace(title + " " + meta.URL)
	}
	ap := &proto.Approval{
		ID: ids.New("ap"), Kind: proto.ApprovalElicitation, Title: truncateString(title, 512),
		ToolCall: meta.ElicitationID, ToolKind: req.Kind, Detail: boundDetail(req.Raw),
		Options: []proto.ApprovalOption{{ID: "accept", Name: "Accept", Kind: "allow_once"}, {ID: "decline", Name: "Decline", Kind: "reject_once"}},
	}
	d, ok := r.park(ctx, proto.AgentReportElicitation, ap)
	if !ok {
		return acp.NewCreateElicitationResponseCancel(), nil
	}
	if d.Denied || d.Option == "decline" {
		return acp.NewCreateElicitationResponseDecline(), nil
	}
	if d.Option == "cancel" {
		return acp.NewCreateElicitationResponseCancel(), nil
	}
	var b bytes.Buffer
	b.WriteString(`{"action":"accept"`)
	if len(d.Content) > 0 && json.Valid(d.Content) {
		b.WriteString(`,"content":`)
		b.Write(d.Content)
	}
	b.WriteString(`}`)
	return acp.CreateElicitationResponse{Kind: "accept", Raw: b.Bytes()}, nil
}

func (h *agentHandler) CompleteElicitation(ctx context.Context, n acp.CompleteElicitationNotification) error {
	// The harness finished a URL elicitation on its own; anything still
	// parked for it is answered "cancel" by the harness withdrawing the
	// request, which the client library already routes to the handler ctx.
	return nil
}

func (h *agentHandler) Ext(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	return nil, proto.Err(proto.CodeUnsupported, "extension method %q is not supported", method)
}

// lastStderrLine returns the final non-blank line of a redacted stderr chunk,
// bounded so a stack trace cannot balloon an error string.
func lastStderrLine(chunk []byte) string {
	lines := bytes.Split(bytes.TrimSpace(chunk), []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(string(lines[i])); line != "" {
			if len(line) > 200 {
				line = line[:200] + "…"
			}
			return line
		}
	}
	return ""
}
