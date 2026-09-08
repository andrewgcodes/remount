package sim

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// waitForNodeEvent tails control's canonical log for a node-origin event of
// one type attributed to one session, and fails if none arrives in time.
func waitForNodeEvent(t *testing.T, c interface {
	TailEvents(context.Context, uint64, string, ...client.EventFilterOption) (<-chan proto.Event, error)
}, ws, typ, session string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tail, err := c.TailEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case e, ok := <-tail:
			if !ok {
				t.Fatalf("event tail closed before a node-origin %s for %s arrived", typ, session)
			}
			if e.Type == typ && e.Origin == "node" && (e.Session == session || bytes.Contains(e.Payload, []byte(session))) {
				return
			}
		case <-ctx.Done():
			t.Fatalf("control never received a node-origin %s for %s: the node's audit stream is not reaching control\n%s", typ, session, describeEvents(c, ws))
		}
	}
}

// describeEvents lists what control holds for ws, so a failure says what did
// arrive rather than only what did not.
func describeEvents(c interface {
	TailEvents(context.Context, uint64, string, ...client.EventFilterOption) (<-chan proto.Event, error)
}, ws string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tail, err := c.TailEvents(ctx, 0, ws)
	if err != nil {
		return "tail: " + err.Error()
	}
	var b strings.Builder
	for {
		select {
		case e, ok := <-tail:
			if !ok {
				return b.String()
			}
			fmt.Fprintf(&b, "  seq=%d origin=%s type=%s producer_seq=%d session=%s\n", e.Seq, e.Origin, e.Type, e.ProducerSeq, e.Session)
		case <-ctx.Done():
			return b.String()
		}
	}
}

// TestNodeEventsSurviveANodeRestart: a node's observations (s.opened,
// cred.used, egress.*) reach control's canonical log before and after the node
// process restarts under its durable identity.
//
// The node keeps its event store in memory, so a restart begins numbering
// producer sequences again at 1 while control still holds the watermark from
// the previous life. Every post-restart event then collides with a sequence
// control has already recorded, and control's dedupe answers "changed" —
// so the node's forwarder retried the same batch forever and nothing it
// observed after the restart was ever recorded. On a live deployment that was
// invisible: sessions ran, credentials were brokered, and control's log showed
// none of it while doctor reported healthy.
func TestNodeEventsSurviveANodeRestart(t *testing.T) {
	w := newWorldExpiring(t)
	dataDir := t.TempDir()
	configure := func(options *node.Options) {
		options.DataDir = dataDir
		options.Labels = map[string]string{"ev-restart": "yes"}
	}
	n := w.nodeWith("ev-restart", configure)
	c := w.client("ev-restart-client")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n.ID()}})
	run := func() string {
		s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "echo ok"}})
		if err != nil {
			t.Fatal(err)
		}
		collectSessionChunks(t, ctx, s)
		return s.ID
	}
	// Control: the harness sees node-origin events at all.
	waitForNodeEvent(t, c, ws.ID, "s.opened", run())

	w.stopNode("ev-restart")
	restarted := w.nodeWith("ev-restart", configure)
	if restarted.ID() != n.ID() {
		t.Fatalf("durable node identity changed across restart: %s != %s", restarted.ID(), n.ID())
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "producer-seq.cbor")); err != nil {
		t.Fatalf("the first life left no producer watermark: %v", err)
	}
	waitForNodeEvent(t, c, ws.ID, "s.opened", run())
}
