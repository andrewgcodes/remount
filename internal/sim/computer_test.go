package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/computer/fakecdp"
	"remount.dev/remount/internal/proto"
)

// waitEvent polls the workspace's event history until typ appears. Events
// travel node -> control asynchronously, so a poll is the honest wait.
func waitEvent(t *testing.T, ctx context.Context, c *client.Client, ws, typ string) proto.Event {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		evs, err := c.ReadEvents(ctx, 0, ws)
		if err != nil {
			t.Fatalf("read events: %v", err)
		}
		for _, e := range evs {
			if e.Type == typ {
				return e
			}
		}
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			var seen []string
			for _, e := range evs {
				seen = append(seen, e.Type)
			}
			t.Fatalf("event %s never appeared; saw %v", typ, seen)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func attachComputer(t *testing.T, ctx context.Context, c *client.Client, ws string, port int) *client.Computer {
	t.Helper()
	cmp, err := c.CreateComputer(ctx, proto.ComputerCreateReq{
		WS:       ws,
		Launch:   &proto.ComputerLaunch{Attach: true, Port: port},
		Viewport: proto.ComputerViewport{Width: 800, Height: 600},
	})
	if err != nil {
		t.Fatalf("computer.create: %v", err)
	}
	return cmp
}

// A computer is a CDP conversation the node holds over the same resolved port
// path a port session uses. Nothing here needs a real browser: the fake is the
// contract the node's client is written against.
func TestComputerAttachFakeCDP(t *testing.T) {
	fake := fakecdp.New()
	defer fake.Close()

	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 90*time.Second)

	cmp := attachComputer(t, ctx, c, ws.ID, fake.Port())
	if cmp.CDPVersion() == "" || cmp.Session() != "" {
		t.Fatalf("attach spawned a session or lost the version: %q / %q", cmp.CDPVersion(), cmp.Session())
	}
	if v := cmp.Viewport(); v.Width != 800 || v.Height != 600 {
		t.Fatalf("viewport = %+v", v)
	}

	shot, err := cmp.Screenshot(ctx)
	if err != nil {
		t.Fatalf("screenshot: %v", err)
	}
	if !bytes.Equal(shot.PNG, fakecdp.PNG) || shot.Width != 800 {
		t.Fatalf("screenshot %d bytes %dx%d", len(shot.PNG), shot.Width, shot.Height)
	}
	if err := cmp.Click(ctx, 100, 200); err != nil {
		t.Fatalf("click: %v", err)
	}
	if err := cmp.Type(ctx, "remount"); err != nil {
		t.Fatalf("type: %v", err)
	}
	if err := cmp.Key(ctx, "Enter", 0); err != nil {
		t.Fatalf("key: %v", err)
	}
	nav, err := cmp.Navigate(ctx, "http://example.test/")
	if err != nil {
		t.Fatalf("navigate: %v", err)
	}
	if nav.Status != proto.ComputerNavigateLoaded {
		t.Fatalf("navigate status = %q", nav.Status)
	}
	value, err := cmp.Eval(ctx, "1+1")
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	var got string
	if err := json.Unmarshal(value, &got); err != nil || got != "fake" {
		t.Fatalf("eval value %s (%v)", value, err)
	}

	// The fake recorded the CDP the operations mapped to.
	if _, ok := fake.Called("Input.insertText"); !ok {
		t.Fatalf("type did not reach insertText: %v", fake.Methods())
	}
	if _, ok := fake.Called("Input.dispatchKeyEvent"); !ok {
		t.Fatalf("key did not reach dispatchKeyEvent: %v", fake.Methods())
	}

	state, err := cmp.Get(ctx)
	if err != nil || state.State != proto.ComputerStateReady {
		t.Fatalf("get = %+v (%v)", state, err)
	}

	created := waitEvent(t, ctx, c, ws.ID, proto.EvComputerCreated)
	var payload struct {
		Computer string `cbor:"computer"`
		Attach   bool   `cbor:"attach"`
	}
	if err := proto.Unmarshal(created.Payload, &payload); err != nil {
		t.Fatalf("decode computer.created: %v", err)
	}
	if payload.Computer != cmp.ID() || !payload.Attach {
		t.Fatalf("computer.created payload = %+v", payload)
	}

	if err := cmp.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Closing twice is the postcondition the caller asked for.
	if err := cmp.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
	waitEvent(t, ctx, c, ws.ID, proto.EvComputerClosed)

	if _, err := cmp.Screenshot(ctx); !errors.Is(err, &proto.Error{Code: proto.CodeNotFound}) {
		t.Fatalf("screenshot after close = %v", err)
	}
}

// Input carries a sequence: a retried batch is applied at most once, exactly
// as session input is deduplicated by iseq.
func TestComputerInputDeduplicatesByISeq(t *testing.T) {
	fake := fakecdp.New()
	defer fake.Close()

	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 90*time.Second)
	cmp := attachComputer(t, ctx, c, ws.ID, fake.Port())
	defer func() { _ = cmp.Close(ctx) }()

	countInserts := func() int {
		n := 0
		for _, call := range fake.Calls() {
			if call.Method == "Input.insertText" {
				n++
			}
		}
		return n
	}
	if err := cmp.Type(ctx, "once"); err != nil {
		t.Fatalf("type: %v", err)
	}
	first := countInserts()
	if first != 1 {
		t.Fatalf("first type produced %d inserts", first)
	}
	// Replay the way a client that lost its response and reconnected does: a
	// fresh handle restarts its sequence at 1, which the node already applied.
	for i := 0; i < 3; i++ {
		replay := c.Computer(ws.ID, cmp.ID())
		if err := replay.Input(ctx, proto.ComputerAction{Kind: proto.ComputerActionType, Text: "again"}); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}
	if got := countInserts(); got != first {
		t.Fatalf("replayed input applied %d extra times", got-first)
	}
	// A new sequence still advances, so dedupe is not a stuck computer.
	if err := cmp.Type(ctx, "twice"); err != nil {
		t.Fatalf("second type: %v", err)
	}
	if got := countInserts(); got != first+1 {
		t.Fatalf("a fresh sequence applied %d times", got-first)
	}
}

// A browser that dies is reported as closed with a stable reason, and the next
// operation fails with the same code and reason rather than hanging.
func TestComputerCrashReportsClosed(t *testing.T) {
	fake := fakecdp.New()
	defer fake.Close()

	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 90*time.Second)
	cmp := attachComputer(t, ctx, c, ws.ID, fake.Port())

	fake.Crash()

	deadline := time.Now().Add(30 * time.Second)
	var state *proto.ComputerGetRes
	for {
		var err error
		state, err = cmp.Get(ctx)
		if err != nil {
			t.Fatalf("computer.get after crash: %v", err)
		}
		if state.State == proto.ComputerStateClosed {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("computer stayed %q after the browser died", state.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state.Reason != proto.ReasonBrowserCrashed {
		t.Fatalf("crash reason = %q", state.Reason)
	}
	_, err := cmp.Screenshot(ctx)
	if !errors.Is(err, &proto.Error{Code: proto.CodeClosed}) {
		t.Fatalf("screenshot after crash = %v", err)
	}
	if got := proto.ErrorReason(err); got != proto.ReasonBrowserCrashed {
		t.Fatalf("screenshot reason = %q", got)
	}
	closed := waitEvent(t, ctx, c, ws.ID, proto.EvComputerClosed)
	var payload struct {
		Reason string `cbor:"reason"`
	}
	if err := proto.Unmarshal(closed.Payload, &payload); err != nil {
		t.Fatalf("decode computer.closed: %v", err)
	}
	if payload.Reason != proto.ReasonBrowserCrashed {
		t.Fatalf("computer.closed reason = %q", payload.Reason)
	}
}

// A completed download becomes a tenant-scoped artifact, reported by
// computer.downloads and announced as an event.
func TestComputerDownloadBecomesArtifact(t *testing.T) {
	fake := fakecdp.New()
	defer fake.Close()

	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 90*time.Second)
	cmp := attachComputer(t, ctx, c, ws.ID, fake.Port())
	defer func() { _ = cmp.Close(ctx) }()

	info, err := c.WorkspaceInfo(ctx, ws.ID)
	if err != nil {
		t.Fatalf("workspace info: %v", err)
	}
	// The node created the download directory when the computer was created;
	// stand in for the browser by writing the file it would have saved.
	body := []byte("id,value\n1,remount\n")
	downloads := filepath.Join(info.Root, ".remount", "downloads")
	if err := os.WriteFile(filepath.Join(downloads, "report.csv"), body, 0o600); err != nil {
		t.Fatalf("write download: %v", err)
	}

	fake.Emit("", "Browser.downloadWillBegin", map[string]any{
		"guid": "G1", "url": "http://files.test/report.csv", "suggestedFilename": "report.csv",
	})
	fake.Emit("", "Browser.downloadProgress", map[string]any{
		"guid": "G1", "state": "completed", "receivedBytes": len(body), "totalBytes": len(body),
	})

	ev := waitEvent(t, ctx, c, ws.ID, proto.EvComputerDownload)
	var payload struct {
		Computer string `cbor:"computer"`
		Artifact string `cbor:"artifact"`
		Filename string `cbor:"filename"`
	}
	if err := proto.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatalf("decode computer.download: %v", err)
	}
	if payload.Computer != cmp.ID() || payload.Artifact == "" || payload.Filename != "report.csv" {
		t.Fatalf("computer.download payload = %+v", payload)
	}

	list, err := cmp.Downloads(ctx)
	if err != nil {
		t.Fatalf("downloads: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("downloads = %+v", list)
	}
	if list[0].State != proto.ComputerDownloadCompleted || list[0].Artifact != payload.Artifact {
		t.Fatalf("download record = %+v", list[0])
	}
	// Bytes is the file's own length, not the published archive's.
	if list[0].Bytes != int64(len(body)) {
		t.Fatalf("download bytes = %d, want %d", list[0].Bytes, len(body))
	}
}

// Releasing the workspace closes every computer on it and says why.
func TestComputerClosesWithWorkspace(t *testing.T) {
	fake := fakecdp.New()
	defer fake.Close()

	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 90*time.Second)
	cmp := attachComputer(t, ctx, c, ws.ID, fake.Port())

	if err := c.DestroyWorkspace(ctx, ws.ID); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	ev := waitEvent(t, ctx, c, ws.ID, proto.EvComputerClosed)
	var payload struct {
		Reason string `cbor:"reason"`
	}
	if err := proto.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatalf("decode computer.closed: %v", err)
	}
	if payload.Reason != proto.ComputerClosedReasonWorkspaceReleased {
		t.Fatalf("computer.closed reason = %q", payload.Reason)
	}
	if _, err := cmp.Get(ctx); err == nil {
		t.Fatal("computer.get succeeded after the workspace was destroyed")
	}
}
