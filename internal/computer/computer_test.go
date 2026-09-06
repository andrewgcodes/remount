package computer_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"remount.dev/remount/internal/computer"
	"remount.dev/remount/internal/computer/fakecdp"
	"remount.dev/remount/internal/proto"
)

func dialer(addr string) computer.Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

func connect(t *testing.T, s *fakecdp.Server, adjust func(*computer.Options)) *computer.Client {
	t.Helper()
	opts := computer.Options{
		Dial:         dialer(s.Addr()),
		Endpoint:     s.Addr(),
		Viewport:     proto.ComputerViewport{Width: 800, Height: 600},
		ReadyTimeout: 10 * time.Second,
		CallTimeout:  10 * time.Second,
		LoadTimeout:  5 * time.Second,
	}
	if adjust != nil {
		adjust(&opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := computer.Connect(ctx, opts)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestConnectPreparesPage(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	c := connect(t, s, func(o *computer.Options) { o.DownloadPath = "/ws/.remount/downloads" })

	if c.Version() != "HeadlessChrome/fake" {
		t.Fatalf("version = %q", c.Version())
	}
	for _, want := range []string{
		"Browser.setDownloadBehavior", "Target.attachToTarget",
		"Page.enable", "Runtime.enable", "Emulation.setDeviceMetricsOverride",
	} {
		if _, ok := s.Called(want); !ok {
			t.Fatalf("%s was not called; got %v", want, s.Methods())
		}
	}
	metrics, _ := s.Called("Emulation.setDeviceMetricsOverride")
	if got := string(metrics.Param("width")); got != "800" {
		t.Fatalf("viewport width param = %s", got)
	}
	behavior, _ := s.Called("Browser.setDownloadBehavior")
	if got := string(behavior.Param("downloadPath")); got != `"/ws/.remount/downloads"` {
		t.Fatalf("downloadPath = %s", got)
	}
}

func TestScreenshotReturnsBytes(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	c := connect(t, s, nil)

	res, err := c.Screenshot(context.Background())
	if err != nil {
		t.Fatalf("screenshot: %v", err)
	}
	if !bytes.Equal(res.PNG, fakecdp.PNG) {
		t.Fatalf("screenshot bytes differ: %d bytes", len(res.PNG))
	}
	if res.Width != 800 || res.Height != 600 {
		t.Fatalf("screenshot reported %dx%d", res.Width, res.Height)
	}
	shot, _ := s.Called("Page.captureScreenshot")
	if got := string(shot.Param("format")); got != `"png"` {
		t.Fatalf("format = %s", got)
	}
	if shot.Param("clip") == nil {
		t.Fatal("screenshot was not clipped to the viewport")
	}
}

func TestScreenshotOverCapIsRefused(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	big := bytes.Repeat([]byte{0x89}, proto.ComputerMaxScreenshotBytes+1)
	s.Handle("Page.captureScreenshot", func(fakecdp.Call) (any, error) {
		return map[string]any{"data": base64.StdEncoding.EncodeToString(big)}, nil
	})
	c := connect(t, s, nil)

	_, err := c.Screenshot(context.Background())
	if !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("oversized screenshot error = %v", err)
	}
}

func TestActionsMapToCDPCalls(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	c := connect(t, s, nil)

	actions := []proto.ComputerAction{
		{Kind: proto.ComputerActionClick, X: 10, Y: 20},
		{Kind: proto.ComputerActionType, Text: "hello"},
		{Kind: proto.ComputerActionKey, Key: "Enter"},
		{Kind: proto.ComputerActionScroll, X: 5, Y: 6, DY: 120},
		{Kind: proto.ComputerActionDrag, X: 1, Y: 2, ToX: 3, ToY: 4},
	}
	if err := c.Apply(context.Background(), actions); err != nil {
		t.Fatalf("apply: %v", err)
	}
	calls := s.Calls()
	var mouse, insert, keys int
	var wheel, pressed bool
	for _, call := range calls {
		switch call.Method {
		case "Input.dispatchMouseEvent":
			mouse++
			if string(call.Param("type")) == `"mouseWheel"` {
				wheel = true
				if string(call.Param("deltaY")) != "120" {
					t.Fatalf("scroll deltaY = %s", call.Param("deltaY"))
				}
			}
			if string(call.Param("type")) == `"mousePressed"` {
				pressed = true
				if string(call.Param("button")) != `"left"` {
					t.Fatalf("click button = %s", call.Param("button"))
				}
			}
		case "Input.insertText":
			insert++
			if string(call.Param("text")) != `"hello"` {
				t.Fatalf("insertText text = %s", call.Param("text"))
			}
		case "Input.dispatchKeyEvent":
			keys++
			if got := string(call.Param("key")); got != `"Enter"` {
				t.Fatalf("key = %s", got)
			}
		}
	}
	// click: move+press+release; scroll: 1; drag: move+press+move+release.
	if mouse != 8 {
		t.Fatalf("mouse events = %d (%v)", mouse, s.Methods())
	}
	if insert != 1 || keys != 2 {
		t.Fatalf("insertText=%d keyEvents=%d", insert, keys)
	}
	if !wheel || !pressed {
		t.Fatalf("wheel=%v pressed=%v", wheel, pressed)
	}
}

func TestInvalidActionsRejectedBeforeDispatch(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	c := connect(t, s, nil)
	before := len(s.Calls())

	cases := []proto.ComputerAction{
		{Kind: proto.ComputerActionClick, X: 10, Y: 20000},
		{Kind: proto.ComputerActionKey, Key: "F13"},
		{Kind: proto.ComputerActionType},
		{Kind: "teleport"},
	}
	for _, bad := range cases {
		// A valid action ahead of the bad one must not be dispatched either.
		err := c.Apply(context.Background(), []proto.ComputerAction{
			{Kind: proto.ComputerActionMove, X: 1, Y: 1}, bad,
		})
		if !errors.Is(err, &proto.Error{Code: proto.CodeBadRequest}) {
			t.Fatalf("%v: err = %v", bad, err)
		}
		if got := proto.ErrorReason(err); got != proto.ReasonInputRejected {
			t.Fatalf("%v: reason = %q", bad, got)
		}
	}
	if len(s.Calls()) != before {
		t.Fatalf("a rejected batch dispatched %d calls", len(s.Calls())-before)
	}
}

func TestNavigateWaitsForLoad(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	s.Handle("Runtime.evaluate", func(call fakecdp.Call) (any, error) {
		if string(call.Param("expression")) == `"document.title"` {
			return map[string]any{"result": map[string]any{"value": "Fake Page"}}, nil
		}
		return map[string]any{"result": map[string]any{"value": "http://example.test/final"}}, nil
	})
	c := connect(t, s, nil)

	res, err := c.Navigate(context.Background(), "http://example.test/")
	if err != nil {
		t.Fatalf("navigate: %v", err)
	}
	if res.Status != proto.ComputerNavigateLoaded {
		t.Fatalf("status = %q", res.Status)
	}
	if res.Title != "Fake Page" || res.URL != "http://example.test/final" {
		t.Fatalf("navigate = %+v", res)
	}
}

func TestNavigateErrorTextIsDenied(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	s.Handle("Page.navigate", func(fakecdp.Call) (any, error) {
		return map[string]any{"frameId": "F", "errorText": "net::ERR_TUNNEL_CONNECTION_FAILED"}, nil
	})
	c := connect(t, s, nil)

	_, err := c.Navigate(context.Background(), "http://blocked.test/")
	if !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("blocked navigation = %v", err)
	}
	if got := proto.ErrorReason(err); got != proto.ReasonNavigationDenied {
		t.Fatalf("reason = %q", got)
	}
}

func TestEvalReturnsJSONValue(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	s.Handle("Runtime.evaluate", func(fakecdp.Call) (any, error) {
		return map[string]any{"result": map[string]any{"value": []any{1, 2, 3}}}, nil
	})
	c := connect(t, s, nil)

	raw, err := c.Eval(context.Background(), "[1,2,3]")
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	var got []int
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(got) != 3 || got[2] != 3 {
		t.Fatalf("eval value = %v", got)
	}
	call, _ := s.Called("Runtime.evaluate")
	if string(call.Param("returnByValue")) != "true" {
		t.Fatalf("returnByValue = %s", call.Param("returnByValue"))
	}
}

func TestCrashSurfacesBrowserCrashed(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	closed := make(chan string, 1)
	c := connect(t, s, func(o *computer.Options) {
		o.OnClosed = func(reason string) { closed <- reason }
	})

	s.Crash()
	select {
	case reason := <-closed:
		if reason != proto.ReasonBrowserCrashed {
			t.Fatalf("close reason = %q", reason)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no close callback after crash")
	}
	_, err := c.Screenshot(context.Background())
	if !errors.Is(err, &proto.Error{Code: proto.CodeClosed}) {
		t.Fatalf("post-crash screenshot = %v", err)
	}
	if got := proto.ErrorReason(err); got != proto.ReasonBrowserCrashed {
		t.Fatalf("post-crash reason = %q", got)
	}
	if c.Reason() != proto.ReasonBrowserCrashed {
		t.Fatalf("client reason = %q", c.Reason())
	}
}

func TestDownloadsTracked(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	done := make(chan computer.Download, 2)
	c := connect(t, s, func(o *computer.Options) {
		o.DownloadPath = "/ws/.remount/downloads"
		o.OnDownload = func(d computer.Download) { done <- d }
	})

	s.Emit("", "Browser.downloadWillBegin", map[string]any{
		"guid": "G1", "url": "http://files.test/report.csv", "suggestedFilename": "report.csv",
	})
	s.Emit("", "Browser.downloadProgress", map[string]any{
		"guid": "G1", "state": "completed", "receivedBytes": 42, "totalBytes": 42,
	})
	select {
	case d := <-done:
		if d.GUID != "G1" || d.Filename != "report.csv" || d.Bytes != 42 {
			t.Fatalf("download = %+v", d)
		}
		if d.State != proto.ComputerDownloadCompleted {
			t.Fatalf("state = %q", d.State)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no download callback")
	}
	list := c.Downloads()
	if len(list) != 1 || list[0].URL != "http://files.test/report.csv" {
		t.Fatalf("downloads = %+v", list)
	}
}

func TestPathTraversalFilenameIsReducedToBase(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	done := make(chan computer.Download, 1)
	c := connect(t, s, func(o *computer.Options) {
		o.DownloadPath = "/ws/.remount/downloads"
		o.OnDownload = func(d computer.Download) { done <- d }
	})
	s.Emit("", "Browser.downloadWillBegin", map[string]any{
		"guid": "G2", "url": "http://files.test/x", "suggestedFilename": "../../etc/passwd",
	})
	s.Emit("", "Browser.downloadProgress", map[string]any{"guid": "G2", "state": "completed"})
	select {
	case d := <-done:
		if d.Filename != "passwd" {
			t.Fatalf("filename = %q", d.Filename)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no download callback")
	}
	_ = c
}

func TestUnreachableEndpointIsNotHealthy(t *testing.T) {
	// A port nothing listens on must be reported as unavailable, never as a
	// browser that is merely slow.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = computer.Connect(ctx, computer.Options{
		Dial:         dialer(addr),
		Endpoint:     addr,
		ReadyTimeout: 300 * time.Millisecond,
	})
	if !errors.Is(err, &proto.Error{Code: proto.CodeTimeout}) {
		t.Fatalf("connect to dead port = %v", err)
	}
	if got := proto.ErrorReason(err); got != proto.ReasonDisplayUnavailable {
		t.Fatalf("reason = %q", got)
	}
}
