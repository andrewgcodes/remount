package computer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
)

// DefaultPort is the DevTools port a computer uses when the request names none.
const DefaultPort = 9222

// DefaultViewport is used when a request declares no viewport.
var DefaultViewport = proto.ComputerViewport{Width: 1280, Height: 720}

// DefaultReadyTimeout bounds the wait for a freshly spawned browser to answer
// on its DevTools port.
const DefaultReadyTimeout = 30 * time.Second

// DefaultCallTimeout bounds one CDP round trip.
const DefaultCallTimeout = 30 * time.Second

// DefaultLoadTimeout bounds a navigation's wait for the load event.
const DefaultLoadTimeout = 30 * time.Second

// Options configures one CDP conversation.
type Options struct {
	// Dial opens a fresh connection to the browser's DevTools port.
	Dial Dialer
	// Endpoint is the authority used to build DevTools URLs. It never decides
	// where the bytes go — Dial does — but it must be a syntactically valid
	// host:port so net/http will accept the request.
	Endpoint string
	Viewport proto.ComputerViewport
	// DownloadPath is the workspace-side directory the browser saves into.
	DownloadPath string
	ReadyTimeout time.Duration
	CallTimeout  time.Duration
	LoadTimeout  time.Duration
	// OnDownload is called, off the read loop, each time a download reaches a
	// terminal state. It must not block for long.
	OnDownload func(Download)
	// OnClosed is called once when the conversation ends unexpectedly.
	OnClosed func(reason string)
}

// Download is one file the browser fetched.
type Download struct {
	GUID     string
	URL      string
	Filename string
	State    string
	Bytes    int64
}

// Client is one live CDP conversation with a page.
type Client struct {
	opts    Options
	conn    *conn
	page    string // CDP session id of the attached page target
	target  string
	version string

	unsubscribe func()

	mu        sync.Mutex
	downloads map[string]*Download
	order     []string
	reason    string

	watchers sync.WaitGroup
	stopped  chan struct{}
	stopOnce sync.Once
}

// Connect attaches to a browser already listening on the DevTools port and
// prepares a page for input: a viewport override, download events, and the
// Page/Runtime domains conformance asserts against.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if opts.Dial == nil {
		return nil, proto.Err(proto.CodeBadRequest, "computer: no dialer")
	}
	if opts.Viewport.Width <= 0 || opts.Viewport.Height <= 0 {
		opts.Viewport = DefaultViewport
	}
	if opts.ReadyTimeout <= 0 {
		opts.ReadyTimeout = DefaultReadyTimeout
	}
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = DefaultCallTimeout
	}
	if opts.LoadTimeout <= 0 {
		opts.LoadTimeout = DefaultLoadTimeout
	}
	version, err := waitReady(ctx, opts.Endpoint, opts.Dial, opts.ReadyTimeout)
	if err != nil {
		return nil, err
	}
	// The debugger URL comes from inside the workspace. Keep its path, which
	// names the browser session, and discard its authority: where the bytes go
	// is the node's decision, never the browser's.
	wsPath := "/devtools/browser"
	if parsed, perr := url.Parse(version.WebSocketDebuggerURL); perr == nil && parsed.Path != "" {
		wsPath = parsed.Path
	}
	cn, err := dialCDP(ctx, opts.Endpoint, wsPath, opts.Dial)
	if err != nil {
		return nil, proto.ErrReason(proto.CodeUnreachable, proto.ReasonDisplayUnavailable,
			"devtools websocket: %v", err)
	}
	c := &Client{
		opts:      opts,
		conn:      cn,
		version:   version.Browser,
		downloads: map[string]*Download{},
		stopped:   make(chan struct{}),
	}
	c.unsubscribe = cn.onEvent(c.handleEvent)
	if err := c.setup(ctx); err != nil {
		c.Close()
		return nil, err
	}
	c.watchers.Add(1)
	go c.watchClose()
	return c, nil
}

// Version is the browser identification string from /json/version.
func (c *Client) Version() string { return c.version }

// Viewport is the CSS-pixel rectangle every action's coordinates refer to.
func (c *Client) Viewport() proto.ComputerViewport { return c.opts.Viewport }

func (c *Client) setup(ctx context.Context) error {
	call, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	defer cancel()
	if c.opts.DownloadPath != "" {
		// Downloads are allowed but confined: the browser writes only into the
		// node-chosen directory, and the node publishes from there.
		if err := c.conn.call(call, "", "Browser.setDownloadBehavior", map[string]any{
			"behavior":      "allow",
			"downloadPath":  c.opts.DownloadPath,
			"eventsEnabled": true,
		}, nil); err != nil {
			return err
		}
	}
	target, err := c.resolveTarget(call)
	if err != nil {
		return err
	}
	c.target = target
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.conn.call(call, "", "Target.attachToTarget", map[string]any{
		"targetId": target, "flatten": true,
	}, &attached); err != nil {
		return err
	}
	if attached.SessionID == "" {
		return proto.ErrReason(proto.CodeUnsupported, proto.ReasonDisplayUnavailable,
			"devtools attached no page session")
	}
	c.page = attached.SessionID
	if err := c.conn.call(call, c.page, "Page.enable", nil, nil); err != nil {
		return err
	}
	if err := c.conn.call(call, c.page, "Runtime.enable", nil, nil); err != nil {
		return err
	}
	return c.conn.call(call, c.page, "Emulation.setDeviceMetricsOverride", map[string]any{
		"width": c.opts.Viewport.Width, "height": c.opts.Viewport.Height,
		"deviceScaleFactor": 1, "mobile": false,
	}, nil)
}

func (c *Client) resolveTarget(ctx context.Context) (string, error) {
	var targets struct {
		TargetInfos []struct {
			TargetID string `json:"targetId"`
			Type     string `json:"type"`
		} `json:"targetInfos"`
	}
	if err := c.conn.call(ctx, "", "Target.getTargets", nil, &targets); err == nil {
		for _, t := range targets.TargetInfos {
			if t.Type == "page" && t.TargetID != "" {
				return t.TargetID, nil
			}
		}
	}
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := c.conn.call(ctx, "", "Target.createTarget", map[string]any{
		"url": "about:blank", "width": c.opts.Viewport.Width, "height": c.opts.Viewport.Height,
	}, &created); err != nil {
		return "", err
	}
	if created.TargetID == "" {
		return "", proto.ErrReason(proto.CodeUnsupported, proto.ReasonDisplayUnavailable,
			"devtools created no page target")
	}
	return created.TargetID, nil
}

// watchClose turns a dead socket into one observable terminal reason.
func (c *Client) watchClose() {
	defer c.watchers.Done()
	select {
	case <-c.conn.closed:
	case <-c.stopped:
		return
	}
	c.mu.Lock()
	first := c.reason == ""
	if first {
		c.reason = proto.ReasonBrowserCrashed
	}
	reason := c.reason
	c.mu.Unlock()
	if first && c.opts.OnClosed != nil {
		c.opts.OnClosed(reason)
	}
}

// Err reports the terminal failure, or nil while the conversation is live.
func (c *Client) Err() error {
	if err := c.conn.terminalErr(); err != nil {
		return closedErr(err)
	}
	return nil
}

// Reason is the stable cause of a finished conversation, or "".
func (c *Client) Reason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// Close ends the conversation and joins every goroutine it started.
// Cancellation is not completion: nothing here returns while a watcher runs.
func (c *Client) Close() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		if c.reason == "" {
			c.reason = proto.ComputerClosedReasonClosed
		}
		c.mu.Unlock()
		close(c.stopped)
	})
	if c.unsubscribe != nil {
		c.unsubscribe()
	}
	c.conn.close()
	c.watchers.Wait()
}

// ---------------------------------------------------------------------------
// page operations
// ---------------------------------------------------------------------------

// Screenshot captures the viewport as a PNG, clipped to the declared viewport
// so the bytes and the coordinate system agree.
func (c *Client) Screenshot(ctx context.Context) (proto.ComputerScreenshotRes, error) {
	call, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	defer cancel()
	var out struct {
		Data string `json:"data"`
	}
	err := c.conn.call(call, c.page, "Page.captureScreenshot", map[string]any{
		"format": "png",
		"clip": map[string]any{
			"x": 0, "y": 0,
			"width": c.opts.Viewport.Width, "height": c.opts.Viewport.Height,
			"scale": 1,
		},
		"captureBeyondViewport": false,
	}, &out)
	if err != nil {
		return proto.ComputerScreenshotRes{}, err
	}
	png, derr := base64.StdEncoding.DecodeString(out.Data)
	if derr != nil {
		return proto.ComputerScreenshotRes{}, proto.Err(proto.CodeInternal, "decode screenshot: %v", derr)
	}
	if len(png) > proto.ComputerMaxScreenshotBytes {
		return proto.ComputerScreenshotRes{}, proto.Err(proto.CodeResourceExhausted,
			"screenshot is %d bytes, over the %d byte cap", len(png), proto.ComputerMaxScreenshotBytes)
	}
	return proto.ComputerScreenshotRes{
		PNG: png, Width: c.opts.Viewport.Width, Height: c.opts.Viewport.Height,
	}, nil
}

// Navigate loads url and waits for the page's load event or the load timeout.
// A timeout is a reported status, not an error: the page may still be useful.
func (c *Client) Navigate(ctx context.Context, target string) (proto.ComputerNavigateRes, error) {
	loaded := make(chan struct{})
	var once sync.Once
	remove := c.conn.onEvent(func(m message) {
		if m.SessionID == c.page && m.Method == "Page.loadEventFired" {
			once.Do(func() { close(loaded) })
		}
	})
	defer remove()

	call, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	// Page.navigate also answers with frameId and loaderId; errorText is the
	// only part that changes what the caller must be told.
	var nav struct {
		ErrorText string `json:"errorText"`
	}
	err := c.conn.call(call, c.page, "Page.navigate", map[string]any{"url": target}, &nav)
	cancel()
	if err != nil {
		return proto.ComputerNavigateRes{}, err
	}
	if nav.ErrorText != "" {
		// The broker is CONNECT-only and cannot see a URL, so an egress refusal
		// surfaces here as the browser's own network error.
		return proto.ComputerNavigateRes{}, proto.ErrReason(proto.CodeDenied,
			proto.ReasonNavigationDenied, "navigate %s: %s", target, nav.ErrorText)
	}
	status := proto.ComputerNavigateLoaded
	select {
	case <-loaded:
	case <-c.conn.closed:
		return proto.ComputerNavigateRes{}, c.Err()
	case <-ctx.Done():
		return proto.ComputerNavigateRes{}, ctx.Err()
	case <-time.After(c.opts.LoadTimeout):
		status = proto.ComputerNavigateTimeout
	}
	res := proto.ComputerNavigateRes{URL: target, Status: status}
	if raw, err := c.Eval(ctx, "document.title"); err == nil {
		var title string
		if json.Unmarshal(raw, &title) == nil {
			res.Title = title
		}
	}
	if raw, err := c.Eval(ctx, "document.location.href"); err == nil {
		var href string
		if json.Unmarshal(raw, &href) == nil && href != "" {
			res.URL = href
		}
	}
	return res, nil
}

// Eval runs an expression in the page and returns its JSON value.
func (c *Client) Eval(ctx context.Context, expression string) (json.RawMessage, error) {
	call, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	defer cancel()
	var out struct {
		Result struct {
			Value json.RawMessage `json:"value"`
		} `json:"result"`
		ExceptionDetails *struct {
			Text string `json:"text"`
		} `json:"exceptionDetails"`
	}
	if err := c.conn.call(call, c.page, "Runtime.evaluate", map[string]any{
		"expression": expression, "returnByValue": true, "awaitPromise": true,
	}, &out); err != nil {
		return nil, err
	}
	if out.ExceptionDetails != nil {
		return nil, proto.Err(proto.CodeBadRequest, "evaluate: %s", out.ExceptionDetails.Text)
	}
	return out.Result.Value, nil
}

// ---------------------------------------------------------------------------
// downloads
// ---------------------------------------------------------------------------

func (c *Client) handleEvent(m message) {
	switch m.Method {
	case "Browser.downloadWillBegin", "Page.downloadWillBegin":
		var p struct {
			GUID              string `json:"guid"`
			URL               string `json:"url"`
			SuggestedFilename string `json:"suggestedFilename"`
		}
		if json.Unmarshal(m.Params, &p) != nil || p.GUID == "" {
			return
		}
		c.mu.Lock()
		if _, seen := c.downloads[p.GUID]; !seen {
			c.order = append(c.order, p.GUID)
		}
		c.downloads[p.GUID] = &Download{
			GUID: p.GUID, URL: p.URL,
			Filename: path.Base(strings.ReplaceAll(p.SuggestedFilename, `\`, "/")),
			State:    proto.ComputerDownloadInProgress,
		}
		c.mu.Unlock()
	case "Browser.downloadProgress", "Page.downloadProgress":
		var p struct {
			GUID          string  `json:"guid"`
			State         string  `json:"state"`
			ReceivedBytes float64 `json:"receivedBytes"`
			TotalBytes    float64 `json:"totalBytes"`
		}
		if json.Unmarshal(m.Params, &p) != nil || p.GUID == "" {
			return
		}
		c.mu.Lock()
		d := c.downloads[p.GUID]
		if d == nil {
			d = &Download{GUID: p.GUID, State: proto.ComputerDownloadInProgress}
			c.downloads[p.GUID] = d
			c.order = append(c.order, p.GUID)
		}
		if p.ReceivedBytes > 0 {
			d.Bytes = int64(p.ReceivedBytes)
		}
		switch p.State {
		case "completed":
			d.State = proto.ComputerDownloadCompleted
		case "canceled":
			d.State = proto.ComputerDownloadCanceled
		default:
			d.State = proto.ComputerDownloadInProgress
		}
		finished := d.State != proto.ComputerDownloadInProgress
		snapshot := *d
		c.mu.Unlock()
		if finished && c.opts.OnDownload != nil {
			c.opts.OnDownload(snapshot)
		}
	}
}

// Downloads lists what the browser has fetched, oldest first.
func (c *Client) Downloads() []Download {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Download, 0, len(c.order))
	seen := map[string]bool{}
	for _, guid := range c.order {
		if seen[guid] {
			continue
		}
		seen[guid] = true
		if d := c.downloads[guid]; d != nil {
			out = append(out, *d)
		}
	}
	return out
}
