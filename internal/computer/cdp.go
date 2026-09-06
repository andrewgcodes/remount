// Package computer speaks the Chrome DevTools Protocol to a browser running
// inside a workspace.
//
// It never opens a socket of its own. The caller supplies a dial function that
// yields a byte-accurate path to one workspace TCP port — in the node that is
// the same resolved path `port.open` uses, so every backend that can forward a
// workspace port can host a computer. See ADR 0088.
package computer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/proto"
)

// Dialer opens one connection to the browser's DevTools port. Each call must
// return a fresh connection; the caller closes it.
type Dialer func(ctx context.Context) (net.Conn, error)

// readLimit bounds one CDP message. Screenshots arrive base64-encoded inside a
// JSON response, so the limit must exceed the screenshot cap with room for the
// encoding overhead and the envelope; anything larger is a protocol error, not
// a large picture.
const readLimit = 4*proto.ComputerMaxScreenshotBytes/3 + (1 << 20)

// message is one CDP frame in either direction.
type message struct {
	ID        int64           `json:"id,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *cdpError       `json:"error,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
}

type cdpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func (e *cdpError) Error() string {
	if e.Data == "" {
		return fmt.Sprintf("cdp error %d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("cdp error %d: %s (%s)", e.Code, e.Message, e.Data)
}

// conn is a CDP JSON-RPC conversation over one WebSocket.
type conn struct {
	ws *websocket.Conn

	writeMu sync.Mutex

	mu       sync.Mutex
	nextID   int64
	pending  map[int64]chan message
	handlers map[int]func(message)
	nextHnd  int
	err      error

	closed chan struct{} // closed when the read loop has stopped
	once   sync.Once
}

func dialCDP(ctx context.Context, endpoint string, path string, dial Dialer) (*conn, error) {
	client := &http.Client{Transport: &http.Transport{
		DialContext:       func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
		DisableKeepAlives: true,
	}}
	target := (&url.URL{Scheme: "ws", Host: endpoint, Path: path}).String()
	ws, _, err := websocket.Dial(ctx, target, &websocket.DialOptions{
		HTTPClient:      client,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(readLimit)
	c := &conn{
		ws:       ws,
		pending:  map[int64]chan message{},
		handlers: map[int]func(message){},
		closed:   make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func (c *conn) readLoop() {
	defer close(c.closed)
	for {
		typ, data, err := c.ws.Read(context.Background())
		if err != nil {
			c.fail(err)
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var m message
		if err := json.Unmarshal(data, &m); err != nil {
			c.fail(fmt.Errorf("decode cdp message: %w", err))
			return
		}
		if m.ID != 0 && m.Method == "" {
			c.mu.Lock()
			ch := c.pending[m.ID]
			delete(c.pending, m.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- m
			}
			continue
		}
		c.mu.Lock()
		fns := make([]func(message), 0, len(c.handlers))
		for _, fn := range c.handlers {
			fns = append(fns, fn)
		}
		c.mu.Unlock()
		for _, fn := range fns {
			fn(m)
		}
	}
}

// fail records the terminal error and wakes every waiter. The browser going
// away is the ordinary case, so it is reported as closed, not internal.
func (c *conn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	pending := c.pending
	c.pending = map[int64]chan message{}
	terminal := c.err
	c.mu.Unlock()
	for id, ch := range pending {
		ch <- message{ID: id, Error: &cdpError{Code: -1, Message: terminal.Error()}}
	}
}

// onEvent registers an event handler and returns a function that removes it.
// Handlers run on the read loop, so they must not call back into the conn.
func (c *conn) onEvent(fn func(message)) func() {
	c.mu.Lock()
	id := c.nextHnd
	c.nextHnd++
	c.handlers[id] = fn
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.handlers, id)
		c.mu.Unlock()
	}
}

func (c *conn) call(ctx context.Context, sessionID, method string, params any, out any) error {
	var raw json.RawMessage
	if params != nil {
		encoded, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = encoded
	}
	c.mu.Lock()
	if c.err != nil {
		terminal := c.err
		c.mu.Unlock()
		return closedErr(terminal)
	}
	c.nextID++
	id := c.nextID
	ch := make(chan message, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	frame, err := json.Marshal(message{ID: id, Method: method, Params: raw, SessionID: sessionID})
	if err != nil {
		c.forget(id)
		return err
	}
	c.writeMu.Lock()
	writeErr := c.ws.Write(ctx, websocket.MessageText, frame)
	c.writeMu.Unlock()
	if writeErr != nil {
		c.forget(id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return closedErr(writeErr)
	}
	select {
	case <-ctx.Done():
		c.forget(id)
		return ctx.Err()
	case m := <-ch:
		if m.Error != nil {
			if m.Error.Code == -1 {
				return closedErr(errors.New(m.Error.Message))
			}
			return proto.Err(proto.CodeBadRequest, "%s: %s", method, m.Error.Message)
		}
		if out == nil {
			return nil
		}
		if len(m.Result) == 0 {
			return nil
		}
		return json.Unmarshal(m.Result, out)
	}
}

// send writes one CDP call and does not wait for its reply.
//
// It exists for the Fetch responses that unblock a paused request. Those are
// issued from inside the read loop's own event handler, where waiting for a
// reply would deadlock: the reply can only arrive on the loop that is waiting.
// An unmatched reply is dropped by readLoop, which is exactly what should
// happen to one nobody is waiting for. See ADR 0095.
func (c *conn) send(ctx context.Context, sessionID, method string, params any) error {
	var raw json.RawMessage
	if params != nil {
		encoded, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = encoded
	}
	c.mu.Lock()
	if c.err != nil {
		terminal := c.err
		c.mu.Unlock()
		return closedErr(terminal)
	}
	c.nextID++
	id := c.nextID
	c.mu.Unlock()
	frame, err := json.Marshal(message{ID: id, Method: method, Params: raw, SessionID: sessionID})
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	writeErr := c.ws.Write(ctx, websocket.MessageText, frame)
	c.writeMu.Unlock()
	if writeErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return closedErr(writeErr)
	}
	return nil
}

func (c *conn) forget(id int64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// close tears the socket down and joins the read loop. Cancellation is not
// completion: this returns only once the reader has stopped.
func (c *conn) close() {
	c.once.Do(func() {
		_ = c.ws.Close(websocket.StatusNormalClosure, "")
	})
	<-c.closed
}

func (c *conn) terminalErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// closedErr maps a dead conversation onto the stable code the protocol
// promises for a browser that went away.
func closedErr(cause error) error {
	return proto.ErrReason(proto.CodeClosed, proto.ReasonBrowserCrashed,
		"devtools connection ended: %v", cause)
}

// probe fetches one JSON document from the DevTools HTTP endpoint.
func probe(ctx context.Context, endpoint, path string, dial Dialer, out any) error {
	client := &http.Client{Transport: &http.Transport{
		DialContext:       func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
		DisableKeepAlives: true,
	}}
	target := (&url.URL{Scheme: "http", Host: endpoint, Path: path}).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return proto.Err(proto.CodeUnreachable, "devtools %s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// versionInfo is the subset of /json/version the node uses.
type versionInfo struct {
	Browser              string `json:"Browser"`
	ProtocolVersion      string `json:"Protocol-Version"`
	WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
}

// waitReady polls /json/version until the browser answers or the deadline
// passes. A browser that never answers is unavailable, never healthy.
func waitReady(ctx context.Context, endpoint string, dial Dialer, timeout time.Duration) (versionInfo, error) {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		var v versionInfo
		err := probe(attempt, endpoint, "/json/version", dial, &v)
		cancel()
		if err == nil && v.WebSocketDebuggerURL != "" {
			return v, nil
		}
		if err == nil {
			err = proto.Err(proto.CodeUnreachable, "devtools /json/version has no debugger url")
		}
		last = err
		if ctx.Err() != nil {
			return versionInfo{}, ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return versionInfo{}, proto.ErrReason(proto.CodeTimeout, proto.ReasonDisplayUnavailable,
				"browser devtools port %s did not become ready: %v", endpoint, last)
		}
		select {
		case <-ctx.Done():
			return versionInfo{}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
