// Package fakecdp is a scriptable Chrome DevTools Protocol endpoint for tests.
//
// It answers /json/version and one DevTools WebSocket, records every method it
// was asked for, and lets a test override any method or emit any event. It is
// not a browser: it renders nothing and understands no page semantics. It
// exists so the node's CDP client, its input mapping and its download-to-
// artifact path can be proven without a Chromium binary on the machine.
package fakecdp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/coder/websocket"
)

// Call is one CDP method the fake received.
type Call struct {
	Method    string
	SessionID string
	Params    json.RawMessage
}

// Param decodes one field of the recorded params as JSON.
func (c Call) Param(name string) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(c.Params, &m) != nil {
		return nil
	}
	return m[name]
}

// PNG is a 1x1 opaque PNG. Tests assert on the bytes, not the picture.
var PNG = func() []byte {
	const b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		panic(err)
	}
	return raw
}()

// Handler answers one CDP method. Returning a nil result sends `{}`.
type Handler func(call Call) (any, error)

// Server is a fake DevTools endpoint on loopback.
type Server struct {
	http *httptest.Server

	mu       sync.Mutex
	calls    []Call
	handlers map[string]Handler
	sockets  map[*websocket.Conn]struct{}
	page     string // the CDP session id handed out by Target.attachToTarget
	closed   bool
}

// New starts a fake DevTools endpoint. Close it when the test ends.
func New() *Server {
	s := &Server{
		handlers: map[string]Handler{},
		sockets:  map[*websocket.Conn]struct{}{},
		page:     "SESSION-1",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/json/version", s.serveVersion)
	mux.HandleFunc("/devtools/browser/fake", s.serveSocket)
	s.http = httptest.NewServer(mux)
	return s
}

// Addr is the host:port the fake listens on.
func (s *Server) Addr() string { return s.http.Listener.Addr().String() }

// Port is the TCP port the fake listens on.
func (s *Server) Port() int { return s.http.Listener.Addr().(*net.TCPAddr).Port }

// PageSession is the CDP session id the fake attaches page calls to.
func (s *Server) PageSession() string { return s.page }

// Handle overrides one CDP method.
func (s *Server) Handle(method string, h Handler) {
	s.mu.Lock()
	s.handlers[method] = h
	s.mu.Unlock()
}

// Calls returns every method received so far, in order.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// Methods returns just the method names received so far, in order.
func (s *Server) Methods() []string {
	out := []string{}
	for _, c := range s.Calls() {
		out = append(out, c.Method)
	}
	return out
}

// Called reports whether method was received at least once, and the first call.
func (s *Server) Called(method string) (Call, bool) {
	for _, c := range s.Calls() {
		if c.Method == method {
			return c, true
		}
	}
	return Call{}, false
}

// Emit sends an unsolicited CDP event to every connected client.
func (s *Server) Emit(sessionID, method string, params any) {
	raw, err := json.Marshal(params)
	if err != nil {
		return
	}
	s.broadcast(map[string]any{"method": method, "params": json.RawMessage(raw), "sessionId": sessionID})
}

func (s *Server) broadcast(payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	sockets := make([]*websocket.Conn, 0, len(s.sockets))
	for c := range s.sockets {
		sockets = append(sockets, c)
	}
	s.mu.Unlock()
	for _, c := range sockets {
		_ = c.Write(context.Background(), websocket.MessageText, body)
	}
}

// Crash drops every DevTools socket without a close handshake, the way a
// browser process dying does.
func (s *Server) Crash() {
	s.mu.Lock()
	sockets := make([]*websocket.Conn, 0, len(s.sockets))
	for c := range s.sockets {
		sockets = append(sockets, c)
	}
	s.sockets = map[*websocket.Conn]struct{}{}
	s.mu.Unlock()
	for _, c := range sockets {
		_ = c.CloseNow()
	}
}

// Close stops the fake and drops every socket.
func (s *Server) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.Crash()
	s.http.Close()
}

func (s *Server) serveVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"Browser":              "HeadlessChrome/fake",
		"Protocol-Version":     "1.3",
		"webSocketDebuggerUrl": fmt.Sprintf("ws://%s/devtools/browser/fake", r.Host),
	})
}

func (s *Server) serveSocket(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	c.SetReadLimit(16 << 20)
	s.mu.Lock()
	s.sockets[c] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sockets, c)
		s.mu.Unlock()
		_ = c.CloseNow()
	}()
	for {
		typ, data, err := c.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var req struct {
			ID        int64           `json:"id"`
			Method    string          `json:"method"`
			Params    json.RawMessage `json:"params"`
			SessionID string          `json:"sessionId"`
		}
		if json.Unmarshal(data, &req) != nil {
			continue
		}
		call := Call{Method: req.Method, SessionID: req.SessionID, Params: req.Params}
		s.mu.Lock()
		s.calls = append(s.calls, call)
		handler := s.handlers[req.Method]
		s.mu.Unlock()

		var result any
		var handlerErr error
		if handler != nil {
			result, handlerErr = handler(call)
		} else {
			result, handlerErr = s.defaultHandler(call)
		}
		response := map[string]any{"id": req.ID}
		if req.SessionID != "" {
			response["sessionId"] = req.SessionID
		}
		if handlerErr != nil {
			response["error"] = map[string]any{"code": -32000, "message": handlerErr.Error()}
		} else if result == nil {
			response["result"] = map[string]any{}
		} else {
			response["result"] = result
		}
		body, err := json.Marshal(response)
		if err != nil {
			return
		}
		if err := c.Write(r.Context(), websocket.MessageText, body); err != nil {
			return
		}
	}
}

func (s *Server) defaultHandler(call Call) (any, error) {
	switch call.Method {
	case "Target.getTargets":
		return map[string]any{"targetInfos": []any{}}, nil
	case "Target.createTarget":
		return map[string]any{"targetId": "TARGET-1"}, nil
	case "Target.attachToTarget":
		return map[string]any{"sessionId": s.page}, nil
	case "Page.captureScreenshot":
		return map[string]any{"data": base64.StdEncoding.EncodeToString(PNG)}, nil
	case "Page.navigate":
		// A real browser fires the load event after responding. Do the same,
		// from a goroutine, so the client's waiter sees the ordering it must
		// tolerate in production.
		go s.Emit(s.page, "Page.loadEventFired", map[string]any{"timestamp": 1})
		return map[string]any{"frameId": "FRAME-1", "loaderId": "LOADER-1"}, nil
	case "Runtime.evaluate":
		return map[string]any{"result": map[string]any{"type": "string", "value": "fake"}}, nil
	}
	return nil, nil
}
