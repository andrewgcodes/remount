package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// The terminal WebSocket is the xterm.js endpoint. Binary messages are raw
// bytes in both directions: output from the session to the browser, input
// from the browser to the session. Text messages are JSON control frames:
//
//	browser -> server: {"type":"resize","rows":R,"cols":C}
//	                   {"type":"signal","signal":"SIGINT"}
//	                   {"type":"input","data":"<base64>"}   (an alternative to binary)
//	                   {"type":"eof"}                        (close stdin)
//	server -> browser: {"type":"open","session":S,"next":N} once, first
//	                   {"type":"gap","from":F,"to":T}        output between F and T was evicted
//	                   {"type":"exit","code":C,"signal":..,"error":..,"reason":..} last
//
// Without ?session= a new pty runs the requested program (default /bin/sh)
// in the workspace; with ?session=ID&from=N the socket attaches to an
// existing session and replays retained output from N, which is how a
// reconnecting UI resumes, and how a pty-mode agent's transcript is watched
// live.

type terminalControl struct {
	Type   string `json:"type"`
	Rows   uint16 `json:"rows,omitempty"`
	Cols   uint16 `json:"cols,omitempty"`
	Signal string `json:"signal,omitempty"`
	Data   string `json:"data,omitempty"`
}

type terminalEvent struct {
	Type    string `json:"type"`
	Session string `json:"session,omitempty"`
	Next    uint64 `json:"next,omitempty"`
	From    uint64 `json:"from,omitempty"`
	To      uint64 `json:"to,omitempty"`
	Code    int    `json:"code,omitempty"`
	Signal  string `json:"signal,omitempty"`
	Error   string `json:"error,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// terminalWriteTimeout bounds one WebSocket write; a browser that stops
// reading is disconnected rather than allowed to hold the session's output.
const terminalWriteTimeout = 30 * time.Second

func (s *Server) handleTerminal(w http.ResponseWriter, r *http.Request) {
	if !isUpgrade(r) {
		badRequest(w, "the terminal is a WebSocket endpoint")
		return
	}
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	q := r.URL.Query()
	a, err := cl.AgentMaterialized(r.Context(), r.PathValue("id"), false, "")
	if err != nil {
		writeError(w, err)
		return
	}
	var (
		sess *client.Session
		kill bool
	)
	if sid := q.Get("session"); sid != "" {
		var from uint64
		if v := q.Get("from"); v != "" {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				badRequest(w, "from must be a non-negative integer")
				return
			}
			from = n
		}
		sess, err = cl.Attach(r.Context(), a.WS, sid, from)
	} else {
		program := q["program"]
		if len(program) == 0 {
			program = []string{"/bin/sh"}
		}
		rows, cols := dim(q.Get("rows"), 24), dim(q.Get("cols"), 80)
		sess, err = cl.Exec(r.Context(), proto.SOpenReq{WS: a.WS, Kind: proto.SessionPTY, Program: program, Rows: rows, Cols: cols, Cwd: q.Get("cwd")})
		// A shell the browser opened dies with the socket; an attached
		// session belongs to whoever opened it and is only detached.
		kill = true
	}
	if err != nil {
		writeError(w, err)
		return
	}
	c, err := acceptWS(w, r)
	if err != nil {
		_ = sess.Close(context.WithoutCancel(r.Context()), kill)
		return
	}
	metrics.HTTPTerminalAttaches.Inc()
	s.serveTerminal(r.Context(), c, sess, kill)
}

func dim(v string, def uint16) uint16 {
	n, err := strconv.ParseUint(v, 10, 16)
	if err != nil || n == 0 {
		return def
	}
	return uint16(n)
}

func (s *Server) serveTerminal(parent context.Context, c *websocket.Conn, sess *client.Session, kill bool) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer func() {
		// Session teardown must not depend on the request that is ending.
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
		defer ccancel()
		_ = sess.Close(cctx, kill)
	}()
	send := func(typ websocket.MessageType, b []byte) error {
		wctx, wcancel := context.WithTimeout(ctx, terminalWriteTimeout)
		defer wcancel()
		return c.Write(wctx, typ, b)
	}
	sendEvent := func(ev terminalEvent) error {
		b, _ := json.Marshal(ev)
		return send(websocket.MessageText, b)
	}
	if err := sendEvent(terminalEvent{Type: "open", Session: sess.ID, Next: sess.Next()}); err != nil {
		_ = c.CloseNow()
		return
	}
	// Input: browser -> session.
	go func() {
		defer cancel()
		for {
			typ, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			switch typ {
			case websocket.MessageBinary:
				if err := sess.Input(ctx, b, false); err != nil {
					return
				}
			case websocket.MessageText:
				var ctl terminalControl
				if json.Unmarshal(b, &ctl) != nil {
					continue
				}
				var err error
				switch ctl.Type {
				case "resize":
					if ctl.Rows > 0 && ctl.Cols > 0 {
						err = sess.Resize(ctx, ctl.Rows, ctl.Cols)
					}
				case "signal":
					if ctl.Signal != "" {
						err = sess.Signal(ctx, ctl.Signal)
					}
				case "input":
					data, derr := base64.StdEncoding.DecodeString(ctl.Data)
					if derr == nil {
						err = sess.Input(ctx, data, false)
					}
				case "eof":
					err = sess.Input(ctx, nil, true)
				}
				if err != nil && ctx.Err() == nil {
					s.opts.Logger.Debug("terminal control", "session", sess.ID, "type", ctl.Type, "err", err)
				}
			}
		}
	}()
	// Output: session -> browser.
	for {
		select {
		case <-ctx.Done():
			_ = c.Close(websocket.StatusGoingAway, "closed")
			return
		case ch, ok := <-sess.Chunks():
			if !ok {
				if err := sess.Err(); err != nil {
					_ = c.Close(websocket.StatusInternalError, truncateReason(err.Error()))
				} else {
					_ = c.Close(websocket.StatusNormalClosure, "session ended")
				}
				return
			}
			switch ch.Stream {
			case proto.StreamStdout, proto.StreamStderr:
				if err := send(websocket.MessageBinary, ch.Data); err != nil {
					return
				}
			case proto.StreamGap:
				var gap proto.Gap
				if proto.Unmarshal(ch.Data, &gap) == nil {
					if err := sendEvent(terminalEvent{Type: "gap", From: gap.From, To: gap.To}); err != nil {
						return
					}
				}
			case proto.StreamExit:
				var exit proto.ExitInfo
				_ = proto.Unmarshal(ch.Data, &exit)
				_ = sendEvent(terminalEvent{Type: "exit", Code: exit.Code, Signal: exit.Signal, Error: exit.Error, Reason: exit.Reason})
				_ = c.Close(websocket.StatusNormalClosure, "exit")
				return
			}
		}
	}
}
