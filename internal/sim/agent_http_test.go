package sim

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// The HTTP agent API is the surface a UI or an external client uses. These
// tests drive it exactly as a browser or curl would: bearer or cookie
// credentials, JSON bodies, SSE and WebSocket streams, and the preview
// proxy, against a real node running the fake ACP harness.

type httpAPI struct {
	t     *testing.T
	base  string
	token string
	hc    *http.Client
}

func (w *world) api(token string) *httpAPI {
	return &httpAPI{t: w.t, base: w.http.URL, token: token, hc: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

type httpResult struct {
	status int
	header http.Header
	body   []byte
}

func (h *httpAPI) do(ctx context.Context, method, path string, body any, hdr map[string]string) httpResult {
	h.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	case string:
		rd = strings.NewReader(b)
	default:
		js, err := json.Marshal(b)
		if err != nil {
			h.t.Fatal(err)
		}
		rd = bytes.NewReader(js)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, rd)
	if err != nil {
		h.t.Fatal(err)
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	if body != nil {
		if _, ok := body.([]byte); !ok {
			if _, isStr := body.(string); !isStr {
				req.Header.Set("Content-Type", "application/json")
			}
		}
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := h.hc.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return httpResult{status: resp.StatusCode, header: resp.Header, body: out}
}

func (h *httpAPI) json(ctx context.Context, method, path string, body any, hdr map[string]string, want int, out any) httpResult {
	h.t.Helper()
	res := h.do(ctx, method, path, body, hdr)
	if res.status != want {
		h.t.Fatalf("%s %s = %d, want %d: %s", method, path, res.status, want, res.body)
	}
	if out != nil && len(res.body) > 0 {
		if err := json.Unmarshal(res.body, out); err != nil {
			h.t.Fatalf("%s %s: %v: %s", method, path, err, res.body)
		}
	}
	return res
}

func (h *httpAPI) agent(ctx context.Context, id string) *proto.Agent {
	h.t.Helper()
	var a proto.Agent
	h.json(ctx, "GET", "/v1/agents/"+id, nil, nil, 200, &a)
	return &a
}

func (h *httpAPI) waitAgent(ctx context.Context, id, what string, pred func(*proto.Agent) bool) *proto.Agent {
	h.t.Helper()
	for {
		a := h.agent(ctx, id)
		if pred(a) {
			return a
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("waiting for %s: agent %s = %+v", what, id, a)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (h *httpAPI) wsDial(ctx context.Context, path string, withToken bool) *websocket.Conn {
	h.t.Helper()
	url := "ws" + strings.TrimPrefix(h.base, "http") + path
	opts := &websocket.DialOptions{}
	if withToken {
		opts.Subprotocols = []string{"remount.bearer." + base64.RawURLEncoding.EncodeToString([]byte(h.token))}
	}
	c, resp, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		h.t.Fatalf("dial %s: %v (status %d)", path, err, status)
	}
	return c
}

func errorCode(t *testing.T, res httpResult) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(res.body, &body); err != nil {
		t.Fatalf("error body %q: %v", res.body, err)
	}
	return body.Error.Code
}

// sseEvents reads SSE frames until pred returns true or ctx ends.
func sseEvents(t *testing.T, ctx context.Context, base, path, token string, pred func(event, data string) bool) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "GET", base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("sse %s: %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	var event, data string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event != "" && pred(event, data) {
				return
			}
			event, data = "", ""
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data += strings.TrimPrefix(line, "data: ")
		}
	}
	if ctx.Err() == nil {
		t.Fatalf("sse %s ended: %v", path, sc.Err())
	}
}

// wokenReasons lists the by field of every agent.woken event, in order.
func wokenReasons(t *testing.T, evs []proto.Event) []string {
	t.Helper()
	var out []string
	for _, e := range evs {
		if e.Type != proto.EvAgentWoken {
			continue
		}
		var p struct {
			By string `cbor:"by"`
		}
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("agent.woken payload: %v", err)
		}
		out = append(out, p.By)
	}
	return out
}

func TestAgentHTTPAuthAndLifecycle(t *testing.T) {
	w := newWorldWith(t, func(o *server.Options) {
		o.LeaseSec = 30
		o.PublicURL = "https://remount.example"
		o.CORSOrigins = []string{"https://ui.example"}
	})
	w.node("n1", nil)
	ctx := ctxT(t, 120*time.Second)
	api := w.api("tok")

	// No credential, wrong credential, and a cookie on a mutation are refused
	// before anything is looked up.
	for _, bad := range []*httpAPI{w.api(""), w.api("nope")} {
		if res := bad.do(ctx, "GET", "/v1/agents", nil, nil); res.status != 401 {
			t.Fatalf("token %q: GET /v1/agents = %d %s", bad.token, res.status, res.body)
		}
	}
	// Preflight succeeds for a listed origin and names the allowed headers.
	pre := api.do(ctx, "OPTIONS", "/v1/agents", nil, map[string]string{
		"Origin": "https://ui.example", "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization,idempotency-key",
	})
	if pre.status != 204 || pre.header.Get("Access-Control-Allow-Origin") != "https://ui.example" ||
		!strings.Contains(pre.header.Get("Access-Control-Allow-Headers"), "Idempotency-Key") || pre.header.Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("preflight = %d %v", pre.status, pre.header)
	}
	if other := api.do(ctx, "OPTIONS", "/v1/agents", nil, map[string]string{"Origin": "https://evil.example", "Access-Control-Request-Method": "POST"}); other.header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("unlisted origin allowed: %v", other.header)
	}

	create := map[string]any{
		"name":      "http-e2e",
		"workspace": map[string]any{"name": "http-ws", "labels": map[string]string{"team": "http"}, "env": map[string]string{"LEAK_TOKEN": "tok-http-canary-0123456789"}},
		"spec":      map[string]any{"recipe": "custom", "task": "say hello", "acp_command": fakeACPCommand(t, "echo")},
		"policy":    map[string]any{"approve": proto.ApproveOnRequest},
	}
	var a, again proto.Agent
	res := api.json(ctx, "POST", "/v1/agents", create, map[string]string{"Idempotency-Key": "create-1"}, 201, &a)
	if a.ID == "" || a.WS == "" || res.header.Get("Location") != "/v1/agents/"+a.ID || a.URL != "https://remount.example/a/"+a.ID {
		t.Fatalf("created = %+v location=%q", a, res.header.Get("Location"))
	}
	api.json(ctx, "POST", "/v1/agents", create, map[string]string{"Idempotency-Key": "create-1"}, 201, &again)
	if again.ID != a.ID {
		t.Fatalf("idempotent replay created %s, want %s", again.ID, a.ID)
	}
	if bad := api.do(ctx, "POST", "/v1/agents", `{"name":"x","bogus":1}`, map[string]string{"Content-Type": "application/json"}); bad.status != 400 {
		t.Fatalf("unknown field accepted: %d %s", bad.status, bad.body)
	}
	var list proto.AgentListRes
	api.json(ctx, "GET", "/v1/agents", nil, nil, 200, &list)
	if len(list.Agents) != 1 || list.Agents[0].ID != a.ID {
		t.Fatalf("list = %+v", list)
	}
	// The stable URL carries no capability and, without an operator UI,
	// serves the resource itself.
	var linked proto.Agent
	api.json(ctx, "GET", "/a/"+a.ID, nil, nil, 200, &linked)
	if linked.ID != a.ID {
		t.Fatalf("/a/%s = %+v", a.ID, linked)
	}
	if res := w.api("").do(ctx, "GET", "/a/"+a.ID, nil, nil); res.status != 401 {
		t.Fatalf("/a/ without credential = %d", res.status)
	}

	idle := api.waitAgent(ctx, a.ID, "idle after the task", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && (a.Status == proto.AgentIdle || a.Status == proto.AgentWaitingInput)
	})
	if idle.TranscriptNext == 0 {
		t.Fatalf("no transcript mirrored: %+v", idle)
	}

	// The durable transcript pages by cursor and renders ACP frames as JSON.
	var page struct {
		Records []struct {
			Index  uint64          `json:"index"`
			Stream string          `json:"stream"`
			Frame  json.RawMessage `json:"frame"`
		} `json:"records"`
		Next uint64 `json:"next"`
	}
	api.json(ctx, "GET", "/v1/agents/"+a.ID+"/transcript?from=0", nil, nil, 200, &page)
	if len(page.Records) == 0 || page.Next != idle.TranscriptNext {
		t.Fatalf("transcript page = %+v", page)
	}
	joined, _ := json.Marshal(page)
	if !bytes.Contains(joined, []byte("say hello")) || bytes.Contains(joined, []byte("tok-http-canary")) {
		t.Fatalf("transcript page: %s", joined)
	}
	streams := map[string]bool{}
	for _, r := range page.Records {
		streams[r.Stream] = true
	}
	if !streams["acp_in"] || !streams["acp_out"] {
		t.Fatalf("streams = %v", streams)
	}
	if res := api.do(ctx, "GET", fmt.Sprintf("/v1/agents/%s/transcript?from=%d&limit=5", a.ID, idle.TranscriptNext-2), nil, nil); res.status != 200 || !bytes.Contains(res.body, []byte(`"index":`+fmt.Sprint(idle.TranscriptNext-2))) {
		t.Fatalf("partial page: %d %s", res.status, res.body)
	}
	// A reconnecting EventSource resends its original ?from together with
	// Last-Event-ID; the header is the newer cursor and wins, or every
	// reconnect replays from the start.
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/transcript?from=0&limit=1", nil, map[string]string{"Last-Event-ID": fmt.Sprint(idle.TranscriptNext - 1)}); res.status != 200 ||
		!bytes.Contains(res.body, []byte(`"index":`+fmt.Sprint(idle.TranscriptNext-1))) || bytes.Contains(res.body, []byte(`"index":0,`)) {
		t.Fatalf("Last-Event-ID did not win over ?from: %d %s", res.status, res.body)
	}

	// SSE resumes from Last-Event-ID and pushes new records as a follow-up
	// turn happens; the second turn is sent while the stream is open.
	sseCtx, sseCancel := context.WithCancel(ctx)
	defer sseCancel()
	turnSeen := make(chan struct{})
	go func() {
		api.json(ctx, "POST", "/v1/agents/"+a.ID+"/messages", map[string]any{"text": "and again"}, map[string]string{"Idempotency-Key": "msg-1"}, 202, nil)
	}()
	sseEvents(t, sseCtx, w.http.URL, "/v1/agents/"+a.ID+"/transcript", "tok", func(event, data string) bool {
		if event == "record" && strings.Contains(data, "and again") {
			close(turnSeen)
			return true
		}
		return false
	})
	<-turnSeen
	two := api.waitAgent(ctx, a.ID, "second turn", func(a *proto.Agent) bool { return len(a.Runs) == 1 && a.Runs[0].Turns == 2 })

	// The WebSocket form carries the same records.
	wsCtx, wsCancel := context.WithTimeout(ctx, 20*time.Second)
	conn := api.wsDial(wsCtx, "/v1/agents/"+a.ID+"/transcript?from=0", true)
	_, first, err := conn.Read(wsCtx)
	if err != nil || !bytes.Contains(first, []byte(`"type":"record"`)) {
		t.Fatalf("ws transcript first message: %v %s", err, first)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	wsCancel()
	// A WebSocket without a credential is refused at the HTTP layer.
	if _, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(w.http.URL, "http")+"/v1/agents/"+a.ID+"/transcript", nil); err == nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("unauthenticated ws accepted: err=%v", err)
	}

	// Filesystem access is jailed to the workspace and round-trips bytes.
	api.json(ctx, "PUT", "/v1/agents/"+a.ID+"/fs/notes/hello.txt", "hello over http\n", map[string]string{"Content-Type": "application/octet-stream"}, 204, nil)
	if got := api.json(ctx, "GET", "/v1/agents/"+a.ID+"/fs/notes/hello.txt", nil, nil, 200, nil); string(got.body) != "hello over http\n" {
		t.Fatalf("fs read = %q", got.body)
	}
	var dir struct {
		Entries []struct {
			Name string `json:"name"`
		} `json:"entries"`
	}
	api.json(ctx, "GET", "/v1/agents/"+a.ID+"/fs/notes/", nil, nil, 200, &dir)
	if len(dir.Entries) != 1 || dir.Entries[0].Name != "hello.txt" {
		t.Fatalf("dir = %+v", dir)
	}
	if esc := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/fs/../../etc/passwd", nil, nil); esc.status == 200 {
		t.Fatalf("jail escape served: %s", esc.body)
	}
	if esc := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/fs/notes/%2e%2e/%2e%2e/%2e%2e/etc/passwd", nil, nil); esc.status == 200 {
		t.Fatalf("encoded jail escape served: %s", esc.body)
	}
	api.json(ctx, "DELETE", "/v1/agents/"+a.ID+"/fs/notes/hello.txt", nil, nil, 204, nil)
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/fs/notes/hello.txt", nil, nil); res.status != 404 {
		t.Fatalf("deleted file = %d", res.status)
	}

	if runtime.GOOS == "windows" {
		t.Log("unavailable: the session PTY backend uses Unix PTYs; ConPTY is not implemented")
	} else {
		// Terminal: a PTY over WebSocket with binary I/O and JSON control.
		termCtx, termCancel := context.WithTimeout(ctx, 30*time.Second)
		term := api.wsDial(termCtx, "/v1/agents/"+a.ID+"/terminal?program=/bin/sh&rows=20&cols=80", true)
		typ, open, err := term.Read(termCtx)
		if err != nil || typ != websocket.MessageText || !bytes.Contains(open, []byte(`"type":"open"`)) {
			t.Fatalf("terminal open = %v %v %s", typ, err, open)
		}
		if err := term.Write(termCtx, websocket.MessageBinary, []byte("echo term-$((20+22))\n")); err != nil {
			t.Fatal(err)
		}
		var seen []byte
		for !bytes.Contains(seen, []byte("term-42")) {
			typ, msg, err := term.Read(termCtx)
			if err != nil {
				t.Fatalf("terminal read: %v (so far %q)", err, seen)
			}
			if typ == websocket.MessageBinary {
				seen = append(seen, msg...)
			}
		}
		if err := term.Write(termCtx, websocket.MessageText, []byte(`{"type":"resize","rows":40,"cols":120}`)); err != nil {
			t.Fatal(err)
		}
		if err := term.Write(termCtx, websocket.MessageBinary, []byte("exit 3\n")); err != nil {
			t.Fatal(err)
		}
		for {
			typ, msg, err := term.Read(termCtx)
			if err != nil {
				t.Fatalf("terminal did not report exit: %v", err)
			}
			if typ == websocket.MessageText && bytes.Contains(msg, []byte(`"type":"exit"`)) {
				if !bytes.Contains(msg, []byte(`"code":3`)) {
					t.Fatalf("exit = %s", msg)
				}
				break
			}
		}
		_ = term.Close(websocket.StatusNormalClosure, "")
		termCancel()
	}

	// A browser session cookie authenticates the preview proxy and the
	// stable link only: preview content is same-origin and untrusted, so a
	// cookie honoured on any route that reads or executes in a workspace
	// would let one workspace's page act on every agent.
	sess := api.json(ctx, "POST", "/v1/session", nil, nil, 204, nil)
	var cookie string
	for _, c := range (&http.Response{Header: sess.header}).Cookies() {
		if c.Name == "remount_session" {
			cookie = c.Value
			if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
				t.Fatalf("cookie flags = %+v", c)
			}
		}
	}
	if cookie == "" {
		t.Fatalf("no session cookie set: %v", sess.header)
	}
	viaCookie := w.api("")
	cookieHdr := map[string]string{"Cookie": "remount_session=" + cookie}
	// The stable link without an operator UI is the agent record itself
	// (task, inbox text, parent); a preview page must not read it with the
	// cookie either.
	for _, c := range []struct{ method, path string }{
		{"GET", "/a/" + a.ID},
		{"GET", "/v1/agents"},
		{"GET", "/v1/agents/" + a.ID},
		{"GET", "/v1/agents/" + a.ID + "/transcript"},
		{"GET", "/v1/agents/" + a.ID + "/approvals"},
		{"GET", "/v1/agents/" + a.ID + "/diff?wake=true"},
		{"GET", "/v1/agents/" + a.ID + "/fs/notes/hello.txt"},
		{"GET", "/v1/agents/" + a.ID + "/fs/"},
		{"POST", "/v1/agents/" + a.ID + "/messages"},
		{"PUT", "/v1/agents/" + a.ID + "/fs/x"},
	} {
		var body any
		if c.method != "GET" {
			body = map[string]any{"text": "x"}
		}
		if res := viaCookie.do(ctx, c.method, c.path, body, cookieHdr); res.status != 401 || errorCode(t, res) != proto.CodeUnauthorized {
			t.Fatalf("cookie-only %s %s = %d %s", c.method, c.path, res.status, res.body)
		}
	}
	// The terminal upgrade is the code-execution route; a cookie handshake
	// is refused before any program starts.
	if _, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(w.http.URL, "http")+"/v1/agents/"+a.ID+"/terminal", &websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {"remount_session=" + cookie}}}); err == nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("cookie terminal accepted: err=%v", err)
	}
	if _, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(w.http.URL, "http")+"/v1/agents/"+a.ID+"/transcript", &websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {"remount_session=" + cookie}}}); err == nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("cookie transcript ws accepted: err=%v", err)
	}

	// Preview proxy: a program listening inside the workspace answers under
	// /v1/agents/{id}/ports/{port}/ with the prefix it is mounted at and
	// without ever seeing the API credential.
	var gotAuth, gotCookie, gotPrefix, gotPath, gotProto string
	up := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotAuth, gotCookie, gotPrefix, gotPath, gotProto = r.Header.Get("Authorization"), r.Header.Get("Cookie"), r.Header.Get("X-Forwarded-Prefix"), r.URL.RequestURI(), r.Header.Get("Sec-WebSocket-Protocol")
		rw.Header().Set("X-Upstream", "yes")
		fmt.Fprint(rw, "preview body")
	}))
	defer up.Close()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(up.URL, "http://"))
	prefix := "/v1/agents/" + a.ID + "/ports/" + portStr
	if res := api.do(ctx, "GET", prefix, nil, nil); res.status != 307 || res.header.Get("Location") != prefix+"/" {
		t.Fatalf("port root = %d %v", res.status, res.header)
	}
	pv := viaCookie.do(ctx, "GET", prefix+"/app/index.html?x=1", nil, cookieHdr)
	if pv.status != 200 || string(pv.body) != "preview body" || pv.header.Get("X-Upstream") != "yes" {
		t.Fatalf("preview = %d %v %s", pv.status, pv.header, pv.body)
	}
	if gotAuth != "" || strings.Contains(gotCookie, "remount_session") || gotPrefix != prefix || gotPath != "/app/index.html?x=1" {
		t.Fatalf("upstream saw auth=%q cookie=%q prefix=%q path=%q", gotAuth, gotCookie, gotPrefix, gotPath)
	}
	if res := w.api("").do(ctx, "GET", prefix+"/", nil, nil); res.status != 401 {
		t.Fatalf("unauthenticated preview = %d", res.status)
	}
	crossSite := map[string]string{"Cookie": "remount_session=" + cookie, "Origin": "https://evil.example"}
	if res := viaCookie.do(ctx, "GET", prefix+"/app/index.html", nil, crossSite); res.status != 403 {
		t.Fatalf("cross-site cookie preview = %d %s", res.status, res.body)
	}
	sameSite := map[string]string{"Cookie": "remount_session=" + cookie, "Origin": "https://ui.example"}
	if res := viaCookie.do(ctx, "GET", prefix+"/app/index.html", nil, sameSite); res.status != 200 {
		t.Fatalf("listed-origin cookie preview = %d %s", res.status, res.body)
	}
	// A browser WebSocket through the proxy carries the credential as a
	// subprotocol; that token is stripped like the header and the program's
	// own subprotocols pass.
	bearerProto := "remount.bearer." + base64.RawURLEncoding.EncodeToString([]byte("tok"))
	pv = w.api("").do(ctx, "GET", prefix+"/ws", nil, map[string]string{"Sec-WebSocket-Protocol": bearerProto + ", chat, " + bearerProto})
	if pv.status != 200 || gotProto != "chat" || gotAuth != "" {
		t.Fatalf("subprotocol preview = %d upstream proto=%q auth=%q", pv.status, gotProto, gotAuth)
	}
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/ports/1/", nil, nil); res.status != 503 || errorCode(t, res) != proto.CodeUnreachable {
		t.Fatalf("closed port = %d %s", res.status, res.body)
	}

	// Diff on a tree without git is unreachable, not a fake empty diff.
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/diff", nil, nil); res.status != 503 {
		t.Fatalf("diff without git = %d %s", res.status, res.body)
	}

	// Sleep: reads from the mirror never wake the workspace; diff refuses
	// without wake=true and wakes with it, saying who woke it.
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/sleep", nil, map[string]string{"Idempotency-Key": "sleep-1"}, 200, nil)
	asleep := api.waitAgent(ctx, a.ID, "sleeping", func(a *proto.Agent) bool { return a.Status == proto.AgentSleeping })
	api.json(ctx, "GET", "/v1/agents/"+a.ID+"/transcript?from=0&limit=1", nil, nil, 200, nil)
	if api.agent(ctx, a.ID).Status != proto.AgentSleeping {
		t.Fatal("reading the transcript woke the agent")
	}
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/diff", nil, nil); res.status != 409 || errorCode(t, res) != proto.CodeConflict {
		t.Fatalf("diff while asleep = %d %s", res.status, res.body)
	}
	if res := api.do(ctx, "GET", "/v1/agents/"+a.ID+"/diff?wake=true", nil, nil); res.status != 503 || errorCode(t, res) != proto.CodeUnreachable {
		t.Fatalf("diff wake=true = %d %s", res.status, res.body)
	}
	woken := api.waitAgent(ctx, a.ID, "awake after diff", func(a *proto.Agent) bool { return a.Status != proto.AgentSleeping })
	if woken.WS == asleep.WS && woken.Status == proto.AgentSleeping {
		t.Fatalf("agent still asleep: %+v", woken)
	}
	c := w.client("events")
	evs, err := c.ReadEvents(ctx, 0, a.WS)
	if err != nil {
		t.Fatal(err)
	}
	if wokenBy := wokenReasons(t, evs); len(wokenBy) != 1 || wokenBy[0] != proto.AgentWokenByDiff {
		t.Fatalf("woken by = %v", wokenBy)
	}
	// Sleep again; a preview request wakes with by=preview.
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/sleep", nil, map[string]string{"Idempotency-Key": "sleep-2"}, 200, nil)
	api.waitAgent(ctx, a.ID, "sleeping again", func(a *proto.Agent) bool { return a.Status == proto.AgentSleeping })
	if pv := api.do(ctx, "GET", prefix+"/again", nil, nil); pv.status != 200 || gotPath != "/again" {
		t.Fatalf("preview after sleep = %d %s path=%q", pv.status, pv.body, gotPath)
	}
	evs, _ = c.ReadEvents(ctx, 0, a.WS)
	if wokenBy := wokenReasons(t, evs); len(wokenBy) != 2 || wokenBy[1] != proto.AgentWokenByPreview {
		t.Fatalf("woken by = %v", wokenBy)
	}

	// Explicit wake is idempotent and refuses a bad reason.
	if res := api.do(ctx, "POST", "/v1/agents/"+a.ID+"/wake", map[string]any{"by": "timer"}, nil); res.status != 400 {
		t.Fatalf("wake by=timer = %d %s", res.status, res.body)
	}
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/wake", nil, map[string]string{"Idempotency-Key": "wake-1"}, 200, nil)

	// Cancel, then destroy; the destroyed agent stays readable and the
	// transcript stream ends with done.
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/cancel", nil, map[string]string{"Idempotency-Key": "cancel-1"}, 200, nil)
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/destroy", nil, map[string]string{"Idempotency-Key": "destroy-1"}, 204, nil)
	gone := api.waitAgent(ctx, a.ID, "destroyed", func(a *proto.Agent) bool { return a.Status == proto.AgentDestroyed })
	if gone.Status != proto.AgentDestroyed {
		t.Fatalf("status = %s", gone.Status)
	}
	sseEvents(t, ctx, w.http.URL, fmt.Sprintf("/v1/agents/%s/transcript?from=%d", a.ID, two.TranscriptNext), "tok", func(event, _ string) bool {
		return event == "done"
	})
	if res := api.do(ctx, "POST", "/v1/agents/"+a.ID+"/messages", map[string]any{"text": "x"}, nil); res.status != 409 {
		t.Fatalf("message to destroyed = %d %s", res.status, res.body)
	}
}

func TestAgentHTTPApprovalsAndFork(t *testing.T) {
	w := newAgentWorld(t)
	w.node("n1", nil)
	ctx := ctxT(t, 120*time.Second)
	api := w.api("tok")
	var a proto.Agent
	api.json(ctx, "POST", "/v1/agents", map[string]any{
		"workspace": map[string]any{"name": "approval-http"},
		"spec":      map[string]any{"recipe": "custom", "task": "deploy", "acp_command": fakeACPCommand(t, "permission")},
		"policy":    map[string]any{"approve": proto.ApproveOnRequest},
	}, nil, 201, &a)
	api.waitAgent(ctx, a.ID, "waiting_approval", func(a *proto.Agent) bool { return a.Status == proto.AgentWaitingApproval })
	var pendingRes proto.ApprovalListRes
	api.json(ctx, "GET", "/v1/agents/"+a.ID+"/approvals?status=pending", nil, nil, 200, &pendingRes)
	pending := pendingRes.Approvals
	if len(pending) != 1 || pending[0].Kind != proto.ApprovalToolCall {
		t.Fatalf("pending = %+v", pending)
	}
	var one proto.Approval
	api.json(ctx, "GET", "/v1/approvals/"+pending[0].ID, nil, nil, 200, &one)
	if one.ID != pending[0].ID {
		t.Fatalf("approval = %+v", one)
	}
	var decided proto.Approval
	api.json(ctx, "POST", "/v1/approvals/"+one.ID, map[string]any{"option": "allow"}, map[string]string{"Idempotency-Key": "dec-1"}, 200, &decided)
	if decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Option != "allow" {
		t.Fatalf("decided = %+v", decided)
	}
	if res := api.do(ctx, "POST", "/v1/approvals/"+one.ID, map[string]any{"denied": true}, nil); res.status != 409 {
		t.Fatalf("second decision = %d %s", res.status, res.body)
	}
	api.waitAgent(ctx, a.ID, "turn after approval", func(a *proto.Agent) bool {
		return a.PendingApprovals == 0 && len(a.Runs) == 1 && a.Runs[0].Turns == 1
	})
	// Fork copies the workspace into a new agent with its own id and URL.
	var kid proto.Agent
	api.json(ctx, "POST", "/v1/agents/"+a.ID+"/fork", map[string]any{"name": "kid"}, map[string]string{"Idempotency-Key": "fork-1"}, 201, &kid)
	if kid.ID == a.ID || kid.WS == a.WS || kid.ForkedFrom != a.ID {
		t.Fatalf("fork = %+v", kid)
	}
	var list proto.AgentListRes
	api.json(ctx, "GET", "/v1/agents", nil, nil, 200, &list)
	if len(list.Agents) != 2 {
		t.Fatalf("list after fork = %d", len(list.Agents))
	}
}
