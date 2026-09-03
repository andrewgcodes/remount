package sim

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

type consoleWorkspaceResponse struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	Node       string `json:"node"`
	Generation uint64 `json:"generation"`
}

type consoleTerminalResponse struct {
	Type   string `json:"type"`
	Seq    uint64 `json:"seq"`
	Stream string `json:"stream"`
	Data   []byte `json:"data"`
}

func waitConsoleWorkspace(t *testing.T, ctx context.Context, api *httpAPI, id string, predicate func(consoleWorkspaceResponse) bool) consoleWorkspaceResponse {
	t.Helper()
	for {
		var workspace consoleWorkspaceResponse
		api.json(ctx, http.MethodGet, "/v1/console/workspaces/"+id, nil, nil, http.StatusOK, &workspace)
		if predicate(workspace) {
			return workspace
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for console workspace %s: %+v", id, workspace)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func dialConsoleTerminal(t *testing.T, ctx context.Context, w *world, workspace, session string, from uint64) *websocket.Conn {
	t.Helper()
	path := "/v1/console/workspaces/" + url.PathEscape(workspace) + "/terminal?session=" + url.QueryEscape(session) + "&from=" + strconv.FormatUint(from, 10)
	protocol := "remount.bearer." + base64.RawURLEncoding.EncodeToString([]byte("tok"))
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(w.http.URL, "http")+path,
		&websocket.DialOptions{Subprotocols: []string{protocol}})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		t.Fatalf("terminal dial = %v (status %d)", err, status)
	}
	return conn
}

func dialConsoleTerminalSince(t *testing.T, ctx context.Context, w *world, workspace, session string, since time.Time) *websocket.Conn {
	t.Helper()
	path := "/v1/console/workspaces/" + url.PathEscape(workspace) + "/terminal?session=" + url.QueryEscape(session) + "&since=" + url.QueryEscape(since.UTC().Format(time.RFC3339))
	protocol := "remount.bearer." + base64.RawURLEncoding.EncodeToString([]byte("tok"))
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(w.http.URL, "http")+path, &websocket.DialOptions{Subprotocols: []string{protocol}})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		t.Fatalf("terminal since dial = %v (status %d)", err, status)
	}
	return conn
}

func readConsoleTerminal(t *testing.T, ctx context.Context, conn *websocket.Conn) consoleTerminalResponse {
	t.Helper()
	_, body, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event consoleTerminalResponse
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatalf("terminal event: %v: %s", err, body)
	}
	return event
}

func TestConsoleE15SimulatedOperatorFlow(t *testing.T) {
	var approvedHits atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/approve" {
			approvedHits.Add(1)
		}
		_, _ = io.WriteString(w, "upstream-ok")
	}))
	defer upstream.Close()
	upstreamHost := strings.TrimPrefix(upstream.URL, "https://")
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	w := newWorld(t, control.Binding{ID: "b_console", Secret: "console-secret", Destinations: []string{upstreamHost}, TTLSec: 60})
	n1 := w.nodeWithBrokerRoots("console-n1", map[string]string{"console": "one"}, roots)
	n2 := w.nodeWithBrokerRoots("console-n2", map[string]string{"console": "two"}, roots)
	api := w.api("tok")
	ctx := ctxT(t, 90*time.Second)
	var fleet struct {
		Nodes []map[string]any `json:"nodes"`
	}
	api.json(ctx, http.MethodGet, "/v1/console/fleet", nil, nil, http.StatusOK, &fleet)
	if len(fleet.Nodes) != 2 {
		t.Fatalf("console fleet nodes=%d", len(fleet.Nodes))
	}

	var created consoleWorkspaceResponse
	api.json(ctx, http.MethodPost, "/v1/console/workspaces", map[string]string{"name": "e15"},
		map[string]string{"Idempotency-Key": "console-create"}, http.StatusCreated, &created)
	workspace := waitConsoleWorkspace(t, ctx, api, created.ID, func(workspace consoleWorkspaceResponse) bool {
		return workspace.State == proto.WSClaimed
	})
	if workspace.Generation != 1 {
		t.Fatalf("initial generation = %d", workspace.Generation)
	}

	var opened struct {
		Session string `json:"session"`
	}
	api.json(ctx, http.MethodPost, "/v1/console/workspaces/"+workspace.ID+"/exec",
		map[string]any{"command": []string{"sh", "-c", "printf first; sleep 1; printf second; printf old > note.txt"}},
		map[string]string{"Idempotency-Key": "console-exec"}, http.StatusCreated, &opened)

	firstConn := dialConsoleTerminal(t, ctx, w, workspace.ID, opened.Session, 0)
	if event := readConsoleTerminal(t, ctx, firstConn); event.Type != "open" {
		t.Fatalf("first terminal event = %+v", event)
	}
	var first consoleTerminalResponse
	for first.Type != "chunk" {
		first = readConsoleTerminal(t, ctx, firstConn)
	}
	if string(first.Data) != "first" {
		t.Fatalf("first output = %q", first.Data)
	}
	_ = firstConn.CloseNow() // force the browser-side cut before the second chunk

	secondConn := dialConsoleTerminal(t, ctx, w, workspace.ID, opened.Session, first.Seq+1)
	if event := readConsoleTerminal(t, ctx, secondConn); event.Type != "open" {
		t.Fatalf("reattach event = %+v", event)
	}
	var resumed []byte
	for {
		event := readConsoleTerminal(t, ctx, secondConn)
		if event.Type == "chunk" {
			resumed = append(resumed, event.Data...)
		}
		if event.Type == "exit" {
			break
		}
	}
	_ = secondConn.CloseNow()
	if string(resumed) != "second" {
		t.Fatalf("resumed output = %q", resumed)
	}
	// The console's ten-minute scrub is a real server-side `since` attach. It
	// conservatively replays the sequenced log (or an explicit gap), never a
	// browser-local transcript.
	scrub := dialConsoleTerminalSince(t, ctx, w, workspace.ID, opened.Session, time.Now().Add(-10*time.Minute))
	if event := readConsoleTerminal(t, ctx, scrub); event.Type != "open" {
		t.Fatalf("scrub open=%+v", event)
	}
	var scrubbed []byte
	for {
		event := readConsoleTerminal(t, ctx, scrub)
		if event.Type == "chunk" {
			scrubbed = append(scrubbed, event.Data...)
		}
		if event.Type == "exit" {
			break
		}
	}
	_ = scrub.CloseNow()
	if string(scrubbed) != "firstsecond" {
		t.Fatalf("ten-minute scrub=%q", scrubbed)
	}

	var file struct {
		Path, Content, ETag string
	}
	api.json(ctx, http.MethodGet, "/v1/console/workspaces/"+workspace.ID+"/file?path=note.txt", nil, nil, http.StatusOK, &file)
	if file.Content != "old" || file.ETag == "" {
		t.Fatalf("file = %+v", file)
	}
	api.json(ctx, http.MethodPut, "/v1/console/workspaces/"+workspace.ID+"/file?path=note.txt", map[string]string{"content": "new"},
		map[string]string{"Idempotency-Key": "console-write", "If-Match": file.ETag}, http.StatusOK, nil)
	api.json(ctx, http.MethodGet, "/v1/console/workspaces/"+workspace.ID+"/file?path=note.txt", nil, nil, http.StatusOK, &file)
	if file.Content != "new" {
		t.Fatalf("updated file = %+v", file)
	}

	api.json(ctx, http.MethodPost, "/v1/console/workspaces/"+workspace.ID+"/snapshot", map[string]any{},
		map[string]string{"Idempotency-Key": "console-snapshot"}, http.StatusOK, nil)
	target := n1.ID()
	if workspace.Node == n1.ID() {
		target = n2.ID()
	}
	api.json(ctx, http.MethodPost, "/v1/console/workspaces/"+workspace.ID+"/move", map[string]string{"node": target},
		map[string]string{"Idempotency-Key": "console-move"}, http.StatusOK, nil)
	moved := waitConsoleWorkspace(t, ctx, api, workspace.ID, func(candidate consoleWorkspaceResponse) bool {
		return candidate.State == proto.WSClaimed && candidate.Node == target && candidate.Generation > workspace.Generation
	})
	if moved.Generation <= workspace.Generation {
		t.Fatalf("move did not advance generation: before=%+v after=%+v", workspace, moved)
	}

	var events struct {
		Events []map[string]any `json:"events"`
		Next   uint64           `json:"next"`
	}
	api.json(ctx, http.MethodGet, "/v1/console/events?types=ws.moved&limit=100", nil, nil, http.StatusOK, &events)
	if len(events.Events) == 0 || events.Next == 0 {
		t.Fatalf("move is absent from canonical console timeline: %+v", events)
	}

	// Exercise the console's security timeline and approval endpoint against a
	// real node broker. The workspace sees only a placeholder; one request is
	// substituted, one foreign-destination leak is blocked, and an approval
	// remains parked until the console commits the decision.
	securityClient := w.client("console-security")
	securityWS := mustWS(t, securityClient, proto.WorkspaceSpec{
		Bindings: []string{"b_console"},
		Env:      map[string]string{"API_KEY": "ref:b_console", "API_URL": "${REMOUNT_BROKER}/d/" + upstreamHost},
		Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{
			{ID: "read", Mode: proto.EgressModeAllow, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{upstreamHost}, Methods: []string{http.MethodGet}, PathPrefixes: []string{"/ok"}},
			{ID: "approve", Mode: proto.EgressModeApprove, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{upstreamHost}, Methods: []string{http.MethodPost}, PathPrefixes: []string{"/approve"}},
		}}},
	})
	out, errOut, exit, err := securityClient.Run(ctx, securityWS.ID, "sh", "-c", `curl --fail --silent -H "Authorization: Bearer $API_KEY" "$API_URL/ok"`)
	if err != nil || exit == nil || exit.Code != 0 || string(out) != "upstream-ok" {
		t.Fatalf("credential run err=%v exit=%+v out=%q stderr=%q", err, exit, out, errOut)
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("blocked leak reached foreign server") }))
	defer foreign.Close()
	foreignHost := strings.TrimPrefix(foreign.URL, "http://")
	out, _, _, _ = securityClient.Run(ctx, securityWS.ID, "sh", "-c", `curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$REMOUNT_BROKER/http/`+foreignHost+`/leak"`)
	if string(out) != "403" {
		t.Fatalf("leak status=%q", out)
	}

	type approvalRun struct {
		out, stderr []byte
		exit        *proto.ExitInfo
		err         error
	}
	approvalDone := make(chan approvalRun, 1)
	go func() {
		o, e, x, runErr := securityClient.Run(ctx, securityWS.ID, "sh", "-c", `curl --fail --silent -X POST -H "Authorization: Bearer $API_KEY" "$API_URL/approve"`)
		approvalDone <- approvalRun{out: o, stderr: e, exit: x, err: runErr}
	}()
	var approvalID string
	deadline := time.Now().Add(10 * time.Second)
	for approvalID == "" && time.Now().Before(deadline) {
		var pending struct {
			Approvals []struct{ ID, Status string } `json:"approvals"`
		}
		api.json(ctx, http.MethodGet, "/v1/console/approvals?status=pending", nil, nil, http.StatusOK, &pending)
		if len(pending.Approvals) > 0 {
			approvalID = pending.Approvals[0].ID
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if approvalID == "" || approvedHits.Load() != 0 {
		t.Fatalf("pending approval=%q upstream hits=%d", approvalID, approvedHits.Load())
	}
	api.json(ctx, http.MethodPost, "/v1/approvals/"+url.PathEscape(approvalID), map[string]any{"remember": "none"}, map[string]string{"Idempotency-Key": "console-approve"}, http.StatusOK, nil)
	approved := <-approvalDone
	if approved.err != nil || approved.exit == nil || approved.exit.Code != 0 || string(approved.out) != "upstream-ok" || approvedHits.Load() != 1 {
		t.Fatalf("approved=%+v hits=%d", approved, approvedHits.Load())
	}

	var securityEvents struct {
		Events []struct {
			Type       string         `json:"type"`
			Credential string         `json:"credential"`
			Payload    map[string]any `json:"payload"`
		} `json:"events"`
	}
	api.json(ctx, http.MethodGet, "/v1/console/events?types=cred.used,egress.denied,egress.pending,egress.allowed&limit=100", nil, nil, http.StatusOK, &securityEvents)
	var credentialUsed, leakBlocked, pendingSeen, decidedSeen bool
	for _, event := range securityEvents.Events {
		credentialUsed = credentialUsed || (event.Type == proto.EvCredUsed && event.Credential == "b_console")
		leakBlocked = leakBlocked || (event.Type == proto.EvEgressDenied && event.Payload["decision"] == "leak_blocked")
		pendingSeen = pendingSeen || event.Type == proto.EvEgressPending
		decidedSeen = decidedSeen || event.Type == proto.EvEgressAllowed
	}
	if !credentialUsed || !leakBlocked || !pendingSeen || !decidedSeen {
		t.Fatalf("security timeline used=%v leak=%v pending=%v decided=%v events=%+v", credentialUsed, leakBlocked, pendingSeen, decidedSeen, securityEvents.Events)
	}
}
