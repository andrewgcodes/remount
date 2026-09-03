package sim

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

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
	w := newWorld(t)
	n1 := w.node("console-n1", map[string]string{"console": "one"})
	n2 := w.node("console-n2", map[string]string{"console": "two"})
	api := w.api("tok")
	ctx := ctxT(t, 90*time.Second)

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
}
