package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

type fakeGateway struct {
	mu    sync.Mutex
	calls []string
	fn    func(context.Context, string, json.RawMessage) (any, error)
}

func (f *fakeGateway) Invoke(ctx context.Context, name string, raw json.RawMessage) (any, error) {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.mu.Unlock()
	if f.fn != nil {
		return f.fn(ctx, name, raw)
	}
	return map[string]any{"ok": true, "tool": name}, nil
}

func TestManifestCoversEveryProtocolOperation(t *testing.T) {
	manifest := Manifest()
	names := make(map[string]bool, len(manifest))
	var gotOps []string
	last := ""
	for _, item := range manifest {
		if names[item.Tool.Name] {
			t.Fatalf("duplicate tool %q", item.Tool.Name)
		}
		if item.Tool.Name < last {
			t.Fatalf("manifest not sorted: %q before %q", last, item.Tool.Name)
		}
		names[item.Tool.Name], last = true, item.Tool.Name
		if item.ProtocolOp != "" && strings.HasPrefix(item.Tool.Name, "op_") {
			gotOps = append(gotOps, item.ProtocolOp)
		}
	}
	for _, required := range []string{"agent_create", "message", "get", "transcript_tail", "approve", "run", "handoff", "resume", "events_tail"} {
		if !names[required] {
			t.Errorf("required composite %q missing", required)
		}
	}
	wantOps := protocolConstants(t)
	sort.Strings(gotOps)
	if !reflect.DeepEqual(gotOps, wantOps) {
		t.Fatalf("operation manifest drift\ngot:  %v\nwant: %v", gotOps, wantOps)
	}
}

func protocolConstants(t *testing.T) []string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "proto")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]bool{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			spec, ok := node.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, ident := range spec.Names {
				if !strings.HasPrefix(ident.Name, "Op") || i >= len(spec.Values) {
					continue
				}
				lit, ok := spec.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err == nil {
					values[value] = true
				}
			}
			return true
		})
	}
	// Transport-layer ops are deliberately absent from the manifest. A sealed
	// frame and a key exchange are consumed by the layer that produced them;
	// they are not operations a harness can invoke, and advertising a tool
	// that can never work is worse than not advertising one at all.
	for _, transportOnly := range []string{proto.OpE2EEKeyExchange, proto.OpE2EESealed} {
		if !values[transportOnly] {
			t.Fatalf("%s is excluded from the MCP manifest but no longer exists; drop the exclusion", transportOnly)
		}
		delete(values, transportOnly)
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func TestLegacyAndModernDiscovery(t *testing.T) {
	s := NewServer(Options{Gateway: &fakeGateway{}, Version: "test"})
	init := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`))
	assertResultField(t, init, "protocolVersion", LegacyVersion)
	discover := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":"d","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`))
	var result map[string]any
	remarshal(t, discover.Result, &result)
	if result["resultType"] != "complete" || result["cacheScope"] != "private" {
		t.Fatalf("bad discover result: %#v", result)
	}
	versions := result["supportedVersions"].([]any)
	if len(versions) != 1 || versions[0] != ModernVersion {
		t.Fatalf("bad versions: %#v", versions)
	}
}

func TestToolCallResultAndStableError(t *testing.T) {
	g := &fakeGateway{fn: func(_ context.Context, name string, _ json.RawMessage) (any, error) {
		if name == "get" {
			return map[string]any{"id": "a_1", "token": "must-not-leak", "root": "/private/node"}, nil
		}
		return nil, proto.Err(proto.CodeNotFound, "agent missing")
	}}
	s := NewServer(Options{Gateway: g, Version: "test"})
	ok := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get","arguments":{"id":"a_1"}}}`))
	raw, _ := json.Marshal(ok)
	if bytes.Contains(raw, []byte("must-not-leak")) || bytes.Contains(raw, []byte("/private/node")) {
		t.Fatalf("sensitive field leaked: %s", raw)
	}
	bad := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"message","arguments":{}}}`))
	var result toolResult
	remarshal(t, bad.Result, &result)
	if !result.IsError {
		t.Fatalf("expected execution error: %#v", bad.Result)
	}
	structured := result.StructuredContent.(map[string]any)
	if structured["code"] != proto.CodeNotFound {
		t.Fatalf("unstable code: %#v", structured)
	}
}

func TestSecretShapedOutputRedacted(t *testing.T) {
	g := &fakeGateway{fn: func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"stdout": "safe\nOPENAI_API_KEY=sk-canary\nREMOUNT_BROKER=http://capability\nend"}, nil
	}}
	s := NewServer(Options{Gateway: g})
	res := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"run","arguments":{"ws":"ws_x","program":["env"]}}}`))
	raw, _ := json.Marshal(res)
	if bytes.Contains(raw, []byte("sk-canary")) || bytes.Contains(raw, []byte("capability")) {
		t.Fatalf("credential-shaped line leaked: %s", raw)
	}
}

func TestProtocolErrorsAndLimit(t *testing.T) {
	s := NewServer(Options{Gateway: &fakeGateway{}, MaxMessageBytes: 100})
	if got := s.Handle(context.Background(), []byte(`not-json`)); got.Error == nil || got.Error.Code != -32700 {
		t.Fatalf("parse: %#v", got)
	}
	if got := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":{},"method":"ping"}`)); got.Error == nil || got.Error.Code != -32600 {
		t.Fatalf("id: %#v", got)
	}
	if got := s.Handle(context.Background(), bytes.Repeat([]byte("x"), 101)); got.Error == nil || got.Error.Code != -32600 {
		t.Fatalf("limit: %#v", got)
	}
	if got := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"missing","arguments":{}}}`)); got.Error == nil || got.Error.Code != -32602 {
		t.Fatalf("tool: %#v", got)
	}
}

func TestStdioCancellationAndJoin(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	g := &fakeGateway{fn: func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}}
	s := NewServer(Options{Gateway: g, CallTimeout: time.Minute})
	inR, inW := ioPipe(t)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- s.ServeStdio(context.Background(), inR, &out) }()
	_, _ = inW.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"get\",\"arguments\":{\"id\":\"a\"}}}\n"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("call did not start")
	}
	_, _ = inW.Write([]byte("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":7,\"reason\":\"test\"}}\n"))
	_ = inW.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("call not cancelled")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve did not join")
	}
	if out.Len() != 0 {
		t.Fatalf("cancelled request must not receive a response: %s", out.String())
	}
}

func TestHTTPAuthOriginAndRoutingHeaders(t *testing.T) {
	s := NewServer(Options{Gateway: &fakeGateway{}, BearerToken: "mcp-secret"})
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get","arguments":{"id":"a"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	request := func(origin, token, method, name string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/mcp", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		r.Header.Set("MCP-Protocol-Version", ModernVersion)
		r.Header.Set("Mcp-Method", method)
		r.Header.Set("Mcp-Name", name)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	if got := request("", "", "tools/call", "get"); got.Code != http.StatusUnauthorized {
		t.Fatalf("auth status %d", got.Code)
	}
	if got := request("https://evil.example", "mcp-secret", "tools/call", "get"); got.Code != http.StatusForbidden {
		t.Fatalf("origin status %d", got.Code)
	}
	if got := request("javascript://localhost", "mcp-secret", "tools/call", "get"); got.Code != http.StatusForbidden {
		t.Fatalf("non-http loopback origin status %d", got.Code)
	}
	if got := request("http://localhost:9000", "mcp-secret", "wrong", "get"); got.Code != http.StatusBadRequest {
		t.Fatalf("header status %d", got.Code)
	} else if !strings.Contains(got.Body.String(), `"code":-32020`) {
		t.Fatalf("header mismatch code: %s", got.Body.String())
	}
	if got := request("http://localhost:9000", "mcp-secret", "tools/call", "get"); got.Code != http.StatusOK {
		t.Fatalf("success status %d: %s", got.Code, got.Body.String())
	}
}

func TestHTTPRequiresMatchingModernMetadataAndAccept(t *testing.T) {
	s := NewServer(Options{Gateway: &fakeGateway{}})
	makeRequest := func(body, accept string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/mcp", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", accept)
		r.Header.Set("MCP-Protocol-Version", ModernVersion)
		r.Header.Set("Mcp-Method", "tools/list")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	missingMeta := `{"jsonrpc":"2.0","id":9,"method":"tools/list","params":{}}`
	if got := makeRequest(missingMeta, "application/json, text/event-stream"); got.Code != http.StatusBadRequest || !strings.Contains(got.Body.String(), `"code":-32020`) {
		t.Fatalf("missing metadata: %d %s", got.Code, got.Body.String())
	}
	modern := `{"jsonrpc":"2.0","id":9,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	if got := makeRequest(modern, "application/json"); got.Code != http.StatusNotAcceptable {
		t.Fatalf("accept status: %d %s", got.Code, got.Body.String())
	}
}

func assertResultField(t *testing.T, response *response, field string, want any) {
	t.Helper()
	var result map[string]any
	remarshal(t, response.Result, &result)
	if result[field] != want {
		t.Fatalf("%s=%#v, want %#v", field, result[field], want)
	}
}

func remarshal(t *testing.T, in, out any) {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
}

func ioPipe(t *testing.T) (*io.PipeReader, *io.PipeWriter) { t.Helper(); return io.Pipe() }
