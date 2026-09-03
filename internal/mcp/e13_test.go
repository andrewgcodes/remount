package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// TestE13NoKeyContract is the deterministic distribution lane. It exercises
// the same MCP messages a headless harness sends, without an API key or a live
// provider. Key-gated jobs run the two actual harnesses in integration/mcp.
func TestE13NoKeyContract(t *testing.T) {
	const secretCanary = "mcp-e13-secret-canary"
	g := &fakeGateway{fn: func(_ context.Context, name string, raw json.RawMessage) (any, error) {
		switch name {
		case "agent_create":
			var args map[string]any
			_ = json.Unmarshal(raw, &args)
			if args["parent"] != "a_parent" {
				return nil, fmt.Errorf("parent contract missing")
			}
			return map[string]any{"id": "a_child", "ws": "ws_child", "status": "creating"}, nil
		case "get":
			return map[string]any{"id": "a_child", "ws": "ws_child", "status": "waiting_input"}, nil
		case "transcript_tail":
			return map[string]any{"records": []any{map[string]any{"text": "completed child task\nREMOUNT_TOKEN=" + secretCanary}}, "next": 1, "done": true}, nil
		case "events_tail":
			return map[string]any{"events": []any{map[string]any{"seq": 1, "type": "agent.child.finished", "workspace": "ws_child"}}, "next": 2}, nil
		default:
			return nil, fmt.Errorf("unexpected tool %s", name)
		}
	}}
	s := NewServer(Options{Gateway: g, Version: "e13"})
	steps := []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"e13-no-key","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"agent_create","arguments":{"parent":"a_parent","recipe":"opencode","task":"run the second harness"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get","arguments":{"id":"a_child"}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"transcript_tail","arguments":{"id":"a_child","from":0,"limit":100}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"events_tail","arguments":{"from":0,"ws":"ws_child"}}}`,
	}
	var transcript strings.Builder
	for _, step := range steps {
		response := s.Handle(context.Background(), []byte(step))
		if response == nil || response.Error != nil {
			t.Fatalf("E13 step failed: %#v", response)
		}
		raw, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		transcript.Write(raw)
		transcript.WriteByte('\n')
	}
	if strings.Contains(transcript.String(), secretCanary) {
		t.Fatalf(".remount/env-shaped content leaked into MCP transcript:\n%s", transcript.String())
	}
	if len(steps) >= 20 {
		t.Fatalf("E13 used %d turns/messages", len(steps))
	}
	for _, want := range []string{"agent_create", "message", "get", "transcript_tail", "approve", "op_ws_create"} {
		if !strings.Contains(transcript.String(), `"name":"`+want+`"`) {
			t.Fatalf("tools/list omitted %s", want)
		}
	}
}
