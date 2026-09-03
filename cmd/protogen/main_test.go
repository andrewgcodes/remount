package main

import (
	"go/parser"
	"testing"
)

func TestOperationTypes(t *testing.T) {
	req, res := operationTypes("WSCreateReq -> Workspace")
	if req != "WSCreateReq" || res != "Workspace" {
		t.Fatalf("got %q -> %q", req, res)
	}
	req, res = operationTypes("-> WSListRes")
	if req != "" || res != "WSListRes" {
		t.Fatalf("empty request got %q -> %q", req, res)
	}
	req, res = operationTypes("AgentRunReq -> AgentRunRes: starts work")
	if req != "AgentRunReq" || res != "AgentRunRes" {
		t.Fatalf("annotated response got %q -> %q", req, res)
	}
}

func TestTypeMappings(t *testing.T) {
	expr, err := parser.ParseExpr("[]byte")
	if err != nil {
		t.Fatal(err)
	}
	if got := pythonType(expr); got != "bytes" {
		t.Fatalf("python type = %q", got)
	}
	if got := tsType(expr); got != "Uint8Array" {
		t.Fatalf("typescript type = %q", got)
	}
	if got := jsonSchema(expr)["contentEncoding"]; got != "base64" {
		t.Fatalf("schema encoding = %v", got)
	}
	pointer, err := parser.ParseExpr("*Workspace")
	if err != nil {
		t.Fatal(err)
	}
	if got := jsonSchema(pointer)["anyOf"]; got == nil {
		t.Fatal("pointer schema does not permit null")
	}
}

func TestModelIncludesProtocolAndAgentHTTPTypes(t *testing.T) {
	m, err := parseModel("../..")
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, declaration := range m.Types {
		types[declaration.Name] = true
	}
	if !types["Workspace"] || !types["Agent"] || !types["Approval"] {
		t.Fatalf("missing required generated types: Workspace=%t Agent=%t Approval=%t", types["Workspace"], types["Agent"], types["Approval"])
	}
	if m.ProtocolVersion != 1 {
		t.Fatalf("protocol version = %d", m.ProtocolVersion)
	}
	ops := map[string]operation{}
	for _, op := range m.Operations {
		ops[op.Name] = op
	}
	if got := ops["ws.create"]; got.Request != "WSCreateReq" || got.Response != "Workspace" {
		t.Fatalf("ws.create = %+v", got)
	}
	if got := ops["ws.list"]; got.Request != "" || got.Response != "WSListRes" {
		t.Fatalf("ws.list = %+v", got)
	}
	if got := ops["agent.transcript"]; got.Request != "AgentTranscriptReq" || got.Response != "AgentTranscriptRes" {
		t.Fatalf("agent.transcript = %+v", got)
	}
}
