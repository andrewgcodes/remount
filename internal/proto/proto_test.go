package proto

import (
	"errors"
	"reflect"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestFrameRoundTrip(t *testing.T) {
	req := NewReq(7, "n_abc", OpSOpen, SOpenReq{WS: "ws_1", Kind: SessionExec, Program: []string{"echo", "hi"}})
	b, err := EncodeFrame(req)
	if err != nil {
		t.Fatal(err)
	}
	f, err := DecodeFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if f.V != Version || f.T != KindReq || f.ID != 7 || f.To != "n_abc" || f.Op != OpSOpen {
		t.Fatalf("bad header: %+v", f)
	}
	var body SOpenReq
	if err := f.Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.WS != "ws_1" || body.Program[1] != "hi" {
		t.Fatalf("bad body: %+v", body)
	}
}

func TestDeterministicEncoding(t *testing.T) {
	a := MustMarshal(map[string]int{"b": 2, "a": 1})
	b := MustMarshal(map[string]int{"a": 1, "b": 2})
	if string(a) != string(b) {
		t.Fatal("encoding not deterministic")
	}
}

// A newer peer may add fields; an older peer must ignore them.
func TestUnknownFieldsIgnored(t *testing.T) {
	type future struct {
		V     uint8  `cbor:"v"`
		T     string `cbor:"t"`
		Op    string `cbor:"op"`
		Extra string `cbor:"extra"`
		Body  []byte `cbor:"body"`
	}
	b, _ := cbor.Marshal(future{V: 2, T: KindReq, Op: "x.new", Extra: "surprise", Body: MustMarshal(map[string]any{"k": 1, "z": []int{1}})})
	f, err := DecodeFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if f.Op != "x.new" || f.V != 2 {
		t.Fatalf("%+v", f)
	}
	var body struct {
		K int `cbor:"k"`
	}
	if err := f.Decode(&body); err != nil || body.K != 1 {
		t.Fatalf("body: %v %+v", err, body)
	}
}

func TestDecodeRejectsNoKind(t *testing.T) {
	b, _ := cbor.Marshal(map[string]any{"v": 1})
	if _, err := DecodeFrame(b); err == nil {
		t.Fatal("expected error")
	}
	if _, err := DecodeFrame([]byte{0xff, 0x00}); err == nil {
		t.Fatal("expected error on garbage")
	}
}

func TestErrorIs(t *testing.T) {
	e := Err(CodeEvicted, "seq %d gone", 5)
	e.Oldest = 9
	var wrapped error = e
	if !errors.Is(wrapped, &Error{Code: CodeEvicted}) {
		t.Fatal("errors.Is by code failed")
	}
	if e.Error() != "evicted: seq 5 gone" {
		t.Fatal(e.Error())
	}
	res := NewErrRes(&Frame{ID: 3, From: "c_1"}, e)
	b, _ := EncodeFrame(res)
	f, _ := DecodeFrame(b)
	if f.Err == nil || f.Err.Oldest != 9 || f.To != "c_1" || f.ID != 3 {
		t.Fatalf("%+v", f)
	}
}

func TestChunkBody(t *testing.T) {
	c := Frame{V: Version, T: KindChunk, S: "s_1", Seq: 42, Body: MustMarshal(ChunkBody{Stream: StreamStdout, Data: []byte("hello\n")})}
	b, _ := EncodeFrame(&c)
	f, _ := DecodeFrame(b)
	var cb ChunkBody
	if err := f.Decode(&cb); err != nil {
		t.Fatal(err)
	}
	if f.Seq != 42 || cb.Stream != StreamStdout || string(cb.Data) != "hello\n" {
		t.Fatalf("%+v %+v", f, cb)
	}
}

func TestNormalizeSecurityMakesTypedEgressFailClosed(t *testing.T) {
	original := SecuritySpec{Network: NetworkPolicy{Rules: []EgressRule{{
		ID: " packages ", Protocol: "HTTPS", Hosts: []string{"REGISTRY.EXAMPLE:443"},
		Methods: []string{"head", "get"}, PathPrefixes: []string{"/v2"},
		SharedState: SharedStateImmutableRead,
	}}}}
	security, err := NormalizeSecurity(original)
	if err != nil {
		t.Fatal(err)
	}
	rule := security.Network.Rules[0]
	if security.Network.Default != NetworkDefaultDeny || rule.ID != "packages" ||
		rule.Protocol != EgressProtocolHTTPS || rule.Hosts[0] != "registry.example:443" ||
		!reflect.DeepEqual(rule.Methods, []string{"GET", "HEAD"}) {
		t.Fatalf("normalized security=%#v", security)
	}
	if original.Network.Default != "" || original.Network.Rules[0].ID != " packages " ||
		original.Network.Rules[0].Protocol != "HTTPS" || original.Network.Rules[0].Hosts[0] != "REGISTRY.EXAMPLE:443" ||
		!reflect.DeepEqual(original.Network.Rules[0].Methods, []string{"head", "get"}) {
		t.Fatalf("normalization mutated caller-owned policy: %#v", original)
	}
	security.Network.Rules[0].Hosts[0] = "changed.example"
	security.Network.Rules[0].Methods[0] = "POST"
	if original.Network.Rules[0].Hosts[0] != "REGISTRY.EXAMPLE:443" || original.Network.Rules[0].Methods[0] != "head" {
		t.Fatalf("normalized policy aliases caller-owned slices: %#v", original)
	}
}

func TestNormalizeSecurityRejectsUnenforceableEgressRules(t *testing.T) {
	tests := []struct {
		name string
		rule EgressRule
	}{
		{"missing id", EgressRule{Protocol: "https", Hosts: []string{"example.com"}}},
		{"unknown protocol", EgressRule{ID: "x", Protocol: "udp", Hosts: []string{"example.com"}}},
		{"missing host", EgressRule{ID: "x", Protocol: "https"}},
		{"invalid host", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"https://example.com"}}},
		{"userinfo host", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"user@example.com"}}},
		{"unicode host", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"café.example"}}},
		{"interior wildcard", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"api.*.example"}}},
		{"zero port", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, Ports: []uint16{0}}},
		{"unclean path", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, PathPrefixes: []string{"/ok/../admin"}}},
		{"encoded path", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, PathPrefixes: []string{"/ok%2fadmin"}}},
		{"negative limit", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, MaxRequests: -1}},
		{"unknown shared state", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, SharedState: "mystery"}},
		{"connect byte claim", EgressRule{ID: "x", Protocol: "connect", Hosts: []string{"example.com"}, MaxResponseBytes: 1}},
		{"connect shared state", EgressRule{ID: "x", Protocol: "connect", Hosts: []string{"example.com"}, SharedState: SharedStateGlobalWrite}},
		{"immutable write", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, Methods: []string{"POST"}, SharedState: SharedStateImmutableRead}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NormalizeSecurity(SecuritySpec{Network: NetworkPolicy{Rules: []EgressRule{tt.rule}}})
			if err == nil {
				t.Fatal("invalid rule accepted")
			}
			var protocolErr *Error
			if !errors.As(err, &protocolErr) || protocolErr.Code != CodeBadRequest {
				t.Fatalf("error=%v", err)
			}
		})
	}
	_, err := NormalizeSecurity(SecuritySpec{Network: NetworkPolicy{Rules: []EgressRule{
		{ID: "duplicate", Protocol: "https", Hosts: []string{"a.example"}},
		{ID: "duplicate", Protocol: "https", Hosts: []string{"b.example"}},
	}}})
	if err == nil {
		t.Fatal("duplicate rule id accepted")
	}
}
