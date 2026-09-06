package proto

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestFrameRoundTrip(t *testing.T) {
	req := NewReq(7, "n_abc", OpSOpen, SOpenReq{
		WS: "ws_1", Kind: SessionExec, Program: []string{"echo", "hi"},
		Sensitive: true, AuthOperation: &AuthOperation{Recipe: "claude", Action: AuthActionStatus},
	})
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
	if body.WS != "ws_1" || body.Program[1] != "hi" || !body.Sensitive ||
		body.AuthOperation == nil || body.AuthOperation.Recipe != "claude" || body.AuthOperation.Action != AuthActionStatus {
		t.Fatalf("bad body: %+v", body)
	}
}

func TestAuthOperationValidate(t *testing.T) {
	tests := []struct {
		auth    *AuthOperation
		wantErr bool
	}{
		{nil, false},
		{&AuthOperation{Recipe: "claude", Action: AuthActionLogin}, false},
		{&AuthOperation{Recipe: "codex", Action: AuthActionStatus}, false},
		{&AuthOperation{Recipe: "claude", Action: AuthActionLogout}, false},
		{&AuthOperation{Action: AuthActionLogin}, true},
		{&AuthOperation{Recipe: "claude", Action: "refresh"}, true},
	}
	for _, tc := range tests {
		if err := tc.auth.Validate(); (err != nil) != tc.wantErr {
			t.Errorf("Validate(%+v) = %v", tc.auth, err)
		}
	}
}

func TestEncodeFrameDefaultsVersionWithoutMutatingCaller(t *testing.T) {
	frame := &Frame{T: KindPing}
	raw, err := EncodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.V != 0 {
		t.Fatalf("EncodeFrame mutated caller version to %d", frame.V)
	}
	decoded, err := DecodeFrame(raw)
	if err != nil || decoded.V != Version {
		t.Fatalf("decoded frame = %+v, err=%v", decoded, err)
	}
}

func TestV1GoldenFixture(t *testing.T) {
	rawHex, err := os.ReadFile("testdata/v1-request.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(rawHex)))
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	var request WSGetReq
	if err := frame.Decode(&request); err != nil {
		t.Fatal(err)
	}
	if frame.V != Version || frame.T != KindReq || frame.ID != 7 || frame.To != PeerControl || frame.Op != OpWSGet || request.ID != "ws_fixture" {
		t.Fatalf("fixture changed semantics: frame=%+v request=%+v", frame, request)
	}
	reencoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, raw) {
		t.Fatalf("v1 encoding changed:\n got %x\nwant %x", reencoded, raw)
	}
}

func TestDeterministicEncoding(t *testing.T) {
	a := MustMarshal(map[string]int{"b": 2, "a": 1})
	b := MustMarshal(map[string]int{"a": 1, "b": 2})
	if string(a) != string(b) {
		t.Fatal("encoding not deterministic")
	}
}

// A peer may add fields within the negotiated version; an older peer ignores
// those fields without accepting a different semantic version.
func TestUnknownFieldsIgnored(t *testing.T) {
	type future struct {
		V     uint8  `cbor:"v"`
		T     string `cbor:"t"`
		Op    string `cbor:"op"`
		Extra string `cbor:"extra"`
		Body  []byte `cbor:"body"`
	}
	b, _ := cbor.Marshal(future{V: Version, T: KindReq, Op: "x.new", Extra: "surprise", Body: MustMarshal(map[string]any{"k": 1, "z": []int{1}})})
	f, err := DecodeFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if f.Op != "x.new" || f.V != Version {
		t.Fatalf("%+v", f)
	}
	var body struct {
		K int `cbor:"k"`
	}
	if err := f.Decode(&body); err != nil || body.K != 1 {
		t.Fatalf("body: %v %+v", err, body)
	}
}

func TestSessionSubscriptionFieldIsWireCompatible(t *testing.T) {
	type legacyAttach struct {
		S    string `cbor:"s"`
		From uint64 `cbor:"from"`
	}
	type legacyClose struct {
		S    string `cbor:"s"`
		Kill bool   `cbor:"kill,omitempty"`
	}

	var oldAttach legacyAttach
	if err := Unmarshal(MustMarshal(SAttachReq{S: "s_1", From: 7, Subscription: "sub_1"}), &oldAttach); err != nil {
		t.Fatal(err)
	}
	if oldAttach.S != "s_1" || oldAttach.From != 7 {
		t.Fatalf("legacy attach = %+v", oldAttach)
	}
	var newAttach SAttachReq
	if err := Unmarshal(MustMarshal(legacyAttach{S: "s_1", From: 7}), &newAttach); err != nil {
		t.Fatal(err)
	}
	if newAttach.Subscription != "" {
		t.Fatalf("legacy attach subscription = %q", newAttach.Subscription)
	}

	var oldClose legacyClose
	if err := Unmarshal(MustMarshal(SCloseReq{S: "s_1", Kill: true, Subscription: "sub_1"}), &oldClose); err != nil {
		t.Fatal(err)
	}
	if oldClose.S != "s_1" || !oldClose.Kill {
		t.Fatalf("legacy close = %+v", oldClose)
	}
	var newClose SCloseReq
	if err := Unmarshal(MustMarshal(legacyClose{S: "s_1", Kill: true}), &newClose); err != nil {
		t.Fatal(err)
	}
	if newClose.Subscription != "" {
		t.Fatalf("legacy close subscription = %q", newClose.Subscription)
	}
}

func TestSessionOpenKindIsWireCompatible(t *testing.T) {
	type legacyOpenRes struct {
		S            string `cbor:"s"`
		Next         uint64 `cbor:"next"`
		LastInputSeq uint64 `cbor:"last_iseq,omitempty"`
	}

	var oldResponse legacyOpenRes
	if err := Unmarshal(MustMarshal(SOpenRes{S: "s_1", Next: 8, LastInputSeq: 3, Kind: SessionPTY}), &oldResponse); err != nil {
		t.Fatal(err)
	}
	if oldResponse.S != "s_1" || oldResponse.Next != 8 || oldResponse.LastInputSeq != 3 {
		t.Fatalf("legacy response = %+v", oldResponse)
	}

	var newResponse SOpenRes
	if err := Unmarshal(MustMarshal(legacyOpenRes{S: "s_1", Next: 8, LastInputSeq: 3}), &newResponse); err != nil {
		t.Fatal(err)
	}
	if newResponse.Kind != "" {
		t.Fatalf("legacy response kind = %q", newResponse.Kind)
	}
}

func TestAgentBindingSpecsAreWireCompatible(t *testing.T) {
	type legacyAgentSpec struct {
		Recipe    string   `cbor:"recipe"`
		Task      string   `cbor:"task"`
		Providers []string `cbor:"providers,omitempty"`
		Primary   string   `cbor:"primary,omitempty"`
		Sandbox   string   `cbor:"sandbox,omitempty"`
	}

	var old legacyAgentSpec
	if err := Unmarshal(MustMarshal(AgentSpec{
		Recipe: "codex", Task: "work", Providers: []string{"openai"}, Primary: "openai",
		BindingSpecs: []string{"b_team:openai"}, Sandbox: AgentSandboxWorkspaceWrite,
	}), &old); err != nil {
		t.Fatal(err)
	}
	if old.Recipe != "codex" || old.Primary != "openai" || old.Sandbox != AgentSandboxWorkspaceWrite {
		t.Fatalf("legacy agent spec = %+v", old)
	}

	var current AgentSpec
	if err := Unmarshal(MustMarshal(legacyAgentSpec{
		Recipe: "codex", Task: "work", Providers: []string{"openai"},
		Primary: "openai", Sandbox: AgentSandboxWorkspaceWrite,
	}), &current); err != nil {
		t.Fatal(err)
	}
	if len(current.BindingSpecs) != 0 {
		t.Fatalf("legacy binding specs = %v", current.BindingSpecs)
	}
}

func TestReleaseEpochIsWireCompatible(t *testing.T) {
	type legacyWorkspace struct {
		ID               string `cbor:"id"`
		Generation       uint64 `cbor:"gen"`
		ReleaseOperation string `cbor:"release_operation,omitempty"`
	}
	type legacyReleaseRequest struct {
		WS          string `cbor:"ws"`
		Gen         uint64 `cbor:"gen"`
		OperationID string `cbor:"operation,omitempty"`
	}

	var oldWorkspace legacyWorkspace
	if err := Unmarshal(MustMarshal(Workspace{
		ID: "ws_1", Generation: 4, ReleaseEpoch: 9, ReleaseOperation: "rel_9",
	}), &oldWorkspace); err != nil {
		t.Fatal(err)
	}
	if oldWorkspace.ID != "ws_1" || oldWorkspace.Generation != 4 || oldWorkspace.ReleaseOperation != "rel_9" {
		t.Fatalf("legacy workspace = %+v", oldWorkspace)
	}
	var currentWorkspace Workspace
	if err := Unmarshal(MustMarshal(legacyWorkspace{
		ID: "ws_1", Generation: 4, ReleaseOperation: "rel_legacy",
	}), &currentWorkspace); err != nil {
		t.Fatal(err)
	}
	if currentWorkspace.ReleaseEpoch != 0 {
		t.Fatalf("legacy workspace release epoch = %d", currentWorkspace.ReleaseEpoch)
	}

	var oldRequest legacyReleaseRequest
	if err := Unmarshal(MustMarshal(WSReleaseReq{
		WS: "ws_1", Gen: 4, ReleaseEpoch: 9, OperationID: "rel_9",
	}), &oldRequest); err != nil {
		t.Fatal(err)
	}
	if oldRequest.WS != "ws_1" || oldRequest.Gen != 4 || oldRequest.OperationID != "rel_9" {
		t.Fatalf("legacy release request = %+v", oldRequest)
	}
	var currentRequest WSReleaseReq
	if err := Unmarshal(MustMarshal(legacyReleaseRequest{
		WS: "ws_1", Gen: 4, OperationID: "rel_legacy",
	}), &currentRequest); err != nil {
		t.Fatal(err)
	}
	if currentRequest.ReleaseEpoch != 0 {
		t.Fatalf("legacy release request epoch = %d", currentRequest.ReleaseEpoch)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	for _, version := range []uint8{0, Version + 1} {
		b, err := cbor.Marshal(Frame{V: version, T: KindReq, Op: "test"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeFrame(b); err == nil {
			t.Fatalf("version %d was accepted", version)
		}
	}
}

func TestNegotiateCapabilities(t *testing.T) {
	got, err := NegotiateCapabilities([]string{"optional.future", CapabilityV1})
	if err != nil || !reflect.DeepEqual(got, []string{CapabilityV1}) {
		t.Fatalf("negotiated %v, %v", got, err)
	}
	if _, err := NegotiateCapabilities(nil); !errors.Is(err, &Error{Code: CodeUnsupported}) {
		t.Fatalf("missing baseline capability: %v", err)
	}
	// A named capability without the baseline is not a connection.
	if _, err := NegotiateCapabilities([]string{CapabilityAuthzPush}); !errors.Is(err, &Error{Code: CodeUnsupported}) {
		t.Fatalf("named capability without v1: %v", err)
	}
	// The result is canonical order, not the peer's order, and never echoes
	// identifiers this release does not implement.
	got, err = NegotiateCapabilities([]string{CapabilityEncryptedArtifacts, CapabilityAuthzPush, "x-vendor", CapabilityV1})
	if err != nil || !reflect.DeepEqual(got, []string{CapabilityV1, CapabilityAuthzPush}) {
		t.Fatalf("negotiated %v, %v", got, err)
	}
	full, err := NegotiateCapabilities(PeerCapabilities())
	if err != nil || !reflect.DeepEqual(full, PeerCapabilities()) {
		t.Fatalf("self-negotiation = %v, %v", full, err)
	}
}

// Every profile-required capability is one this release's peers actually
// offer, or the release would refuse itself under that profile.
func TestSecurityCapabilitiesAreImplemented(t *testing.T) {
	for _, profile := range []string{SecurityLocal, SecurityIsolated, SecurityMultiTenant, "", "unknown"} {
		required := SecurityCapabilities(profile)
		if missing := MissingCapabilities(PeerCapabilities(), required); len(missing) > 0 {
			t.Fatalf("profile %q requires unimplemented %v", profile, missing)
		}
		if profile == SecurityLocal || profile == "" || profile == "unknown" {
			if len(required) != 0 {
				t.Fatalf("profile %q requires %v; local must accept an old peer", profile, required)
			}
			continue
		}
		if len(required) == 0 {
			t.Fatalf("profile %q requires nothing", profile)
		}
		// An old peer offers only v1 and must be refused by these profiles.
		if missing := MissingCapabilities([]string{CapabilityV1}, required); !reflect.DeepEqual(missing, required) {
			t.Fatalf("profile %q: old peer missing %v, want %v", profile, missing, required)
		}
	}
	// Missing is reported in canonical order regardless of the required order.
	missing := MissingCapabilities([]string{CapabilityV1}, []string{CapabilityApprovals, CapabilityAuthzPush, CapabilityV1})
	if !reflect.DeepEqual(missing, []string{CapabilityAuthzPush, CapabilityApprovals}) {
		t.Fatalf("missing = %v", missing)
	}
	required := SecurityCapabilities(SecurityIsolated)
	required[0] = "mutated"
	if SecurityCapabilities(SecurityIsolated)[0] == "mutated" {
		t.Fatal("SecurityCapabilities returned shared storage")
	}
}

// The hello golden fixture pins how named capabilities travel on the wire:
// a plain ordered array of exact identifiers, so an old peer sees strings it
// does not know and simply never echoes them.
func TestV1HelloGoldenFixture(t *testing.T) {
	rawHex, err := os.ReadFile("testdata/v1-hello.hex")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(rawHex)))
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	var hello Hello
	if err := frame.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	wantCaps := []string{CapabilityV1, CapabilityAuthzPush, CapabilityControllerEpoch, CapabilitySessionCap, CapabilityChunkedArtifacts, CapabilityApprovals, CapabilityEncryptedArtifacts}
	if frame.T != KindHello || frame.ID != 1 || hello.Peer != "n_fixture" || hello.Role != RoleNode || !reflect.DeepEqual(hello.Caps, wantCaps) {
		t.Fatalf("fixture changed semantics: frame=%+v hello=%+v", frame, hello)
	}
	reencoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, raw) {
		t.Fatalf("v1 hello encoding changed:\n got %x\nwant %x", reencoded, raw)
	}
	negotiated, err := NegotiateCapabilities(hello.Caps)
	// The fixture is deliberately an older release. Newly implemented named
	// capabilities are not retroactively added to its bytes, and capabilities
	// it offered that this release retired are not echoed.
	wantNegotiated := []string{CapabilityV1, CapabilityAuthzPush, CapabilityControllerEpoch, CapabilitySessionCap, CapabilityChunkedArtifacts}
	if err != nil || !reflect.DeepEqual(negotiated, wantNegotiated) {
		t.Fatalf("fixture hello negotiated %v, %v", negotiated, err)
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
		{"unknown mode", EgressRule{ID: "x", Mode: "promptish", Protocol: "https", Hosts: []string{"example.com"}}},
		{"unknown shared state", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, SharedState: "mystery"}},
		{"connect byte claim", EgressRule{ID: "x", Protocol: "connect", Hosts: []string{"example.com"}, MaxResponseBytes: 1}},
		{"connect shared state", EgressRule{ID: "x", Protocol: "connect", Hosts: []string{"example.com"}, SharedState: SharedStateGlobalWrite}},
		{"immutable write", EgressRule{ID: "x", Protocol: "https", Hosts: []string{"example.com"}, Methods: []string{"POST"}, SharedState: SharedStateImmutableRead}},
		{"unknown connector", EgressRule{ID: "x", Connector: "shell", Protocol: "https", Hosts: []string{"example.com"}}},
		{"package over plaintext", EgressRule{ID: "x", Connector: EgressConnectorPackage, Protocol: "http", Hosts: []string{"example.com"}}},
		{"package mutable state", EgressRule{ID: "x", Connector: EgressConnectorPackage, Protocol: "https", Hosts: []string{"example.com"}, SharedState: SharedStateScopedWrite}},
		{"package request body", EgressRule{ID: "x", Connector: EgressConnectorPackage, Protocol: "https", Hosts: []string{"example.com"}, MaxRequestBytes: 1}},
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

func TestNormalizePackageConnectorDefaultsToImmutableReads(t *testing.T) {
	security, err := NormalizeSecurity(SecuritySpec{Network: NetworkPolicy{Rules: []EgressRule{{
		ID: "packages", Connector: " PACKAGE ", Protocol: "HTTPS", Hosts: []string{"registry.example"},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	rule := security.Network.Rules[0]
	if rule.Connector != EgressConnectorPackage || rule.SharedState != SharedStateImmutableRead ||
		!reflect.DeepEqual(rule.Methods, []string{"GET", "HEAD"}) {
		t.Fatalf("package connector normalized unsafely: %+v", rule)
	}
}
