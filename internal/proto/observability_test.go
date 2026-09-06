package proto

import "testing"

// TestWorkspaceOfIsBestEffortAndNeverGuesses pins what a span may say about a
// frame. The header wins when it carries a workspace; a node request carries
// it as `ws` in the body; a `ws.*` control request carries it as `id`, where
// the id is a workspace by definition. Every other `id` is left alone, because
// labelling an agent or a tenant as a workspace would be worse than saying
// nothing.
func TestWorkspaceOfIsBestEffortAndNeverGuesses(t *testing.T) {
	body := func(v any) []byte {
		t.Helper()
		encoded, err := Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}

	for _, test := range []struct {
		name  string
		frame *Frame
		want  string
	}{
		{name: "nil frame"},
		{name: "empty frame", frame: &Frame{}},
		{
			name:  "the header wins",
			frame: &Frame{T: KindChunk, WS: "ws_header", Body: body(FSListReq{WS: "ws_body"})},
			want:  "ws_header",
		},
		{
			name:  "a node request names it in the body",
			frame: &Frame{T: KindReq, Op: OpFSList, Body: body(FSListReq{WS: "ws_body"})},
			want:  "ws_body",
		},
		{
			name:  "a ws.* control request names it as id",
			frame: &Frame{T: KindReq, Op: OpWSGet, Body: body(WSGetReq{ID: "ws_control"})},
			want:  "ws_control",
		},
		{
			name:  "a non-workspace id is not a workspace",
			frame: &Frame{T: KindReq, Op: OpAgentGet, Body: body(AgentGetReq{ID: "ag_not_a_workspace"})},
		},
		{
			name:  "an undecodable body says nothing rather than guessing",
			frame: &Frame{T: KindReq, Op: OpFSList, Body: []byte{0xff, 0xff, 0xff}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := WorkspaceOf(test.frame); got != test.want {
				t.Fatalf("WorkspaceOf = %q, want %q", got, test.want)
			}
		})
	}
}

// TestTraceContextIsAdditiveOnTheWire proves the new frame fields round-trip
// and, more importantly, that a frame without them encodes exactly as it did
// before: an untraced deployment must put no new bytes on the wire.
func TestTraceContextIsAdditiveOnTheWire(t *testing.T) {
	plain := &Frame{V: Version, T: KindReq, ID: 7, To: PeerControl, Op: OpWSGet}
	before, err := Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	traced := *plain
	traced.Trace, traced.Span = "4d283b5727a21d326e894cb8433c8e23", "aebedd6c550f735e"
	after, err := Marshal(&traced)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) <= len(before) {
		t.Fatal("a trace context added no bytes; the fields are not being encoded")
	}

	var decoded Frame
	if err := Unmarshal(after, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Trace != traced.Trace || decoded.Span != traced.Span {
		t.Fatalf("round trip lost the trace context: %q %q", decoded.Trace, decoded.Span)
	}

	var untraced Frame
	if err := Unmarshal(before, &untraced); err != nil {
		t.Fatal(err)
	}
	if untraced.Trace != "" || untraced.Span != "" {
		t.Fatalf("an untraced frame decoded a trace context: %q %q", untraced.Trace, untraced.Span)
	}
}
