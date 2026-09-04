package conformance

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// updateGolden regenerates the checked-in fixtures. It is an environment
// variable rather than a flag so a CI run cannot pass it by accident.
var updateGolden = os.Getenv("REMOUNT_CONFORMANCE_UPDATE_GOLDEN") == "1"

func goldenPath(name string) string { return filepath.Join("testdata", "golden", name) }

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := goldenPath(name)
	if updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden %s: %v (regenerate with REMOUNT_CONFORMANCE_UPDATE_GOLDEN=1)", name, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s drifted: the encoding of a v1 structure changed.\n got %x\nwant %x", name, got, want)
	}
}

// goldenRequestFrame is a fixed v1 request frame. Its bytes are the contract:
// if this encoding changes, every peer that pinned v1 is affected, which is
// exactly what §13 says requires a version bump rather than a quiet edit.
func goldenRequestFrame() *Frame {
	body, err := Marshal(WSGetReq{ID: "ws_conformance_golden"})
	if err != nil {
		panic(err)
	}
	return &Frame{V: FrameVersion, T: KindReq, ID: 7, To: PeerControl, Op: "ws.get", Body: body}
}

func goldenHello() Hello {
	return Hello{
		Peer: "c_conformance_golden",
		Role: "client",
		Caps: append([]string(nil), KnownCapabilities...),
	}
}

func goldenInfoChunk() *Frame {
	payload, err := Marshal(SessionInfo{
		ID: "s_conformance_golden", WS: "ws_conformance_golden",
		Kind: "exec", Program: []string{"/bin/echo", "conformance"}, OpenedAt: 1767225600000,
	})
	if err != nil {
		panic(err)
	}
	body, err := Marshal(ChunkBody{St: ChunkInfo, D: payload})
	if err != nil {
		panic(err)
	}
	return &Frame{V: FrameVersion, T: KindChunk, S: "s_conformance_golden", WS: "ws_conformance_golden", Seq: 0, Body: body}
}

func TestGoldenV1FramesReEncodeByteForByte(t *testing.T) {
	req, err := EncodeFrame(goldenRequestFrame())
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "req-frame.cbor", req)

	hello, err := Marshal(goldenHello())
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "hello-all-capabilities.cbor", hello)

	chunk, err := EncodeFrame(goldenInfoChunk())
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "chunk-info.cbor", chunk)
}

// The golden manifest is the contract in JSON. A requirement that changes
// meaning without changing its version, or a tier that changes silently,
// shows up here.
func TestGoldenManifestIsUnchanged(t *testing.T) {
	body, err := json.MarshalIndent(Standard(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "manifest.json", append(body, '\n'))
}

func TestGoldenFramesDecodeToTheSameValues(t *testing.T) {
	raw, err := os.ReadFile(goldenPath("req-frame.cbor"))
	if err != nil {
		t.Skipf("golden not generated yet: %v", err)
	}
	f, err := DecodeFrame(raw)
	if err != nil {
		t.Fatalf("the golden request frame no longer decodes: %v", err)
	}
	if f.T != KindReq || f.Op != "ws.get" || f.To != PeerControl || f.ID != 7 {
		t.Fatalf("the golden request frame decoded to %+v", f)
	}
	var req WSGetReq
	if err := Unmarshal(f.Body, &req); err != nil {
		t.Fatal(err)
	}
	if req.ID != "ws_conformance_golden" {
		t.Fatalf("the golden body decoded to %q", req.ID)
	}
}

func TestDecodeFrameRejectsAnUnnegotiatedVersion(t *testing.T) {
	f := goldenRequestFrame()
	f.V = FrameVersion + 1
	raw, err := EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeFrame(raw); err == nil {
		t.Fatal("DecodeFrame accepted a frame whose version was never negotiated")
	}
}

func TestDecodeFrameRejectsAFrameWithNoKind(t *testing.T) {
	raw, err := Marshal(map[string]any{"v": FrameVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeFrame(raw); err == nil {
		t.Fatal("DecodeFrame accepted a frame carrying no kind")
	}
}

// §2 rule 2: unknown fields are ignored within a negotiated version. A
// conformance client that broke on one would reject a newer, compatible peer.
func TestUnknownFieldsAreIgnoredWithinAVersion(t *testing.T) {
	raw, err := Marshal(map[string]any{
		"v": FrameVersion, "t": KindRes, "id": uint64(9),
		"x_field_from_a_later_release": "ignored",
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := DecodeFrame(raw)
	if err != nil {
		t.Fatalf("a frame carrying an additive field was rejected: %v", err)
	}
	if f.ID != 9 {
		t.Fatalf("the known fields did not survive: %+v", f)
	}
}

func TestLinkURLDerivation(t *testing.T) {
	for in, want := range map[string]string{
		"http://127.0.0.1:7443":  "ws://127.0.0.1:7443/v1/link",
		"https://example.test/":  "wss://example.test/v1/link",
		"http://example.test///": "ws://example.test///v1/link",
	} {
		if got := linkURL(in); got != want {
			t.Errorf("linkURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStableErrorCodeHelpers(t *testing.T) {
	err := error(&WireError{Code: "conflict", Msg: "already decided"})
	if !IsCode(err, "conflict") {
		t.Error("IsCode did not match a wire error's code")
	}
	if CodeOf(err) != "conflict" {
		t.Errorf("CodeOf returned %q", CodeOf(err))
	}
	if !KnownCode("resource_exhausted") {
		t.Error("resource_exhausted is one of §2's stable codes")
	}
	if KnownCode("teapot") {
		t.Error("KnownCode accepted an invented code")
	}
	if CodeOf(os.ErrNotExist) != "" {
		t.Error("CodeOf invented a code for a non-protocol error")
	}
}
