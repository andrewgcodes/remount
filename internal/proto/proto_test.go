package proto

import (
	"errors"
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
