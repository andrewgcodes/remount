package node

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestRedactorLiteralsAndShapes(t *testing.T) {
	r := newRedactor([]string{"cap-token-0123456789", "shrt", `quo"te-secret-value`})
	in := []byte(`{"text":"broker http://h/c/cap-token-0123456789/ key sk-abcdefghijklmnopqrstuvwxyz0123 ` +
		`Authorization: Bearer abcdefghijklmnopqrstu ghp_` + strings.Repeat("a", 36) + ` AKIAABCDEFGHIJKLMNOP ` +
		`\"api_key\": \"topsecretvalue\" password=hunter2hunter2 quo\"te-secret-value shrt plain words stay"}`)
	out, _ := r.apply(in)
	for _, leaked := range []string{"cap-token-0123456789", "sk-abcdefghijklmnopqrstuvwxyz0123", "abcdefghijklmnopqrstu", "ghp_aaaa", "AKIAABCDEFGHIJKLMNOP", "topsecretvalue", "hunter2hunter2", `quo\"te-secret-value`} {
		if bytes.Contains(out, []byte(leaked)) {
			t.Errorf("leaked %q in %s", leaked, out)
		}
	}
	for _, kept := range []string{"plain words stay", "shrt", "Bearer ", `\"api_key\": \"`} {
		if !bytes.Contains(out, []byte(kept)) {
			t.Errorf("over-redacted %q: %s", kept, out)
		}
	}
	if !json.Valid(out) {
		t.Fatalf("redaction broke the JSON: %s", out)
	}
	if got, changed := r.apply([]byte(`{"a":1}`)); changed || !bytes.Equal(got, []byte(`{"a":1}`)) {
		t.Fatalf("clean frame changed: %s", got)
	}
}

func TestBoundFrameKeepsEnvelopeAndValidity(t *testing.T) {
	small := []byte(`{"jsonrpc":"2.0","id":1,"method":"x"}`)
	if got, cut := boundFrame(small); cut || !bytes.Equal(got, small) {
		t.Fatalf("small frame changed: %s %v", got, cut)
	}
	big := []byte(`{"jsonrpc":"2.0","id":"req-7","method":"session/update","params":{"blob":"` + strings.Repeat("x", proto.MaxACPTranscriptFrame) + `"}}`)
	got, cut := boundFrame(big)
	if !cut || len(got) > 512 || !json.Valid(got) {
		t.Fatalf("bounded = %s cut=%v", got, cut)
	}
	var env struct {
		ID      string `json:"id"`
		Method  string `json:"method"`
		Remount struct {
			Truncated bool `json:"truncated"`
			Bytes     int  `json:"bytes"`
		} `json:"_remount"`
	}
	if err := json.Unmarshal(got, &env); err != nil || env.ID != "req-7" || env.Method != "session/update" || !env.Remount.Truncated || env.Remount.Bytes != len(big) {
		t.Fatalf("envelope = %+v err=%v", env, err)
	}
	// Garbage over the limit still yields a valid, bounded record.
	if got, cut := boundFrame(bytes.Repeat([]byte{'{'}, proto.MaxACPTranscriptFrame+1)); !cut || !json.Valid(got) {
		t.Fatalf("garbage bounded = %s", got)
	}
}
