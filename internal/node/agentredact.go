package node

import (
	"bytes"
	"encoding/json"
	"strconv"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/redact"
)

// redactedMark replaces every secret-shaped value in a recorded frame. It is
// plain ASCII with no JSON metacharacters, so substituting it inside a JSON
// string leaves the frame parseable.
const redactedMark = redact.Mark

// redactor scrubs transcript frames. The scrubbing itself lives in
// internal/redact so the broker's rejection bodies, audit reasons and wire
// errors are scrubbed by the same expressions; this type is the node-local
// spelling its transcript call sites already use.
//
// It knows the literals that must never leave the node (the broker token,
// lease placeholders and the secrets behind them) and falls back to shape
// matching for anything else. Values never legitimately appear in a
// transcript: the workspace holds only broker placeholders, and even those are
// replaced because a placeholder together with the broker token is a usable
// credential on this node.
type redactor struct {
	*redact.Redactor
}

// newRedactor builds a redactor for the given literals. Short literals are
// dropped: replacing every occurrence of a three-character string would
// destroy the transcript without protecting anything.
func newRedactor(literals []string) *redactor {
	return &redactor{Redactor: redact.NewRedactor(literals)}
}

// apply returns frame with every secret-shaped value replaced and whether
// anything changed. The input is not modified.
func (r *redactor) apply(frame []byte) ([]byte, bool) { return r.Redactor.Apply(frame) }

// boundFrame cuts a frame that exceeds proto.MaxACPTranscriptFrame. The
// result is still one JSON object carrying the envelope (id, method) so a
// reader can correlate it, plus how large the original was; the payload is
// gone rather than half-present, because a truncated JSON document is worse
// than none for every consumer.
func boundFrame(frame []byte) ([]byte, bool) {
	if len(frame) <= proto.MaxACPTranscriptFrame {
		return frame, false
	}
	var env struct {
		ID     json.RawMessage `json:"id,omitempty"`
		Method string          `json:"method,omitempty"`
	}
	_ = json.Unmarshal(frame, &env)
	var b bytes.Buffer
	b.WriteString(`{"jsonrpc":"2.0"`)
	if len(env.ID) > 0 && len(env.ID) < 256 {
		b.WriteString(`,"id":`)
		b.Write(env.ID)
	}
	if env.Method != "" {
		b.WriteString(`,"method":`)
		b.WriteString(strconv.Quote(env.Method))
	}
	b.WriteString(`,"_remount":{"truncated":true,"bytes":`)
	b.WriteString(strconv.Itoa(len(frame)))
	b.WriteString(`}}`)
	return b.Bytes(), true
}
