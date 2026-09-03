package node

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"

	"remount.dev/remount/internal/proto"
)

// redactedMark replaces every secret-shaped value in a recorded frame. It is
// plain ASCII with no JSON metacharacters, so substituting it inside a JSON
// string leaves the frame parseable.
const redactedMark = "[redacted]"

// secretPatterns are the shapes of well-known credentials plus the generic
// "key = value" forms a harness echoes when it prints its environment. Each
// pattern's last group is the part to replace; earlier groups are kept.
// Values never legitimately appear in a transcript: the workspace holds only
// broker placeholders, and even those are replaced because a placeholder
// together with the broker token is a usable credential on this node.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`()(sk-[A-Za-z0-9_-]{20,})`),                                                                           // OpenAI, Anthropic, Stripe
	regexp.MustCompile(`()(AKIA[0-9A-Z]{16})`),                                                                                // AWS access key id
	regexp.MustCompile(`()(gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})`),                                         // GitHub tokens
	regexp.MustCompile(`()(xox[abprs]-[A-Za-z0-9-]{10,})`),                                                                    // Slack
	regexp.MustCompile(`()(AIza[0-9A-Za-z_-]{35})`),                                                                           // Google API key
	regexp.MustCompile(`()(eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})`),                                   // JWT
	regexp.MustCompile(`(?i)(bearer\s+)([A-Za-z0-9._~+/=-]{16,})`),                                                            // Authorization header value
	regexp.MustCompile(`(https?://[^\s"'/]+/c/)([A-Za-z0-9._~-]{16,})`),                                                       // broker capability URL
	regexp.MustCompile(`(?i)((?:api[_-]?key|secret|token|password|passwd)(?:\\?["'])?\s*[:=]\s*(?:\\?["'])?)([^"'\s\\]{8,})`), // KEY=value, "key": "value", also JSON-escaped inside a string
}

// redactor scrubs transcript frames. It knows the literals that must never
// leave the node (the broker token, lease placeholders and the secrets behind
// them) and falls back to shape matching for anything else.
type redactor struct {
	literals [][]byte
}

// newRedactor builds a redactor for the given literals. Short literals are
// dropped: replacing every occurrence of a three-character string would
// destroy the transcript without protecting anything.
func newRedactor(literals []string) *redactor {
	r := &redactor{}
	for _, lit := range literals {
		if len(lit) < 8 {
			continue
		}
		r.literals = append(r.literals, []byte(lit))
		// The same value may be JSON-escaped inside a frame.
		if esc, err := json.Marshal(lit); err == nil {
			esc = esc[1 : len(esc)-1]
			if !bytes.Equal(esc, []byte(lit)) {
				r.literals = append(r.literals, esc)
			}
		}
	}
	return r
}

// apply returns frame with every secret-shaped value replaced and whether
// anything changed. The input is not modified.
func (r *redactor) apply(frame []byte) ([]byte, bool) {
	out := frame
	changed := false
	for _, lit := range r.literals {
		if bytes.Contains(out, lit) {
			out = bytes.ReplaceAll(out, lit, []byte(redactedMark))
			changed = true
		}
	}
	for _, re := range secretPatterns {
		if !re.Match(out) {
			continue
		}
		out = re.ReplaceAll(out, []byte("${1}"+redactedMark))
		changed = true
	}
	if !changed {
		return frame, false
	}
	return out, true
}

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
