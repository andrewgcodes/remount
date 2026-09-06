// Package redact scrubs credential-shaped values out of text that is about to
// leave the node: an agent transcript, a broker rejection body, an audit
// reason, a wire error message.
//
// It exists because the same scrubbing is needed in more than one place and
// getting it wrong is silent. A workspace never legitimately holds a real
// secret, so anything credential-shaped in caller-facing text is either a
// harness echoing its environment or a defect; either way it must not be
// recorded or returned. Redaction is defence in depth, not the control that
// keeps secrets out of the audit trail: the broker records binding ids and
// decision classes and never carries a secret value in the first place.
//
// A scan that finds nothing and a scan that cannot work look identical, so
// every consumer of this package must prove its scan by planting a synthetic
// canary the same scan has to find. Never plant a real credential.
package redact

import (
	"bytes"
	"encoding/json"
	"regexp"
)

// Mark replaces every secret-shaped value. It is plain ASCII with no JSON
// metacharacters, so substituting it inside a JSON string leaves the document
// parseable.
const Mark = "[redacted]"

// minLiteral is the shortest literal worth replacing. Replacing every
// occurrence of a three-character string would destroy the text without
// protecting anything.
const minLiteral = 8

// patterns are the shapes of well-known credentials plus the generic
// "key = value" forms a harness echoes when it prints its environment. Each
// pattern's last group is the part to replace; earlier groups are kept.
var patterns = []*regexp.Regexp{
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

// Patterns returns a copy of the credential shapes this package matches.
// Callers that need the expressions themselves (a conformance scan, or a test
// proving its own instrument works) get their own slice; the package's
// expressions are never handed out for mutation.
func Patterns() []*regexp.Regexp {
	return append([]*regexp.Regexp(nil), patterns...)
}

// Bytes returns b with every secret-shaped value replaced, and whether
// anything changed. The input is not modified.
func Bytes(b []byte) ([]byte, bool) {
	out := b
	changed := false
	for _, re := range patterns {
		if !re.Match(out) {
			continue
		}
		out = re.ReplaceAll(out, []byte("${1}"+Mark))
		changed = true
	}
	if !changed {
		return b, false
	}
	return out, true
}

// String returns s with every secret-shaped value replaced. It is the form
// caller-facing text uses: a rejection body, an audit reason, a wire error.
func String(s string) string {
	if s == "" {
		return s
	}
	out, changed := Bytes([]byte(s))
	if !changed {
		return s
	}
	return string(out)
}

// Redactor scrubs text that has known literals as well as unknown shapes. It
// knows the values that must never leave the node (a broker capability, lease
// placeholders and the secrets behind them) and falls back to shape matching
// for anything else.
type Redactor struct {
	literals [][]byte
}

// NewRedactor builds a Redactor for the given literals. Literals shorter than
// eight bytes are dropped.
func NewRedactor(literals []string) *Redactor {
	r := &Redactor{}
	for _, lit := range literals {
		if len(lit) < minLiteral {
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

// Apply returns frame with every known literal and every secret-shaped value
// replaced, and whether anything changed. The input is not modified.
func (r *Redactor) Apply(frame []byte) ([]byte, bool) {
	out := frame
	changed := false
	for _, lit := range r.literals {
		if bytes.Contains(out, lit) {
			out = bytes.ReplaceAll(out, lit, []byte(Mark))
			changed = true
		}
	}
	if scrubbed, patternChanged := Bytes(out); patternChanged {
		out, changed = scrubbed, true
	}
	if !changed {
		return frame, false
	}
	return out, true
}

// String returns s with this Redactor's literals and every secret-shaped
// value replaced.
func (r *Redactor) String(s string) string {
	if s == "" {
		return s
	}
	out, changed := r.Apply([]byte(s))
	if !changed {
		return s
	}
	return string(out)
}
