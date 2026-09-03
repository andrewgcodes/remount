// Package yamlite parses the YAML subset Remount recipes are written in.
//
// The binary is static and dependency-light by policy, so recipes do not pull
// in a full YAML implementation. What is supported is exactly what a recipe
// needs and nothing that would surprise a YAML author who stays inside it:
//
//   - block mappings (`key: value`) and block sequences (`- item`) nested by
//     indentation;
//   - flow sequences of scalars on one line (`[a, "b", c]`);
//   - plain, single-quoted and double-quoted scalars (double quotes honor
//     \n \t \" \\ escapes);
//   - literal (`|`, `|-`) and folded (`>`, `>-`) block scalars;
//   - `#` comments, `---` document start, blank lines;
//   - `true`/`false`, integers and `null`/`~` as typed scalars when unquoted.
//
// Anchors, aliases, tags, multi-document files, flow mappings and complex keys
// are rejected with a line-numbered error rather than misread. A recipe that
// needs one of those is a recipe that should be simpler.
package yamlite

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Unmarshal parses src and stores the result in v using encoding/json field
// rules, so recipe structs carry ordinary `json:"..."` tags.
func Unmarshal(src []byte, v any) error {
	tree, err := Parse(src)
	if err != nil {
		return err
	}
	b, err := json.Marshal(tree)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("yamlite: %w", err)
	}
	return nil
}

// Parse returns the document as nested map[string]any, []any and scalars
// (string, bool, int64, nil).
func Parse(src []byte) (any, error) {
	p := &parser{}
	for i, raw := range strings.Split(strings.ReplaceAll(string(src), "\r\n", "\n"), "\n") {
		p.lines = append(p.lines, line{no: i + 1, raw: raw})
	}
	p.skipBlank()
	if p.pos < len(p.lines) && strings.TrimSpace(p.lines[p.pos].raw) == "---" {
		p.pos++
	}
	p.skipBlank()
	if p.pos >= len(p.lines) {
		return map[string]any{}, nil
	}
	v, err := p.parseNode(p.indent(p.pos))
	if err != nil {
		return nil, err
	}
	p.skipBlank()
	if p.pos < len(p.lines) {
		return nil, p.errorf(p.pos, "unexpected content at indent %d", p.indent(p.pos))
	}
	return v, nil
}

type line struct {
	no  int
	raw string
}

type parser struct {
	lines []line
	pos   int
}

func (p *parser) errorf(at int, format string, args ...any) error {
	no := 0
	if at < len(p.lines) {
		no = p.lines[at].no
	}
	return fmt.Errorf("yamlite: line %d: %s", no, fmt.Sprintf(format, args...))
}

// isBlank ignores empty lines and whole-line comments; both are structurally
// invisible in the supported subset.
func isBlank(raw string) bool {
	t := strings.TrimSpace(raw)
	return t == "" || strings.HasPrefix(t, "#")
}

func (p *parser) skipBlank() {
	for p.pos < len(p.lines) && isBlank(p.lines[p.pos].raw) {
		p.pos++
	}
}

func (p *parser) indent(i int) int {
	raw := p.lines[i].raw
	n := 0
	for n < len(raw) && raw[n] == ' ' {
		n++
	}
	if n < len(raw) && raw[n] == '\t' {
		return -1
	}
	return n
}

func (p *parser) parseNode(indent int) (any, error) {
	p.skipBlank()
	if p.pos >= len(p.lines) {
		return nil, nil
	}
	if p.indent(p.pos) < 0 {
		return nil, p.errorf(p.pos, "tabs are not allowed for indentation")
	}
	content := strings.TrimLeft(p.lines[p.pos].raw, " ")
	if content == "-" || strings.HasPrefix(content, "- ") {
		return p.parseSequence(indent)
	}
	if _, _, ok := splitKey(content); ok {
		return p.parseMapping(indent)
	}
	// A bare scalar document or a scalar continuation line.
	v, err := scalar(stripComment(content), p.pos, p)
	if err != nil {
		return nil, err
	}
	p.pos++
	return v, nil
}

func (p *parser) parseMapping(indent int) (any, error) {
	out := map[string]any{}
	for {
		p.skipBlank()
		if p.pos >= len(p.lines) {
			break
		}
		ind := p.indent(p.pos)
		if ind < indent {
			break
		}
		if ind > indent {
			return nil, p.errorf(p.pos, "unexpected indent %d (expected %d)", ind, indent)
		}
		content := strings.TrimLeft(p.lines[p.pos].raw, " ")
		if strings.HasPrefix(content, "- ") || content == "-" {
			return nil, p.errorf(p.pos, "sequence item where a mapping key was expected")
		}
		key, rest, ok := splitKey(content)
		if !ok {
			return nil, p.errorf(p.pos, "expected `key: value`")
		}
		if _, dup := out[key]; dup {
			return nil, p.errorf(p.pos, "duplicate key %q", key)
		}
		val, err := p.parseValue(rest, indent)
		if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (p *parser) parseSequence(indent int) (any, error) {
	out := []any{}
	for {
		p.skipBlank()
		if p.pos >= len(p.lines) {
			break
		}
		ind := p.indent(p.pos)
		if ind < indent {
			break
		}
		if ind > indent {
			return nil, p.errorf(p.pos, "unexpected indent %d (expected %d)", ind, indent)
		}
		content := strings.TrimLeft(p.lines[p.pos].raw, " ")
		if content != "-" && !strings.HasPrefix(content, "- ") {
			break
		}
		afterDash := strings.TrimPrefix(content, "-")
		rest := strings.TrimSpace(afterDash)
		if rest == "" || strings.HasPrefix(rest, "#") {
			// Nested block on the following lines.
			p.pos++
			p.skipBlank()
			if p.pos >= len(p.lines) || p.indent(p.pos) <= indent {
				out = append(out, nil)
				continue
			}
			v, err := p.parseNode(p.indent(p.pos))
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			continue
		}
		if _, _, ok := splitKey(rest); ok && !isQuoted(rest) && !strings.HasPrefix(rest, "[") {
			// `- key: value` starts an inline mapping whose further keys are
			// indented to the column where `key` begins.
			inner := indent + 1 + (len(afterDash) - len(strings.TrimLeft(afterDash, " ")))
			p.lines[p.pos].raw = strings.Repeat(" ", inner) + rest
			v, err := p.parseMapping(inner)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			continue
		}
		v, err := p.parseValue(rest, indent)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// parseValue handles the text after `key:` or `- `: an inline scalar, a flow
// sequence, a block scalar indicator, or nothing (a nested block follows).
func (p *parser) parseValue(rest string, indent int) (any, error) {
	at := p.pos
	rest = strings.TrimSpace(rest)
	switch {
	case rest == "" || strings.HasPrefix(rest, "#"):
		p.pos++
		p.skipBlank()
		if p.pos >= len(p.lines) || p.indent(p.pos) <= indent {
			// `key:` with nothing nested is null; but a sequence at the same
			// indent as its parent key is the common YAML idiom.
			if p.pos < len(p.lines) && p.indent(p.pos) == indent {
				c := strings.TrimLeft(p.lines[p.pos].raw, " ")
				if c == "-" || strings.HasPrefix(c, "- ") {
					return p.parseSequence(indent)
				}
			}
			return nil, nil
		}
		return p.parseNode(p.indent(p.pos))
	case rest == "|" || rest == "|-" || rest == ">" || rest == ">-" || rest == "|+" || rest == ">+":
		p.pos++
		return p.blockScalar(rest, indent)
	case strings.HasPrefix(rest, "["):
		p.pos++
		return flowSequence(stripComment(rest), at, p)
	case strings.HasPrefix(rest, "{"):
		return nil, p.errorf(at, "flow mappings are not supported; use block style")
	case strings.HasPrefix(rest, "&") || strings.HasPrefix(rest, "*") || strings.HasPrefix(rest, "!"):
		return nil, p.errorf(at, "anchors, aliases and tags are not supported")
	}
	p.pos++
	return scalar(stripComment(rest), at, p)
}

func (p *parser) blockScalar(indicator string, parentIndent int) (any, error) {
	var body []string
	blockIndent := -1
	for p.pos < len(p.lines) {
		raw := p.lines[p.pos].raw
		if strings.TrimSpace(raw) == "" {
			body = append(body, "")
			p.pos++
			continue
		}
		ind := p.indent(p.pos)
		if ind < 0 {
			return nil, p.errorf(p.pos, "tabs are not allowed for indentation")
		}
		if blockIndent < 0 {
			if ind <= parentIndent {
				break
			}
			blockIndent = ind
		}
		if ind < blockIndent {
			break
		}
		body = append(body, raw[blockIndent:])
		p.pos++
	}
	// Trailing blank lines belong to the chomping indicator, not the block.
	trailing := 0
	for len(body) > 0 && body[len(body)-1] == "" {
		body = body[:len(body)-1]
		trailing++
	}
	if blockIndent < 0 && len(body) == 0 {
		return "", nil
	}
	var text string
	if indicator[0] == '|' {
		text = strings.Join(body, "\n")
	} else {
		text = fold(body)
	}
	switch indicator {
	case "|", ">":
		text += "\n"
	case "|+", ">+":
		text += "\n" + strings.Repeat("\n", trailing)
	}
	return text, nil
}

// fold joins lines with spaces, keeping paragraph breaks and more-indented
// lines verbatim, which is the part of `>` semantics recipes rely on.
func fold(body []string) string {
	var b strings.Builder
	for i, l := range body {
		if i == 0 {
			b.WriteString(l)
			continue
		}
		prev := body[i-1]
		switch {
		case l == "":
			b.WriteString("\n")
		case prev == "" || strings.HasPrefix(l, " ") || strings.HasPrefix(prev, " "):
			if prev != "" {
				b.WriteString("\n")
			}
			b.WriteString(l)
		default:
			b.WriteString(" ")
			b.WriteString(l)
		}
	}
	return b.String()
}

// splitKey recognizes `key: rest` and `key:`; the key is plain or quoted and
// may not contain a colon unless quoted.
func splitKey(content string) (key, rest string, ok bool) {
	if content == "" {
		return "", "", false
	}
	if content[0] == '"' || content[0] == '\'' {
		end := closingQuote(content)
		if end < 0 || end+1 >= len(content) || content[end+1] != ':' {
			return "", "", false
		}
		k, err := unquote(content[:end+1])
		if err != nil {
			return "", "", false
		}
		rest = content[end+2:]
		if rest != "" && rest[0] != ' ' {
			return "", "", false
		}
		return k, rest, true
	}
	if content[0] == '[' || content[0] == '{' || content[0] == '-' && (len(content) == 1 || content[1] == ' ') {
		return "", "", false
	}
	for i := 0; i < len(content); i++ {
		c := content[i]
		if c == '#' && i > 0 && content[i-1] == ' ' {
			return "", "", false
		}
		if c == ':' && (i+1 == len(content) || content[i+1] == ' ') {
			key = strings.TrimSpace(content[:i])
			if key == "" || strings.ContainsAny(key, "\"'") {
				return "", "", false
			}
			return key, content[i+1:], true
		}
	}
	return "", "", false
}

func closingQuote(s string) int {
	q := s[0]
	for i := 1; i < len(s); i++ {
		if q == '"' && s[i] == '\\' {
			i++
			continue
		}
		if s[i] == q {
			if q == '\'' && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			return i
		}
	}
	return -1
}

func isQuoted(s string) bool {
	return len(s) > 0 && (s[0] == '"' || s[0] == '\'')
}

// stripComment removes a trailing ` # comment` from an unquoted scalar and
// from after a closing quote; a `#` inside quotes is content.
func stripComment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	if isQuoted(s) {
		end := closingQuote(s)
		if end < 0 {
			return s
		}
		tail := strings.TrimSpace(s[end+1:])
		if tail == "" || strings.HasPrefix(tail, "#") {
			return s[:end+1]
		}
		return s
	}
	if i := strings.Index(s, " #"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	if strings.HasPrefix(s, "#") {
		return ""
	}
	return s
}

func unquote(s string) (string, error) {
	if s[0] == '\'' {
		if len(s) < 2 || s[len(s)-1] != '\'' {
			return "", fmt.Errorf("unterminated single-quoted string")
		}
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), nil
	}
	if len(s) < 2 || s[len(s)-1] != '"' {
		return "", fmt.Errorf("unterminated double-quoted string")
	}
	var b strings.Builder
	inner := s[1 : len(s)-1]
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(inner) {
			return "", fmt.Errorf("dangling escape")
		}
		switch inner[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case '"':
			b.WriteByte('"')
		case '\\':
			b.WriteByte('\\')
		case '/':
			b.WriteByte('/')
		case '0':
			b.WriteByte(0)
		default:
			return "", fmt.Errorf("unsupported escape \\%c", inner[i])
		}
	}
	return b.String(), nil
}

// scalar types an unquoted token the way YAML 1.2 core schema would for the
// values recipes use; anything else is a string.
func scalar(s string, at int, p *parser) (any, error) {
	if s == "" {
		return nil, nil
	}
	if isQuoted(s) {
		end := closingQuote(s)
		if end != len(s)-1 {
			return nil, p.errorf(at, "unexpected text after closing quote")
		}
		v, err := unquote(s)
		if err != nil {
			return nil, p.errorf(at, "%v", err)
		}
		return v, nil
	}
	switch s {
	case "true", "True", "TRUE":
		return true, nil
	case "false", "False", "FALSE":
		return false, nil
	case "null", "Null", "NULL", "~":
		return nil, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	if strings.HasPrefix(s, "&") || strings.HasPrefix(s, "*") || strings.HasPrefix(s, "!") {
		return nil, p.errorf(at, "anchors, aliases and tags are not supported")
	}
	return s, nil
}

func flowSequence(s string, at int, p *parser) (any, error) {
	if !strings.HasSuffix(s, "]") {
		return nil, p.errorf(at, "flow sequence must close on the same line")
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	out := []any{}
	if inner == "" {
		return out, nil
	}
	for len(inner) > 0 {
		inner = strings.TrimLeft(inner, " ")
		var tok string
		if isQuoted(inner) {
			end := closingQuote(inner)
			if end < 0 {
				return nil, p.errorf(at, "unterminated quote in flow sequence")
			}
			tok = inner[:end+1]
			inner = strings.TrimLeft(inner[end+1:], " ")
			if inner != "" && inner[0] != ',' {
				return nil, p.errorf(at, "expected ',' in flow sequence")
			}
		} else {
			i := strings.IndexByte(inner, ',')
			if i < 0 {
				i = len(inner)
			}
			tok = strings.TrimSpace(inner[:i])
			if strings.ContainsAny(tok, "[]{}") {
				return nil, p.errorf(at, "nested flow collections are not supported")
			}
			inner = inner[i:]
		}
		v, err := scalar(tok, at, p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		inner = strings.TrimPrefix(inner, ",")
	}
	return out, nil
}
