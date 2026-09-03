// Command acpgen turns the vendored Agent Client Protocol JSON schema
// (spec/acp/schema.json, spec/acp/meta.json) into Go types and method
// names for internal/acp. It is deliberately small: objects become structs,
// string enumerations become string types with constants, tagged unions
// become a discriminator plus the raw variant with typed accessors, and
// anything it does not understand becomes json.RawMessage so a newer schema
// never produces code that fails to build.
//
//	go run ./cmd/acpgen            # rewrite internal/acp/*_gen.go
//	go run ./cmd/acpgen -check     # exit 1 when the committed output is stale
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type schema struct {
	Title                string             `json:"title"`
	Description          string             `json:"description"`
	Type                 json.RawMessage    `json:"type"`
	Ref                  string             `json:"$ref"`
	AllOf                []*schema          `json:"allOf"`
	AnyOf                []*schema          `json:"anyOf"`
	OneOf                []*schema          `json:"oneOf"`
	Properties           map[string]*schema `json:"properties"`
	Required             []string           `json:"required"`
	Items                *schema            `json:"items"`
	AdditionalProperties json.RawMessage    `json:"additionalProperties"`
	Const                json.RawMessage    `json:"const"`
	Format               string             `json:"format"`
	Defs                 map[string]*schema `json:"$defs"`
}

// types returns the JSON types named by "type", which may be a string or a
// list such as ["string", "null"].
func (s *schema) types() []string {
	if len(s.Type) == 0 {
		return nil
	}
	var one string
	if json.Unmarshal(s.Type, &one) == nil {
		return []string{one}
	}
	var many []string
	if json.Unmarshal(s.Type, &many) == nil {
		return many
	}
	return nil
}

func (s *schema) hasType(t string) bool {
	for _, x := range s.types() {
		if x == t {
			return true
		}
	}
	return false
}

func (s *schema) refName() string {
	if s.Ref != "" {
		return strings.TrimPrefix(s.Ref, "#/$defs/")
	}
	if len(s.AllOf) == 1 && s.AllOf[0].Ref != "" {
		return s.AllOf[0].Ref[len("#/$defs/"):]
	}
	return ""
}

// nullable reports whether the schema is anyOf [X, null] and returns X.
func (s *schema) nullable() (*schema, bool) {
	if len(s.AnyOf) != 2 {
		return nil, false
	}
	for i, v := range s.AnyOf {
		if v.hasType("null") {
			return s.AnyOf[1-i], true
		}
	}
	return nil, false
}

type gen struct {
	defs   map[string]*schema
	buf    bytes.Buffer
	kinds  map[string]string // def name -> struct | enum | union | scalar | raw
	unions map[string]*union
}

type union struct {
	tag      string
	variants []variant
}

type variant struct {
	konst string // tag value; "" for the untagged fallback
	ref   string // referenced def
	name  string // Go-friendly variant name
	doc   string
}

var initialisms = map[string]string{"id": "ID", "url": "URL", "uri": "URI", "http": "HTTP", "sse": "SSE", "mcp": "MCP", "json": "JSON", "acp": "ACP", "api": "API"}

// goName converts a JSON property or constant to an exported Go identifier.
func goName(s string) string {
	var out strings.Builder
	upper := true
	var word strings.Builder
	flush := func() {
		w := word.String()
		if w == "" {
			return
		}
		if init, ok := initialisms[strings.ToLower(w)]; ok {
			out.WriteString(init)
		} else {
			out.WriteString(strings.ToUpper(w[:1]) + w[1:])
		}
		word.Reset()
	}
	for _, r := range s {
		switch {
		case r == '_' || r == '-' || r == '/' || r == ' ' || r == '$':
			flush()
			upper = true
		case unicode.IsUpper(r) && word.Len() > 0:
			flush()
			word.WriteRune(r)
		default:
			_ = upper
			word.WriteRune(r)
		}
	}
	flush()
	return out.String()
}

func doc(w *bytes.Buffer, name, description string) {
	description = strings.TrimSpace(description)
	if description == "" {
		fmt.Fprintf(w, "// %s is defined by the ACP schema.\n", name)
		return
	}
	para := description
	if i := strings.Index(para, "\n\n"); i >= 0 {
		para = para[:i]
	}
	para = strings.Join(strings.Fields(para), " ")
	first := name
	if !strings.HasPrefix(para, name) {
		first = name + ": " + para
	} else {
		first = para
	}
	for _, line := range wrap(first, 76) {
		fmt.Fprintf(w, "// %s\n", line)
	}
}

func wrap(s string, width int) []string {
	var lines []string
	var cur strings.Builder
	for _, f := range strings.Fields(s) {
		if cur.Len() > 0 && cur.Len()+1+len(f) > width {
			lines = append(lines, cur.String())
			cur.Reset()
		}
		if cur.Len() > 0 {
			cur.WriteByte(' ')
		}
		cur.WriteString(f)
	}
	if cur.Len() > 0 {
		lines = append(lines, cur.String())
	}
	return lines
}

func (g *gen) classify() {
	g.kinds = map[string]string{}
	g.unions = map[string]*union{}
	names := make([]string, 0, len(g.defs))
	for n := range g.defs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		s := g.defs[name]
		switch {
		case len(s.OneOf) > 0 && allStringConsts(s.OneOf), len(s.AnyOf) > 0 && allStringConsts(s.AnyOf):
			g.kinds[name] = "enum"
		case len(s.OneOf) > 0 || len(s.AnyOf) > 0:
			if u := g.union(name, s); u != nil {
				g.kinds[name] = "union"
				g.unions[name] = u
			} else if allIntegerConsts(s.AnyOf) || allIntegerConsts(s.OneOf) {
				g.kinds[name] = "intenum"
			} else {
				g.kinds[name] = "raw"
			}
		case s.hasType("object") && len(s.Properties) > 0:
			g.kinds[name] = "struct"
		case s.hasType("string"):
			g.kinds[name] = "string"
		case s.hasType("integer"):
			g.kinds[name] = "int"
		default:
			g.kinds[name] = "raw"
		}
	}
}

// allStringConsts accepts string enumerations, including open ones whose
// last variant is a plain string standing for "other".
func allStringConsts(vs []*schema) bool {
	consts := 0
	for _, v := range vs {
		if !v.hasType("string") || len(v.Properties) > 0 {
			return false
		}
		if len(v.Const) > 0 {
			consts++
		}
	}
	return consts > 0 && consts >= len(vs)-1
}

func allIntegerConsts(vs []*schema) bool {
	if len(vs) == 0 {
		return false
	}
	for _, v := range vs {
		if !v.hasType("integer") {
			return false
		}
	}
	return true
}

// union recognizes oneOf/anyOf lists whose variants are objects carrying at
// most one constant discriminator property. A variant's payload is the def
// it references, an inline struct synthesized from its other properties, or
// nothing. Untagged variants (no constant) are the fallback for an empty Kind
// and are dropped when they have no typed payload.
func (g *gen) union(name string, s *schema) *union {
	vs := s.OneOf
	if vs == nil {
		vs = s.AnyOf
	}
	u := &union{}
	seen := map[string]bool{}
	type pending struct {
		name string
		s    *schema
	}
	var synth []pending
	for _, v := range vs {
		if !v.hasType("object") && v.refName() == "" {
			return nil
		}
		var tag, konst string
		inline := &schema{Description: v.Description, Properties: map[string]*schema{}, Required: nil}
		for pn, p := range v.Properties {
			if len(p.Const) > 0 {
				var c string
				if json.Unmarshal(p.Const, &c) != nil {
					return nil
				}
				if tag != "" {
					return nil
				}
				tag, konst = pn, c
				continue
			}
			inline.Properties[pn] = p
		}
		for _, r := range v.Required {
			if r != tag {
				inline.Required = append(inline.Required, r)
			}
		}
		if tag != "" {
			if u.tag != "" && u.tag != tag {
				return nil
			}
			u.tag = tag
		}
		vname := goName(konst)
		if konst == "" {
			vname = goName(v.Title)
		}
		ref := v.refName()
		switch {
		case ref != "":
			if g.defs[ref] == nil || len(inline.Properties) > 0 {
				return nil
			}
		case len(inline.Properties) > 0:
			if vname == "" {
				return nil
			}
			ref = name + vname
			synth = append(synth, pending{ref, inline})
		case konst == "":
			continue
		}
		if vname == "" || seen[vname] {
			return nil
		}
		seen[vname] = true
		u.variants = append(u.variants, variant{konst: konst, ref: ref, name: vname, doc: v.Description})
	}
	if u.tag == "" || len(u.variants) == 0 {
		return nil
	}
	for _, p := range synth {
		p.s.Type = json.RawMessage(`"object"`)
		if p.s.Description == "" {
			p.s.Description = p.name + " is the inline payload of one " + name + " variant."
		}
		g.defs[p.name] = p.s
		g.kinds[p.name] = "struct"
	}
	return u
}

// fieldType resolves a property schema to a Go type. Optional struct-valued
// properties are pointers so that an absent object is distinguishable from
// an empty one.
func (g *gen) fieldType(p *schema, required bool) (typ string, omitempty bool) {
	if inner, ok := p.nullable(); ok {
		t, _ := g.fieldType(inner, true)
		if strings.HasPrefix(t, "[]") || strings.HasPrefix(t, "map[") || t == "json.RawMessage" {
			return t, true
		}
		return "*" + t, true
	}
	if ref := p.refName(); ref != "" {
		kind := g.kinds[ref]
		if !required && (kind == "struct" || kind == "union") {
			return "*" + ref, true
		}
		return ref, !required
	}
	if len(p.AnyOf) > 0 || len(p.OneOf) > 0 {
		return "json.RawMessage", true
	}
	types := p.types()
	nullable := false
	var base string
	for _, t := range types {
		if t == "null" {
			nullable = true
		} else {
			base = t
		}
	}
	switch base {
	case "string":
		if nullable {
			return "*string", true
		}
		return "string", !required
	case "boolean":
		if nullable {
			return "*bool", true
		}
		return "bool", !required
	case "integer":
		if nullable {
			return "*int64", true
		}
		return "int64", !required
	case "number":
		if nullable {
			return "*float64", true
		}
		return "float64", !required
	case "array":
		item := "json.RawMessage"
		if p.Items != nil {
			item, _ = g.fieldType(p.Items, true)
		}
		return "[]" + item, !required
	case "object":
		if len(p.AdditionalProperties) > 0 && p.AdditionalProperties[0] == '{' {
			var ap schema
			if json.Unmarshal(p.AdditionalProperties, &ap) == nil {
				vt, _ := g.fieldType(&ap, true)
				return "map[string]" + vt, !required
			}
		}
		return "json.RawMessage", true
	}
	return "json.RawMessage", true
}

func (g *gen) emitStruct(name string, s *schema) {
	doc(&g.buf, name, s.Description)
	fmt.Fprintf(&g.buf, "type %s struct {\n", name)
	req := map[string]bool{}
	for _, r := range s.Required {
		req[r] = true
	}
	// Required properties first in schema order, then the optional ones
	// alphabetically, _meta last.
	props := append([]string(nil), s.Required...)
	var optional []string
	for pn := range s.Properties {
		if !req[pn] && pn != "_meta" {
			optional = append(optional, pn)
		}
	}
	sort.Strings(optional)
	props = append(props, optional...)
	if _, ok := s.Properties["_meta"]; ok {
		props = append(props, "_meta")
	}
	for _, pn := range props {
		p := s.Properties[pn]
		fname := goName(pn)
		if pn == "_meta" {
			fname = "Meta"
		}
		typ, omit := g.fieldType(p, req[pn])
		tag := pn
		if omit {
			tag += ",omitempty"
		}
		if d := firstSentence(p.Description); d != "" {
			for _, line := range wrap(d, 72) {
				fmt.Fprintf(&g.buf, "\t// %s\n", line)
			}
		}
		fmt.Fprintf(&g.buf, "\t%s %s `json:\"%s\"`\n", fname, typ, tag)
	}
	g.buf.WriteString("}\n\n")
}

func (g *gen) emitEnum(name string, s *schema) {
	doc(&g.buf, name, s.Description)
	fmt.Fprintf(&g.buf, "type %s string\n\n", name)
	fmt.Fprintf(&g.buf, "// Values of %s.\nconst (\n", name)
	vs := s.OneOf
	if vs == nil {
		vs = s.AnyOf
	}
	for _, v := range vs {
		if len(v.Const) == 0 {
			continue
		}
		var c string
		_ = json.Unmarshal(v.Const, &c)
		if d := firstLine(v.Description); d != "" {
			fmt.Fprintf(&g.buf, "\t// %s%s: %s\n", name, goName(c), d)
		}
		fmt.Fprintf(&g.buf, "\t%s%s %s = %q\n", name, goName(c), name, c)
	}
	g.buf.WriteString(")\n\n")
}

func (g *gen) emitIntEnum(name string, s *schema) {
	doc(&g.buf, name, s.Description)
	fmt.Fprintf(&g.buf, "type %s int64\n\n", name)
	vs := s.AnyOf
	if vs == nil {
		vs = s.OneOf
	}
	var consts []*schema
	for _, v := range vs {
		if len(v.Const) > 0 {
			consts = append(consts, v)
		}
	}
	if len(consts) == 0 {
		return
	}
	fmt.Fprintf(&g.buf, "// Well-known values of %s.\nconst (\n", name)
	for _, v := range consts {
		fmt.Fprintf(&g.buf, "\t%s%s %s = %s\n", name, goName(v.Title), name, string(v.Const))
	}
	g.buf.WriteString(")\n\n")
}

// firstSentence returns the first paragraph up to its first sentence end.
func firstSentence(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "\n\n"); i >= 0 {
		s = s[:i]
	}
	s = strings.Join(strings.Fields(s), " ")
	for i := 0; i+1 < len(s); i++ {
		if (s[i] == '.' || s[i] == '!' || s[i] == '?') && s[i+1] == ' ' {
			return s[:i+1]
		}
	}
	return s
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

func (g *gen) emitUnion(name string, s *schema, u *union) {
	doc(&g.buf, name, s.Description)
	fmt.Fprintf(&g.buf, "//\n// %s is a tagged union on %q. Kind holds the discriminator and Raw the\n// whole JSON object; the As* methods decode a variant and the New%s* constructors\n// build one. An unrecognized Kind still round-trips unchanged.\n", name, u.tag, name)
	fmt.Fprintf(&g.buf, "type %s struct {\n\tKind string\n\tRaw json.RawMessage\n}\n\n", name)
	fmt.Fprintf(&g.buf, "// Kinds of %s.\nconst (\n", name)
	for _, v := range u.variants {
		if v.konst == "" {
			continue
		}
		fmt.Fprintf(&g.buf, "\t%sKind%s = %q\n", name, v.name, v.konst)
	}
	g.buf.WriteString(")\n\n")
	fmt.Fprintf(&g.buf, "// MarshalJSON writes the variant object.\nfunc (u %s) MarshalJSON() ([]byte, error) {\n\tif len(u.Raw) == 0 {\n\t\treturn nil, fmt.Errorf(\"acp: empty %s\")\n\t}\n\treturn u.Raw, nil\n}\n\n", name, name)
	fmt.Fprintf(&g.buf, "// UnmarshalJSON keeps the object and reads its %q discriminator.\nfunc (u *%s) UnmarshalJSON(b []byte) error {\n\tvar tag struct {\n\t\tKind string `json:%q`\n\t}\n\tif err := json.Unmarshal(b, &tag); err != nil {\n\t\treturn err\n\t}\n\tu.Kind = tag.Kind\n\tu.Raw = append(json.RawMessage(nil), b...)\n\treturn nil\n}\n\n", u.tag, name, u.tag)
	for _, v := range u.variants {
		if v.ref == "" {
			fmt.Fprintf(&g.buf, "// New%s%s builds a %s of kind %q, which carries no payload.\nfunc New%s%s() %s {\n\treturn %s{Kind: %q, Raw: json.RawMessage(`{%q: %q}`)}\n}\n\n", name, v.name, name, v.konst, name, v.name, name, name, v.konst, u.tag, v.konst)
			continue
		}
		if v.konst != "" {
			fmt.Fprintf(&g.buf, "// New%s%s builds a %s of kind %q.\nfunc New%s%s(v %s) (%s, error) {\n\treturn make%s(%q, v)\n}\n\n", name, v.name, name, v.konst, name, v.name, v.ref, name, name, v.konst)
			fmt.Fprintf(&g.buf, "// As%s decodes the %q variant.\nfunc (u %s) As%s() (%s, error) {\n\tvar v %s\n\tif u.Kind != %q {\n\t\treturn v, fmt.Errorf(\"acp: %s is %%q, not %%q\", u.Kind, %q)\n\t}\n\terr := json.Unmarshal(u.Raw, &v)\n\treturn v, err\n}\n\n", v.name, v.konst, name, v.name, v.ref, v.ref, v.konst, name, v.konst)
		} else {
			fmt.Fprintf(&g.buf, "// New%s%s builds the untagged %q variant of %s.\nfunc New%s%s(v %s) (%s, error) {\n\treturn make%s(\"\", v)\n}\n\n", name, v.name, v.name, name, name, v.name, v.ref, name, name)
			fmt.Fprintf(&g.buf, "// As%s decodes the untagged %q variant, which applies when Kind is empty.\nfunc (u %s) As%s() (%s, error) {\n\tvar v %s\n\tif u.Kind != \"\" {\n\t\treturn v, fmt.Errorf(\"acp: %s is %%q, not untagged\", u.Kind)\n\t}\n\terr := json.Unmarshal(u.Raw, &v)\n\treturn v, err\n}\n\n", v.name, v.name, name, v.name, v.ref, v.ref, name)
		}
	}
	fmt.Fprintf(&g.buf, "func make%s(kind string, v any) (%s, error) {\n\traw, err := withTag(%q, kind, v)\n\tif err != nil {\n\t\treturn %s{}, err\n\t}\n\treturn %s{Kind: kind, Raw: raw}, nil\n}\n\n", name, name, u.tag, name, name)
}

func (g *gen) emitTypes() {
	g.buf.WriteString("// Code generated by cmd/acpgen from spec/acp/schema.json. DO NOT EDIT.\n\npackage acp\n\nimport (\n\t\"encoding/json\"\n\t\"fmt\"\n)\n\n")
	names := make([]string, 0, len(g.defs))
	for n := range g.defs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		s := g.defs[name]
		switch g.kinds[name] {
		case "struct":
			g.emitStruct(name, s)
		case "enum":
			g.emitEnum(name, s)
		case "intenum":
			g.emitIntEnum(name, s)
		case "union":
			g.emitUnion(name, s, g.unions[name])
		case "string":
			doc(&g.buf, name, s.Description)
			fmt.Fprintf(&g.buf, "type %s string\n\n", name)
		case "int":
			doc(&g.buf, name, s.Description)
			fmt.Fprintf(&g.buf, "type %s int64\n\n", name)
		default:
			doc(&g.buf, name, s.Description)
			fmt.Fprintf(&g.buf, "//\n// %s has no fixed shape in the schema and is kept as raw JSON.\ntype %s = json.RawMessage\n\n", name, name)
		}
	}
	g.buf.WriteString(`// withTag marshals v and sets the discriminator property on the result.
func withTag(tag, kind string, v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if kind == "" {
		return raw, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("acp: %s variant must be an object: %w", tag, err)
	}
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	kindRaw, err := json.Marshal(kind)
	if err != nil {
		return nil, err
	}
	obj[tag] = kindRaw
	return json.Marshal(obj)
}
`)
}

type meta struct {
	Version         int               `json:"version"`
	AgentMethods    map[string]string `json:"agentMethods"`
	ClientMethods   map[string]string `json:"clientMethods"`
	ProtocolMethods map[string]string `json:"protocolMethods"`
}

func emitMethods(m meta) []byte {
	var b bytes.Buffer
	b.WriteString("// Code generated by cmd/acpgen from spec/acp/meta.json. DO NOT EDIT.\n\npackage acp\n\n")
	fmt.Fprintf(&b, "// SchemaProtocolVersion is the ACP protocol version the vendored schema describes.\nconst SchemaProtocolVersion ProtocolVersion = %d\n\n", m.Version)
	emit := func(title string, ms map[string]string) {
		keys := make([]string, 0, len(ms))
		for k := range ms {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "// %s\nconst (\n", title)
		for _, k := range keys {
			fmt.Fprintf(&b, "\tMethod%s = %q\n", goName(k), ms[k])
		}
		b.WriteString(")\n\n")
	}
	emit("Methods the client calls on the agent.", m.AgentMethods)
	emit("Methods the agent calls on the client.", m.ClientMethods)
	emit("Methods either side may send.", m.ProtocolMethods)
	return b.Bytes()
}

func main() {
	check := flag.Bool("check", false, "verify the committed output is current instead of writing it")
	specDir := flag.String("spec", "spec/acp", "directory holding schema.json and meta.json")
	outDir := flag.String("out", "internal/acp", "package directory to write *_gen.go into")
	flag.Parse()

	raw, err := os.ReadFile(filepath.Join(*specDir, "schema.json"))
	if err != nil {
		fatal(err)
	}
	var root schema
	if err := json.Unmarshal(raw, &root); err != nil {
		fatal(err)
	}
	metaRaw, err := os.ReadFile(filepath.Join(*specDir, "meta.json"))
	if err != nil {
		fatal(err)
	}
	var m meta
	if err := json.Unmarshal(metaRaw, &m); err != nil {
		fatal(err)
	}
	g := &gen{defs: root.Defs}
	g.classify()
	g.emitTypes()
	outputs := map[string][]byte{
		"types_gen.go":   g.buf.Bytes(),
		"methods_gen.go": emitMethods(m),
	}
	stale := false
	for name, src := range outputs {
		formatted, err := format.Source(src)
		if err != nil {
			os.WriteFile(filepath.Join(os.TempDir(), "acpgen-bad-"+name), src, 0o600)
			fatal(fmt.Errorf("%s: %w (unformatted output kept in %s)", name, err, os.TempDir()))
		}
		path := filepath.Join(*outDir, name)
		if *check {
			have, _ := os.ReadFile(path)
			if !bytes.Equal(have, formatted) {
				fmt.Fprintf(os.Stderr, "%s is stale: run make acpgen and commit\n", path)
				stale = true
			}
			continue
		}
		if err := os.WriteFile(path, formatted, 0o644); err != nil {
			fatal(err)
		}
	}
	if stale {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "acpgen:", err)
	os.Exit(1)
}
