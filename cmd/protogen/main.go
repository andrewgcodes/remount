// Command protogen derives language-neutral and SDK protocol types from the
// Go protocol declarations. It intentionally uses syntax trees rather than
// reflection so generation doesn't execute package initialization.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type field struct {
	GoName   string
	JSONName string
	Type     ast.Expr
	Optional bool
}

type declaration struct {
	Name   string
	Fields []field
}

type operation struct {
	Constant string `json:"constant"`
	Name     string `json:"name"`
	Request  string `json:"request,omitempty"`
	Response string `json:"response,omitempty"`
}

type model struct {
	ProtocolVersion int
	Types           []declaration
	Operations      []operation
}

func main() {
	check := flag.Bool("check", false, "fail if generated files differ")
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	m, err := parseModel(*root)
	if err != nil {
		fatal(err)
	}
	outputs, err := render(m)
	if err != nil {
		fatal(err)
	}
	for name, data := range outputs {
		path := filepath.Join(*root, name)
		if *check {
			old, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(old, data) {
				fatal(fmt.Errorf("generated file is stale: %s (run go run ./cmd/protogen)", name))
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			fatal(err)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "protogen:", err)
	os.Exit(1)
}

func parseModel(root string) (model, error) {
	fset := token.NewFileSet()
	dir := filepath.Join(root, "internal", "proto")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return model{}, err
	}
	files := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return model{}, parseErr
		}
		if file.Name.Name != "proto" {
			continue
		}
		files[path] = file
	}
	if len(files) == 0 {
		return model{}, errors.New("internal/proto package not found")
	}
	fileNames := make([]string, 0, len(files))
	for name := range files {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)
	var m model
	seenTypes := map[string]bool{}
	for _, fileName := range fileNames {
		file := files[fileName]
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			switch gen.Tok {
			case token.TYPE:
				for _, spec := range gen.Specs {
					ts := spec.(*ast.TypeSpec)
					st, ok := ts.Type.(*ast.StructType)
					if !ok || seenTypes[ts.Name.Name] {
						continue
					}
					seenTypes[ts.Name.Name] = true
					d := declaration{Name: ts.Name.Name}
					for _, sf := range st.Fields.List {
						if len(sf.Names) == 0 || sf.Tag == nil {
							continue
						}
						tag, err := strconv.Unquote(sf.Tag.Value)
						if err != nil {
							return model{}, err
						}
						jsonTag := tagValue(tag, "json")
						if jsonTag == "" || jsonTag == "-" {
							continue
						}
						parts := strings.Split(jsonTag, ",")
						d.Fields = append(d.Fields, field{GoName: sf.Names[0].Name, JSONName: parts[0], Type: sf.Type, Optional: contains(parts[1:], "omitempty")})
					}
					m.Types = append(m.Types, d)
				}
			case token.CONST:
				for _, spec := range gen.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if name.Name == "Version" && i < len(vs.Values) {
							if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.INT {
								m.ProtocolVersion, err = strconv.Atoi(lit.Value)
								if err != nil {
									return model{}, fmt.Errorf("parse protocol version: %w", err)
								}
							}
						}
						if !strings.HasPrefix(name.Name, "Op") || i >= len(vs.Values) {
							continue
						}
						lit, ok := vs.Values[i].(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							continue
						}
						value, err := strconv.Unquote(lit.Value)
						if err != nil {
							return model{}, err
						}
						comment := ""
						if vs.Comment != nil {
							comment = vs.Comment.Text()
						}
						req, res := operationTypes(comment)
						m.Operations = append(m.Operations, operation{Constant: name.Name, Name: value, Request: req, Response: res})
					}
				}
			}
		}
	}
	if m.ProtocolVersion == 0 {
		return model{}, errors.New("protocol Version constant not found")
	}
	typeByName := map[string]declaration{}
	for _, d := range m.Types {
		typeByName[d.Name] = d
	}
	for i := range m.Operations {
		stem := strings.TrimPrefix(m.Operations[i].Constant, "Op")
		if _, ok := typeByName[m.Operations[i].Request]; !ok {
			m.Operations[i].Request = ""
			if _, exists := typeByName[stem+"Req"]; exists {
				m.Operations[i].Request = stem + "Req"
			}
		}
		if _, ok := typeByName[m.Operations[i].Response]; !ok {
			m.Operations[i].Response = ""
			if _, exists := typeByName[stem+"Res"]; exists {
				m.Operations[i].Response = stem + "Res"
			}
		}
	}
	sort.Slice(m.Types, func(i, j int) bool { return m.Types[i].Name < m.Types[j].Name })
	sort.Slice(m.Operations, func(i, j int) bool { return m.Operations[i].Name < m.Operations[j].Name })
	return m, nil
}

func tagValue(tag, key string) string {
	for tag != "" {
		tag = strings.TrimLeft(tag, " ")
		i := strings.IndexByte(tag, ':')
		if i <= 0 {
			return ""
		}
		name := tag[:i]
		tag = tag[i+1:]
		if tag == "" || tag[0] != '"' {
			return ""
		}
		_, rest, ok := strings.Cut(tag[1:], "\"")
		if !ok {
			return ""
		}
		value := tag[1 : len(tag)-len(rest)-1]
		tag = rest
		if name == key {
			return value
		}
	}
	return ""
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func operationTypes(comment string) (string, string) {
	comment = strings.TrimSpace(comment)
	left, right, ok := strings.Cut(comment, "->")
	if !ok {
		return "", ""
	}
	leftFields := strings.Fields(strings.TrimSpace(left))
	rightFields := strings.Fields(strings.TrimSpace(right))
	if len(rightFields) == 0 {
		return "", ""
	}
	req := ""
	if len(leftFields) > 0 {
		req = leftFields[len(leftFields)-1]
	}
	res := strings.Trim(rightFields[0], ";,.: ")
	if req == "{}" || req == "nil" {
		req = ""
	}
	if res == "{}" || res == "nil" {
		res = ""
	}
	return req, res
}

type schemaDocument struct {
	Schema          string                `json:"$schema"`
	ID              string                `json:"$id"`
	Title           string                `json:"title"`
	ProtocolVersion int                   `json:"protocolVersion"`
	Definitions     map[string]schemaType `json:"$defs"`
	Operations      []operation           `json:"operations"`
}

type schemaType struct {
	Type                 string                    `json:"type"`
	Properties           map[string]map[string]any `json:"properties,omitempty"`
	Required             []string                  `json:"required,omitempty"`
	AdditionalProperties bool                      `json:"additionalProperties"`
}

func render(m model) (map[string][]byte, error) {
	defs := make(map[string]schemaType, len(m.Types))
	for _, d := range m.Types {
		s := schemaType{Type: "object", Properties: map[string]map[string]any{}, AdditionalProperties: true}
		for _, f := range d.Fields {
			s.Properties[f.JSONName] = jsonSchema(f.Type)
			if !f.Optional {
				s.Required = append(s.Required, f.JSONName)
			}
		}
		sort.Strings(s.Required)
		defs[d.Name] = s
	}
	doc := schemaDocument{Schema: "https://json-schema.org/draft/2020-12/schema", ID: "https://remount.dev/spec/protocol.schema.json", Title: fmt.Sprintf("Remount protocol v%d", m.ProtocolVersion), ProtocolVersion: m.ProtocolVersion, Definitions: defs, Operations: m.Operations}
	schema, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	schema = append(schema, '\n')
	return map[string][]byte{
		"spec/protocol.schema.json":       schema,
		"sdk/python/src/remount/types.py": []byte(renderPython(m)),
		"sdk/typescript/src/types.ts":     []byte(renderTypeScript(m)),
	}, nil
}

func jsonSchema(expr ast.Expr) map[string]any {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "string":
			return map[string]any{"type": "string"}
		case "bool":
			return map[string]any{"type": "boolean"}
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
			return map[string]any{"type": "integer"}
		case "float32", "float64":
			return map[string]any{"type": "number"}
		case "any":
			return map[string]any{}
		default:
			return map[string]any{"$ref": "#/$defs/" + t.Name}
		}
	case *ast.StarExpr:
		return map[string]any{"anyOf": []map[string]any{jsonSchema(t.X), {"type": "null"}}}
	case *ast.ArrayType:
		if id, ok := t.Elt.(*ast.Ident); ok && id.Name == "byte" {
			return map[string]any{"type": "string", "contentEncoding": "base64"}
		}
		return map[string]any{"type": "array", "items": jsonSchema(t.Elt)}
	case *ast.MapType:
		return map[string]any{"type": "object", "additionalProperties": jsonSchema(t.Value)}
	case *ast.InterfaceType:
		return map[string]any{}
	case *ast.SelectorExpr:
		if x, ok := t.X.(*ast.Ident); ok && x.Name == "json" && t.Sel.Name == "RawMessage" {
			return map[string]any{}
		}
		return map[string]any{}
	default:
		return map[string]any{}
	}
}

func pythonType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "string":
			return "str"
		case "bool":
			return "bool"
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
			return "int"
		case "float32", "float64":
			return "float"
		case "any":
			return "Any"
		default:
			return strconv.Quote(t.Name)
		}
	case *ast.StarExpr:
		return "Optional[" + pythonType(t.X) + "]"
	case *ast.ArrayType:
		if id, ok := t.Elt.(*ast.Ident); ok && id.Name == "byte" {
			return "bytes"
		}
		return "list[" + pythonType(t.Elt) + "]"
	case *ast.MapType:
		return "dict[" + pythonType(t.Key) + ", " + pythonType(t.Value) + "]"
	default:
		return "Any"
	}
}

func renderPython(m model) string {
	var b strings.Builder
	b.WriteString("# Code generated by cmd/protogen; DO NOT EDIT.\nfrom __future__ import annotations\n\nfrom typing import Any, NotRequired, Optional, Required, TypedDict\n\n")
	for _, d := range m.Types {
		fmt.Fprintf(&b, "%s = TypedDict(%q, {\n", d.Name, d.Name)
		for _, f := range d.Fields {
			presence := "Required"
			if f.Optional {
				presence = "NotRequired"
			}
			fmt.Fprintf(&b, "    %q: %s[%s],\n", f.JSONName, presence, pythonType(f.Type))
		}
		b.WriteString("}, total=False)\n\n")
	}
	b.WriteString("OPERATIONS: dict[str, dict[str, object]] = {\n")
	for _, op := range m.Operations {
		fmt.Fprintf(&b, "    %q: {\"constant\": %q, \"request\": %q, \"response\": %q},\n", op.Name, op.Constant, op.Request, op.Response)
	}
	b.WriteString("}\n")
	return b.String()
}

func tsType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "string":
			return "string"
		case "bool":
			return "boolean"
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
			return "number"
		case "float32", "float64":
			return "number"
		case "any":
			return "unknown"
		default:
			return t.Name
		}
	case *ast.StarExpr:
		return tsType(t.X) + " | null"
	case *ast.ArrayType:
		if id, ok := t.Elt.(*ast.Ident); ok && id.Name == "byte" {
			return "Uint8Array"
		}
		return "Array<" + tsType(t.Elt) + ">"
	case *ast.MapType:
		return "Record<string, " + tsType(t.Value) + ">"
	default:
		return "unknown"
	}
}

func renderTypeScript(m model) string {
	var b strings.Builder
	b.WriteString("// Code generated by cmd/protogen; DO NOT EDIT.\n\n")
	for _, d := range m.Types {
		fmt.Fprintf(&b, "export interface %s {\n", d.Name)
		for _, f := range d.Fields {
			optional := ""
			if f.Optional {
				optional = "?"
			}
			fmt.Fprintf(&b, "  %q%s: %s;\n", f.JSONName, optional, tsType(f.Type))
		}
		b.WriteString("}\n\n")
	}
	b.WriteString("export const OPERATIONS = {\n")
	for _, op := range m.Operations {
		fmt.Fprintf(&b, "  %q: { constant: %q, request: %q, response: %q },\n", op.Name, op.Constant, op.Request, op.Response)
	}
	b.WriteString("} as const;\n")
	return b.String()
}
