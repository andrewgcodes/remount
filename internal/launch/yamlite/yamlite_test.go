package yamlite

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseRecipeShapedDocument(t *testing.T) {
	src := `---
# A recipe.
name: opencode
description: "OpenCode: terminal agent"   # trailing comment
image: ghcr.io/remount/workspace:latest
auth: api_key
providers: [anthropic, "openai", openrouter]
hosts:
  - registry.npmjs.org
  - 'github.com'
install: |
  set -e
  command -v opencode >/dev/null 2>&1 || npm i -g opencode-ai

  echo done
configure:
  - path: .config/opencode/opencode.json
    mode: 0644
    content: >-
      {"provider": "x",
       "model": "y"}
  -
    path: .config/other
    content: "a\nb"
command: ["opencode", "run", "{{.Task}}"]
state_dirs: []
path_keyed: true
retries: 3
nothing: ~
empty:
`
	got, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"name":        "opencode",
		"description": "OpenCode: terminal agent",
		"image":       "ghcr.io/remount/workspace:latest",
		"auth":        "api_key",
		"providers":   []any{"anthropic", "openai", "openrouter"},
		"hosts":       []any{"registry.npmjs.org", "github.com"},
		"install":     "set -e\ncommand -v opencode >/dev/null 2>&1 || npm i -g opencode-ai\n\necho done\n",
		"configure": []any{
			map[string]any{
				"path":    ".config/opencode/opencode.json",
				"mode":    int64(644),
				"content": "{\"provider\": \"x\",\n \"model\": \"y\"}",
			},
			map[string]any{"path": ".config/other", "content": "a\nb"},
		},
		"command":    []any{"opencode", "run", "{{.Task}}"},
		"state_dirs": []any{},
		"path_keyed": true,
		"retries":    int64(3),
		"nothing":    nil,
		"empty":      nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parse mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestUnmarshalIntoStructRejectsUnknownFields(t *testing.T) {
	type doc struct {
		Name  string   `json:"name"`
		Hosts []string `json:"hosts"`
	}
	var d doc
	if err := Unmarshal([]byte("name: a\nhosts: [x]\n"), &d); err != nil {
		t.Fatal(err)
	}
	if d.Name != "a" || len(d.Hosts) != 1 {
		t.Fatalf("got %+v", d)
	}
	if err := Unmarshal([]byte("name: a\nbogus: 1\n"), &d); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown field must be rejected, got %v", err)
	}
}

func TestSequenceAtParentIndent(t *testing.T) {
	got, err := Parse([]byte("hosts:\n- a\n- b\nname: x\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"hosts": []any{"a", "b"}, "name": "x"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
}

func TestRejectsUnsupportedConstructs(t *testing.T) {
	cases := map[string]string{
		"tabs":          "name:\n\t- a\n",
		"flow mapping":  "cfg: {a: 1}\n",
		"anchor":        "a: &x 1\n",
		"alias":         "a: *x\n",
		"tag":           "a: !!str 1\n",
		"duplicate":     "a: 1\na: 2\n",
		"bad indent":    "a:\n  b: 1\n c: 2\n",
		"open quote":    "a: \"x\n",
		"nested flow":   "a: [[1]]\n",
		"after quote":   "a: \"x\" y\n",
		"seq in map":    "a: 1\n- b\n",
		"multiline seq": "a: [1,\n 2]\n",
	}
	for name, src := range cases {
		if _, err := Parse([]byte(src)); err == nil {
			t.Errorf("%s: expected error for %q", name, src)
		} else if !strings.Contains(err.Error(), "line ") {
			t.Errorf("%s: error lacks line number: %v", name, err)
		}
	}
}

func TestBlockScalarChomping(t *testing.T) {
	src := "keep: |+\n  a\n\n\nstrip: |-\n  b\n\nclip: >\n  c\n  d\n\n  e\nlast: 1\n"
	got, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]any)
	if m["keep"] != "a\n\n\n" {
		t.Errorf("keep = %q", m["keep"])
	}
	if m["strip"] != "b" {
		t.Errorf("strip = %q", m["strip"])
	}
	if m["clip"] != "c d\ne\n" {
		t.Errorf("clip = %q", m["clip"])
	}
	if m["last"] != int64(1) {
		t.Errorf("last = %#v", m["last"])
	}
}

func TestEmptyDocument(t *testing.T) {
	got, err := Parse([]byte("# only a comment\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.(map[string]any)) != 0 {
		t.Fatalf("got %#v", got)
	}
}

func FuzzParse(f *testing.F) {
	f.Add("a: 1\nb:\n  - x\n  - y: 2\n    z: |\n      q\n")
	f.Add("- a\n- [1, 'b', \"c\"]\n")
	f.Add("a: {")
	f.Fuzz(func(t *testing.T, src string) {
		// Must never panic; errors are fine.
		_, _ = Parse([]byte(src))
	})
}
