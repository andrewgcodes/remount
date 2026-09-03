package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The HTTP API is the surface a UI is written against, so an undocumented
// route is a bug in the same way an unimplemented documented one is. This
// test reads the routes out of the mux registration and requires each path to
// appear in docs/api.md and in the route block of spec/PROTOCOL.md §6.3.

var routeRE = regexp.MustCompile(`mux\.HandleFunc\("(?:(GET|POST|PUT|DELETE|HEAD) )?([^"]+)"`)

var actionRE = regexp.MustCompile(`(?s)func \(s \*Server\) handleAgentAction.*?\n}`)

var caseRE = regexp.MustCompile(`case "(\w+)":`)

// docPath reduces a mux pattern to what the docs must literally contain: a
// trailing catch-all wildcard is only a suffix, so the prefix is what has to
// be named.
func docPath(pat string) string {
	if i := strings.Index(pat, "...}"); i >= 0 {
		return pat[:strings.LastIndex(pat[:i], "{")]
	}
	return pat
}

func registeredRoutes(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("api.go")
	if err != nil {
		t.Fatalf("read api.go: %v", err)
	}
	// The one multiplexed route dispatches on a path value; the docs name the
	// actions, not the wildcard, so expand it from the handler's own switch.
	var actions []string
	for _, m := range caseRE.FindAllStringSubmatch(actionRE.FindString(string(src)), -1) {
		actions = append(actions, m[1])
	}
	if len(actions) == 0 {
		t.Fatal("handleAgentAction has no action cases; this check went blind")
	}
	var routes []string
	for _, m := range routeRE.FindAllStringSubmatch(string(src), -1) {
		pat := docPath(m[2])
		if strings.Contains(pat, "{action}") {
			for _, a := range actions {
				routes = append(routes, strings.ReplaceAll(pat, "{action}", a))
			}
			continue
		}
		routes = append(routes, pat)
	}
	if len(routes) < 15 {
		t.Fatalf("found only %d routes in api.go; the registration shape changed and this check went blind", len(routes))
	}
	return routes
}

func TestRoutesAreDocumented(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, doc := range []string{"docs/api.md", "spec/PROTOCOL.md"} {
		b, err := os.ReadFile(filepath.Join(root, doc))
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		text := string(b)
		if doc == "spec/PROTOCOL.md" {
			_, after, ok := strings.Cut(text, "### 6.3 The agent HTTP API")
			if !ok {
				t.Fatal("spec/PROTOCOL.md has no HTTP API section")
			}
			text, _, _ = strings.Cut(after, "## 7.")
		}
		for _, route := range registeredRoutes(t) {
			if !strings.Contains(text, route) {
				t.Errorf("%s does not document route %s", doc, route)
			}
		}
	}
}
