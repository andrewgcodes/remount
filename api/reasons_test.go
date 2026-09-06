package api

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

// TestEveryProtocolReasonIsPublic reads the protocol's own Reason* constants
// and asserts this package re-exports each one under the same name. A public
// SDK whose reason vocabulary lags the wire would force a caller back into
// internal/proto, which the compatibility policy forbids.
//
// It parses source rather than reflecting because constants have no runtime
// identity: an untyped string constant that is missing simply does not exist.
func TestEveryProtocolReasonIsPublic(t *testing.T) {
	protocol := stringConstants(t, filepath.Join("..", "internal", "proto"), "Reason")
	if len(protocol) == 0 {
		t.Fatal("no proto.Reason* constants were found; the scan itself is broken")
	}
	public := stringConstants(t, ".", "Reason")
	for name, value := range protocol {
		alias, ok := public[name]
		if !ok {
			t.Errorf("api does not export %s (%q); add it beside the others", name, value)
			continue
		}
		if want := "proto." + name; alias != want {
			t.Errorf("api.%s is %s, want %s so the two can never drift", name, alias, want)
		}
	}
	for name := range public {
		if _, ok := protocol[name]; !ok {
			t.Errorf("api exports %s, which the protocol does not declare", name)
		}
	}
}

func TestIsNarrowsOnCodeAndReason(t *testing.T) {
	denied := proto.ErrReason(proto.CodeDenied, proto.ReasonEgressDenied, "api.example.com is not bound")
	wrapped := fmt.Errorf("run: %w", denied)

	if !Is(wrapped, CodeDenied, ReasonEgressDenied) {
		t.Fatal("Is did not match a wrapped error on its code and reason")
	}
	if !Is(wrapped, CodeDenied, "") {
		t.Fatal("an empty reason must match any reason")
	}
	if Is(wrapped, CodeDenied, ReasonPermissionDenied) {
		t.Fatal("Is matched a different reason under the same code")
	}
	if Is(wrapped, CodeConflict, ReasonEgressDenied) {
		t.Fatal("Is matched a different code")
	}
	if Is(errors.New("dial tcp: connection refused"), CodeDenied, "") {
		t.Fatal("Is matched a non-protocol error")
	}
	if got := ErrorReason(wrapped); got != ReasonEgressDenied {
		t.Fatalf("ErrorReason = %q, want %q", got, ReasonEgressDenied)
	}

	// A code carrying no reason still matches on the code, so a caller that
	// narrows never loses the coarser match it had before reasons existed.
	bare := proto.Err(proto.CodeDenied, "denied")
	if !Is(bare, CodeDenied, "") || Is(bare, CodeDenied, ReasonEgressDenied) {
		t.Fatal("a reasonless error must match the code and no reason")
	}
	if ErrorReason(bare) != "" {
		t.Fatalf("ErrorReason on a reasonless error = %q", ErrorReason(bare))
	}
}

// stringConstants returns every `Name = "value"` constant in dir whose name
// starts with prefix.
func stringConstants(t *testing.T, dir, prefix string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, ident := range vs.Names {
					if !strings.HasPrefix(ident.Name, prefix) || i >= len(vs.Values) {
						continue
					}
					switch value := vs.Values[i].(type) {
					case *ast.BasicLit:
						if value.Kind != token.STRING {
							continue
						}
						unquoted, err := strconv.Unquote(value.Value)
						if err != nil {
							t.Fatal(err)
						}
						out[ident.Name] = unquoted
					case *ast.SelectorExpr:
						// api re-exports each reason as an alias. Record the
						// alias target so the caller asserts the two names
						// line up, rather than comparing values that a stale
						// hand-copied literal could still get right.
						if x, ok := value.X.(*ast.Ident); ok {
							out[ident.Name] = x.Name + "." + value.Sel.Name
						}
					}
				}
			}
		}
	}
	return out
}
