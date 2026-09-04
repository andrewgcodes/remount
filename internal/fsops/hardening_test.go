package fsops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// A search whose root cannot be walked must say so. WalkDir hands the root's
// own failure to the callback, which skips unreadable entries on purpose, so a
// missing, non-directory or jail-escaping root used to return an empty match
// list -- a wrong answer indistinguishable from a real one.
func TestSearchRootFailureIsNotAnEmptyResult(t *testing.T) {
	f, root := newFS(t)
	if err := f.Write("src/a.go", []byte("needle\n"), 0, false, true); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "s.txt"), []byte("needle\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "esc")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, code string }{
		{"does/not/exist", proto.CodeNotFound},
		{"src/a.go/deeper", proto.CodeBadRequest},
		{"esc", proto.CodeDenied},
	} {
		res, err := f.Search(tc.path, "needle", "", 0)
		if codeOf(err) != tc.code {
			t.Fatalf("Search(%q) = (%+v, %v); want code %q", tc.path, res, err, tc.code)
		}
	}
	// A usable root still searches.
	res, err := f.Search("src", "needle", "", 0)
	if err != nil || len(res.Matches) != 1 {
		t.Fatalf("Search(src) = (%+v, %v)", res, err)
	}
}

// max comes straight off the wire. Without a ceiling one request accumulates a
// reply the transport cannot carry, so the caller gets a frame error instead of
// a truncated result -- and the node holds every match in memory first.
func TestSearchResultsStayInsideOneFrame(t *testing.T) {
	f, _ := newFS(t)
	// Wide lines: the reply is bounded by accumulated match text.
	wide := strings.Repeat("m", MaxLineText) + "\n"
	var fat strings.Builder
	for i := 0; i < 1000; i++ {
		fat.WriteString(wide)
	}
	for i := 0; i < 2; i++ {
		if err := f.Write(string(rune('a'+i))+".txt", []byte(fat.String()), 0, false, false); err != nil {
			t.Fatal(err)
		}
	}
	res, err := f.Search("/", "m", "", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Matches) == 0 {
		t.Fatalf("unbounded search reported %d matches and truncated=%v", len(res.Matches), res.Truncated)
	}
	encoded, err := proto.EncodeFrame(&proto.Frame{T: "res", ID: 1, Body: mustCBOR(t, res)})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > transport.MaxFrameBytes {
		t.Fatalf("search reply is %d bytes; the transport refuses anything over %d",
			len(encoded), transport.MaxFrameBytes)
	}

	// Narrow lines: the reply is bounded by the result ceiling.
	g, _ := newFS(t)
	var many strings.Builder
	for i := 0; i < MaxSearchMax*4; i++ {
		many.WriteString("m\n")
	}
	if err := g.Write("many.txt", []byte(many.String()), 0, false, false); err != nil {
		t.Fatal(err)
	}
	res, err = g.Search("/", "m", "", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Matches) != MaxSearchMax {
		t.Fatalf("caller-supplied max was honoured: %d matches, truncated=%v", len(res.Matches), res.Truncated)
	}
}

func mustCBOR(t *testing.T, v any) []byte {
	t.Helper()
	b, err := proto.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A negative offset is a caller error, not a read of the whole file that also
// reports eof:false. The old behaviour returned every byte and told a
// paginating caller there was more.
func TestReadRejectsNegativeOffset(t *testing.T) {
	f, _ := newFS(t)
	if err := f.Write("a.txt", []byte("hello"), 0, false, false); err != nil {
		t.Fatal(err)
	}
	res, err := f.Read("a.txt", -3, 0)
	if codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("Read(offset=-3) = (%+v, %v); want bad_request", res, err)
	}
}

// os.Root refuses setuid and type bits with an opaque error. A caller's bad
// mode must be bad_request, never internal, and must never create the file.
func TestWriteRejectsNonPermissionModeBits(t *testing.T) {
	f, root := newFS(t)
	for _, mode := range []uint32{0o4755, 0o2755, 1 << 31} {
		if err := f.Write("s.sh", []byte("x"), mode, false, false); codeOf(err) != proto.CodeBadRequest {
			t.Fatalf("Write(mode=%#o) code = %q: %v", mode, codeOf(err), err)
		}
		if _, err := os.Lstat(filepath.Join(root, "s.sh")); !os.IsNotExist(err) {
			t.Fatalf("Write(mode=%#o) created the file anyway: %v", mode, err)
		}
	}
	if err := f.Write("s.sh", []byte("x"), 0o755, false, false); err != nil {
		t.Fatal(err)
	}
}

// The jail's denial used to be recognised by a substring of the formatted
// error, and that formatted error embeds the caller's own path. A workspace
// could therefore name a directory "path escapes from parent" and make every
// ordinary miss under it report a jail escape -- fabricating exactly the audit
// signal an operator treats as an attack. Meanwhile a real escape, including
// one through a rename, must still be denied.
func TestJailDenialIsNotSpoofableByAFileName(t *testing.T) {
	f, root := newFS(t)
	if err := f.Write("path escapes from parent/keep", []byte("x"), 0, false, true); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"path escapes from parent/missing", "nowhere/path escapes from parent"} {
		if _, err := f.Read(p, 0, 0); codeOf(err) != proto.CodeNotFound {
			t.Fatalf("Read(%q) code = %q: %v; want not_found", p, codeOf(err), err)
		}
	}
	if err := f.Remove("path escapes from parent/missing", false); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("Remove code = %q: %v; want not_found", codeOf(err), err)
	}

	// Real containment failures keep their denial, through every entry point.
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "esc")); err != nil {
		t.Fatal(err)
	}
	if err := f.Rename("path escapes from parent/keep", "esc/stolen"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("Rename out code = %q: %v; want denied", codeOf(err), err)
	}
	if err := f.Mkdir("esc/a"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("Mkdir code = %q: %v; want denied", codeOf(err), err)
	}
	if err := f.Remove("esc/anything", false); codeOf(err) != proto.CodeDenied {
		t.Fatalf("Remove code = %q: %v; want denied", codeOf(err), err)
	}
	if _, err := f.List("esc"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("List code = %q: %v; want denied", codeOf(err), err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("workspace reached outside its root: %v %v", entries, err)
	}
}
