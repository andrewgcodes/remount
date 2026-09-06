package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"remount.dev/remount/internal/computer"
	"remount.dev/remount/internal/proto"
)

// The default browser command line is a documented contract: a second builder
// writing the browser image has to match its port, profile and window flags.
func TestDefaultBrowserProgramShape(t *testing.T) {
	argv := DefaultBrowserProgram("127.0.0.1", 9333, "/ws/.remount/browser/default",
		proto.ComputerViewport{Width: 1024, Height: 768})
	joined := strings.Join(argv, " ")
	for _, want := range []string{
		"--headless=new",
		"--remote-debugging-address=127.0.0.1",
		"--remote-debugging-port=9333",
		"--user-data-dir=/ws/.remount/browser/default",
		"--window-size=1024,768",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("default program is missing %s: %v", want, argv)
		}
	}
	if argv[0] != "chromium" || argv[len(argv)-1] != "about:blank" {
		t.Fatalf("default program = %v", argv)
	}
	// A backend that resolves a workspace port to a container or sandbox
	// address cannot reach a browser that binds the workspace's own loopback,
	// and Chromium binds loopback whatever --remote-debugging-address says.
	// The default launch therefore forwards, and the browser stays the
	// foreground process so its exit is still the session's exit.
	forwarded := DefaultBrowserProgram("172.17.0.2", 9222, "/ws/.remount/browser/default",
		proto.ComputerViewport{Width: 1024, Height: 768})
	if len(forwarded) != 3 || forwarded[0] != "sh" || forwarded[1] != "-c" {
		t.Fatalf("forwarded launch = %v", forwarded)
	}
	for _, want := range []string{
		"socat TCP-LISTEN:9222,fork,reuseaddr TCP:127.0.0.1:9223",
		"--remote-debugging-port=9223",
		"wait $browser",
		"kill $forwarder",
	} {
		if !strings.Contains(forwarded[2], want) {
			t.Fatalf("forwarded launch is missing %q: %s", want, forwarded[2])
		}
	}
}

// A profile path is interpolated into a shell command on the forwarding path,
// so it has to survive quoting rather than split into two words.
func TestForwardedLaunchQuotesTheProfilePath(t *testing.T) {
	argv := DefaultBrowserProgram("172.17.0.2", 9222, "/ws dir/.remount/browser/it's",
		proto.ComputerViewport{Width: 800, Height: 600})
	if !strings.Contains(argv[2], `'--user-data-dir=/ws dir/.remount/browser/it'\''s'`) {
		t.Fatalf("profile path was not quoted: %s", argv[2])
	}
}

// The profile name becomes a directory inside the node-owned .remount tree, so
// anything that is not one safe path segment must be refused, not sanitized.
func TestProfileNameRejectsEscapes(t *testing.T) {
	for _, bad := range []string{"..", ".", "a/b", `a\b`, "", strings.Repeat("x", 65), "-lead"} {
		if profileName.MatchString(bad) {
			t.Fatalf("profile name %q was accepted", bad)
		}
	}
	for _, good := range []string{"default", "a", "work-1", "a.b_c"} {
		if !profileName.MatchString(good) {
			t.Fatalf("profile name %q was refused", good)
		}
	}
}

// Publishing the wrong file is worse than failing, so only a regular file
// named by the download's suggested name or its GUID is ever archived.
func TestDownloadFilenamePicksARegularFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "report.csv"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "G-DIR"), 0o755); err != nil {
		t.Fatal(err)
	}

	name, size, err := downloadFilename(dir, computer.Download{GUID: "G1", Filename: "report.csv"})
	if err != nil || name != "report.csv" || size != 1 {
		t.Fatalf("suggested name = %q size %d (%v)", name, size, err)
	}

	if err := os.WriteFile(filepath.Join(dir, "G2"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	name, size, err = downloadFilename(dir, computer.Download{GUID: "G2", Filename: "absent.bin"})
	if err != nil || name != "G2" || size != 1 {
		t.Fatalf("guid fallback = %q size %d (%v)", name, size, err)
	}

	// A directory, a traversal and a name that matches nothing all fail.
	for _, d := range []computer.Download{
		{GUID: "G-DIR"},
		{GUID: "..", Filename: "../../etc/passwd"},
		{GUID: "missing", Filename: "missing"},
	} {
		if _, _, err := downloadFilename(dir, d); err == nil {
			t.Fatalf("download %+v resolved to a file", d)
		} else if proto.ErrorReason(err) != proto.ReasonDownloadBlocked {
			t.Fatalf("download %+v reason = %q", d, proto.ErrorReason(err))
		}
	}
}

// A computer that has closed reports the same stable code and reason to every
// later operation, rather than a bare "closed" a caller cannot act on.
func TestComputerHandleLiveReportsReason(t *testing.T) {
	h := &computerHandle{id: "cmp_1", state: proto.ComputerStateReady}
	if err := h.live(); err != nil {
		t.Fatalf("ready handle = %v", err)
	}
	if !h.markClosed(proto.ComputerStateClosed, proto.ReasonBrowserCrashed) {
		t.Fatal("first close did not take")
	}
	if h.markClosed(proto.ComputerStateClosed, proto.ComputerClosedReasonClosed) {
		t.Fatal("a second close claimed the transition")
	}
	err := h.live()
	if err == nil || proto.ErrorReason(err) != proto.ReasonBrowserCrashed {
		t.Fatalf("closed handle = %v (reason %q)", err, proto.ErrorReason(err))
	}
	state, reason := h.snapshot()
	if state != proto.ComputerStateClosed || reason != proto.ReasonBrowserCrashed {
		t.Fatalf("snapshot = %q/%q", state, reason)
	}
}
