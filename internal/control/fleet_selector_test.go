package control

import (
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestWorkspaceSelectorTargetsExactlyOneWorkspace(t *testing.T) {
	selector := proto.WorkspaceSelector{Workspace: "ws_wanted"}
	if !selectorSpecified(selector) {
		t.Fatal("workspace-only selector was rejected as empty")
	}
	for _, test := range []struct {
		id   string
		want bool
	}{{"ws_wanted", true}, {"ws_other", false}} {
		workspace := &proto.Workspace{ID: test.id, State: proto.WSClaimed}
		if got := workspaceMatches(selector, workspace, "process"); got != test.want {
			t.Fatalf("workspaceMatches(%q) = %t, want %t", test.id, got, test.want)
		}
	}
}
