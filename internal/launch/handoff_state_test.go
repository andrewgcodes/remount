package launch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

func writeHandoffState(t *testing.T, root, name, content string) string {
	t.Helper()
	file := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestHandoffBackendPreflight(t *testing.T) {
	backend := proto.BackendDescriptor{Name: "docker", Runtime: proto.RuntimeCaps{MountPath: true}, Security: proto.BackendSecurityCaps{Isolation: "container", EgressMode: "cooperative_proxy", BrokerIdentity: "token"}}
	node := proto.NodeStatus{Online: true, Info: proto.NodeInfo{BackendDescriptors: []proto.BackendDescriptor{backend}}}
	for _, tc := range []struct {
		name  string
		spec  proto.WorkspaceSpec
		nodes []proto.NodeStatus
		want  bool
	}{
		{"compatible", proto.WorkspaceSpec{MountPath: "/checkout"}, []proto.NodeStatus{node}, true},
		{"no-node", proto.WorkspaceSpec{MountPath: "/checkout"}, nil, false},
		{"wrong-backend", proto.WorkspaceSpec{Requires: proto.Requires{Backend: "gvisor"}}, []proto.NodeStatus{node}, false},
		{"security", proto.WorkspaceSpec{Security: proto.SecuritySpec{Profile: proto.SecurityIsolated}}, []proto.NodeStatus{node}, false},
		{"offline", proto.WorkspaceSpec{}, []proto.NodeStatus{{Info: node.Info}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := handoffBackend(tc.spec, tc.nodes)
			if (err == nil) != tc.want {
				t.Fatalf("backend = %v; want supported %v", err, tc.want)
			}
		})
	}
}

func TestHandoffSavedLiveFixture(t *testing.T) {
	fixture := os.Getenv("REMOUNT_HANDOFF_FIXTURE")
	recipe := os.Getenv("REMOUNT_HANDOFF_RECIPE")
	if fixture == "" {
		t.Skip("unavailable: set REMOUNT_HANDOFF_FIXTURE to an isolated synthetic fixture")
	}
	if recipe != "claude" && recipe != "codex" {
		t.Fatal("REMOUNT_HANDOFF_RECIPE must be claude or codex")
	}
	dir, err := filepath.EvalSymlinks(filepath.Join(fixture, "checkout"))
	if err != nil {
		t.Fatal(err)
	}
	selected, cleanup, err := handoffState(filepath.Join(fixture, "home"), dir, &Recipe{Name: recipe})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	extra := selected.extra
	if len(extra) == 0 {
		t.Fatal("no conversation selected")
	}
	var archive bytes.Buffer
	manifest, err := packHandoff(dir, localfs.PackOptions{Extra: extra}, &archive)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("selected %d conversation files; packed %d files, %d bytes", len(extra), manifest.Files, manifest.Bytes)
}

func FuzzHandoffMetadata(f *testing.F) {
	dir, err := filepath.EvalSymlinks(f.TempDir())
	if err != nil {
		f.Fatal(err)
	}
	cwd, err := json.Marshal(dir)
	if err != nil {
		f.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	f.Add(false, []byte(fmt.Sprintf("{\"type\":\"user\",\"cwd\":%s,\"sessionId\":%q}\n", cwd, id)))
	f.Add(true, []byte(fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"cwd\":%s,\"id\":%q}}\n{\"type\":\"response_item\",\"payload\":{\"type\":\"message\"}}\n", cwd, id)))
	f.Add(false, []byte("{\n"))
	f.Fuzz(func(t *testing.T, codex bool, data []byte) {
		if len(data) > 1<<20 {
			t.Skip("bounded metadata fuzz input")
		}
		recipe := "claude"
		if codex {
			recipe = "codex"
		}
		got, ok := handoffMetadata(data, recipe, dir, id)
		if ok && got != id {
			t.Fatalf("accepted a different conversation %q", got)
		}
	})
}

func TestHandoffMetadata(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := json.Marshal(dir)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	claude := fmt.Sprintf(`{"type":"user","cwd":%s,"sessionId":%q,"message":{"role":"user","content":"fixture"}}`, cwd, id) + "\n"
	codex := fmt.Sprintf(`{"type":"session_meta","payload":{"cwd":%s,"id":%q}}`, cwd, id) + "\n"
	message := "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\"}}\n"
	for _, tc := range []struct {
		name, recipe, data string
		want               bool
	}{
		{"claude", "claude", claude, true},
		{"claude-summary", "claude", "{\"type\":\"summary\",\"summary\":\"fixture\"}\n" + claude, true},
		{"claude-partial", "claude", claude + "{", false},
		{"claude-conflicting-cwd", "claude", claude + strings.Replace(claude, string(cwd), `"/not-this-project"`, 1), false},
		{"claude-conflicting-id", "claude", claude + strings.ReplaceAll(claude, id, "22222222-2222-4222-8222-222222222222"), false},
		{"claude-missing-cwd", "claude", fmt.Sprintf("{\"sessionId\":%q}\n", id), false},
		{"claude-empty", "claude", "", false},
		{"codex", "codex", codex + message, true},
		{"codex-metadata-only", "codex", codex, false},
		{"codex-meta-not-first", "codex", message + codex, false},
		{"codex-partial", "codex", codex + message + "{", false},
		{"codex-conflicting-meta", "codex", codex + message + strings.Replace(codex, string(cwd), `"/not-this-project"`, 1), false},
		{"codex-conflicting-context", "codex", codex + message + "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/not-this-project\"}}\n", false},
		{"codex-conflicting-session-id", "codex", strings.Replace(codex, `"payload":{`, `"payload":{"session_id":"other",`, 1) + message, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := handoffMetadata([]byte(tc.data), tc.recipe, dir, id)
			if ok != tc.want || ok && got != id {
				t.Fatalf("metadata = %q, %v; want valid %v", got, ok, tc.want)
			}
		})
	}
}

func TestHandoffStateStagesOnlyMatchingSessionAndAttachments(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: symlink creation needs elevated Windows permissions")
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	cwd, err := json.Marshal(alias)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	home := t.TempDir()
	prefix := ".claude/projects/" + claudeProjectKey(alias) + "/" + id
	content := fmt.Sprintf("{\"type\":\"user\",\"cwd\":%s,\"sessionId\":%q}\n", cwd, id)
	local := writeHandoffState(t, home, prefix+".jsonl", content)
	writeHandoffState(t, home, prefix+"/subagents/agent-worker.jsonl", content)
	writeHandoffState(t, home, prefix+"/tool-results/tool-output.txt", "saved tool output")
	for _, name := range []string{".env", ".env.production", "auth.json", "settings.json", ".codex/auth.json", "subagents/settings.json", "tool-results/.env.txt"} {
		writeHandoffState(t, home, prefix+"/"+name, "forbidden")
	}
	outside := writeHandoffState(t, t.TempDir(), "secret.txt", "forbidden")
	if err := os.Symlink(outside, filepath.Join(home, filepath.FromSlash(prefix+"/tool-results/escape.txt"))); err != nil {
		t.Fatal(err)
	}
	selected, cleanup, err := handoffState(home, dir, &Recipe{Name: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	extra := selected.extra
	if len(extra) != 3 {
		t.Fatalf("selected %d files: %+v", len(extra), extra)
	}
	writeHandoffState(t, home, prefix+".jsonl", "changed after selection")
	for _, file := range extra {
		if !strings.HasPrefix(file.Archive, ".claude/projects/"+claudeProjectKey(dir)+"/") {
			t.Errorf("did not rekey alias to canonical project: %s", file.Archive)
		}
		if file.Local == local || filepath.Base(file.Local) != filepath.Base(file.Archive) {
			t.Errorf("file was not privately staged with the same basename: %+v", file)
		}
		data, err := os.ReadFile(file.Local)
		if err != nil || strings.Contains(string(data), "forbidden") || strings.Contains(string(data), "changed") {
			t.Errorf("staged %s = %q, %v", file.Local, data, err)
		}
	}
	cleanup()
	if _, err := os.Stat(extra[0].Local); !os.IsNotExist(err) {
		t.Fatalf("staged state survived cleanup: %v", err)
	}
}

func TestHandoffStateRejectsSymlinkedParentsAndLargeState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: symlink creation needs elevated Windows permissions")
	}
	home := t.TempDir()
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	outside := t.TempDir()
	writeHandoffState(t, outside, "transcript.jsonl", "outside state")
	if err := os.Symlink(outside, filepath.Join(home, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := handoffReadRegular(root, "linked/transcript.jsonl"); err == nil {
		t.Fatal("followed symlinked parent")
	}
	file := writeHandoffState(t, home, "large.jsonl", "")
	if err := os.Truncate(file, handoffMaxFileBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := handoffReadRegular(root, "large.jsonl"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("large state = %v", err)
	}
}

func TestHandoffStateLatestUsesMatchingMetadataNotProjectKey(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := json.Marshal(dir)
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	const id = "11111111-1111-4111-8111-111111111111"
	const wrongID = "22222222-2222-4222-8222-222222222222"
	prefix := ".claude/projects/" + claudeProjectKey(dir) + "/"
	valid := writeHandoffState(t, home, prefix+id+".jsonl", fmt.Sprintf("{\"type\":\"user\",\"cwd\":%s,\"sessionId\":%q}\n", cwd, id))
	writeHandoffState(t, home, prefix+wrongID+".jsonl", fmt.Sprintf("{\"type\":\"user\",\"cwd\":\"/wrong-project\",\"sessionId\":%q}\n", wrongID))
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(valid, old, old); err != nil {
		t.Fatal(err)
	}
	selected, cleanup, err := handoffState(home, dir, &Recipe{Name: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	extra := selected.extra
	if len(extra) != 1 || extra[0].Archive != prefix+id+".jsonl" {
		t.Fatalf("selected %+v", extra)
	}
}
