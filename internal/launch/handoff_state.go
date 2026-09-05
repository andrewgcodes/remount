package launch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

const (
	handoffMaxFileBytes    = 64 << 20
	handoffMaxStateBytes   = 256 << 20
	handoffMaxStateFiles   = 4096
	handoffMaxStateEntries = 100000
)

var handoffPrivatePaths = []string{".claude", ".claude.json", ".codex", ".env", ".env.*"}

var conversationIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type handoffStateSelection struct {
	extra        []localfs.ExtraTree
	conversation string
	transcript   string
}

func validateConversationPath(recipe, id, transcript string) error {
	parts := strings.Split(transcript, "/")
	valid := path.Clean(transcript) == transcript
	switch recipe {
	case "claude":
		valid = valid && len(parts) == 4 && parts[0] == ".claude" && parts[1] == "projects" && parts[3] == id+".jsonl"
	case "codex":
		valid = valid && len(parts) == 6 && parts[0] == ".codex" && parts[1] == "sessions" && strings.HasPrefix(parts[5], "rollout-") && strings.HasSuffix(parts[5], "-"+id+".jsonl")
	default:
		valid = false
	}
	if !valid {
		return errors.New("conversation transcript path does not match the selected recipe and UUID")
	}
	return nil
}

type handoffConversation struct {
	name     string
	archive  string
	id       string
	modified time.Time
	data     []byte
}

func handoffBackend(spec proto.WorkspaceSpec, nodes []proto.NodeStatus) error {
	for _, node := range nodes {
		if !node.Online {
			continue
		}
		for _, backend := range node.Info.BackendDescriptors {
			if spec.Requires.Backend != "" && backend.Name != spec.Requires.Backend {
				continue
			}
			if spec.MountPath != "" && spec.MountPath != proto.DefaultMountPath && !backend.Runtime.MountPath {
				continue
			}
			if err := proto.ValidateBackendSecurity(spec.Security, backend); err == nil {
				return nil
			}
		}
	}
	if spec.MountPath != "" && spec.MountPath != proto.DefaultMountPath {
		return fmt.Errorf("handoff needs an online node with a namespaced backend for %s satisfying backend %q and security %s; nothing was uploaded", spec.MountPath, spec.Requires.Backend, spec.Security.Profile)
	}
	return fmt.Errorf("handoff needs an online node satisfying backend %q and security %s; nothing was uploaded", spec.Requires.Backend, spec.Security.Profile)
}

func packHandoff(dir string, opts localfs.PackOptions, w io.Writer) (localfs.Manifest, error) {
	var m localfs.Manifest
	sel, err := localfs.Select(dir, opts)
	if err != nil {
		return m, err
	}
	defer sel.Close()
	m.Warnings = sel.Warnings
	trees := []artifact.Tree{{Root: sel.Dir, Skip: func(rel string, isDir bool) bool {
		return artifact.Excluded(rel, handoffPrivatePaths) || sel.Skip(rel, isDir)
	}}}
	for _, e := range opts.Extra {
		base := filepath.Base(e.Local)
		if base != path.Base(e.Archive) {
			return m, fmt.Errorf("saved state file %s must keep its name in the archive", e.Local)
		}
		trees = append(trees, artifact.Tree{Root: filepath.Dir(e.Local), Prefix: path.Dir(e.Archive), Skip: func(rel string, _ bool) bool {
			return rel != base
		}})
	}
	stats, err := artifact.SnapshotTrees(trees, w)
	if err != nil {
		return m, err
	}
	m.Files, m.Dirs, m.Symlinks, m.Bytes, m.Excluded = stats.Files, stats.Dirs, stats.Symlinks, stats.Bytes, stats.Skipped
	return m, nil
}

func scopedHandoff(r *Recipe) bool {
	return r.Name == "claude" || r.Name == "codex"
}

func claudeProjectKey(dir string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, dir)
}

func handoffCanonicalCWD(cwd, dir string) bool {
	if !filepath.IsAbs(cwd) {
		return false
	}
	canonical, err := filepath.EvalSymlinks(cwd)
	return err == nil && canonical == dir
}

func handoffMetadata(data []byte, recipe, dir, wantID string) (string, bool) {
	return conversationMetadata(data, recipe, wantID, func(cwd string) bool { return handoffCanonicalCWD(cwd, dir) })
}

func conversationMetadata(data []byte, recipe, wantID string, matchesCWD func(string) bool) (string, bool) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), handoffMaxFileBytes)
	seen := false
	conversation := false
	id := ""
	first := true
	for scanner.Scan() {
		var row struct {
			Type      string `json:"type"`
			CWD       string `json:"cwd"`
			SessionID string `json:"sessionId"`
			Payload   struct {
				Type      string `json:"type"`
				CWD       string `json:"cwd"`
				ID        string `json:"id"`
				SessionID string `json:"session_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return "", false
		}
		if recipe == "codex" {
			if first && row.Type != "session_meta" {
				return "", false
			}
			first = false
			if row.Type == "turn_context" && row.Payload.CWD != "" && !matchesCWD(row.Payload.CWD) {
				return "", false
			}
			if row.Type == "session_meta" && row.Payload.SessionID != "" && row.Payload.SessionID != wantID {
				return "", false
			}
			if row.Type != "session_meta" {
				if row.Type == "response_item" && row.Payload.Type == "message" || row.Type == "event_msg" && row.Payload.Type == "user_message" {
					conversation = true
				}
				continue
			}
			row.CWD, row.SessionID = row.Payload.CWD, row.Payload.ID
		} else if row.Type == "user" || row.Type == "assistant" {
			conversation = true
		}
		if row.CWD == "" && row.SessionID == "" {
			continue
		}
		if row.CWD != "" && !matchesCWD(row.CWD) {
			return "", false
		}
		if row.SessionID != "" {
			if row.SessionID != wantID {
				return "", false
			}
			id = row.SessionID
		}
		if row.CWD != "" && row.SessionID != "" {
			seen = true
		}
	}
	return id, seen && conversation && scanner.Err() == nil
}

func handoffReadRegular(root *os.Root, name string) ([]byte, fs.FileInfo, error) {
	current := root
	defer func() {
		if current != root {
			current.Close()
		}
	}()
	parts := strings.Split(name, "/")
	for _, part := range parts[:len(parts)-1] {
		st, err := current.Lstat(part)
		if err != nil {
			return nil, nil, err
		}
		if !st.IsDir() {
			return nil, nil, fmt.Errorf("saved state path %s is not a regular path", name)
		}
		next, err := current.OpenRoot(part)
		if err != nil {
			return nil, nil, err
		}
		opened, err := next.Stat(".")
		if err != nil || !os.SameFile(st, opened) {
			next.Close()
			return nil, nil, fmt.Errorf("saved state directory %s changed during selection; retry handoff", name)
		}
		if current != root {
			current.Close()
		}
		current = next
	}
	base := parts[len(parts)-1]
	before, err := current.Lstat(base)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > handoffMaxFileBytes {
		return nil, nil, fmt.Errorf("saved state file %s is not regular or exceeds %d bytes", name, handoffMaxFileBytes)
	}
	f, err := current.Open(base)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !st.Mode().IsRegular() || !os.SameFile(before, st) {
		return nil, nil, fmt.Errorf("saved state file %s changed during selection; retry handoff", name)
	}
	data, err := io.ReadAll(io.LimitReader(f, handoffMaxFileBytes+1))
	if err != nil {
		return nil, nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if len(data) > handoffMaxFileBytes || int64(len(data)) != st.Size() || after.Size() != st.Size() || !after.ModTime().Equal(st.ModTime()) {
		return nil, nil, fmt.Errorf("saved state file %s changed or exceeds %d bytes; retry handoff after saving", name, handoffMaxFileBytes)
	}
	return data, st, nil
}

func handoffState(home, dir string, r *Recipe) (handoffStateSelection, func(), error) {
	root, err := os.OpenRoot(home)
	if err != nil {
		return handoffStateSelection{}, nil, fmt.Errorf("read saved %s conversations: %w", r.Name, err)
	}
	defer root.Close()
	state := "." + r.Name
	st, err := root.Lstat(state)
	if err != nil || !st.IsDir() {
		return handoffStateSelection{}, nil, fmt.Errorf("no saved %s conversation for %s under %s (state must be a real directory)", r.Name, dir, home)
	}
	var latest *handoffConversation
	entries := 0
	err = fs.WalkDir(root.FS(), state, func(name string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > handoffMaxStateEntries {
			return fmt.Errorf("saved state discovery exceeds %d entries", handoffMaxStateEntries)
		}
		parts := strings.Split(name, "/")
		if d.IsDir() {
			if len(parts) == 2 && parts[1] != "projects" && r.Name == "claude" || len(parts) == 2 && parts[1] != "sessions" && r.Name == "codex" {
				return fs.SkipDir
			}
			if r.Name == "claude" && len(parts) > 3 || r.Name == "codex" && len(parts) > 5 {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || !strings.HasSuffix(name, ".jsonl") {
			return nil
		}
		base := strings.TrimSuffix(path.Base(name), ".jsonl")
		id := base
		if r.Name == "claude" {
			if len(parts) != 4 || parts[1] != "projects" || strings.HasPrefix(base, "agent-") {
				return nil
			}
		} else {
			if len(parts) != 6 || parts[1] != "sessions" || !strings.HasPrefix(base, "rollout-") || len(base) < 36 {
				return nil
			}
			id = base[len(base)-36:]
		}
		if !conversationIDPattern.MatchString(id) {
			return nil
		}
		data, info, err := handoffReadRegular(root, name)
		if err != nil {
			return err
		}
		if _, ok := handoffMetadata(data, r.Name, dir, id); !ok {
			return nil
		}
		if latest == nil || info.ModTime().After(latest.modified) || info.ModTime().Equal(latest.modified) && name > latest.name {
			archive := name
			if r.Name == "claude" {
				archive = ".claude/projects/" + claudeProjectKey(dir) + "/" + path.Base(name)
			}
			latest = &handoffConversation{name: name, archive: archive, id: id, modified: info.ModTime(), data: data}
		}
		return nil
	})
	if err != nil {
		return handoffStateSelection{}, nil, fmt.Errorf("select saved %s conversation: %w", r.Name, err)
	}
	if latest == nil {
		return handoffStateSelection{}, nil, fmt.Errorf("no saved %s conversation matching checkout %s and session metadata under %s; save a conversation in that checkout before handoff", r.Name, dir, home)
	}
	stage, err := os.MkdirTemp("", "remount-handoff-state-*")
	if err != nil {
		return handoffStateSelection{}, nil, err
	}
	cleanup := func() { _ = os.RemoveAll(stage) }
	var extra []localfs.ExtraTree
	var total int64
	add := func(name string, data []byte) error {
		total += int64(len(data))
		if total > handoffMaxStateBytes || len(extra) >= handoffMaxStateFiles {
			return fmt.Errorf("saved conversation exceeds handoff limit (%d bytes, %d files)", handoffMaxStateBytes, handoffMaxStateFiles)
		}
		local := filepath.Join(stage, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(local), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(local, data, 0o600); err != nil {
			return err
		}
		extra = append(extra, localfs.ExtraTree{Local: local, Archive: name})
		return nil
	}
	if err := add(latest.archive, latest.data); err != nil {
		cleanup()
		return handoffStateSelection{}, nil, err
	}
	if r.Name == "claude" {
		subtree := strings.TrimSuffix(latest.name, ".jsonl")
		archive := strings.TrimSuffix(latest.archive, ".jsonl")
		err = fs.WalkDir(root.FS(), subtree, func(name string, d fs.DirEntry, walkErr error) error {
			if errors.Is(walkErr, fs.ErrNotExist) && name == subtree {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			entries++
			if entries > handoffMaxStateEntries {
				return fmt.Errorf("saved state discovery exceeds %d entries", handoffMaxStateEntries)
			}
			if d.IsDir() {
				if name != subtree && name != subtree+"/subagents" && name != subtree+"/tool-results" {
					return fs.SkipDir
				}
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			rel := strings.TrimPrefix(name, subtree+"/")
			subagent := path.Dir(rel) == "subagents" && strings.HasPrefix(path.Base(rel), "agent-") && strings.HasSuffix(rel, ".jsonl")
			toolResult := path.Dir(rel) == "tool-results" && strings.HasSuffix(rel, ".txt") && !strings.HasPrefix(path.Base(rel), ".")
			if !subagent && !toolResult {
				return nil
			}
			data, _, err := handoffReadRegular(root, name)
			if err != nil {
				return err
			}
			if subagent {
				if _, ok := handoffMetadata(data, r.Name, dir, latest.id); !ok {
					return fmt.Errorf("saved subagent %s does not match the selected conversation metadata", name)
				}
			}
			return add(archive+"/"+rel, data)
		})
		if err != nil {
			cleanup()
			return handoffStateSelection{}, nil, fmt.Errorf("read saved %s conversation attachments: %w", r.Name, err)
		}
	}
	return handoffStateSelection{extra: extra, conversation: latest.id, transcript: latest.archive}, cleanup, nil
}
