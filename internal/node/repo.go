package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

// cloneTimeout bounds the server-side clone. A clone that has not finished by
// then fails materialization; the fresh tree is destroyed, never served
// half-populated.
const cloneTimeout = 15 * time.Minute

// cloneOutputLimit caps how much of git's output is kept for the error
// message. Git prints URLs, never credentials, but the bound keeps a hostile
// server from filling the log through a failed clone.
const cloneOutputLimit = 16 << 10

var commitSHA = regexp.MustCompile(`^[0-9a-f]{40}$|^[0-9a-f]{64}$`)

// cloneMarkerPath records, inside the node-owned .remount directory, that the
// declared repository's clone finished. It is excluded from snapshots like
// the env file; a restore carries its checkout and never consults it.
const cloneMarkerPath = EnvFileDir + "/repo"

// needsClone reports whether a first materialization of spec must clone: a
// repository is declared and no snapshot already carries the checkout.
func needsClone(spec proto.WorkspaceSpec) bool {
	return spec.Repo.URL != "" && spec.RestoreFrom == ""
}

func cloneCompleted(handle workspace.Handle) bool {
	entry, err := handle.FS().Stat(cloneMarkerPath)
	return err == nil && !entry.IsDir
}

func markCloneCompleted(handle workspace.Handle, commit string) error {
	if err := handle.FS().Mkdir(EnvFileDir); err != nil {
		return err
	}
	return handle.FS().Write(cloneMarkerPath, []byte(commit+"\n"), 0o644, false, true)
}

// gitConfigEnv returns the GIT_CONFIG_* variables that route the declared
// repository's host through the workspace broker's git connector and
// authenticate with a lease placeholder. It is node-owned environment, never a
// file in the tree: the broker's address changes on every materialize and
// anything written into .git/config would travel in a snapshot and go stale
// after a move. The placeholder is safe to expose; the broker substitutes the
// real token at the edge and records any misuse as leak_blocked.
func gitConfigEnv(b *broker.Broker, repo proto.RepoSpec, leases []proto.BindingLease) []string {
	if b == nil || repo.URL == "" {
		return nil
	}
	_, host, _, err := proto.ParseRepoURL(repo.URL)
	if err != nil {
		return nil
	}
	gitBase := b.GitURL() + "/" + host + "/"
	entries := [][2]string{
		{"url." + gitBase + ".insteadOf", "https://" + host + "/"},
		// Repository objects are byte-identical across hosts. A Windows user's
		// global autocrlf setting must not rewrite the managed checkout.
		{"core.autocrlf", "false"},
		// Credentials arrive as an Authorization header the broker rewrites, so
		// no helper may cache or prompt for anything.
		{"credential.helper", ""},
		// Every fetch goes through the connector over plain HTTP to the broker;
		// nothing else (ssh, file, git://) can be reached from a repo clone.
		{"protocol.allow", "never"},
		{"protocol.http.allow", "always"},
		{"protocol.https.allow", "always"},
	}
	if lease, ok := leaseForHost(leases, host); ok {
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + broker.Placeholder(lease)))
		entries = append(entries, [2]string{"http." + gitBase + ".extraHeader", "Authorization: Basic " + basic})
	}
	env := []string{"GIT_CONFIG_COUNT=" + strconv.Itoa(len(entries)), "GIT_TERMINAL_PROMPT=0"}
	for i, kv := range entries {
		env = append(env, fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, kv[0]), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, kv[1]))
	}
	return env
}

// leaseForHost picks the lease whose destinations cover host. The first match
// in lease order wins; two bindings for one host is a policy mistake the
// control plane already rejects.
func leaseForHost(leases []proto.BindingLease, host string) (proto.BindingLease, bool) {
	for _, lease := range leases {
		if proto.HostMatchesAny(host, lease.Destinations) {
			return lease, true
		}
	}
	return proto.BindingLease{}, false
}

// cloneRepo materializes w.Spec.Repo into an otherwise empty workspace tree.
//
// Authority: the node's live claim on the workspace. Resource: the workspace
// tree. Irreversible action: none here; the caller destroys the fresh tree on
// failure, which loses nothing because no client has seen it. Durable commit
// point: ws.ready, which the caller only sends after this returns nil and the
// completion marker is written. Postcondition: the tree is a checkout of the
// requested ref and the returned commit is what repo.cloned names.
//
// Git runs inside the workspace through the backend (so a docker workspace
// clones inside its container with its own git) and reaches the hosting
// service only through the broker's /git/ surface: the workspace never holds
// the credential, and the connector refuses everything that is not a
// smart-HTTP fetch of exactly the declared repository.
func (n *Node) cloneRepo(ctx context.Context, w *ws) (string, error) {
	repo, err := w.Spec.Repo.Normalize()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, cloneTimeout)
	defer cancel()
	env := append(w.broker.EnvFor(), gitConfigEnv(w.broker, repo, w.leases)...)
	run := func(args ...string) (string, error) {
		spec := session.Spec{WS: w.ID, Kind: proto.SessionExec, Program: append([]string{"git"}, args...), Cwd: "", Env: env}
		if err := w.handle.Prepare(&spec); err != nil {
			return "", err
		}
		cmd := exec.CommandContext(ctx, spec.Program[0], spec.Program[1:]...)
		cmd.Dir = spec.Cwd
		cmd.Env = spec.Env
		cmd.WaitDelay = 5 * time.Second
		var out bytes.Buffer
		cmd.Stdout = &limitedBuffer{buf: &out, limit: cloneOutputLimit}
		cmd.Stderr = cmd.Stdout
		err := cmd.Run()
		if err != nil {
			if ctx.Err() != nil {
				return "", proto.Err(proto.CodeUnreachable, "git %s: %v", args[0], ctx.Err())
			}
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				// git echoes the URL it dialed, which is the broker capability;
				// the error travels into ws.released and node logs.
				output := strings.ReplaceAll(strings.TrimSpace(out.String()), w.broker.BaseURL(), "$REMOUNT_BROKER")
				return "", proto.Err(proto.CodeInternal, "git %s exited %d: %s", args[0], exit.ExitCode(), output)
			}
			return "", proto.Err(proto.CodeInternal, "git %s: %v", args[0], err)
		}
		return strings.TrimSpace(out.String()), nil
	}
	source := repo.URL + ".git"
	depth := []string{}
	if repo.Depth > 0 {
		depth = []string{"--depth", strconv.Itoa(repo.Depth)}
	}
	switch {
	case repo.Ref == "":
		if _, err := run(append([]string{"clone", "-q", "--no-hardlinks"}, append(depth, source, ".")...)...); err != nil {
			return "", err
		}
	case commitSHA.MatchString(repo.Ref):
		// clone --branch does not take a commit. Fetch exactly that object
		// (hosting services permit reachable-SHA wants) and detach onto it. The
		// clone's depth is the requested one so a full-history request is not
		// silently shallow.
		if _, err := run(append([]string{"clone", "-q", "--no-checkout", "--no-hardlinks"}, append(depth, source, ".")...)...); err != nil {
			return "", err
		}
		if _, err := run(append([]string{"fetch", "-q"}, append(depth, "origin", repo.Ref)...)...); err != nil {
			return "", err
		}
		if _, err := run("checkout", "-q", "--detach", repo.Ref); err != nil {
			return "", err
		}
	default:
		if _, err := run(append([]string{"clone", "-q", "--no-hardlinks", "--branch", repo.Ref}, append(depth, source, ".")...)...); err != nil {
			return "", err
		}
	}
	if repo.Branch != "" {
		if _, err := run("checkout", "-q", "-b", repo.Branch); err != nil {
			return "", err
		}
	}
	commit, err := run("rev-parse", "--verify", "HEAD")
	if err != nil {
		return "", err
	}
	if !commitSHA.MatchString(commit) {
		return "", proto.Err(proto.CodeInternal, "git rev-parse returned %q", commit)
	}
	return commit, nil
}

// limitedBuffer keeps the first limit bytes and drops the rest silently, so a
// chatty or hostile git never grows an error message without bound.
type limitedBuffer struct {
	buf   *bytes.Buffer
	limit int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if room := l.limit - l.buf.Len(); room > 0 {
		if len(p) > room {
			l.buf.Write(p[:room])
		} else {
			l.buf.Write(p)
		}
	}
	return len(p), nil
}

// cloneEventPayload is what repo.cloned carries: enough for a reader to know
// which tree a workspace started from, nothing about how it was fetched.
func cloneEventPayload(repo proto.RepoSpec, commit, backend string) map[string]any {
	payload := map[string]any{"repo": repo.URL, "commit": commit, "backend": backend}
	if repo.Ref != "" {
		payload["ref"] = repo.Ref
	}
	if repo.Depth > 0 {
		payload["depth"] = repo.Depth
	}
	if repo.Branch != "" {
		payload["branch"] = repo.Branch
	}
	return payload
}
