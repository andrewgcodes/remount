package sim

import (
	"bytes"
	"crypto/x509"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/connector/gittest"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

const fakeInstallationToken = "ghs_FAKE_INSTALLATION_TOKEN_0123456789"

// gitWorld is a hosting service behind TLS, a control plane that holds the
// token as a binding, and a node that trusts the service's certificate.
type gitWorld struct {
	*world
	svc  *gittest.Service
	host string
}

func newGitWorld(t *testing.T) *gitWorld {
	t.Helper()
	svc := gittest.New(t, fakeInstallationToken)
	up := httptest.NewTLSServer(svc.Handler())
	t.Cleanup(up.Close)
	host := strings.TrimPrefix(up.URL, "https://")
	w := newWorld(t, control.Binding{ID: "b_gh", Secret: fakeInstallationToken, Destinations: []string{host}, TTLSec: 60})
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	w.nodeWithBrokerRoots("n1", nil, roots)
	w.nodeWithBrokerRoots("n2", nil, roots)
	return &gitWorld{world: w, svc: svc, host: host}
}

// scanForToken counts the regular files under root that contain the token.
// It is a count, not a boolean, so a planted canary proves the scan runs.
func scanForToken(t *testing.T, root, token string) []string {
	t.Helper()
	var hits []string
	err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if bytes.Contains(b, []byte(token)) {
				hits = append(hits, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hits
}

func eventSeq(evs []proto.Event, typ string) (uint64, []byte) {
	for _, e := range evs {
		if e.Type == typ {
			return e.Seq, e.Payload
		}
	}
	return 0, nil
}

// ADR 0054: the node clones the declared repository through its own broker
// before ws.ready; the workspace sees the checkout and a placeholder, never
// the token; the clone and every git round trip are events.
func TestRepoClonedAtMaterializeWithoutTokenInWorkspace(t *testing.T) {
	g := newGitWorld(t)
	initial := g.svc.CreateRepo(t, "acme/app", map[string]string{"README": "hello\n", "src/main.go": "package main\n"})
	c := g.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Bindings: []string{"b_gh"},
		Repo:     proto.RepoSpec{URL: "https://" + g.host + "/acme/app", Ref: "main"},
	})
	ctx := ctxT(t, 120*time.Second)

	readme, err := c.ReadFile(ctx, ws.ID, "README")
	if err != nil || string(readme) != "hello\n" {
		t.Fatalf("checkout missing: %v %q", err, readme)
	}
	head, _, exit, err := c.Run(ctx, ws.ID, "git", "rev-parse", "HEAD")
	if err != nil || exit.Code != 0 || strings.TrimSpace(string(head)) != initial {
		t.Fatalf("HEAD %q exit %+v err %v; want %s", head, exit, err, initial)
	}

	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Node events reach the canonical log asynchronously, so the proof that
	// the clone preceded readiness is the checkout read above, not seq order.
	clonedSeq, payload := eventSeq(evs, proto.EvRepoCloned)
	if clonedSeq == 0 {
		t.Fatal("no repo.cloned event")
	}
	var cloned struct {
		Commit string `cbor:"commit"`
		Repo   string `cbor:"repo"`
		Ref    string `cbor:"ref"`
	}
	if err := proto.Unmarshal(payload, &cloned); err != nil || cloned.Commit != initial || cloned.Ref != "main" || !strings.HasSuffix(cloned.Repo, "/acme/app") {
		t.Fatalf("repo.cloned payload %+v err %v", cloned, err)
	}
	fetches := 0
	for _, e := range evs {
		if e.Type != proto.EvCredUsed && e.Type != proto.EvEgressAllowed {
			continue
		}
		var p struct {
			Connector string `cbor:"connector"`
			Op        string `cbor:"op"`
			Repo      string `cbor:"repo"`
		}
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.Connector == proto.EgressConnectorGit {
			if p.Op != "fetch" || p.Repo != "acme/app" {
				t.Fatalf("git egress event %+v", p)
			}
			fetches++
		}
	}
	if fetches == 0 {
		t.Fatal("no git egress events attributed to the clone")
	}

	// Leak scan: tree (including .git and .remount/env), session env, events.
	info, err := c.WorkspaceInfo(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if hits := scanForToken(t, info.Root, fakeInstallationToken); len(hits) != 0 {
		t.Fatalf("token on workspace disk: %v", hits)
	}
	if err := c.WriteFile(ctx, ws.ID, "canary.txt", []byte("x "+fakeInstallationToken+" y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if hits := scanForToken(t, info.Root, fakeInstallationToken); len(hits) != 1 || filepath.Base(hits[0]) != "canary.txt" {
		t.Fatalf("scan did not find the planted canary: %v", hits)
	}
	out, _, _, _ := c.Run(ctx, ws.ID, "sh", "-c", `env | grep -c `+fakeInstallationToken+` || true`)
	if strings.TrimSpace(string(out)) != "0" {
		t.Fatalf("token in session environment (%s matches)", strings.TrimSpace(string(out)))
	}
	out, _, _, _ = c.Run(ctx, ws.ID, "sh", "-c", `env | grep -c '^GIT_CONFIG_KEY_' || true`)
	if strings.TrimSpace(string(out)) == "0" {
		t.Fatal("session environment carries no git routing through the broker")
	}
	for _, e := range evs {
		if bytes.Contains(e.Payload, []byte(fakeInstallationToken)) {
			t.Fatalf("token in event %s", e.Type)
		}
	}
	for _, seen := range g.svc.SeenAuthorization() {
		if strings.Contains(seen, "ref:b_gh") {
			t.Fatal("placeholder forwarded to the hosting service")
		}
	}

	// The checkout is a working remote: a session commits and pushes through
	// the same broker, and the hosting service ends up at the new commit.
	script := `git config user.email w@example.invalid && git config user.name w && git add canary.txt && git commit -q -m c && git push -q origin HEAD:main && git rev-parse HEAD`
	out, errb, exit, err := c.Run(ctx, ws.ID, "sh", "-c", script)
	if err != nil || exit.Code != 0 {
		t.Fatalf("push from workspace: %v %+v %s %s", err, exit, out, errb)
	}
	if g.svc.Head(t, "acme/app", "main") != strings.TrimSpace(string(out)) {
		t.Fatalf("hosting service main %s, workspace pushed %s", g.svc.Head(t, "acme/app", "main"), out)
	}

	// After a move the checkout travels in the snapshot, the repository is
	// not cloned again, and git still routes through the new node's broker.
	requests := g.svc.Requests()
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: ""})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitClaimed(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	if g.svc.Requests() != requests {
		t.Fatal("move re-cloned the repository")
	}
	out, errb, exit, err = c.Run(ctx, ws.ID, "sh", "-c", "git fetch -q origin && git rev-parse HEAD && cat canary.txt | grep -c "+fakeInstallationToken)
	if err != nil || exit.Code != 0 || !strings.Contains(string(out), "1") {
		t.Fatalf("fetch after move: %v %+v %s %s", err, exit, out, errb)
	}
	evs, _ = c.ReadEvents(ctx, 1, ws.ID)
	clones := 0
	for _, e := range evs {
		if e.Type == proto.EvRepoCloned {
			clones++
		}
	}
	if clones != 1 {
		t.Fatalf("repo.cloned emitted %d times across a move", clones)
	}
	envFile, _ := c.ReadFile(ctx, ws.ID, node.EnvFilePath)
	if !strings.Contains(string(envFile), "GIT_CONFIG_") || strings.Contains(string(envFile), fakeInstallationToken) {
		t.Fatalf("env file after move:\n%s", envFile)
	}
}

// A clone that cannot complete fails materialization closed: the workspace
// never reaches claimed with an empty tree, and once the repository exists
// the retry clones it for real.
func TestRepoCloneFailureIsNotServedEmpty(t *testing.T) {
	g := newGitWorld(t)
	c := g.client("c1")
	ctx := ctxT(t, 120*time.Second)
	spec := proto.WorkspaceSpec{
		Bindings: []string{"b_gh"},
		Repo:     proto.RepoSpec{URL: "https://" + g.host + "/acme/late", Ref: "main"},
	}
	ws, err := c.CreateWorkspace(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitClaimed(ctxT(t, 4*time.Second), ws.ID); err == nil {
		t.Fatal("workspace became claimed although its repository does not exist")
	}
	cur, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.State == proto.WSClaimed {
		t.Fatalf("state %s with a failed clone", cur.State)
	}
	evs, _ := c.ReadEvents(ctx, 1, ws.ID)
	failures := 0
	for _, e := range evs {
		if e.Type == proto.EvWSReleased && bytes.Contains(e.Payload, []byte("clone")) {
			failures++
			if bytes.Contains(e.Payload, []byte("/c/")) {
				t.Fatalf("broker capability leaked into ws.released: %s", e.Payload)
			}
		}
		if e.Type == proto.EvRepoCloned || e.Type == proto.EvWSClaimed {
			t.Fatalf("%s emitted for a failed clone", e.Type)
		}
	}
	// Retries back off (1s, 2s, ...) instead of spinning: a few attempts in
	// four seconds, not hundreds.
	if failures == 0 || failures > 6 {
		t.Fatalf("%d failed materializations in 4s; want a handful with backoff", failures)
	}

	// The repository appears; the next materialization clones it rather than
	// adopting whatever the failed attempt left behind.
	commit := g.svc.CreateRepo(t, "acme/late", map[string]string{"LATE": "yes\n"})
	claimed, err := c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatalf("workspace never recovered after the repository appeared: %v", err)
	}
	late, err := c.ReadFile(ctx, claimed.ID, "LATE")
	if err != nil || string(late) != "yes\n" {
		t.Fatalf("recovered workspace has no checkout: %v %q", err, late)
	}
	head, _, _, _ := c.Run(ctx, ws.ID, "git", "rev-parse", "HEAD")
	if strings.TrimSpace(string(head)) != commit {
		t.Fatalf("HEAD %q want %s", head, commit)
	}
}

// A repository declared without a covering binding still clones when the
// hosting service is public; with a private one the clone fails closed
// instead of prompting or hanging.
func TestRepoWithoutBindingUsesNoCredential(t *testing.T) {
	svc := gittest.New(t, "")
	initial := svc.CreateRepo(t, "pub/lib", map[string]string{"L": "1\n"})
	up := httptest.NewTLSServer(svc.Handler())
	defer up.Close()
	host := strings.TrimPrefix(up.URL, "https://")
	w := newWorld(t)
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	w.nodeWithBrokerRoots("n1", nil, roots)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{Repo: proto.RepoSpec{URL: "https://" + host + "/pub/lib"}})
	ctx := ctxT(t, 60*time.Second)
	head, _, _, _ := c.Run(ctx, ws.ID, "git", "rev-parse", "HEAD")
	if strings.TrimSpace(string(head)) != initial {
		t.Fatalf("HEAD %q want %s", head, initial)
	}
	for _, auth := range svc.SeenAuthorization() {
		if auth != "" {
			t.Fatalf("credential sent to a public repository: %q", auth)
		}
	}
}

// Ref shapes: a tag, a full commit SHA (detached, not the branch tip) and a
// shallow depth each produce exactly the tree they name.
func TestRepoRefShapes(t *testing.T) {
	g := newGitWorld(t)
	first := g.svc.CreateRepo(t, "acme/shapes", map[string]string{"V": "1\n"})
	second := g.svc.Commit(t, "acme/shapes", "v2", map[string]string{"V": "2\n"})
	if first == second {
		t.Fatal("fixture has one commit")
	}
	c := g.client("c1")
	ctx := ctxT(t, 180*time.Second)
	url := "https://" + g.host + "/acme/shapes"
	git := func(ws string, args ...string) string {
		t.Helper()
		out, _, exit, err := c.Run(ctx, ws, append([]string{"git"}, args...)...)
		if err != nil || exit.Code != 0 {
			t.Fatalf("git %v: exit %+v err %v: %s", args, exit, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	tagged := mustWS(t, c, proto.WorkspaceSpec{Bindings: []string{"b_gh"}, Repo: proto.RepoSpec{URL: url, Ref: "v2"}})
	if head := git(tagged.ID, "rev-parse", "HEAD"); head != second {
		t.Fatalf("tag checkout HEAD %s want %s", head, second)
	}

	pinned := mustWS(t, c, proto.WorkspaceSpec{Bindings: []string{"b_gh"}, Repo: proto.RepoSpec{URL: url, Ref: first}})
	if head := git(pinned.ID, "rev-parse", "HEAD"); head != first {
		t.Fatalf("sha checkout HEAD %s want %s", head, first)
	}
	if v, _ := c.ReadFile(ctx, pinned.ID, "V"); string(v) != "1\n" {
		t.Fatalf("sha checkout tree V=%q", v)
	}
	if git(pinned.ID, "rev-parse", "--is-shallow-repository") != "false" {
		t.Fatal("full-history sha checkout came out shallow")
	}

	shallow := mustWS(t, c, proto.WorkspaceSpec{Bindings: []string{"b_gh"}, Repo: proto.RepoSpec{URL: url, Ref: "main", Depth: 1}})
	if head := git(shallow.ID, "rev-parse", "HEAD"); head != second {
		t.Fatalf("shallow HEAD %s want %s", head, second)
	}
	if n := git(shallow.ID, "rev-list", "--count", "HEAD"); n != "1" {
		t.Fatalf("shallow depth 1 has %s commits", n)
	}
	evs, _ := c.ReadEvents(ctx, 1, shallow.ID)
	_, payload := eventSeq(evs, proto.EvRepoCloned)
	var cloned struct {
		Depth  int    `cbor:"depth"`
		Commit string `cbor:"commit"`
	}
	if err := proto.Unmarshal(payload, &cloned); err != nil || cloned.Depth != 1 || cloned.Commit != second {
		t.Fatalf("repo.cloned %+v err %v", cloned, err)
	}
}
