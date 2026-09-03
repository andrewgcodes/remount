package broker

import (
	"crypto/x509"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"remount.dev/remount/internal/connector/gittest"
	"remount.dev/remount/internal/proto"
)

const realGitToken = "ghs_REAL_INSTALLATION_TOKEN"

// gitThroughBroker runs git with the same GIT_CONFIG_* routing a workspace
// receives: https://<host>/ rewritten to the broker's /git/<host>/ and the
// lease placeholder as the Basic password. The real token is not in its env.
func gitThroughBroker(t *testing.T, b *Broker, host, placeholder, dir string, args ...string) (string, error) {
	t.Helper()
	gitBase := b.GitURL() + "/" + host + "/"
	entries := [][2]string{
		{"url." + gitBase + ".insteadOf", "https://" + host + "/"},
		{"credential.helper", ""},
	}
	if placeholder != "" {
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + placeholder))
		entries = append(entries, [2]string{"http." + gitBase + ".extraHeader", "Authorization: Basic " + basic})
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=w", "GIT_AUTHOR_EMAIL=w@example.invalid", "GIT_COMMITTER_NAME=w", "GIT_COMMITTER_EMAIL=w@example.invalid",
		"GIT_CONFIG_COUNT=" + strconv.Itoa(len(entries)),
	}
	for i, kv := range entries {
		env = append(env, "GIT_CONFIG_KEY_"+strconv.Itoa(i)+"="+kv[0], "GIT_CONFIG_VALUE_"+strconv.Itoa(i)+"="+kv[1])
	}
	for _, kv := range env {
		if strings.Contains(kv, realGitToken) {
			t.Fatalf("test env carries the real token: %s", kv)
		}
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func gitPolicyRule(host string, push bool, repos ...string) proto.EgressRule {
	return proto.EgressRule{
		ID: "code", Connector: proto.EgressConnectorGit, Protocol: proto.EgressProtocolHTTPS,
		Hosts: []string{host}, Repos: repos, Push: push,
	}
}

// startGitBroker fronts svc with TLS and starts a broker holding a lease for
// it. configure sees the upstream host so it can write rules or a RepoSpec.
func startGitBroker(t *testing.T, svc *gittest.Service, rec *recorder, configure func(host string, o *Options)) (*Broker, string) {
	t.Helper()
	up := httptest.NewTLSServer(svc.Handler())
	t.Cleanup(up.Close)
	pu, _ := url.Parse(up.URL)
	pool := x509.NewCertPool()
	pool.AddCert(up.Certificate())
	opts := Options{
		WS: "ws_t", Tenant: "t", Generation: 1, Principal: "a_test",
		Leases:       []proto.BindingLease{{ID: "b_gh", Secret: realGitToken, Destinations: []string{pu.Host}}},
		AllowPrivate: []string{"127.0.0.1", "localhost"}, Audit: rec.add, RootCAs: pool,
	}
	if configure != nil {
		configure(pu.Host, &opts)
	}
	b := New(opts)
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, pu.Host
}

func (r *recorder) find(connector, op, decision string) (Audit, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.ev {
		if e.Connector == connector && e.Op == op && e.Decision == decision {
			return e, true
		}
	}
	return Audit{}, false
}

// A workspace clones and pushes with only a placeholder; the hosting service
// receives the real token; the audit names connector, op and repo; nothing
// on disk in the clone carries the token.
func TestGitConnectorCloneAndPushThroughBroker(t *testing.T) {
	svc := gittest.New(t, realGitToken)
	initial := svc.CreateRepo(t, "acme/app", map[string]string{"README": "hi\n"})
	rec := &recorder{}
	b, host := startGitBroker(t, svc, rec, func(host string, o *Options) {
		o.Network = proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{gitPolicyRule(host, true, "acme/*")}}
	})
	dir := t.TempDir()
	out, err := gitThroughBroker(t, b, host, "ref:b_gh", dir, "clone", "-q", "https://"+host+"/acme/app.git", "work")
	if err != nil {
		t.Fatalf("clone through broker: %v\n%s", err, out)
	}
	work := filepath.Join(dir, "work")
	if head, _ := gitThroughBroker(t, b, host, "", work, "rev-parse", "HEAD"); strings.TrimSpace(head) != initial {
		t.Fatalf("cloned HEAD %q want %q", head, initial)
	}
	if err := os.WriteFile(filepath.Join(work, "NEW"), []byte("pushed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "NEW"}, {"commit", "-q", "-m", "push through broker"}, {"push", "-q", "origin", "HEAD:main"}} {
		if out, err := gitThroughBroker(t, b, host, "ref:b_gh", work, args...); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	pushed, _ := gitThroughBroker(t, b, host, "", work, "rev-parse", "HEAD")
	if svc.Head(t, "acme/app", "main") != strings.TrimSpace(pushed) {
		t.Fatalf("hosting service main=%s want %s", svc.Head(t, "acme/app", "main"), pushed)
	}
	if svc.Pushes() == 0 {
		t.Fatal("no receive-pack reached the hosting service")
	}
	for _, auth := range svc.SeenAuthorization() {
		if strings.Contains(auth, "ref:b_gh") {
			t.Fatal("placeholder reached the hosting service unsubstituted")
		}
	}
	// The workspace-side tree carries the placeholder at most, never the token.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), realGitToken) {
			t.Fatalf("token written into %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fetch, ok := rec.find(proto.EgressConnectorGit, "fetch", DecisionSubstituted)
	if !ok || fetch.Repo != "acme/app" || fetch.Rule != "code" || fetch.Binding != "b_gh" {
		t.Fatalf("no substituted fetch audit for acme/app: %+v ok=%v", fetch, ok)
	}
	if push, ok := rec.find(proto.EgressConnectorGit, "push", DecisionSubstituted); !ok || push.Repo != "acme/app" {
		t.Fatalf("no substituted push audit: %+v ok=%v", push, ok)
	}
	for _, e := range rec.ev {
		if strings.Contains(e.Reason, realGitToken) || strings.Contains(e.Path, realGitToken) {
			t.Fatalf("token in audit: %+v", e)
		}
	}
}

// Everything outside the covered repository and operation is refused before
// the credential is released: a sibling repo, a push under a fetch-only rule,
// the hosting service's API on the same origin, and the old /d/ path.
func TestGitConnectorRefusesOutsideScope(t *testing.T) {
	svc := gittest.New(t, realGitToken)
	svc.CreateRepo(t, "acme/app", map[string]string{"README": "hi\n"})
	svc.CreateRepo(t, "acme/secret", map[string]string{"KEY": "no\n"})
	rec := &recorder{}
	b, host := startGitBroker(t, svc, rec, func(host string, o *Options) {
		o.Network = proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{gitPolicyRule(host, false, "acme/app")}}
	})
	dir := t.TempDir()
	if out, err := gitThroughBroker(t, b, host, "ref:b_gh", dir, "clone", "-q", "https://"+host+"/acme/app.git", "work"); err != nil {
		t.Fatalf("covered clone failed: %v\n%s", err, out)
	}
	before := svc.Requests()
	if out, err := gitThroughBroker(t, b, host, "ref:b_gh", dir, "clone", "-q", "https://"+host+"/acme/secret.git", "secret"); err == nil {
		t.Fatalf("sibling repository cloned:\n%s", out)
	}
	work := filepath.Join(dir, "work")
	if err := os.WriteFile(filepath.Join(work, "NEW"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "NEW"}, {"commit", "-q", "-m", "x"}} {
		if out, err := gitThroughBroker(t, b, host, "", work, args...); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	if out, err := gitThroughBroker(t, b, host, "ref:b_gh", work, "push", "-q", "origin", "HEAD:main"); err == nil {
		t.Fatalf("push succeeded under a fetch-only rule:\n%s", out)
	}
	if svc.Requests() != before || svc.Pushes() != 0 {
		t.Fatalf("refused operations reached the hosting service: requests %d->%d pushes %d", before, svc.Requests(), svc.Pushes())
	}
	if d, ok := rec.lastDecision(DecisionDenied); !ok || (d.Op != "push" && d.Op != "") {
		t.Fatalf("push denial not audited: %+v", d)
	}
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ref:b_gh"))
	for _, path := range []string{
		"/git/" + host + "/api/v3/user",
		"/git/" + host + "/acme/app/raw/main/README",
		"/git/" + host + "/acme/app.git/info/refs",
		"/git/" + host + "/acme/app.git/info/refs?service=git-receive-pack",
		"/d/" + host + "/acme/app.git/info/refs?service=git-upload-pack",
	} {
		resp, body := get(t, b.BaseURL()+path, map[string]string{"Authorization": auth})
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("%s served: %s", path, body)
		}
	}
	if svc.Requests() != before {
		t.Fatal("a refused path reached the hosting service")
	}
	for _, seen := range svc.SeenAuthorization() {
		if strings.Contains(seen, "ref:b_gh") {
			t.Fatal("placeholder forwarded upstream")
		}
	}
}

// Without typed policy the declared repository is the whole grammar: the
// workspace fetches and pushes exactly it and nothing else on the host.
func TestGitConnectorImplicitRepoRule(t *testing.T) {
	svc := gittest.New(t, realGitToken)
	svc.CreateRepo(t, "acme/app", map[string]string{"README": "hi\n"})
	svc.CreateRepo(t, "acme/other", map[string]string{"README": "no\n"})
	rec := &recorder{}
	b, host := startGitBroker(t, svc, rec, func(host string, o *Options) {
		o.Repo = proto.RepoSpec{URL: "https://" + host + "/acme/app"}
	})
	dir := t.TempDir()
	if out, err := gitThroughBroker(t, b, host, "ref:b_gh", dir, "clone", "-q", "https://"+host+"/acme/app.git", "work"); err != nil {
		t.Fatalf("declared repo clone failed: %v\n%s", err, out)
	}
	if out, err := gitThroughBroker(t, b, host, "ref:b_gh", dir, "clone", "-q", "https://"+host+"/acme/other.git", "other"); err == nil {
		t.Fatalf("undeclared repo cloned:\n%s", out)
	}
	if a, ok := rec.find(proto.EgressConnectorGit, "fetch", DecisionSubstituted); !ok || a.Rule != "repo" {
		t.Fatalf("implicit rule not audited: %+v ok=%v", a, ok)
	}
	// No declared repo and no rule: the surface is closed.
	rec2 := &recorder{}
	b2, host2 := startGitBroker(t, svc, rec2, nil)
	if out, err := gitThroughBroker(t, b2, host2, "ref:b_gh", t.TempDir(), "clone", "-q", "https://"+host2+"/acme/app.git", "work"); err == nil {
		t.Fatalf("clone with neither rule nor declared repo succeeded:\n%s", out)
	}
	// A typed policy that says nothing about git leaves the declared repo
	// reachable; one that does is authoritative and the declaration is not a
	// back door around it.
	b3, host3 := startGitBroker(t, svc, &recorder{}, func(host string, o *Options) {
		o.Repo = proto.RepoSpec{URL: "https://" + host + "/acme/app"}
		o.Network = proto.NetworkPolicy{Default: "deny", Rules: []proto.EgressRule{{
			ID: "models", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"api.example"}, Methods: []string{"POST"},
		}}}
	})
	if out, err := gitThroughBroker(t, b3, host3, "ref:b_gh", t.TempDir(), "clone", "-q", "https://"+host3+"/acme/app.git", "work"); err != nil {
		t.Fatalf("declared repo under an unrelated typed policy failed: %v\n%s", err, out)
	}
	b4, host4 := startGitBroker(t, svc, &recorder{}, func(host string, o *Options) {
		o.Repo = proto.RepoSpec{URL: "https://" + host + "/acme/app"}
		o.Network = proto.NetworkPolicy{Default: "deny", Rules: []proto.EgressRule{gitPolicyRule(host, false, "acme/other")}}
	})
	if out, err := gitThroughBroker(t, b4, host4, "ref:b_gh", t.TempDir(), "clone", "-q", "https://"+host4+"/acme/app.git", "work"); err == nil {
		t.Fatalf("declared repo bypassed a typed git rule that excludes it:\n%s", out)
	}
}
