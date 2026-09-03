// Package gittest runs a real git smart-HTTP hosting service in-process for
// tests: `git http-backend` behind net/http/cgi, fronted by a handler that
// demands a token the way a hosting service does. Tests point the broker at it
// over TLS and prove that the workspace never needs, sees, or leaks that token.
package gittest

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Service is a fake hosting service serving one or more repositories.
type Service struct {
	// Root holds bare repositories at <Root>/<owner>/<name>.git.
	Root string
	// Token is the credential every request must present as
	// "Basic base64(anything:Token)" or "Bearer Token". Empty means public.
	Token string

	requests atomic.Int64
	pushes   atomic.Int64
	mu       sync.Mutex
	seen     []string // Authorization header values observed, for leak scans
}

// Requests counts requests that reached http-backend.
func (s *Service) Requests() int64 { return s.requests.Load() }

// Pushes counts receive-pack requests that reached http-backend.
func (s *Service) Pushes() int64 { return s.pushes.Load() }

// SeenAuthorization returns every Authorization header value the service saw.
func (s *Service) SeenAuthorization() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

// New creates a service rooted in a temp dir.
func New(t testing.TB, token string) *Service {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	return &Service{Root: t.TempDir(), Token: token}
}

// Git runs git with args in dir and fails the test on error.
func Git(t testing.TB, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// CreateRepo makes a bare repository owner/name with one commit on branch
// main containing files, and returns the commit id.
func (s *Service) CreateRepo(t testing.TB, ownerRepo string, files map[string]string) string {
	t.Helper()
	bare := filepath.Join(s.Root, ownerRepo+".git")
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatal(err)
	}
	Git(t, s.Root, "init", "-q", "--bare", "--initial-branch=main", bare)
	Git(t, bare, "config", "http.receivepack", "true")
	work := t.TempDir()
	Git(t, work, "init", "-q", "--initial-branch=main")
	for name, content := range files {
		path := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	Git(t, work, "add", "-A")
	Git(t, work, "commit", "-q", "-m", "initial")
	Git(t, work, "push", "-q", bare, "main")
	return Git(t, bare, "rev-parse", "HEAD")
}

// Commit adds one commit to branch main of owner/name (and tags it when tag
// is non-empty), returning the new SHA.
func (s *Service) Commit(t testing.TB, ownerRepo, tag string, files map[string]string) string {
	t.Helper()
	bare := filepath.Join(s.Root, ownerRepo+".git")
	work := t.TempDir()
	Git(t, work, "clone", "-q", bare, ".")
	for name, content := range files {
		path := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	Git(t, work, "add", "-A")
	Git(t, work, "commit", "-q", "-m", "next")
	if tag != "" {
		Git(t, work, "tag", tag)
		Git(t, work, "push", "-q", "origin", "main", tag)
	} else {
		Git(t, work, "push", "-q", "origin", "main")
	}
	return Git(t, work, "rev-parse", "HEAD")
}

// Head returns the commit a ref points at in owner/name.
func (s *Service) Head(t testing.TB, ownerRepo, ref string) string {
	t.Helper()
	return Git(t, filepath.Join(s.Root, ownerRepo+".git"), "rev-parse", "--verify", ref)
}

// Handler serves the repositories over smart HTTP with token enforcement.
func (s *Service) Handler() http.Handler {
	backend := &cgi.Handler{
		Path: gitExecPath("git-http-backend"),
		Env:  []string{"GIT_PROJECT_ROOT=" + s.Root, "GIT_HTTP_EXPORT_ALL=1"},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		s.mu.Lock()
		s.seen = append(s.seen, auth)
		s.mu.Unlock()
		if s.Token != "" && !s.authorized(auth) {
			w.Header().Set("WWW-Authenticate", `Basic realm="gittest"`)
			http.Error(w, "bad credentials", http.StatusUnauthorized)
			return
		}
		s.requests.Add(1)
		if strings.HasSuffix(r.URL.Path, "/git-receive-pack") {
			s.pushes.Add(1)
		}
		backend.ServeHTTP(w, r)
	})
}

func (s *Service) authorized(auth string) bool {
	scheme, value, ok := strings.Cut(auth, " ")
	if !ok {
		return false
	}
	switch strings.ToLower(scheme) {
	case "bearer":
		return value == s.Token
	case "basic":
		raw, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return false
		}
		_, password, ok := bytes.Cut(raw, []byte(":"))
		return ok && string(password) == s.Token
	}
	return false
}

func gitExecPath(name string) string {
	out, err := exec.Command("git", "--exec-path").Output()
	if err != nil {
		return name
	}
	return filepath.Join(strings.TrimSpace(string(out)), name)
}
