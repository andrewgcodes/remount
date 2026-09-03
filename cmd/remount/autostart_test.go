package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"remount.dev/remount/internal/launch"
)

func TestAutostartAppliesOnlyToTheDefaultLocalServer(t *testing.T) {
	t.Setenv("REMOUNT_SERVER", "")
	t.Setenv("REMOUNT_AUTOSTART", "")
	cases := []struct {
		name   string
		c      common
		env    map[string]string
		enable bool
	}{
		{"default", common{server: defaultServer}, nil, true},
		{"trailing slash", common{server: defaultServer + "/"}, nil, true},
		{"explicit server", common{server: "http://cp.example:7443"}, nil, false},
		{"token", common{server: defaultServer, token: "t"}, nil, false},
		{"env server", common{server: defaultServer}, map[string]string{"REMOUNT_SERVER": defaultServer}, false},
		{"opt out", common{server: defaultServer}, map[string]string{"REMOUNT_AUTOSTART": "0"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := tc.c.autostartEnabled(); got != tc.enable {
				t.Fatalf("autostartEnabled = %v, want %v", got, tc.enable)
			}
		})
	}
}

func TestEnvBindingsNeverHoldTheKey(t *testing.T) {
	for _, p := range launch.Presets() {
		if p.KeyEnv != "" {
			t.Setenv(p.KeyEnv, "")
		}
	}
	if got := envBindings(); len(got) != 0 {
		t.Fatalf("bindings with no keys in the environment: %+v", got)
	}
	t.Setenv("OPENAI_API_KEY", "sk-test-REAL-VALUE")
	t.Setenv("AZURE_OPENAI_API_KEY", "azure-REAL-VALUE") // per-deployment host: cannot be synthesized
	got := envBindings()
	if len(got) != 1 || got[0].ID != "b_openai" || got[0].Secret != "$OPENAI_API_KEY" || got[0].Destinations[0] != "api.openai.com" {
		t.Fatalf("bindings = %+v", got)
	}
	for _, b := range got {
		if strings.Contains(b.Secret, "REAL-VALUE") {
			t.Fatalf("binding %s carries the key value", b.ID)
		}
	}
	if hosts := recipeHosts(); len(hosts) == 0 {
		t.Fatal("no recipe hosts")
	}
}

func TestDefaultBindingsPicksTheRecipesFirstProvider(t *testing.T) {
	recipe, err := launch.Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	local := []localBinding{{ID: "b_openrouter", Secret: "$OPENROUTER_API_KEY"}, {ID: "b_openai", Secret: "$OPENAI_API_KEY"}}
	got, err := defaultBindings(recipe, nil, local)
	if err != nil || len(got) != 1 || got[0].ID != "b_openai" {
		t.Fatalf("default = %+v, %v (recipe providers %v)", got, err, recipe.Providers)
	}
	given := []launch.Binding{{ID: "b_mine"}}
	if got, _ := defaultBindings(recipe, given, local); len(got) != 1 || got[0].ID != "b_mine" {
		t.Fatalf("explicit bindings were overridden: %+v", got)
	}
	if got, _ := defaultBindings(recipe, nil, nil); got != nil {
		t.Fatalf("no local bindings yet picked %+v", got)
	}
	custom, err := launch.Load("custom")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := defaultBindings(custom, nil, []localBinding{{ID: "b_nothing"}}); got != nil {
		t.Fatalf("unknown preset picked %+v", got)
	}
}

func TestEnsureLocalServerReusesARunningServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	// Not the default address, so autostart is off and nothing is spawned
	// or written even though the server is fine.
	dataDir := t.TempDir()
	t.Setenv("REMOUNT_DATA", dataDir)
	t.Setenv("REMOUNT_SERVER", "")
	c := common{server: srv.URL}
	local, err := c.ensureLocalServer(context.Background())
	if err != nil || local != nil {
		t.Fatalf("ensure = %+v, %v", local, err)
	}
	if entries, _ := os.ReadDir(dataDir); len(entries) != 0 {
		t.Fatalf("autostart wrote into %s: %v", dataDir, entries)
	}
	if !healthy(context.Background(), srv.URL) || healthy(context.Background(), "http://127.0.0.1:1") {
		t.Fatal("healthz probe")
	}

	// readLocalBindings trusts the bindings file only while the recorded
	// standalone is alive.
	os.WriteFile(filepath.Join(dataDir, "bindings.json"), []byte(`[{"id":"b_openai","secret":"$OPENAI_API_KEY"}]`), 0o600)
	if got := readLocalBindings(dataDir); got != nil {
		t.Fatalf("bindings without a pid file: %+v", got)
	}
	os.WriteFile(filepath.Join(dataDir, "standalone.pid"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600)
	if got := readLocalBindings(dataDir); len(got) != 1 || got[0].ID != "b_openai" {
		t.Fatalf("bindings for a live pid: %+v", got)
	}
	os.WriteFile(filepath.Join(dataDir, "standalone.pid"), []byte("2147483646\n"), 0o600)
	if got := readLocalBindings(dataDir); got != nil {
		t.Fatalf("bindings for a dead pid: %+v", got)
	}
}
