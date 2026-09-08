package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"remount.dev/remount/internal/launch"
)

// defaultServer is where the CLI looks when neither --server nor
// REMOUNT_SERVER says otherwise, and the only address the CLI will start a
// standalone at on its own.
const defaultServer = "http://127.0.0.1:7443"

// autostartWait bounds how long a freshly started standalone may take to
// answer /healthz before `remount run` gives up on it.
const autostartWait = 20 * time.Second

// localBinding is one entry of the bindings file the autostarted standalone
// reads. The secret is always an $ENV reference, so the file on disk never
// holds a key; the standalone inherits the environment `remount run` had.
type localBinding struct {
	ID           string   `json:"id"`
	Secret       string   `json:"secret"`
	Destinations []string `json:"destinations"`
	TTLSec       int64    `json:"ttl_sec"`
}

// localDataDir is where an autostarted standalone keeps its state: stable
// across working directories so a workspace created from one checkout can
// be resumed from anywhere on the machine.
func localDataDir() (string, error) {
	if d := os.Getenv("REMOUNT_DATA"); d != "" {
		return filepath.Abs(d)
	}
	if d := os.Getenv("XDG_DATA_HOME"); d != "" {
		return filepath.Join(d, "remount"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share", "remount"), nil
}

// autostartEnabled reports whether this invocation may start a standalone
// on its own: only for the default local address with no token, and not
// when REMOUNT_AUTOSTART=0.
func (c *common) autostartEnabled() bool {
	if os.Getenv("REMOUNT_AUTOSTART") == "0" || os.Getenv("REMOUNT_SERVER") != "" || c.token != "" {
		return false
	}
	return strings.TrimSuffix(c.server, "/") == defaultServer
}

func healthy(ctx context.Context, server string) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(server, "/")+"/healthz", nil)
	if err != nil {
		return false
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode == http.StatusOK
}

// envBindings synthesizes one binding per fixed-host provider preset whose
// key variable is set. Ids follow the preset (`b_openai`), which is what
// `--binding b_openai` resolves without naming the preset.
func envBindings() []localBinding {
	var out []localBinding
	for _, p := range launch.Presets() {
		if p.HostParam != "" || p.KeyEnv == "" || os.Getenv(p.KeyEnv) == "" {
			continue
		}
		out = append(out, localBinding{
			ID: "b_" + strings.ReplaceAll(p.Name, "-", "_"), Secret: "$" + p.KeyEnv,
			Destinations: append([]string(nil), p.Hosts...), TTLSec: 900,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// recipeHosts is every host a built-in recipe installs from, plus the
// provider endpoints a subscription login and launch reach directly, so a
// harness started under the autostarted standalone can fetch itself and a
// provider-native login can complete. These carry no credential; API-key
// provider traffic is reached through bindings.
func recipeHosts() []string {
	seen := map[string]bool{}
	var hosts []string
	for _, name := range launch.Builtin() {
		r, err := launch.Load(name)
		if err != nil {
			continue
		}
		all := append([]string(nil), r.Hosts...)
		if r.Subscription != nil {
			all = append(all, r.Subscription.Hosts...)
		}
		for _, h := range all {
			if !seen[h] {
				seen[h] = true
				hosts = append(hosts, h)
			}
		}
	}
	sort.Strings(hosts)
	return hosts
}

// ensureLocalServer makes the default local server reachable, starting
// `remount standalone` in the background when nothing answers there. The
// standalone gets a stable data directory, bindings for every provider key
// in the environment (as $ENV references, never values) and the built-in
// recipes' install hosts on its allow list. It returns the bindings the
// standalone was started with, or nil when the server was already up or
// autostart does not apply.
func (c *common) ensureLocalServer(ctx context.Context) ([]localBinding, error) {
	if !c.autostartEnabled() {
		return nil, nil
	}
	dataDir, err := localDataDir()
	if err != nil {
		return nil, err
	}
	if healthy(ctx, c.server) {
		return readLocalBindings(dataDir), nil
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	bindings := envBindings()
	if len(bindings) == 0 {
		bindings = []localBinding{}
	}
	bindingsPath := filepath.Join(dataDir, "bindings.json")
	raw, err := json.MarshalIndent(bindings, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(bindingsPath, append(raw, '\n'), 0o600); err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(strings.TrimPrefix(defaultServer, "http://"))
	if err != nil {
		return nil, err
	}
	backends := "process"
	if _, err := exec.LookPath("docker"); err == nil {
		backends += ",docker"
	}
	args := []string{"standalone", "--listen", net.JoinHostPort(host, port), "--data", dataDir, "--bindings", bindingsPath, "--backend", backends}
	for _, h := range recipeHosts() {
		args = append(args, "--allow", h)
	}
	logPath := filepath.Join(dataDir, "standalone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()
	cmd := exec.Command(exe, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = detachedProcAttr()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start remount standalone: %w", err)
	}
	pid := cmd.Process.Pid
	// The child is reparented to init when we exit; releasing it here is
	// what lets this process return without waiting on it.
	if err := cmd.Process.Release(); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dataDir, "standalone.pid"), []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(autostartWait)
	for !healthy(ctx, c.server) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) || !processAlive(pid) {
			return nil, fmt.Errorf("remount standalone (pid %d) did not become ready; see %s", pid, logPath)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "started remount standalone in the background (pid %d, data %s, log %s); stop it with: kill %d\n", pid, dataDir, logPath, pid)
	if len(bindings) > 0 {
		var ids []string
		for _, b := range bindings {
			ids = append(ids, b.ID+" ("+b.Secret+")")
		}
		fmt.Fprintf(os.Stderr, "provider bindings from the environment: %s\n", strings.Join(ids, ", "))
	}
	return bindings, nil
}

// localBindings previews, without starting anything, the bindings
// ensureLocalServer will report: those of the autostarted standalone already
// running, or the ones a fresh one would be given. Callers validate a run
// against these before paying for the start.
func (c *common) localBindings(ctx context.Context) []localBinding {
	if !c.autostartEnabled() {
		return nil
	}
	if healthy(ctx, c.server) {
		dataDir, err := localDataDir()
		if err != nil {
			return nil
		}
		return readLocalBindings(dataDir)
	}
	return envBindings()
}

// readLocalBindings returns the bindings an autostarted standalone that is
// still running was started with, or nil when the server at the default
// address is not one this CLI started.
func readLocalBindings(dataDir string) []localBinding {
	raw, err := os.ReadFile(filepath.Join(dataDir, "standalone.pid"))
	if err != nil {
		return nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || !processAlive(pid) {
		return nil
	}
	raw, err = os.ReadFile(filepath.Join(dataDir, "bindings.json"))
	if err != nil {
		return nil
	}
	var out []localBinding
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}

// defaultBindings picks, from the autostarted standalone's bindings, the
// first one the recipe accepts, so `remount run opencode -- task` with
// OPENAI_API_KEY in the environment needs no --binding. It returns nothing
// when the caller named bindings or none fits.
func defaultBindings(recipe *launch.Recipe, given []launch.Binding, local []localBinding) ([]launch.Binding, error) {
	if len(given) > 0 || len(local) == 0 {
		return given, nil
	}
	for _, provider := range recipe.Providers {
		for _, lb := range local {
			b, err := launch.ParseBinding(lb.ID)
			if err != nil || b.Preset.Name != provider {
				continue
			}
			fmt.Fprintf(os.Stderr, "using binding %s (%s) for %s\n", lb.ID, lb.Secret, recipe.Name)
			return []launch.Binding{b}, nil
		}
	}
	return nil, nil
}

func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = signalProcess(p)
	return err == nil || errors.Is(err, syscall.EPERM)
}
