package sim

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/modelfake"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// Plan B §9: the default OpenCode end-to-end lane is deterministic and
// keyless. A real pinned OpenCode runs in a docker workspace and reaches a
// local OpenAI-compatible server through a normal Remount binding. The
// workspace holds `ref:b_openai`; the synthetic upstream value exists only at
// the control plane and the broker.
const (
	// planBOpenCodeVersion and planBOpenCodeIntegrity pin the harness. The
	// integrity is npm's own dist.integrity for that exact version, so a
	// republished tarball fails the lane instead of silently changing what it
	// proves. Bumping either is a visible dependency update.
	planBOpenCodeVersion   = "1.18.27"
	planBOpenCodeIntegrity = "sha512-5xrG2gQEwV2sLus30SZX9GyLbPX3z57BCxddedDM0wx1bgnwlHVLOS/FD2uve7fEZlmkr7KYFbvs65ySz1rwzA=="

	// planBModel is the scripted model; planBSmallModel answers OpenCode's
	// separate title-generator call so it cannot consume a script step.
	planBModel      = "remount-planb"
	planBSmallModel = "remount-planb-aux"

	// planBUpstream is the synthetic server-side bearer the fake demands. It
	// has the shape of a provider key and no authority anywhere. A real
	// credential is never planted, not even to prove a scan works.
	planBUpstream = "sk-remount-modelfake-upstream-000000000000000000"

	// planBTask, planBShellMark and planBDone are the observable ends of the
	// scripted conversation. The shell command assembles the mark rather than
	// containing it, so a search for the mark finds the tool *result* and
	// never the request that asked for it: B5 turns on exactly that
	// distinction.
	planBTask         = "Create a file named GREETING.txt containing exactly the word hello, read it back, then run a shell command."
	planBShellCommand = `printf 'remount-planb-%s-ok\n' shell`
	planBShellMark    = "remount-planb-shell-ok"
	planBDone         = "REMOUNT-PLANB-DONE"
)

// planBScript drives a real OpenCode through four of the behaviors §9 names:
// create a file, read it back, invoke a shell command, and finish with a known
// result. The remaining two, requesting an approval and resuming a previous
// session, belong to the ACP lane, which replays this same script.
func planBScript() []modelfake.Step {
	greeting := proto.DefaultMountPath + "/GREETING.txt"
	return []modelfake.Step{
		{Calls: []modelfake.Call{{Name: "write", Arguments: map[string]any{"filePath": greeting, "content": "hello\n"}}}},
		{Calls: []modelfake.Call{{Name: "read", Arguments: map[string]any{"filePath": greeting}}}},
		{Calls: []modelfake.Call{{Name: "bash", Arguments: map[string]any{"command": planBShellCommand, "description": "shell probe"}}}},
		{Text: "Wrote GREETING.txt, read it back and ran the shell probe. " + planBDone},
	}
}

// planBUpstreamServer starts the local model over TLS, because the broker's
// /d/ path always re-originates over TLS and refuses to substitute a
// credential into a plaintext request.
func planBUpstreamServer(t *testing.T) (*modelfake.Server, string, *x509.CertPool) {
	t.Helper()
	fake, err := modelfake.New(modelfake.Options{
		Bearer: planBUpstream, Model: planBModel, Aux: "Plan B Deterministic Lane",
		Script: planBScript(),
	})
	if err != nil {
		t.Fatal(err)
	}
	up := httptest.NewTLSServer(fake.Handler())
	t.Cleanup(up.Close)
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	return fake, strings.TrimPrefix(up.URL, "https://"), roots
}

// planBPreset points the standard openai preset at the local upstream. The
// broker path, placeholder and header are the real ones; only the destination
// host differs, which is the one thing a hermetic lane must control.
func planBPreset(host string) launch.Preset {
	return launch.Preset{
		Name: "openai", Hosts: []string{host},
		KeyEnv: "OPENAI_API_KEY", BaseURLEnv: "OPENAI_BASE_URL",
		BaseURLPath: "/d/" + host + "/v1", Header: "Authorization: Bearer",
	}
}

// planBWorld builds a world whose node runs the docker backend and whose
// broker trusts the local upstream. It skips when docker is unavailable: a
// lane that cannot run is unavailable, never a pass.
func planBWorld(t *testing.T, host string, roots *x509.CertPool, hosts []string) (*world, *client.Client, *workspace.Docker, *node.Node, string) {
	t.Helper()
	image := os.Getenv("REMOUNT_INTEGRATION_IMAGE")
	if image == "" {
		image = "node:22-bookworm-slim"
	}
	d, _ := workspace.NewDocker(filepath.Join(t.TempDir(), "d"), image)
	if err := d.Available(ctxT(t, 30*time.Second)); err != nil {
		t.Skipf("plan B model lane unavailable: docker: %v", err)
	}
	w := newWorld(t, control.Binding{ID: "b_openai", Secret: planBUpstream, Destinations: []string{host}, TTLSec: 900})
	pb, _ := workspace.NewProcess(filepath.Join(t.TempDir(), "p"))
	n := w.nodeWith("dn", func(o *node.Options) {
		o.Backends = workspace.NewRegistry(pb, d)
		o.Allow = append(o.Allow, append([]string{host}, hosts...)...)
		// The broker refuses a private address unless the bare hostname is
		// allowed; it validates the name it resolved, never the authority.
		bare, _, _ := strings.Cut(host, ":")
		o.AllowPrivate = append(o.AllowPrivate, bare)
		o.BrokerRootCAs = roots
	})
	return w, w.client("c1"), d, n, image
}

// planBShimDir holds the pinned-harness shim inside the workspace, and
// planBPath puts it ahead of the image's own bin directories.
const planBShimDir = ".planb/bin"

var planBPath = proto.DefaultMountPath + "/" + planBShimDir + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// planBShim is the pinned OpenCode launcher. It exists because the recipe's
// install line ends at `command -v opencode`, and a global npm install lives
// in the image rather than the workspace: a restored workspace would silently
// reinstall whatever npm calls latest. The shim is on PATH, so the recipe's
// install is satisfied, and every launch either finds the pinned version or
// reinstalls exactly it after verifying the published tarball still hashes to
// the recorded integrity.
const planBShim = `#!/bin/sh
set -eu
version=%q
integrity=%q
real=/usr/local/bin/opencode
if [ ! -x "$real" ] || [ "$("$real" --version 2>/dev/null || true)" != "$version" ]; then
  node -e '
const crypto = require("crypto");
const [version, want] = process.argv.slice(1);
const url = "https://registry.npmjs.org/opencode-ai/-/opencode-ai-" + version + ".tgz";
fetch(url).then(r => { if (!r.ok) throw new Error("fetch " + r.status); return r.arrayBuffer(); })
  .then(b => {
    const got = "sha512-" + crypto.createHash("sha512").update(Buffer.from(b)).digest("base64");
    if (got !== want) { throw new Error("opencode-ai@" + version + " integrity " + got + " != pinned " + want); }
    console.error("opencode-ai@" + version + " integrity verified");
  })
  .catch(e => { console.error(String(e)); process.exit(1); });
' "$version" "$integrity" 1>&2
  npm install -g "opencode-ai@$version" >/dev/null 2>&1
  test "$("$real" --version)" = "$version"
fi
exec "$real" "$@"
`

// planBEnv is the workspace environment: the binding's placeholder and broker
// base URL, the PATH that reaches the pinned shim, and an npm cache outside
// the tree. The workspace root is $HOME, so npm would otherwise park a quarter
// of a gigabyte of tarballs in it and make every snapshot and leak scan in
// this lane an exercise in walking the package cache.
func planBEnv(b launch.Binding) map[string]string {
	env := b.SessionEnv()
	env["PATH"] = planBPath
	env["npm_config_cache"] = "/tmp/remount-planb-npm"
	return env
}

// planBInstallOpenCode writes the pinned shim and runs it once, so the lane
// fails here rather than mid-conversation if the pin no longer resolves.
func planBInstallOpenCode(t *testing.T, ctx context.Context, c *client.Client, ws string) {
	t.Helper()
	shim := fmt.Sprintf(planBShim, planBOpenCodeVersion, planBOpenCodeIntegrity)
	if err := c.WriteFile(ctx, ws, planBShimDir+"/opencode", []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	out, errOut, exit, err := c.Run(ctx, ws, "/bin/sh", "-c", `command -v opencode && opencode --version`)
	if err != nil || exit == nil || exit.Code != 0 {
		t.Fatalf("pin opencode %s: err=%v exit=%+v\n%s\n%s", planBOpenCodeVersion, err, exit, out, errOut)
	}
	if !bytes.Contains(out, []byte(planBOpenCodeVersion)) {
		t.Fatalf("the shim did not install the pinned version:\n%s\n%s", out, errOut)
	}
	if !bytes.Contains(errOut, []byte("integrity verified")) {
		t.Fatalf("the integrity check did not run:\n%s\n%s", out, errOut)
	}
	if !bytes.Contains(out, []byte(proto.DefaultMountPath+"/"+planBShimDir+"/opencode")) {
		t.Fatalf("PATH does not reach the pinned shim:\n%s", out)
	}
}

// planBWriteCatalog writes the local model catalog. It replaces models.dev:
// OpenCode deep-merges $HOME/.config/opencode/opencode.json under the
// recipe's OPENCODE_CONFIG, so the recipe still owns the broker base URL and
// the `ref:` placeholder while the lane owns model discovery.
func planBWriteCatalog(t *testing.T, ctx context.Context, c *client.Client, ws string) {
	t.Helper()
	catalog := fmt.Sprintf(`{
  "$schema": "https://opencode.ai/config.json",
  "small_model": "openai/%s",
  "provider": { "openai": { "models": { %q: { "name": "Plan B" }, %q: { "name": "Plan B Aux" } } } }
}
`, planBSmallModel, planBModel, planBSmallModel)
	if err := c.WriteFile(ctx, ws, ".config/opencode/opencode.json", []byte(catalog), 0o644); err != nil {
		t.Fatal(err)
	}
}

// planBIsolateModelMetadata blackholes OpenCode's metadata hosts inside the
// container. §9 requires the lane not to need models.dev; pointing those names
// at a dead local address turns that from a claim into an assertion, because a
// run that still depended on them now fails. The npm registry stays reachable:
// the pinned install is a declared prerequisite, not model discovery.
func planBIsolateModelMetadata(t *testing.T, ctx context.Context, c *client.Client, ws string) {
	t.Helper()
	const script = `set -eu
printf '127.0.0.1 models.dev
127.0.0.1 models.opencode.ai
127.0.0.1 opencode.ai
' >> /etc/hosts
grep -c '127.0.0.1 models.dev' /etc/hosts
`
	out, errOut, exit, err := c.Run(ctx, ws, "/bin/sh", "-c", script)
	if err != nil || exit == nil || exit.Code != 0 || !bytes.Contains(out, []byte("1")) {
		t.Fatalf("blackhole model metadata hosts: err=%v exit=%+v\n%s\n%s", err, exit, out, errOut)
	}
}

// planBAssertUpstreamReached proves the broker substituted: every request the
// local model saw carried the synthetic bearer, none carried the workspace's
// placeholder, and the script ran to its end.
func planBAssertUpstreamReached(t *testing.T, fake *modelfake.Server) {
	t.Helper()
	requests := fake.Requests()
	if len(requests) == 0 {
		t.Fatal("the local model saw no request; OpenCode never reached the broker")
	}
	steps := map[int]bool{}
	for i, r := range requests {
		if r.Status != 200 {
			t.Fatalf("request %d %s %s = %d", i, r.Method, r.Path, r.Status)
		}
		if r.Authorization != "Bearer "+planBUpstream {
			t.Fatalf("request %d authorization did not carry the synthetic upstream bearer", i)
		}
		steps[r.Step] = true
	}
	if fake.Saw("ref:b_openai") {
		t.Fatal("the workspace placeholder reached the upstream; the broker did not substitute")
	}
	for step := range planBScript() {
		if !steps[step] {
			t.Fatalf("script step %d never ran; steps seen %v", step, steps)
		}
	}
}

// planBSnapshotHits counts files inside a snapshot whose bytes contain token.
// Scanning the artifact blob directly would find nothing: it is gzipped, so a
// clean result would be an artifact of the compression, not of the workspace.
func planBSnapshotHits(t *testing.T, ctx context.Context, c *client.Client, id, format, token string) []string {
	t.Helper()
	rc, err := c.DownloadSnapshot(ctx, id, format)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	// A chunked snapshot is exported as a plain tar and a legacy one as
	// tar.gz; sniff rather than assume, because reading the wrong one would
	// report a clean scan of nothing.
	buffered := bufio.NewReader(rc)
	var body io.Reader = buffered
	if magic, err := buffered.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(buffered)
		if err != nil {
			t.Fatal(err)
		}
		body = gz
	}
	var hits []string
	tr := tar.NewReader(body)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return hits
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte(token)) {
			hits = append(hits, h.Name)
		}
	}
}

// TestPlanBOpenCodeDeterministicModelLane is Plan B B1, B3 and B6: a real
// pinned OpenCode edits GREETING.txt through the local model and exits zero;
// a client cut mid-stream reattaches to byte-identical output; and the
// synthetic reusable value occurs in zero workspace, snapshot, event or
// diagnostic bytes, proven by a scan that must first find a planted canary.
//
// It needs no credential and no environment variable. The only external
// prerequisites are a docker daemon and the npm registry, and both make the
// lane unavailable rather than green when absent.
func TestPlanBOpenCodeDeterministicModelLane(t *testing.T) {
	fake, host, roots := planBUpstreamServer(t)
	recipe, err := launch.Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	w, c, d, n, image := planBWorld(t, host, roots, recipe.Hosts)
	binding := launch.Binding{ID: "b_openai", Preset: planBPreset(host)}
	ctx := ctxT(t, 10*time.Minute)

	ws := mustWS(t, c, proto.WorkspaceSpec{
		Name: "planb-model", Image: image,
		Requires: proto.Requires{Backend: "docker"},
		Bindings: []string{binding.ID},
		Env:      planBEnv(binding),
	})
	registerDockerWorkspaceCleanup(t, c, d, ws.ID)
	planBInstallOpenCode(t, ctx, c, ws.ID)
	planBIsolateModelMetadata(t, ctx, c, ws.ID)
	planBWriteCatalog(t, ctx, c, ws.ID)

	res, err := launch.Start(ctx, c, launch.Options{
		Recipe: recipe, WS: ws.ID, Task: planBTask,
		Bindings: []launch.Binding{binding},
		Security: proto.SecurityLocal, Sandbox: launch.SandboxWorkspaceWrite,
		Model: "openai/" + planBModel, Timeout: 5 * time.Minute, Stderr: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	// B3: cut the client while OpenCode is streaming. The cut must land on a
	// live stream, so it is driven by arriving bytes rather than by a timer.
	var out bytes.Buffer
	cuts := 0
	for ch := range res.Session.Chunks() {
		switch ch.Stream {
		case proto.StreamStdout, proto.StreamStderr:
			out.Write(ch.Data)
		case proto.StreamGap:
			t.Fatal("session reported a gap")
		}
		if out.Len() > 256*(cuts+1) && cuts < 3 {
			cuts++
			w.cut("c1")
		}
	}
	if err := res.Session.Err(); err != nil {
		t.Fatalf("session: %v\n%s", err, out.String())
	}
	if cuts == 0 {
		t.Fatal("the lane never cut the client; B3 proved nothing")
	}
	if exit := res.Session.Exit(); exit == nil || exit.Code != 0 {
		t.Fatalf("opencode exit = %+v\n%s\n%s", exit, out.String(), egressLog(t, c, ws.ID, planBUpstream))
	}
	// B1: the harness did the work the script asked for, and said so.
	greeting, err := c.ReadFile(ctx, ws.ID, "GREETING.txt")
	if err != nil {
		t.Fatalf("GREETING.txt: %v\n%s", err, out.String())
	}
	if strings.TrimSpace(string(greeting)) != "hello" {
		t.Fatalf("GREETING.txt = %q", greeting)
	}
	for _, want := range []string{planBShellMark, planBDone} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("session output lacks %q:\n%s", want, out.String())
		}
	}
	planBAssertUpstreamReached(t, fake)
	// The recipe, not the test, wired the broker: the config OpenCode read
	// names the placeholder and the broker base URL.
	cfg, err := c.ReadFile(ctx, ws.ID, ".remount/launch/opencode.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cfg), `"apiKey": "ref:b_openai"`) || !strings.Contains(string(cfg), "/d/"+host+"/v1") {
		t.Fatalf("opencode.json did not route through the broker:\n%s", cfg)
	}

	// B3: a second client replays the finished session from seq 0 and must
	// get exactly the bytes the cut client assembled.
	replayClient := w.client("c2")
	replay, err := replayClient.Attach(ctx, ws.ID, res.Session.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var replayed bytes.Buffer
	for ch := range replay.Chunks() {
		switch ch.Stream {
		case proto.StreamStdout, proto.StreamStderr:
			replayed.Write(ch.Data)
		case proto.StreamGap:
			t.Fatal("replay reported a gap")
		}
	}
	if err := replay.Err(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replayed.Bytes(), out.Bytes()) {
		t.Fatalf("replay after %d cuts is not byte-identical: %d bytes replayed, %d collected", cuts, replayed.Len(), out.Len())
	}

	planBLeakScan(t, ctx, c, d, n.ID(), ws.ID, host, planBUpstream)
}

// planBSnapshot takes a snapshot, waiting out the workspace's minimum
// snapshot interval. The scan needs two snapshots a moment apart and the
// control plane rate-limits them; retrying is observing the interval, not
// papering over a failure.
func planBSnapshot(t *testing.T, ctx context.Context, c *client.Client, wsID string) *proto.WSSnapshotRes {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		snap, err := c.Snapshot(ctx, wsID, true)
		if err == nil {
			return snap
		}
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeResourceExhausted || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// planBLeakScan is B6. It covers the workspace tree, OpenCode's own state, the
// snapshot, the event log and diagnostics, and it proves itself first: a
// second synthetic value is planted where each scan must find it, and a scan
// that cannot find its canary fails the lane. A scan that cannot demonstrate
// it works is not evidence.
func planBLeakScan(t *testing.T, ctx context.Context, c *client.Client, d *workspace.Docker, nodeID, wsID, host, token string) {
	// The container wrote as root; the snapshot pass re-owns the tree to the
	// node, so scan it exactly as it would travel.
	first := planBSnapshot(t, ctx, c, wsID)
	// ws.info reports the in-container mount, not a host directory, so the
	// host tree is reached through the docker backend's own layout.
	info, err := c.WorkspaceInfo(ctx, wsID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Root != proto.DefaultMountPath {
		t.Fatalf("docker ws.info root = %q, want %q", info.Root, proto.DefaultMountPath)
	}
	hostRoot := filepath.Join(d.Dir, wsID)
	// OpenCode's own state must actually be present, or "no leak in the
	// harness state" would be a statement about an empty directory.
	stateDir := filepath.Join(hostRoot, ".local", "share", "opencode")
	if entries, err := os.ReadDir(stateDir); err != nil || len(entries) == 0 {
		t.Fatalf("opencode state at %s is absent, so scanning it proves nothing: %v", stateDir, err)
	}
	assertTokenAbsent(t, hostRoot, token)
	if hits := planBSnapshotHits(t, ctx, c, first.Artifact, first.Format, token); len(hits) != 0 {
		t.Fatalf("the synthetic upstream value is in the snapshot: %v", hits)
	}

	// Positive control. The canary is synthetic, unique to this workspace and
	// has no authority; a real credential is never planted.
	canary := "sk-remount-planb-canary-" + wsID
	if err := c.WriteFile(ctx, wsID, "canary.txt", []byte("x "+canary+" y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if hits := scanForToken(t, hostRoot, canary); len(hits) != 1 || filepath.Base(hits[0]) != "canary.txt" {
		t.Fatalf("the tree scan cannot find its own canary: %v", hits)
	}
	second := planBSnapshot(t, ctx, c, wsID)
	if hits := planBSnapshotHits(t, ctx, c, second.Artifact, second.Format, canary); len(hits) != 1 {
		t.Fatalf("the snapshot scan cannot find its own canary: %v", hits)
	}
	if hits := planBSnapshotHits(t, ctx, c, second.Artifact, second.Format, token); len(hits) != 0 {
		t.Fatalf("the synthetic upstream value is in the snapshot: %v", hits)
	}
	assertTokenAbsent(t, hostRoot, token)

	// Events. The scan proves itself on a posted event that carries the
	// canary in its payload, so a clean result for the upstream value is a
	// statement about the payloads and not about an empty read.
	if err := c.PostEvent(ctx, proto.Event{Type: "test.planb.canary", Stream: wsID, Payload: []byte(canary)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	evs, err := c.ReadEvents(ctx, 1, wsID)
	if err != nil {
		t.Fatal(err)
	}
	saw := map[string]int{}
	canaryEvents, credForHost := 0, 0
	for _, e := range evs {
		if bytes.Contains(e.Payload, []byte(token)) {
			t.Fatalf("event %s carries the synthetic upstream value", e.Type)
		}
		if bytes.Contains(e.Payload, []byte(canary)) {
			canaryEvents++
		}
		if e.Type == proto.EvCredUsed && bytes.Contains(e.Payload, []byte(host)) {
			credForHost++
		}
		saw[e.Type]++
	}
	if canaryEvents != 1 {
		t.Fatalf("the event scan cannot find its own canary: %d events matched of %d", canaryEvents, len(evs))
	}
	if saw[proto.EvRunStarted] != 1 || saw[proto.EvRunFinished] != 1 || credForHost == 0 || saw[proto.EvEgressDenied] != 0 {
		t.Fatalf("events: run.started=%d run.finished=%d cred.used(%s)=%d egress.denied=%d\n%s",
			saw[proto.EvRunStarted], saw[proto.EvRunFinished], host, credForHost, saw[proto.EvEgressDenied],
			egressLog(t, c, wsID, token))
	}
	controlDiag, err := c.Diag(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	nodeDiag, err := c.NodeDiag(ctx, nodeID, wsID, true)
	if err != nil {
		t.Fatal(err)
	}
	// Each diagnostic scan is believed only after it finds something it must
	// contain: the control plane names the binding, the node names the
	// workspace. Neither may name the value behind the binding.
	for name, diag := range map[string]struct {
		value any
		must  string
	}{
		"control": {controlDiag, "b_openai"},
		"node":    {nodeDiag, wsID},
	} {
		rendered, err := proto.Marshal(diag.value)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(rendered, []byte(token)) {
			t.Fatalf("%s diagnostics carry the reusable upstream value", name)
		}
		if !bytes.Contains(rendered, []byte(diag.must)) {
			t.Fatalf("the %s diagnostic scan never matched %q, so it is not reading that output", name, diag.must)
		}
	}
}
