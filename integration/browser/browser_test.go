// Package browser is evidence gate B34: a real Chromium, driven only through
// Remount's computer operations, on a real workspace backend.
//
// Everything here runs against a browser the node launched inside a docker
// workspace and talks to over the port substrate (ADR 0088). The fake DevTools
// endpoint in internal/computer/fakecdp proves the client's contract; this
// lane proves the contract describes Chromium.
//
// A prerequisite that is missing is reported by name and the test skips as
// unavailable. It never passes silently: an unearned pass on a browser lane
// would be indistinguishable from a browser that works.
package browser

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// defaultImage is the reference browser image built from images/browser.
const defaultImage = "remount-browser:local"

// mountPath is where the docker backend bind-mounts the workspace root, and
// therefore the file:// prefix the page is served from.
const mountPath = "/work"

// allowedHost is the real public destination the lane browses to through the
// broker. It is the one host this lane's node allows, so reaching it proves
// the whole path: Chromium answers the broker's proxy challenge with the
// workspace capability, the broker authenticates it, host policy permits the
// destination, and the page loads (ADR 0095).
const allowedHost = "example.com"

// deniedHost is the unbound destination the egress step navigates to. It is
// deliberately a host nothing in this lane allows, and it must now be refused
// by host policy rather than for want of proxy authentication.
const deniedHost = "example.org"

func imageName() string {
	if name := os.Getenv("REMOUNT_BROWSER_IMAGE"); name != "" {
		return name
	}
	return defaultImage
}

// allowedHostName is the destination the lane browses to. It is overridable
// because a host that cannot reach the public internet can still run the lane
// against a destination it can reach.
func allowedHostName() string {
	if name := os.Getenv("REMOUNT_BROWSER_ALLOWED_HOST"); name != "" {
		return name
	}
	return allowedHost
}

func deniedHostName() string {
	if name := os.Getenv("REMOUNT_BROWSER_DENIED_HOST"); name != "" {
		return name
	}
	return deniedHost
}

// TestB34BrowserComputerConformance drives one browser through every computer
// operation and asserts the observable postcondition of each: the DOM the page
// actually reached, the artifact a download became, the event the node emitted,
// and the typed failure a caller sees once the browser is gone.
func TestB34BrowserComputerConformance(t *testing.T) {
	image := imageName()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	docker := requireDocker(t, ctx, image)
	c := lane(t, docker, image)

	ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
		Name: "browser-conformance", Image: image,
		Requires: proto.Requires{Backend: "docker"},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerCleanup(t, c, ws.ID)
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatalf("wait claimed: %v", err)
	}

	if err := c.WriteFile(ctx, ws.ID, "page.html", []byte(conformancePage), 0o644); err != nil {
		t.Fatalf("write page: %v", err)
	}

	// --- computer.create -------------------------------------------------
	computer, err := c.CreateComputer(ctx, proto.ComputerCreateReq{WS: ws.ID})
	if err != nil {
		t.Fatalf("computer.create: %v", err)
	}
	t.Logf("step create: computer=%s session=%s cdp=%q viewport=%dx%d",
		computer.ID(), computer.Session(), computer.CDPVersion(),
		computer.Viewport().Width, computer.Viewport().Height)
	if computer.CDPVersion() == "" {
		t.Fatal("computer.create returned no DevTools version")
	}
	created := waitEvent(t, ctx, c, ws.ID, proto.EvComputerCreated)
	if payload := decodePayload(t, created); payload["computer"] != computer.ID() {
		t.Fatalf("computer.created names %v, want %s", payload["computer"], computer.ID())
	}

	// --- computer.navigate to the page under test ------------------------
	nav, err := computer.Navigate(ctx, "file://"+mountPath+"/page.html")
	if err != nil {
		t.Fatalf("computer.navigate: %v", err)
	}
	if nav.Status != proto.ComputerNavigateLoaded {
		t.Fatalf("navigate status = %q, want %q", nav.Status, proto.ComputerNavigateLoaded)
	}
	if nav.Title != "Remount browser conformance" {
		t.Fatalf("navigate title = %q", nav.Title)
	}
	t.Logf("step navigate: %s %q", nav.Status, nav.Title)

	// --- click changes the DOM, and the screenshot changes with it -------
	before, err := computer.Screenshot(ctx)
	if err != nil {
		t.Fatalf("computer.screenshot: %v", err)
	}
	if len(before.PNG) == 0 || before.Width != computer.Viewport().Width {
		t.Fatalf("screenshot %d bytes at %dx%d", len(before.PNG), before.Width, before.Height)
	}
	clickCenter(t, ctx, computer, "#go")
	if got := evalString(t, ctx, computer, `document.getElementById("banner").textContent`); got != "clicked 1" {
		t.Fatalf("banner after click = %q, want %q", got, "clicked 1")
	}
	if got := evalFloat(t, ctx, computer, "window.__clicks"); got != 1 {
		t.Fatalf("click count = %v, want 1", got)
	}
	after, err := computer.Screenshot(ctx)
	if err != nil {
		t.Fatalf("computer.screenshot after click: %v", err)
	}
	if sha256.Sum256(before.PNG) == sha256.Sum256(after.PNG) {
		t.Fatal("screenshot bytes did not change after a visible DOM change")
	}
	t.Logf("step click+screenshot: %d bytes -> %d bytes, digests differ", len(before.PNG), len(after.PNG))

	// --- typing lands exactly, in three different kinds of field ---------
	const fieldText = "remount typed text"
	clickCenter(t, ctx, computer, "#field")
	if err := computer.Type(ctx, fieldText); err != nil {
		t.Fatalf("type into input: %v", err)
	}
	if got := evalString(t, ctx, computer, `document.getElementById("field").value`); got != fieldText {
		t.Fatalf("input value = %q, want %q", got, fieldText)
	}

	const noteText = "contenteditable text"
	clickCenter(t, ctx, computer, "#note")
	if err := computer.Type(ctx, noteText); err != nil {
		t.Fatalf("type into contenteditable: %v", err)
	}
	if got := evalString(t, ctx, computer, `document.getElementById("note").textContent`); got != noteText {
		t.Fatalf("contenteditable text = %q, want %q", got, noteText)
	}

	const frameText = "iframe typed text"
	frame := boundingBox(t, ctx, computer, "#frame")
	// The child input sits at the frame's top-left with no body margin, so a
	// point just inside the frame lands on it.
	if err := computer.Click(ctx, frame.left+150, frame.top+18); err != nil {
		t.Fatalf("click iframe field: %v", err)
	}
	if err := computer.Type(ctx, frameText); err != nil {
		t.Fatalf("type into iframe: %v", err)
	}
	if got := waitEval(t, ctx, computer, "window.__iframeValue", frameText); got != frameText {
		t.Fatalf("iframe value = %q, want %q", got, frameText)
	}
	t.Logf("step type: input, contenteditable and iframe all carry the exact text")

	// --- a download becomes an artifact ----------------------------------
	clickCenter(t, ctx, computer, "#dl")
	download := waitDownload(t, ctx, computer)
	if download.State != proto.ComputerDownloadCompleted {
		t.Fatalf("download state = %q reason %q", download.State, download.Reason)
	}
	if download.Artifact == "" {
		t.Fatalf("download %q produced no artifact (reason %q)", download.Filename, download.Reason)
	}
	if download.Filename != downloadName {
		t.Fatalf("download filename = %q, want %q", download.Filename, downloadName)
	}
	event := waitEvent(t, ctx, c, ws.ID, proto.EvComputerDownload)
	if payload := decodePayload(t, event); payload["artifact"] != download.Artifact {
		t.Fatalf("computer.download names artifact %v, want %s", payload["artifact"], download.Artifact)
	}
	verifyArtifact(t, ctx, c, download.Artifact)
	t.Logf("step download: %s -> %s (%d bytes), artifact bytes verified",
		download.Filename, download.Artifact, download.Bytes)

	// --- brokered browsing to an ALLOWED host actually loads -------------
	//
	// Chromium never volunteers Proxy-Authorization; it waits to be
	// challenged. The node answers that challenge over CDP with the same
	// workspace capability the browser's HTTPS_PROXY carries, so this step
	// fails outright if that answer is missing, wrong, or never reaches the
	// target that issued the request.
	allowed := allowedHostName()
	web, err := computer.Navigate(ctx, "https://"+allowed+"/")
	if err != nil {
		t.Fatalf("navigate to the allowed host %s: %v", allowed, err)
	}
	if web.Status != proto.ComputerNavigateLoaded {
		t.Fatalf("navigate to %s status = %q, want %q", allowed, web.Status, proto.ComputerNavigateLoaded)
	}
	if web.Title == "" {
		t.Fatalf("navigate to %s loaded no title; the page did not come from the network", allowed)
	}
	if status := navigationStatus(t, ctx, computer); status != 200 {
		t.Fatalf("navigate to %s reported HTTP %d, want 200", allowed, status)
	}
	if got := evalString(t, ctx, computer, "document.location.protocol"); got != "https:" {
		t.Fatalf("the allowed page is %s, not https", got)
	}
	allowedEgress := waitEgress(t, ctx, c, ws.ID, proto.EvEgressAllowed, allowed, "allowed")
	t.Logf("step allowed egress: %q loaded over https with HTTP 200, broker recorded decision=%v host=%v",
		web.Title, allowedEgress["decision"], allowedEgress["host"])

	// --- navigation to an unbound host is refused, and recorded ----------
	denied := deniedHostName()
	_, navErr := computer.Navigate(ctx, "https://"+denied+"/")
	if navErr == nil {
		t.Fatalf("navigate to %s succeeded; the broker allows no such destination", denied)
	}
	if code := codeOf(navErr); code != proto.CodeDenied {
		t.Fatalf("navigate to %s = code %q (%v), want %q", denied, code, navErr, proto.CodeDenied)
	}
	if reason := proto.ErrorReason(navErr); reason != proto.ReasonNavigationDenied {
		t.Fatalf("navigate to %s = reason %q, want %q", denied, reason, proto.ReasonNavigationDenied)
	}
	// The refusal must be host policy, not a missing capability: an
	// unauthenticated-only record would mean the browser never identified
	// itself, which refuses every destination equally and proves nothing.
	egress := waitEgress(t, ctx, c, ws.ID, proto.EvEgressDenied, denied, "denied")
	t.Logf("step egress: navigate denied, broker recorded decision=%v reason=%q host=%v",
		egress["decision"], egress["reason"], egress["host"])

	// --- killing the browser is observable, not a hang -------------------
	kill(t, ctx, c, ws.ID)
	state := waitClosed(t, ctx, computer)
	if state.Reason != proto.ReasonBrowserCrashed {
		t.Fatalf("computer.get after kill = state %q reason %q, want reason %q",
			state.State, state.Reason, proto.ReasonBrowserCrashed)
	}
	if _, err := computer.Screenshot(ctx); err == nil {
		t.Fatal("screenshot of a crashed browser succeeded")
	} else if codeOf(err) != proto.CodeClosed || proto.ErrorReason(err) != proto.ReasonBrowserCrashed {
		t.Fatalf("screenshot after crash = %v (code %q reason %q)", err, codeOf(err), proto.ErrorReason(err))
	}
	waitEvent(t, ctx, c, ws.ID, proto.EvComputerDegraded)
	waitEvent(t, ctx, c, ws.ID, proto.EvComputerClosed)
	t.Logf("step crash: computer.get = %s/%s and every later op is closed/browser_crashed",
		state.State, state.Reason)

	// --- sleep and wake: the computer is gone and so is the profile ------
	if _, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, AfterSec: 3600}); err != nil {
		t.Fatalf("ws.sleep: %v", err)
	}
	waitState(t, ctx, c, ws.ID, proto.WSPaused)
	if _, err := c.WakeWorkspace(ctx, ws.ID); err != nil {
		t.Fatalf("ws.wake: %v", err)
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatalf("wait claimed after wake: %v", err)
	}
	if _, err := computer.Get(ctx); err == nil {
		t.Fatal("computer.get after wake succeeded; a computer does not survive a sleep")
	} else if codeOf(err) != proto.CodeNotFound {
		t.Fatalf("computer.get after wake = %v (code %q), want %q", err, codeOf(err), proto.CodeNotFound)
	}
	if _, err := c.Stat(ctx, ws.ID, ".remount/browser/default"); err == nil {
		t.Fatal("the browser profile survived a sleep; .remount is excluded from every snapshot")
	} else if codeOf(err) != proto.CodeNotFound {
		t.Fatalf("stat profile after wake = %v (code %q), want %q", err, codeOf(err), proto.CodeNotFound)
	}
	// The page itself is ordinary workspace payload and must survive.
	if _, err := c.Stat(ctx, ws.ID, "page.html"); err != nil {
		t.Fatalf("page.html did not survive sleep/wake: %v", err)
	}
	t.Logf("step sleep/wake: computer gone, profile absent, ordinary files intact")
}

// ---------------------------------------------------------------------------
// prerequisites
// ---------------------------------------------------------------------------

// requireDocker reports a missing prerequisite by name and skips. A browser
// lane that quietly passed without a browser would be worse than no lane.
func requireDocker(t *testing.T, ctx context.Context, image string) *workspace.Docker {
	t.Helper()
	d, err := workspace.NewDocker(filepath.Join(t.TempDir(), "docker"), image)
	if err != nil {
		t.Skipf("unavailable: docker backend: %v", err)
	}
	probe, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := d.Available(probe); err != nil {
		t.Skipf("unavailable: docker daemon: %v", err)
	}
	inspect := exec.CommandContext(probe, "docker", "image", "inspect", image)
	if out, err := inspect.CombinedOutput(); err != nil {
		t.Skipf("unavailable: image %s is absent (%v); build it with `docker build -t %s images/browser`\n%s",
			image, err, image, out)
	}
	requireContainerNetwork(t, probe, image)
	requireAllowedHostReachable(t, probe)
	return d
}

// requireAllowedHostReachable proves this host can reach the destination the
// allowed-egress step browses to. The broker dials out from here, so a host
// with no route to the public internet is a missing prerequisite and must be
// named as one: reported as a failure it would read as a proxy-auth defect.
func requireAllowedHostReachable(t *testing.T, ctx context.Context) {
	t.Helper()
	host := allowedHostName()
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, "443"))
	if err != nil {
		t.Skipf("unavailable: this host cannot reach %s:443 (%v); the broker dials the allowed "+
			"destination from here, so set REMOUNT_BROWSER_ALLOWED_HOST to one it can reach", host, err)
	}
	_ = conn.Close()
}

// requireContainerNetwork proves this host can reach a container's own address,
// which is how every backend that is not the process backend resolves a
// workspace port. Docker Desktop on macOS and Windows runs the daemon in a VM
// whose container network the host does not route to, so the lane is
// unavailable there and must say so rather than fail at computer.create with a
// timeout that reads like a browser defect.
func requireContainerNetwork(t *testing.T, ctx context.Context, image string) {
	t.Helper()
	const port = "9345"
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", image,
		"socat", "TCP-LISTEN:"+port+",fork,reuseaddr", "SYSTEM:echo ok").Output()
	if err != nil {
		t.Skipf("unavailable: %s cannot run the socat reachability probe: %v", image, err)
	}
	container := strings.TrimSpace(string(out))
	defer func() {
		_ = exec.Command("docker", "rm", "-f", container).Run()
	}()
	address, err := exec.CommandContext(ctx, "docker", "inspect", "--format",
		"{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container).Output()
	ip := strings.TrimSpace(string(address))
	if err != nil || ip == "" {
		t.Skipf("unavailable: the probe container has no address: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort(ip, port), 2*time.Second)
		if dialErr == nil {
			_ = conn.Close()
			return
		}
		last = dialErr
		time.Sleep(250 * time.Millisecond)
	}
	t.Skipf("unavailable: this host does not route to container addresses (%s: %v); "+
		"a computer session reaches the browser at the container's own address, so run "+
		"this lane on a Linux docker host or inside the daemon's VM", ip, last)
}

// lane runs a control plane and one docker-backed node in this process and
// returns a connected client. Exactly one host is allowed: the egress steps
// need one destination policy permits and one it does not, and a lane that
// allowed both or neither could not tell a policy decision from a missing
// capability.
func lane(t *testing.T, docker *workspace.Docker, image string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	data := t.TempDir()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv, err := server.New(server.Options{
		DataDir: filepath.Join(data, "server"), Logger: quiet, Mode: server.ModeStandalone,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ctx, "127.0.0.1:0") }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	addr, err := srv.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		srv.Close()
		t.Fatal(err)
	}
	endpoint := "http://" + addr
	link := "ws://" + addr + "/v1/link"

	n, err := node.New(node.Options{
		DataDir: filepath.Join(data, "node"),
		Dialer: transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
			return transport.DialWS(ctx, link, nil)
		}),
		Labels:      map[string]string{"browser": "true"},
		Backends:    workspace.NewRegistry(docker),
		Allow:       []string{allowedHostName()},
		ArtifactURL: endpoint + "/v1/artifacts",
		// The browser reaches the broker by name, and the name that works is a
		// property of the docker host rather than of Remount.
		BrokerAdvertiseHost: brokerHost(t, image),
		Logger:              quiet, Version: "test",
	})
	if err != nil {
		cancel()
		srv.Close()
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = n.Run(ctx) }()

	c := client.New(client.Options{
		Dialer: transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
			return transport.DialWS(ctx, link, nil)
		}),
		Principal: "browser-conformance", ArtifactURL: endpoint + "/v1/artifacts",
	})
	t.Cleanup(func() {
		_ = c.Close()
		cancel()
		wg.Wait()
		srv.Close()
	})
	waitHealthy(t, endpoint, 60*time.Second)
	t.Logf("lane: server %s, docker node, image %s", endpoint, image)
	return c
}

// brokerHost is the name a container in this docker installation can use to
// reach a listener on the docker host. Docker Desktop answers
// host.docker.internal, which is the node's own default; a plain Linux daemon
// does not, and the bridge gateway is the address that works there. Getting
// this wrong is silent: the browser simply never reaches the broker.
func brokerHost(t *testing.T, image string) string {
	t.Helper()
	if configured := os.Getenv("REMOUNT_BROWSER_BROKER_HOST"); configured != "" {
		return configured
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	probe := exec.CommandContext(ctx, "docker", "run", "--rm", image,
		"getent", "hosts", "host.docker.internal")
	if err := probe.Run(); err == nil {
		return "" // the node's docker default already resolves here
	}
	out, err := exec.CommandContext(ctx, "docker", "network", "inspect", "bridge",
		"--format", "{{(index .IPAM.Config 0).Gateway}}").Output()
	gateway := strings.TrimSpace(string(out))
	if err != nil || gateway == "" {
		t.Fatalf("unavailable: no container-reachable broker address; "+
			"host.docker.internal does not resolve and the bridge gateway is unknown: %v", err)
	}
	t.Logf("lane: broker advertised as the bridge gateway %s", gateway)
	return gateway
}

func waitHealthy(t *testing.T, endpoint string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint + "/healthz")
		if err == nil {
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK && strings.Contains(string(body), `"serving":true`) {
				return
			}
			last = fmt.Errorf("healthz %d: %s", response.StatusCode, body)
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never became healthy: %v", endpoint, last)
}

// registerCleanup destroys the workspace and proves no container survived it.
func registerCleanup(t *testing.T, c *client.Client, wsID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := c.DestroyWorkspace(ctx, wsID); err != nil {
			t.Errorf("cleanup: destroy %s: %v", wsID, err)
		}
		out, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
			"--filter", "label=remount.workspace="+wsID).Output()
		if err != nil {
			t.Errorf("cleanup: docker inventory for %s: %v", wsID, err)
			return
		}
		if strings.TrimSpace(string(out)) != "" {
			t.Errorf("cleanup: containers still labelled %s: %s", wsID, out)
		}
	})
}

// ---------------------------------------------------------------------------
// assertions
// ---------------------------------------------------------------------------

type box struct{ left, top int }

func boundingBox(t *testing.T, ctx context.Context, m *client.Computer, selector string) box {
	t.Helper()
	expression := fmt.Sprintf(
		`(function () { var r = document.querySelector(%q).getBoundingClientRect();
		  return {l: Math.round(r.left), t: Math.round(r.top),
		          w: Math.round(r.width), h: Math.round(r.height)}; })()`, selector)
	raw, err := m.Eval(ctx, expression)
	if err != nil {
		t.Fatalf("eval rect %s: %v", selector, err)
	}
	var rect struct{ L, T, W, H int }
	if err := json.Unmarshal(raw, &rect); err != nil {
		t.Fatalf("decode rect %s from %s: %v", selector, raw, err)
	}
	if rect.W <= 0 || rect.H <= 0 {
		t.Fatalf("%s has no layout: %s", selector, raw)
	}
	return box{left: rect.L, top: rect.T}
}

// clickCenter clicks the middle of an element. Coordinates come from the page
// rather than a constant, because the API's promise is CSS pixels in the
// declared viewport and this is the same arithmetic a caller would do.
func clickCenter(t *testing.T, ctx context.Context, m *client.Computer, selector string) {
	t.Helper()
	expression := fmt.Sprintf(
		`(function () { var r = document.querySelector(%q).getBoundingClientRect();
		  return {l: Math.round(r.left + r.width / 2), t: Math.round(r.top + r.height / 2),
		          w: Math.round(r.width), h: Math.round(r.height)}; })()`, selector)
	raw, err := m.Eval(ctx, expression)
	if err != nil {
		t.Fatalf("eval center %s: %v", selector, err)
	}
	var rect struct{ L, T, W, H int }
	if err := json.Unmarshal(raw, &rect); err != nil {
		t.Fatalf("decode center %s from %s: %v", selector, raw, err)
	}
	if rect.W <= 0 || rect.H <= 0 {
		t.Fatalf("%s has no layout: %s", selector, raw)
	}
	if err := m.Click(ctx, rect.L, rect.T); err != nil {
		t.Fatalf("click %s at %d,%d: %v", selector, rect.L, rect.T, err)
	}
}

func evalString(t *testing.T, ctx context.Context, m *client.Computer, expression string) string {
	t.Helper()
	raw, err := m.Eval(ctx, expression)
	if err != nil {
		t.Fatalf("eval %s: %v", expression, err)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("eval %s returned %s: %v", expression, raw, err)
	}
	return value
}

func evalFloat(t *testing.T, ctx context.Context, m *client.Computer, expression string) float64 {
	t.Helper()
	raw, err := m.Eval(ctx, expression)
	if err != nil {
		t.Fatalf("eval %s: %v", expression, err)
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("eval %s returned %s: %v", expression, raw, err)
	}
	return value
}

// waitEval polls until the page reports want. A postMessage crosses a frame
// boundary asynchronously, so reading once would be a race, not an assertion.
func waitEval(t *testing.T, ctx context.Context, m *client.Computer, expression, want string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var got string
	for time.Now().Before(deadline) && ctx.Err() == nil {
		got = evalString(t, ctx, m, expression)
		if got == want {
			return got
		}
		time.Sleep(100 * time.Millisecond)
	}
	return got
}

func waitDownload(t *testing.T, ctx context.Context, m *client.Computer) proto.ComputerDownload {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last []proto.ComputerDownload
	for time.Now().Before(deadline) && ctx.Err() == nil {
		downloads, err := m.Downloads(ctx)
		if err != nil {
			t.Fatalf("computer.downloads: %v", err)
		}
		last = downloads
		for _, d := range downloads {
			if d.State == proto.ComputerDownloadCompleted && d.Artifact != "" {
				return d
			}
			if d.State == proto.ComputerDownloadBlocked || d.State == proto.ComputerDownloadCanceled {
				return d
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no download reached a terminal state; last listing %+v", last)
	return proto.ComputerDownload{}
}

// verifyArtifact reads the published archive back and compares the file byte
// for byte. An artifact id that names the wrong bytes is worse than none.
func verifyArtifact(t *testing.T, ctx context.Context, c *client.Client, id string) {
	t.Helper()
	body, err := c.DownloadArtifact(ctx, id)
	if err != nil {
		t.Fatalf("download artifact %s: %v", id, err)
	}
	defer body.Close()
	zr, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("artifact %s is not gzip: %v", id, err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	names := []string{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("artifact %s tar: %v", id, err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		names = append(names, header.Name)
		if filepath.Base(header.Name) != downloadName {
			continue
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("artifact %s read %s: %v", id, header.Name, err)
		}
		if string(content) != downloadBody {
			t.Fatalf("artifact %s holds %q, want %q", id, content, downloadBody)
		}
		return
	}
	t.Fatalf("artifact %s does not contain %s; it holds %v", id, downloadName, names)
}

func waitClosed(t *testing.T, ctx context.Context, m *client.Computer) *proto.ComputerGetRes {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last *proto.ComputerGetRes
	for time.Now().Before(deadline) && ctx.Err() == nil {
		res, err := m.Get(ctx)
		if err != nil {
			t.Fatalf("computer.get: %v", err)
		}
		last = res
		if res.State == proto.ComputerStateClosed {
			return res
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("computer never reported closed; last %+v", last)
	return nil
}

// kill stops the browser the way an operator would: from inside the workspace,
// with no help from the computer API.
func kill(t *testing.T, ctx context.Context, c *client.Client, wsID string) {
	t.Helper()
	stdout, stderr, exit, err := c.Run(ctx, wsID, "sh", "-c", "pkill -9 chromium; exit 0")
	if err != nil {
		t.Fatalf("kill chromium: %v (%s %s)", err, stdout, stderr)
	}
	if exit == nil || exit.Code != 0 {
		t.Fatalf("kill chromium exited %+v: %s %s", exit, stdout, stderr)
	}
}

func waitState(t *testing.T, ctx context.Context, c *client.Client, wsID, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) && ctx.Err() == nil {
		ws, err := c.GetWorkspace(ctx, wsID)
		if err != nil {
			t.Fatalf("ws.get: %v", err)
		}
		last = ws.State
		if ws.State == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("workspace never reached %s; last %s", want, last)
}

// waitEvent polls the workspace's history. Events travel node -> control
// asynchronously, so polling is the honest wait.
func waitEvent(t *testing.T, ctx context.Context, c *client.Client, wsID, typ string) proto.Event {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		events, err := c.ReadEvents(ctx, 0, wsID)
		if err != nil {
			t.Fatalf("read events: %v", err)
		}
		for _, e := range events {
			if e.Type == typ {
				return e
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("event %s never appeared", typ)
	return proto.Event{}
}

// waitEgress returns the broker's own record of one decision about one
// destination. The broker's CONNECT handler is opaque by design, so the host
// is all it can see and all this asserts.
//
// The decision is part of what is waited for, not something read off the first
// event that mentions the host. A proxy challenge is itself recorded as an
// `unauthenticated` egress.denied — that is the 407 the browser is answered
// with, not a refusal — so a test that took the first egress.denied for a host
// would assert the handshake instead of the policy verdict, and would do so
// differently depending on whether the browser had already cached the
// capability for this proxy.
func waitEgress(t *testing.T, ctx context.Context, c *client.Client, wsID, typ, host, decision string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	seen := map[string]int{}
	for time.Now().Before(deadline) && ctx.Err() == nil {
		events, err := c.ReadEvents(ctx, 0, wsID)
		if err != nil {
			t.Fatalf("read events: %v", err)
		}
		for _, e := range events {
			if e.Type != typ {
				continue
			}
			payload := decodePayload(t, e)
			name, _ := payload["host"].(string)
			if !strings.Contains(name, host) {
				continue
			}
			got, _ := payload["decision"].(string)
			seen[got]++
			if got == decision {
				return payload
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no %s event for %s with decision %q; the decisions recorded for it were %v",
		typ, host, decision, seen)
	return nil
}

// navigationStatus reads the HTTP status the browser recorded for the document
// itself. A title and a load event can both come from an error page, so the
// status is what separates "the network answered" from "something rendered".
// A build that does not report one answers 0, which is a failed assertion
// rather than a quietly skipped one.
func navigationStatus(t *testing.T, ctx context.Context, m *client.Computer) int {
	t.Helper()
	raw, err := m.Eval(ctx, `(function () {
	  var e = performance.getEntriesByType("navigation");
	  return (e.length && typeof e[0].responseStatus === "number") ? e[0].responseStatus : 0;
	})()`)
	if err != nil {
		t.Fatalf("eval navigation status: %v", err)
	}
	var status int
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatalf("decode navigation status from %s: %v", raw, err)
	}
	return status
}

func decodePayload(t *testing.T, e proto.Event) map[string]any {
	t.Helper()
	payload := map[string]any{}
	if len(e.Payload) == 0 {
		return payload
	}
	if err := proto.Unmarshal(e.Payload, &payload); err != nil {
		t.Fatalf("decode %s payload: %v", e.Type, err)
	}
	return payload
}

func codeOf(err error) string {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}
