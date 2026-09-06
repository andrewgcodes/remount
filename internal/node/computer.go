package node

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/computer"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/redact"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

// BrowserDir is the workspace-relative directory a computer's browser profiles
// live under. It sits inside EnvFileDir, which every snapshot excludes, so a
// profile is deliberately node-local: it does not travel with a move, a sleep
// or a `remount pull`. That is the same conclusion the desktop recipe in
// docs/harness-integration.md reached the hard way — a live Chromium profile
// contains singleton symlinks and lock files that artifact validation
// correctly refuses, and restoring one onto another host is not meaningful.
const BrowserDir = EnvFileDir + "/browser"

// DownloadDir is the workspace-relative directory a computer's browser is
// allowed to download into. It is also inside EnvFileDir: a downloaded file
// reaches the client as a published artifact, not as snapshot payload.
const DownloadDir = EnvFileDir + "/downloads"

// DefaultBrowserProgram builds the command line the node spawns when a
// computer.create request names no program. It is a variable so a workspace
// image with a differently named or differently flagged browser can replace it
// at build time without a protocol change.
//
// addr is the address the node will dial for this backend (see
// devtoolsBindAddress). When that is loopback the browser is started directly.
// When it is not, the program becomes a two-process launch, because Chromium's
// DevTools HTTP server binds 127.0.0.1 and ignores --remote-debugging-address
// entirely: verified against Chromium 152, where the flag left the listener on
// 127.0.0.1 and every dial to the container address was refused. The forwarder
// is what makes a container or sandbox address reachable at all, so an image
// used for computer sessions on those backends needs socat as well as a
// browser (docs/images.md).
//
// The browser stays the foreground process so that its exit is the session's
// exit and a crash remains observable; the forwarder is killed with it rather
// than left holding the port.
var DefaultBrowserProgram = func(addr string, port int, profileDir string, v proto.ComputerViewport) []string {
	listen := port
	if !loopbackAddress(addr) {
		listen = forwardedDevToolsPort(port)
	}
	argv := []string{
		"chromium",
		"--headless=new",
		"--no-sandbox",
		"--disable-gpu",
		// Chromium's own component, sync, metrics and first-run traffic is
		// issued by the network service outside any page target, so CDP
		// request interception never sees it and the node cannot answer the
		// broker's proxy challenge for it (ADR 0095). Left on, every session
		// files a handful of `unauthenticated` egress denials against Google
		// hosts nobody asked for, which is both noise an operator has to
		// explain away and a workspace announcing itself to a third party.
		// A headless browser under automation needs none of it.
		"--disable-background-networking",
		"--disable-component-update",
		"--disable-default-apps",
		"--disable-sync",
		"--metrics-recording-only",
		"--no-first-run",
		"--no-default-browser-check",
		"--remote-debugging-address=127.0.0.1",
		fmt.Sprintf("--remote-debugging-port=%d", listen),
		"--user-data-dir=" + profileDir,
		fmt.Sprintf("--window-size=%d,%d", v.Width, v.Height),
		"about:blank",
	}
	if listen == port {
		return argv
	}
	return []string{"sh", "-c", fmt.Sprintf(
		"%s & browser=$!; socat TCP-LISTEN:%d,fork,reuseaddr TCP:127.0.0.1:%d & forwarder=$!; "+
			"wait $browser; status=$?; kill $forwarder 2>/dev/null; exit $status",
		shellCommand(argv), port, listen)}
}

// forwardedDevToolsPort is the loopback port the browser itself listens on
// when the node has to reach it through a forwarder. It is derived from the
// requested port so an operator reading the process list can see the pair.
func forwardedDevToolsPort(port int) int {
	if port < 65535 {
		return port + 1
	}
	return port - 1
}

// shellCommand renders argv for `sh -c`. Every word is single-quoted, so a
// profile path with a space or a shell metacharacter stays one word.
func shellCommand(argv []string) string {
	quoted := make([]string, 0, len(argv))
	for _, word := range argv {
		quoted = append(quoted, "'"+strings.ReplaceAll(word, "'", `'\''`)+"'")
	}
	return strings.Join(quoted, " ")
}

// browserEnv points a spawned browser's HOME at its own profile directory.
//
// A browser writes far more than its profile: caches, crash reports, NSS
// databases and a $HOME/.config tree appear whatever --user-data-dir says.
// Left at the workspace root those files become snapshot payload, and a real
// Chromium made `ws.sleep` fail outright with `openat .config: permission
// denied` because the container writes them as root and the node reads them as
// itself. The profile tree is node-local and excluded from every snapshot,
// which is exactly where that state belongs. An explicit HOME in the request
// still wins; this is a default, not a policy.
func browserEnv(env map[string]string, profileDir string) map[string]string {
	merged := make(map[string]string, len(env)+1)
	merged["HOME"] = profileDir
	for name, value := range env {
		merged[name] = value
	}
	return merged
}

// proxyOrderPreference is the order the node reads a proxy URL out of a
// session environment. HTTPS comes first because a browser's navigation is
// what this credential exists for, and the upper-case spelling first because
// that is the one the broker writes.
var proxyOrderPreference = []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"}

// proxyCredentialsFromEnv reads the credential a browser must present when the
// workspace's broker challenges it, out of the session environment the node
// just built for that browser.
//
// There is deliberately no second credential path. The value is whatever
// user-info the browser's own HTTP(S)_PROXY carries, so the node can never
// answer a challenge with an authority the browser was not already handed —
// and a workspace with no broker, or one whose env a caller overrode with an
// unauthenticated proxy, yields nothing and leaves interception off.
func proxyCredentialsFromEnv(env []string) computer.ProxyCredentials {
	assigned := map[string]string{}
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if slices.Contains(proxyOrderPreference, name) {
			// Last assignment wins, which is what every backend's environment
			// does with a repeated name.
			assigned[name] = value
		}
	}
	for _, name := range proxyOrderPreference {
		raw, ok := assigned[name]
		if !ok {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.User == nil {
			continue
		}
		password, _ := parsed.User.Password()
		creds := computer.ProxyCredentials{Username: parsed.User.Username(), Password: password}
		if creds.Set() {
			return creds
		}
	}
	return computer.ProxyCredentials{}
}

// redactProxyCredential removes the broker capability from words the node is
// about to publish. computer.create emits the browser's own command line, and
// the capability authorizes every egress this workspace has, so a launch
// program that names the proxy URL would otherwise land in the durable event
// log and every export of it.
func redactProxyCredential(words []string, creds computer.ProxyCredentials) []string {
	if !creds.Set() || len(words) == 0 {
		return words
	}
	scrub := redact.NewRedactor([]string{creds.Username, creds.Password})
	out := make([]string, len(words))
	for i, word := range words {
		out[i] = scrub.String(word)
	}
	return out
}

// loopbackAddress reports whether the node will dial the workspace's own
// loopback, which is the one case where a browser needs no forwarder.
func loopbackAddress(addr string) bool {
	if addr == "" || addr == "localhost" {
		return true
	}
	ip := net.ParseIP(addr)
	return ip != nil && ip.IsLoopback()
}

// devtoolsBindAddress is the address a spawned browser must listen on for this
// backend. The node dials whatever Backend.Prepare resolves for a workspace
// port, so a browser bound to the workspace's own loopback is unreachable on
// every backend that resolves a port to a container or sandbox address: the
// docker backend answers with the container IP and gVisor with the sandbox's.
// Loopback stays the default where the node dials loopback - the process
// backend, and the firecracker relay, which reaches the guest over vsock.
func (n *Node) devtoolsBindAddress(w *ws, claims proto.GrantClaims, port int) string {
	spec := session.Spec{
		WS: w.ID, Generation: w.Generation, Kind: proto.SessionPort,
		Port: port, Principal: claims.Principal, Tenant: claims.Tenant,
	}
	if err := w.handle.Prepare(&spec); err != nil {
		// The dial will fail for the same reason and report it properly. Keep
		// the loopback answer rather than widening exposure on a guess.
		return "127.0.0.1"
	}
	if loopbackAddress(spec.Host) {
		return "127.0.0.1"
	}
	return spec.Host
}

// profileName is deliberately narrow: the value becomes a directory name
// inside the node-owned .remount tree.
var profileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// computerHandle is one live CDP conversation the node holds on a client's
// behalf, plus everything needed to tear it down exactly once.
type computerHandle struct {
	id         string
	ws         string
	generation uint64
	principal  string
	// session is the managed exec session running the browser, or "" when the
	// client asked us to attach to one it started itself.
	session  string
	viewport proto.ComputerViewport
	cdp      *computer.Client
	// downloadHost is the host-side path of DownloadDir, or "" when the
	// backend exposes no host path and downloads cannot be published.
	downloadHost string

	mu        sync.Mutex
	state     string
	reason    string
	lastISeq  uint64
	published map[string]proto.ComputerDownload

	queue    chan computer.Download
	stop     chan struct{}
	stopOnce sync.Once
	workers  sync.WaitGroup
}

func (h *computerHandle) snapshot() (state, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state, h.reason
}

// markClosed records the terminal state once. It returns true for the caller
// that actually made the transition, so exactly one event is emitted.
func (h *computerHandle) markClosed(state, reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == proto.ComputerStateClosed {
		return false
	}
	h.state, h.reason = state, reason
	return true
}

// live returns the error a request against a dead computer must get. The code
// is stable; the reason says which death it was.
func (h *computerHandle) live() error {
	state, reason := h.snapshot()
	switch state {
	case proto.ComputerStateReady:
		return nil
	case proto.ComputerStateDegraded:
		return proto.ErrReason(proto.CodeClosed, reason, "computer %s is degraded: %s", h.id, reason)
	default:
		return proto.ErrReason(proto.CodeClosed, reason, "computer %s is closed: %s", h.id, reason)
	}
}

// ---------------------------------------------------------------------------
// registry
// ---------------------------------------------------------------------------

func (n *Node) lookupComputer(wsID, id string) (*computerHandle, error) {
	n.mu.Lock()
	h := n.computers[id]
	n.mu.Unlock()
	if h == nil || h.ws != wsID {
		return nil, proto.Err(proto.CodeNotFound, "computer %s is not on this node", id)
	}
	return h, nil
}

// stopComputers closes every computer on a workspace and joins its goroutines.
// Cancellation is not completion: this returns only once each conversation's
// download publisher and close watcher have stopped.
func (n *Node) stopComputers(wsID, reason string) {
	n.mu.Lock()
	var doomed []*computerHandle
	for id, h := range n.computers {
		if h.ws == wsID {
			doomed = append(doomed, h)
			delete(n.computers, id)
		}
	}
	n.mu.Unlock()
	for _, h := range doomed {
		n.teardownComputer(h, reason)
	}
}

// teardownComputer closes one conversation, kills a browser this node spawned,
// and emits computer.closed exactly once.
func (n *Node) teardownComputer(h *computerHandle, reason string) {
	first := h.markClosed(proto.ComputerStateClosed, reason)
	h.stopOnce.Do(func() { close(h.stop) })
	if h.cdp != nil {
		h.cdp.Close()
	}
	h.workers.Wait()
	if h.session != "" {
		n.sessions.Terminate(h.session, "computer closed")
	}
	if first {
		n.emitSession(proto.EvComputerClosed, h.ws, h.principal, h.session, map[string]any{
			"computer": h.id, "reason": reason,
		})
	}
}

// ---------------------------------------------------------------------------
// computer.create
// ---------------------------------------------------------------------------

func (n *Node) computerCreate(ctx context.Context, client string, claims proto.GrantClaims, w *ws, req *proto.ComputerCreateReq) (any, error) {
	clean := *req
	clean.Grant = nil
	key := n.mutationKey(client, w.ID, proto.OpComputerCreate, req.IdempotencyKey)
	raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
		res, err := n.computerCreateOnce(ctx, claims, w, req)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(res)
	})
	if err != nil {
		return nil, err
	}
	var res proto.ComputerCreateRes
	if err := proto.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return res, nil
}

func (n *Node) computerCreateOnce(ctx context.Context, claims proto.GrantClaims, w *ws, req *proto.ComputerCreateReq) (proto.ComputerCreateRes, error) {
	launch := proto.ComputerLaunch{}
	if req.Launch != nil {
		launch = *req.Launch
	}
	port := launch.Port
	if port == 0 {
		port = computer.DefaultPort
	}
	if port < 1 || port > 65535 {
		return proto.ComputerCreateRes{}, proto.Err(proto.CodeBadRequest, "port must be between 1 and 65535")
	}
	viewport := req.Viewport
	if viewport.Width <= 0 || viewport.Height <= 0 {
		viewport = computer.DefaultViewport
	}
	if viewport.Width > 8192 || viewport.Height > 8192 {
		return proto.ComputerCreateRes{}, proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
			"viewport %dx%d is larger than 8192x8192", viewport.Width, viewport.Height)
	}
	profile := req.Profile
	if profile == "" {
		profile = "default"
	}
	if !profileName.MatchString(profile) {
		return proto.ComputerCreateRes{}, proto.ErrReason(proto.CodeBadRequest, proto.ReasonProfileCorrupt,
			"profile name %q is not a single safe path segment", profile)
	}

	// Authorization was checked before this lock; revalidate after taking the
	// tree boundary so a queued create cannot touch a released handle.
	unlock, err := n.lockWorkspaceTree(w, false)
	if err != nil {
		return proto.ComputerCreateRes{}, err
	}
	defer unlock()

	mount := workspace.MountPathOf(w.handle)
	if mount == "" {
		return proto.ComputerCreateRes{}, proto.ErrReason(proto.CodeUnsupported, proto.ReasonBackendUnsupported,
			"backend %s exposes no workspace path for a browser profile", w.handle.Backend())
	}
	profileDir := path.Join(filepath.ToSlash(mount), BrowserDir, profile)
	downloadDir := path.Join(filepath.ToSlash(mount), DownloadDir)
	if err := w.handle.FS().Mkdir(DownloadDir); err != nil {
		return proto.ComputerCreateRes{}, err
	}
	if !launch.Attach {
		if err := w.handle.FS().Mkdir(path.Join(BrowserDir, profile)); err != nil {
			return proto.ComputerCreateRes{}, proto.ErrReason(proto.CodeConflict, proto.ReasonProfileCorrupt,
				"prepare browser profile %s: %v", profile, err)
		}
	}
	downloadHost := ""
	if host, ok := workspace.HostFileSystemOf(w.handle); ok {
		if resolved, rerr := host.Resolve(DownloadDir); rerr == nil {
			downloadHost = resolved
		}
	}

	id := ids.New("cmp")
	h := &computerHandle{
		id: id, ws: w.ID, generation: w.Generation,
		principal:    claims.Principal,
		viewport:     viewport,
		downloadHost: downloadHost,
		state:        proto.ComputerStateReady,
		published:    map[string]proto.ComputerDownload{},
		queue:        make(chan computer.Download, 32),
		stop:         make(chan struct{}),
	}

	// One environment, built once: it is what a spawned browser starts with
	// and it is where the proxy credential the node answers challenges with
	// comes from. A browser the client attached to was started from the same
	// workspace environment, so the same read applies to it.
	sessionEnv := n.sessionEnv(w, browserEnv(req.Env, profileDir))
	proxyAuth := proxyCredentialsFromEnv(sessionEnv)

	var browser *session.Session
	if !launch.Attach {
		program := launch.Program
		if len(program) == 0 {
			program = DefaultBrowserProgram(n.devtoolsBindAddress(w, claims, port), port, profileDir, viewport)
		}
		spec := session.Spec{
			WS: w.ID, Generation: w.Generation, Kind: proto.SessionExec,
			Program: program, Env: sessionEnv,
			Principal: claims.Principal, Tenant: claims.Tenant,
		}
		if err := w.handle.Prepare(&spec); err != nil {
			return proto.ComputerCreateRes{}, err
		}
		s, err := n.sessions.Open(spec)
		if err != nil {
			return proto.ComputerCreateRes{}, err
		}
		if err := n.confirmOpenAuthz(w, claims, s); err != nil {
			return proto.ComputerCreateRes{}, err
		}
		browser = s
		h.session = s.ID
		n.emitSession(proto.EvSOpened, w.ID, claims.Principal, s.ID, map[string]any{
			"s": s.ID, "kind": proto.SessionExec,
			"program": redactProxyCredential(program, proxyAuth), "client": "computer",
		})
	}

	cdp, err := computer.Connect(ctx, computer.Options{
		Dial:         n.computerDialer(w, claims, port),
		Endpoint:     net.JoinHostPort("127.0.0.1", fmt.Sprint(port)),
		Viewport:     viewport,
		DownloadPath: downloadDir,
		ProxyAuth:    proxyAuth,
		OnDownload:   h.enqueueDownload,
		OnClosed:     func(reason string) { n.computerCrashed(h, reason) },
	})
	if err != nil {
		if browser != nil {
			n.sessions.Terminate(browser.ID, "browser never became reachable")
		}
		return proto.ComputerCreateRes{}, err
	}
	h.cdp = cdp

	n.mu.Lock()
	n.computers[id] = h
	n.mu.Unlock()

	h.workers.Add(1)
	go n.publishDownloads(h)
	if browser != nil {
		h.workers.Add(1)
		go n.watchBrowserSession(h, browser)
	}

	n.emitSession(proto.EvComputerCreated, w.ID, claims.Principal, h.session, map[string]any{
		"computer": id, "s": h.session, "port": port, "attach": launch.Attach,
		"profile": profile, "w": viewport.Width, "h": viewport.Height, "cdp": cdp.Version(),
	})
	return proto.ComputerCreateRes{
		Computer: id, Session: h.session, CDPVersion: cdp.Version(), Viewport: viewport,
	}, nil
}

// computerDialer resolves one workspace port through exactly the machinery
// port.open uses, so a node-held conversation and a client's forward take the
// same byte path on every backend.
func (n *Node) computerDialer(w *ws, claims proto.GrantClaims, port int) computer.Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		spec := session.Spec{
			WS: w.ID, Generation: w.Generation, Kind: proto.SessionPort,
			Port: port, Principal: claims.Principal, Tenant: claims.Tenant,
		}
		if err := w.handle.Prepare(&spec); err != nil {
			return nil, err
		}
		return session.DialPort(spec)
	}
}

func (h *computerHandle) enqueueDownload(d computer.Download) {
	select {
	case h.queue <- d:
	case <-h.stop:
	default:
		// A flood of downloads must not block the CDP read loop. The
		// computer.downloads listing still reports the file, without an
		// artifact, so the loss is visible rather than silent.
		h.mu.Lock()
		h.published[d.GUID] = proto.ComputerDownload{
			Filename: d.Filename, URL: d.URL, Bytes: d.Bytes,
			State: proto.ComputerDownloadBlocked, Reason: "download publish queue is full",
		}
		h.mu.Unlock()
	}
}

// computerCrashed records a dead conversation and reports it once.
func (n *Node) computerCrashed(h *computerHandle, reason string) {
	if !h.markClosed(proto.ComputerStateClosed, reason) {
		return
	}
	n.emitSession(proto.EvComputerDegraded, h.ws, h.principal, h.session, map[string]any{
		"computer": h.id, "reason": reason,
	})
	n.emitSession(proto.EvComputerClosed, h.ws, h.principal, h.session, map[string]any{
		"computer": h.id, "reason": reason,
	})
}

// watchBrowserSession turns the spawned browser exiting into the same
// observable close as the socket dying.
func (n *Node) watchBrowserSession(h *computerHandle, s *session.Session) {
	defer h.workers.Done()
	ctx, cancel := context.WithCancel(context.WithoutCancel(context.Background()))
	defer cancel()
	go func() {
		select {
		case <-h.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	if _, err := s.Wait(ctx); err != nil {
		return // the computer is being torn down; teardown owns the event
	}
	n.computerCrashed(h, proto.ReasonBrowserCrashed)
}

// ---------------------------------------------------------------------------
// downloads become artifacts
// ---------------------------------------------------------------------------

// publishDownloads turns each finished download into a tenant-scoped artifact
// through the same store-and-upload path volume.archive uses.
func (n *Node) publishDownloads(h *computerHandle) {
	defer h.workers.Done()
	for {
		select {
		case <-h.stop:
			return
		case d := <-h.queue:
			n.publishDownload(h, d)
		}
	}
}

func (n *Node) publishDownload(h *computerHandle, d computer.Download) {
	record := proto.ComputerDownload{
		Filename: d.Filename, URL: d.URL, Bytes: d.Bytes, State: d.State,
	}
	defer func() {
		h.mu.Lock()
		h.published[d.GUID] = record
		h.mu.Unlock()
	}()
	if d.State != proto.ComputerDownloadCompleted {
		return
	}
	n.mu.Lock()
	w := n.workspaces[h.ws]
	n.mu.Unlock()
	if w == nil || w.Generation != h.generation {
		record.State, record.Reason = proto.ComputerDownloadBlocked, proto.ReasonWorkspaceMoved
		return
	}
	if h.downloadHost == "" {
		record.State, record.Reason = proto.ComputerDownloadBlocked, proto.ReasonBackendUnsupported
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 2*time.Minute)
	defer cancel()
	res, fileBytes, err := n.archiveDownload(ctx, w, h, d)
	if err != nil {
		n.logger.Warn("publish computer download", "ws", h.ws, "computer", h.id, "err", err)
		record.State, record.Reason = proto.ComputerDownloadBlocked, proto.ReasonDownloadBlocked
		return
	}
	// Bytes is the file's own size, not the archive's. Reporting a compressed
	// artifact length as the download's size would be a quietly wrong number
	// in every listing that shows it, so both are named.
	record.Artifact, record.Bytes = res.Artifact, fileBytes
	n.emitSession(proto.EvComputerDownload, h.ws, h.principal, h.session, map[string]any{
		"computer": h.id, "artifact": res.Artifact, "filename": record.Filename,
		"bytes": fileBytes, "artifact_bytes": res.Bytes, "s": h.session,
	})
}

// archiveDownload publishes one downloaded file as a one-entry snapshot. It is
// the single-file sibling of archiveVolumeLocked: same tree boundary, same
// deterministic tar.gz layout, same store limit, same tenant-scoped upload.
func (n *Node) archiveDownload(ctx context.Context, w *ws, h *computerHandle, d computer.Download) (proto.WSSnapshotRes, int64, error) {
	unlock, err := n.lockWorkspaceTree(w, false)
	if err != nil {
		return proto.WSSnapshotRes{}, 0, err
	}
	defer unlock()

	name, fileBytes, err := downloadFilename(h.downloadHost, d)
	if err != nil {
		return proto.WSSnapshotRes{}, 0, err
	}
	pr, pw := io.Pipe()
	producerDone := make(chan error, 1)
	go func() {
		_, serr := artifact.SnapshotFiltered(h.downloadHost, func(rel string, isDir bool) bool {
			return isDir || rel != name
		}, pw)
		_ = pw.CloseWithError(serr)
		producerDone <- serr
	}()
	id, size, err := n.store.PutLimit(pr, n.opts.MaxArtifactBytes)
	if err != nil {
		_ = pr.CloseWithError(err)
		<-producerDone
		return proto.WSSnapshotRes{}, 0, err
	}
	if producerErr := <-producerDone; producerErr != nil {
		return proto.WSSnapshotRes{}, 0, producerErr
	}
	if n.opts.ArtifactURL != "" {
		if err := n.upload(ctx, &w.Workspace, id); err != nil {
			return proto.WSSnapshotRes{}, 0, err
		}
	}
	return proto.WSSnapshotRes{Artifact: id, Bytes: size, Consistency: proto.SnapshotConsistencyLive}, fileBytes, nil
}

// downloadFilename picks the entry inside the download directory that belongs
// to d. Chrome names the file by its suggested name in most builds and by its
// GUID when download events are enabled, so both are checked; nothing else is
// guessed, because publishing the wrong file is worse than failing.
func downloadFilename(dir string, d computer.Download) (string, int64, error) {
	for _, candidate := range []string{d.Filename, d.GUID} {
		if candidate == "" || strings.ContainsAny(candidate, `/\`) {
			continue
		}
		if info, err := os.Lstat(filepath.Join(dir, candidate)); err == nil && info.Mode().IsRegular() {
			return candidate, info.Size(), nil
		}
	}
	return "", 0, proto.ErrReason(proto.CodeNotFound, proto.ReasonDownloadBlocked,
		"download %s left no regular file in the download directory", d.GUID)
}

// ---------------------------------------------------------------------------
// the remaining operations
// ---------------------------------------------------------------------------

func (n *Node) computerScreenshot(ctx context.Context, w *ws, req *proto.ComputerScreenshotReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	if err := h.live(); err != nil {
		return nil, err
	}
	return h.cdp.Screenshot(ctx)
}

func (n *Node) computerInput(ctx context.Context, w *ws, req *proto.ComputerInputReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	if err := h.live(); err != nil {
		return nil, err
	}
	// Deduplicate exactly as session.Input does: a retried action batch is
	// never applied twice, and a stale sequence is a no-op, not an error.
	h.mu.Lock()
	last := h.lastISeq
	if req.ISeq != 0 && req.ISeq <= last {
		h.mu.Unlock()
		return proto.ComputerInputRes{Applied: false, LastInputSeq: last}, nil
	}
	h.mu.Unlock()

	if err := h.cdp.Apply(ctx, req.Actions); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if req.ISeq > h.lastISeq {
		h.lastISeq = req.ISeq
	}
	last = h.lastISeq
	h.mu.Unlock()
	return proto.ComputerInputRes{Applied: true, LastInputSeq: last}, nil
}

func (n *Node) computerNavigate(ctx context.Context, client string, w *ws, req *proto.ComputerNavigateReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	if err := h.live(); err != nil {
		return nil, err
	}
	if req.URL == "" {
		return nil, proto.Err(proto.CodeBadRequest, "navigate needs a url")
	}
	clean := *req
	clean.Grant = nil
	key := n.mutationKey(client, w.ID, proto.OpComputerNavigate, req.IdempotencyKey)
	raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
		res, err := h.cdp.Navigate(ctx, req.URL)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(res)
	})
	if err != nil {
		return nil, err
	}
	var res proto.ComputerNavigateRes
	if err := proto.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return res, nil
}

func (n *Node) computerEval(ctx context.Context, w *ws, req *proto.ComputerEvalReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	if err := h.live(); err != nil {
		return nil, err
	}
	value, err := h.cdp.Eval(ctx, req.Expression)
	if err != nil {
		return nil, err
	}
	return proto.ComputerEvalRes{Value: value}, nil
}

func (n *Node) computerDownloads(w *ws, req *proto.ComputerDownloadsReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	out := proto.ComputerDownloadsRes{}
	h.mu.Lock()
	published := make(map[string]proto.ComputerDownload, len(h.published))
	for guid, record := range h.published {
		published[guid] = record
	}
	h.mu.Unlock()
	for _, d := range h.cdp.Downloads() {
		record, ok := published[d.GUID]
		if !ok {
			record = proto.ComputerDownload{
				Filename: d.Filename, URL: d.URL, Bytes: d.Bytes, State: d.State,
			}
		}
		out.Downloads = append(out.Downloads, record)
	}
	return out, nil
}

func (n *Node) computerClose(ctx context.Context, client string, w *ws, req *proto.ComputerCloseReq) (any, error) {
	clean := *req
	clean.Grant = nil
	key := n.mutationKey(client, w.ID, proto.OpComputerClose, req.IdempotencyKey)
	_, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
		n.mu.Lock()
		h := n.computers[req.Computer]
		if h != nil && h.ws == w.ID {
			delete(n.computers, req.Computer)
		} else {
			h = nil
		}
		n.mu.Unlock()
		if h == nil {
			// Closing a computer that is already gone is the postcondition the
			// caller asked for, so a replay is a no-op rather than an error.
			return proto.Marshal(struct{}{})
		}
		n.teardownComputer(h, proto.ComputerClosedReasonClosed)
		return proto.Marshal(struct{}{})
	})
	if err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

func (n *Node) computerGet(w *ws, req *proto.ComputerGetReq) (any, error) {
	h, err := n.lookupComputer(w.ID, req.Computer)
	if err != nil {
		return nil, err
	}
	state, reason := h.snapshot()
	h.mu.Lock()
	lastISeq := h.lastISeq
	h.mu.Unlock()
	return proto.ComputerGetRes{
		Computer: h.id, State: state, Reason: reason, Viewport: h.viewport,
		Session: h.session, LastInputSeq: lastISeq,
	}, nil
}
