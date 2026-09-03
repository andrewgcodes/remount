package firecracker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

const (
	// GuestProtocolVersion is the host/guest RPC compatibility boundary.
	GuestProtocolVersion = 1
	// GuestVsockPort is the fixed service port in the guest image contract.
	GuestVsockPort = 10789
	guestVsockPort = GuestVsockPort
	maxGuestFrame  = 4 << 20
)

// GuestManifest is generated with the guest image and checked before the
// backend is advertised. The binary digest lets image builders bind this
// external contract to the exact static remount binary installed in the VM.
type GuestManifest struct {
	Protocol     uint16 `json:"protocol"`
	VsockPort    uint32 `json:"vsock_port"`
	WorkspaceDir string `json:"workspace_dir"`
	BinarySHA256 string `json:"binary_sha256"`
}

// GuestOptions configure the concrete Firecracker guest bridge.
type GuestOptions struct {
	Manifest string
	Timeout  time.Duration
}

// GuestBridge implements filesystem and session operations over the exact
// Unix socket backing one VM's virtio-vsock device. Kernel CID routing is the
// authentication boundary; no credential is written into the guest disk.
type GuestBridge struct {
	opts     GuestOptions
	manifest GuestManifest
}

// NewGuestBridge loads the versioned guest image contract.
func NewGuestBridge(opts GuestOptions) (*GuestBridge, error) {
	if opts.Manifest == "" {
		return nil, errors.New("firecracker: guest manifest is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	data, err := os.ReadFile(opts.Manifest)
	if err != nil {
		return nil, fmt.Errorf("firecracker guest manifest: %w", err)
	}
	var manifest GuestManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("firecracker guest manifest: %w", err)
	}
	digest, digestErr := hex.DecodeString(manifest.BinarySHA256)
	if manifest.Protocol != GuestProtocolVersion || manifest.VsockPort != guestVsockPort || manifest.WorkspaceDir != "/workspace" || digestErr != nil || len(digest) != sha256.Size {
		return nil, fmt.Errorf("firecracker: incompatible guest manifest protocol=%d port=%d root=%q", manifest.Protocol, manifest.VsockPort, manifest.WorkspaceDir)
	}
	return &GuestBridge{opts: opts, manifest: manifest}, nil
}

// Probe verifies the external image contract. Per-VM liveness is checked by
// every operation after boot and is never inferred from this static probe.
func (g *GuestBridge) Probe(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := os.Lstat(g.opts.Manifest)
	if err != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("guest manifest unavailable: %w", err)
	}
	return nil
}

// FileSystem returns a generation-bound RPC filesystem. It intentionally does
// not implement workspace.HostFileSystem.
func (g *GuestBridge) FileSystem(endpoint func() (GuestEndpoint, error)) workspace.FileSystem {
	return &guestFS{bridge: g, endpoint: endpoint}
}

// Prepare installs the remote runner without changing portable arguments.
func (g *GuestBridge) Prepare(spec *session.Spec, endpoint GuestEndpoint) error {
	if endpoint.Socket == "" || endpoint.Generation == 0 || endpoint.Workspace != spec.WS {
		return proto.Err(proto.CodeDenied, "guest endpoint does not match session workspace generation")
	}
	spec.Runner = guestRunner{bridge: g, endpoint: endpoint}
	return nil
}

type guestFrame struct {
	V      uint16       `cbor:"v"`
	Type   string       `cbor:"type"`
	Op     string       `cbor:"op,omitempty"`
	Body   []byte       `cbor:"body,omitempty"`
	Stream uint8        `cbor:"stream,omitempty"`
	Err    *proto.Error `cbor:"err,omitempty"`
}

func writeGuest(w io.Writer, frame guestFrame) error {
	frame.V = GuestProtocolVersion
	b, err := proto.Marshal(frame)
	if err != nil {
		return err
	}
	if len(b) > maxGuestFrame {
		return proto.Err(proto.CodeResourceExhausted, "guest frame exceeds %d bytes", maxGuestFrame)
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(b)))
	if err := writeAll(w, size[:]); err != nil {
		return err
	}
	return writeAll(w, b)
}

func readGuest(r io.Reader) (guestFrame, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return guestFrame{}, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n == 0 || n > maxGuestFrame {
		return guestFrame{}, proto.Err(proto.CodeResourceExhausted, "invalid guest frame size %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return guestFrame{}, err
	}
	var frame guestFrame
	if err := proto.Unmarshal(b, &frame); err != nil {
		return guestFrame{}, err
	}
	if frame.V != GuestProtocolVersion {
		return guestFrame{}, proto.Err(proto.CodeUnsupported, "guest protocol %d is not supported", frame.V)
	}
	return frame, nil
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) != 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func (g *GuestBridge) dial(ctx context.Context, endpoint GuestEndpoint) (net.Conn, error) {
	if endpoint.Socket == "" || endpoint.Generation == 0 {
		return nil, proto.Err(proto.CodeClosed, "guest endpoint is not active")
	}
	d := net.Dialer{Timeout: g.opts.Timeout}
	conn, err := d.DialContext(ctx, "unix", endpoint.Socket)
	if err != nil {
		return nil, proto.Err(proto.CodeUnreachable, "guest vsock: %v", err)
	}
	deadline := time.Now().Add(g.opts.Timeout)
	_ = conn.SetDeadline(deadline)
	if err := writeAll(conn, []byte(fmt.Sprintf("CONNECT %d\n", guestVsockPort))); err != nil {
		_ = conn.Close()
		return nil, err
	}
	line, err := readBoundedLine(conn, 64)
	if err != nil || len(line) < 3 || line[:3] != "OK " {
		_ = conn.Close()
		return nil, proto.Err(proto.CodeUnreachable, "guest vsock handshake failed: %v %q", err, line)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func readBoundedLine(r io.Reader, max int) (string, error) {
	buf := make([]byte, 0, max)
	var one [1]byte
	for len(buf) < max {
		if _, err := io.ReadFull(r, one[:]); err != nil {
			return string(buf), err
		}
		if one[0] == '\n' {
			return string(buf), nil
		}
		buf = append(buf, one[0])
	}
	return string(buf), proto.Err(proto.CodeResourceExhausted, "guest handshake line exceeds %d bytes", max)
}

func (g *GuestBridge) call(endpoint GuestEndpoint, op string, request, response any) error {
	ctx, cancel := context.WithTimeout(context.Background(), g.opts.Timeout)
	defer cancel()
	conn, err := g.dial(ctx, endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()
	body, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	if err := writeGuest(conn, guestFrame{Type: "req", Op: op, Body: body}); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(g.opts.Timeout))
	frame, err := readGuest(conn)
	if err != nil {
		return err
	}
	if frame.Type != "res" {
		return proto.Err(proto.CodeInternal, "unexpected guest response %q", frame.Type)
	}
	if frame.Err != nil {
		return frame.Err
	}
	if response != nil && len(frame.Body) != 0 {
		return proto.Unmarshal(frame.Body, response)
	}
	return nil
}

type guestFS struct {
	bridge   *GuestBridge
	endpoint func() (GuestEndpoint, error)
	mu       sync.Mutex
	closed   bool
}

func (f *guestFS) ep() (GuestEndpoint, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return GuestEndpoint{}, proto.Err(proto.CodeClosed, "guest filesystem is closed")
	}
	return f.endpoint()
}
func (f *guestFS) Close() error { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }
func (f *guestFS) call(op string, req, res any) error {
	ep, err := f.ep()
	if err != nil {
		return err
	}
	return f.bridge.call(ep, op, req, res)
}
func (f *guestFS) Read(path string, offset, limit int64) (*proto.FSReadRes, error) {
	var res proto.FSReadRes
	err := f.call("fs.read", proto.FSReadReq{Path: path, Offset: offset, Limit: limit}, &res)
	return &res, err
}
func (f *guestFS) Write(path string, data []byte, mode uint32, appendMode, mkdirp bool) error {
	return f.call("fs.write", proto.FSWriteReq{Path: path, Data: data, Mode: mode, Append: appendMode, MkdirP: mkdirp}, nil)
}
func (f *guestFS) List(path string) ([]proto.FSEntry, error) {
	var res proto.FSListRes
	err := f.call("fs.list", proto.FSListReq{Path: path}, &res)
	return res.Entries, err
}
func (f *guestFS) Stat(path string) (*proto.FSEntry, error) {
	var res proto.FSStatRes
	err := f.call("fs.stat", proto.FSStatReq{Path: path}, &res)
	return &res.Entry, err
}
func (f *guestFS) Mkdir(path string) error {
	return f.call("fs.mkdir", proto.FSMkdirReq{Path: path}, nil)
}
func (f *guestFS) Remove(path string, recursive bool) error {
	return f.call("fs.remove", proto.FSRemoveReq{Path: path, Recursive: recursive}, nil)
}
func (f *guestFS) Rename(from, to string) error {
	return f.call("fs.rename", proto.FSRenameReq{From: from, To: to}, nil)
}
func (f *guestFS) Search(path, pattern, glob string, max int) (*proto.FSSearchRes, error) {
	var res proto.FSSearchRes
	err := f.call("fs.search", proto.FSSearchReq{Path: path, Pattern: pattern, Glob: glob, MaxResults: max}, &res)
	return &res, err
}
func (f *guestFS) Edit(path string, edits []proto.FSEdit) (int, error) {
	var res proto.FSEditRes
	err := f.call("fs.edit", proto.FSEditReq{Path: path, Edits: edits}, &res)
	return res.Replacements, err
}

type guestRunner struct {
	bridge   *GuestBridge
	endpoint GuestEndpoint
}

func (r guestRunner) Start(spec session.Spec) (session.Running, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.bridge.opts.Timeout)
	defer cancel()
	conn, err := r.bridge.dial(ctx, r.endpoint)
	if err != nil {
		return nil, err
	}
	spec.Runner = nil
	body, err := proto.Marshal(spec)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := writeGuest(conn, guestFrame{Type: "req", Op: "session.open", Body: body}); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(r.bridge.opts.Timeout))
	frame, err := readGuest(conn)
	if err != nil || frame.Type != "res" || frame.Err != nil {
		conn.Close()
		if frame.Err != nil {
			return nil, frame.Err
		}
		return nil, fmt.Errorf("guest session open: %w", err)
	}
	var opened struct {
		PID int `cbor:"pid"`
	}
	if err := proto.Unmarshal(frame.Body, &opened); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Time{})
	remote := newGuestRunning(conn, opened.PID)
	go remote.readLoop()
	return remote, nil
}

type guestRunning struct {
	conn net.Conn
	pid  int
	mu   sync.Mutex
	outR *io.PipeReader
	outW *io.PipeWriter
	errR *io.PipeReader
	errW *io.PipeWriter
	done chan proto.ExitInfo
	once sync.Once
}

func newGuestRunning(conn net.Conn, pid int) *guestRunning {
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	return &guestRunning{conn: conn, pid: pid, outR: outR, outW: outW, errR: errR, errW: errW, done: make(chan proto.ExitInfo, 1)}
}
func (r *guestRunning) PID() int              { return r.pid }
func (r *guestRunning) Stdin() io.WriteCloser { return guestInput{running: r} }
func (r *guestRunning) Stdout() io.Reader     { return r.outR }
func (r *guestRunning) Stderr() io.Reader     { return r.errR }
func (r *guestRunning) send(op string, body any) error {
	b, err := proto.Marshal(body)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err = writeGuest(r.conn, guestFrame{Type: "cmd", Op: op, Body: b})
	_ = r.conn.SetWriteDeadline(time.Time{})
	return err
}
func (r *guestRunning) Resize(rows, cols uint16) error {
	return r.send("resize", struct{ Rows, Cols uint16 }{rows, cols})
}
func (r *guestRunning) Signal(name string) error { return r.send("signal", name) }
func (r *guestRunning) CloseWrite() error        { return r.send("input", struct{ EOF bool }{true}) }
func (r *guestRunning) Wait() proto.ExitInfo     { return <-r.done }
func (r *guestRunning) finish(info proto.ExitInfo) {
	r.once.Do(func() {
		_ = r.outW.Close()
		_ = r.errW.Close()
		_ = r.conn.Close()
		r.done <- info
		close(r.done)
	})
}
func (r *guestRunning) readLoop() {
	for {
		frame, err := readGuest(r.conn)
		if err != nil {
			r.finish(proto.ExitInfo{Code: -1, Error: "guest transport: " + err.Error()})
			return
		}
		switch frame.Type {
		case "chunk":
			writer := r.outW
			if frame.Stream == proto.StreamStderr {
				writer = r.errW
			}
			if err := writeAll(writer, frame.Body); err != nil {
				r.finish(proto.ExitInfo{Code: -1, Error: err.Error()})
				return
			}
		case "exit":
			var info proto.ExitInfo
			if err := proto.Unmarshal(frame.Body, &info); err != nil {
				info = proto.ExitInfo{Code: -1, Error: err.Error()}
			}
			r.finish(info)
			return
		}
	}
}

type guestInput struct{ running *guestRunning }

func (w guestInput) Write(data []byte) (int, error) {
	copyData := append([]byte(nil), data...)
	if err := w.running.send("input", struct {
		Data []byte
		EOF  bool
	}{copyData, false}); err != nil {
		return 0, err
	}
	return len(data), nil
}
func (w guestInput) Close() error { return w.running.CloseWrite() }

var _ GuestExecutor = (*GuestBridge)(nil)
var _ workspace.FileSystem = (*guestFS)(nil)
