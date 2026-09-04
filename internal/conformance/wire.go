package conformance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/fxamacker/cbor/v2"
)

// These declarations are a second, independent transcription of
// spec/PROTOCOL.md §2 and §3. They are deliberately NOT imported from
// internal/proto: a conformance decoder that shares a struct definition with
// the encoder it is judging cannot detect a disagreement between the
// implementation and the document.

// FrameVersion is the only protocol version this manifest describes (§13).
const FrameVersion = 1

// Frame kinds (§2).
const (
	KindHello = "hello"
	KindReq   = "req"
	KindRes   = "res"
	KindChunk = "chunk"
	KindEvent = "ev"
	KindPing  = "ping"
	KindPong  = "pong"
)

// PeerControl is the well-known destination of every control-plane
// operation (§1).
const PeerControl = "control"

// CapabilityV1 is the mandatory semantic baseline (§3.1). A hello that does
// not offer it is not a v1 peer.
const CapabilityV1 = "v1"

// KnownCapabilities is the canonical order of §3.1's capability table. The
// spec requires HelloOK.caps to be returned in this order, v1 first.
var KnownCapabilities = []string{
	CapabilityV1,
	"authz-push",
	"controller-epoch",
	"session-cap",
	"chunked-artifacts",
	"tiered-session-logs",
	"identity-admin",
	"approvals",
	"encrypted-artifacts",
}

// StableErrorCodes is the closed set of §2 error codes. An implementation
// that answers with anything else has invented a code its peers cannot match
// on, which is the failure the stability rule exists to prevent.
var StableErrorCodes = []string{
	"bad_request", "not_found", "unsupported", "unauthorized", "conflict",
	"evicted", "unreachable", "internal", "timeout", "closed", "denied",
	"resource_exhausted",
}

// Chunk stream ids (§8).
const (
	ChunkStdout = 1
	ChunkStderr = 2
	ChunkExit   = 3
	ChunkInfo   = 4
	ChunkGap    = 5
)

// WireError is the §2 Error structure.
type WireError struct {
	Code   string `cbor:"code" json:"code"`
	Msg    string `cbor:"msg,omitempty" json:"msg,omitempty"`
	Oldest uint64 `cbor:"oldest,omitempty" json:"oldest,omitempty"`
}

func (e *WireError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

// Frame is the single wire unit of §2.
type Frame struct {
	V               uint8      `cbor:"v"`
	T               string     `cbor:"t"`
	ControllerEpoch uint64     `cbor:"controller_epoch,omitempty"`
	ID              uint64     `cbor:"id,omitempty"`
	Seq             uint64     `cbor:"seq,omitempty"`
	S               string     `cbor:"s,omitempty"`
	WS              string     `cbor:"ws,omitempty"`
	To              string     `cbor:"to,omitempty"`
	From            string     `cbor:"from,omitempty"`
	Op              string     `cbor:"op,omitempty"`
	Body            []byte     `cbor:"body,omitempty"`
	Err             *WireError `cbor:"err,omitempty"`
}

// Hello is the §3 handshake request.
type Hello struct {
	Peer      string            `cbor:"peer,omitempty"`
	Role      string            `cbor:"role,omitempty"`
	Token     string            `cbor:"token,omitempty"`
	Caps      []string          `cbor:"caps,omitempty"`
	PubKey    []byte            `cbor:"pubkey,omitempty"`
	Labels    map[string]string `cbor:"labels,omitempty"`
	Principal string            `cbor:"principal,omitempty"`
	IssuedAt  int64             `cbor:"issued_at,omitempty"`
	Nonce     []byte            `cbor:"nonce,omitempty"`
	Proof     []byte            `cbor:"proof,omitempty"`
}

// HelloOK is the §3 handshake response.
type HelloOK struct {
	Peer            string   `cbor:"peer,omitempty"`
	Caps            []string `cbor:"caps,omitempty"`
	Server          string   `cbor:"server,omitempty"`
	Now             int64    `cbor:"now,omitempty"`
	PubKey          []byte   `cbor:"pubkey,omitempty"`
	LeaseSec        int64    `cbor:"lease_sec,omitempty"`
	Subject         string   `cbor:"subject,omitempty"`
	Tenant          string   `cbor:"tenant,omitempty"`
	NodeToken       string   `cbor:"node_token,omitempty"`
	ControllerEpoch uint64   `cbor:"controller_epoch,omitempty"`
}

// ChunkBody is the §8 session output payload.
type ChunkBody struct {
	St uint8  `cbor:"st"`
	D  []byte `cbor:"d,omitempty"`
}

// ExitInfo is the decoded payload of an exit chunk (§8).
type ExitInfo struct {
	Code   int    `cbor:"code"`
	Signal string `cbor:"signal,omitempty"`
	Error  string `cbor:"error,omitempty"`
	Reason string `cbor:"reason,omitempty"`
}

// Gap is the decoded payload of a gap chunk (§8): the seqs that are gone.
type Gap struct {
	From uint64 `cbor:"from"`
	To   uint64 `cbor:"to"`
}

var (
	encMode cbor.EncMode
	decMode cbor.DecMode
)

func init() {
	var err error
	// CoreDetEncOptions is CBOR's deterministic core encoding. The spec calls
	// for deterministic CBOR for signed structures; using it everywhere keeps
	// the golden fixtures byte-stable.
	if encMode, err = cbor.CoreDetEncOptions().EncMode(); err != nil {
		panic(err)
	}
	if decMode, err = (cbor.DecOptions{MaxArrayElements: 1 << 20, MaxMapPairs: 1 << 20}).DecMode(); err != nil {
		panic(err)
	}
}

// Marshal encodes a value as deterministic CBOR.
func Marshal(v any) ([]byte, error) { return encMode.Marshal(v) }

// Unmarshal decodes CBOR into v.
func Unmarshal(b []byte, v any) error { return decMode.Unmarshal(b, v) }

// EncodeFrame serializes a frame exactly as sent, including a deliberately
// wrong V, because refusing to encode one would make the version-skew
// requirement untestable.
func EncodeFrame(f *Frame) ([]byte, error) { return encMode.Marshal(f) }

// DecodeFrame parses a frame and enforces §2 rule 1: a version that was never
// negotiated fails closed before the kind or body is interpreted.
func DecodeFrame(b []byte) (*Frame, error) {
	var f Frame
	if err := decMode.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("conformance: decode frame: %w", err)
	}
	if f.T == "" {
		return nil, errors.New("conformance: frame has no kind")
	}
	if f.V != FrameVersion {
		return nil, fmt.Errorf("conformance: frame version %d is not the negotiated %d", f.V, FrameVersion)
	}
	return &f, nil
}

// ErrConnClosed is returned once the peer connection is gone.
var ErrConnClosed = errors.New("conformance: connection closed")

// Conn is a black-box relay client: a WebSocket carrying one CBOR frame per
// binary message, with a reader goroutine that demultiplexes responses,
// session chunks and events.
type Conn struct {
	ws  *websocket.Conn
	wmu sync.Mutex

	mu       sync.Mutex
	closed   bool
	nextID   uint64
	pending  map[uint64]chan *Frame
	chunks   map[string]chan *Frame
	events   chan *Frame
	readErr  error
	closeOne sync.Once
	done     chan struct{}

	// Peer is the id the server assigned at hello.
	Peer string
	// Negotiated is HelloOK.caps as returned.
	Negotiated []string
	// Hello is the complete handshake response.
	HelloOK HelloOK
}

// DialOptions selects how a conformance connection presents itself.
type DialOptions struct {
	// Endpoint is the base HTTP URL of the implementation.
	Endpoint string
	// Token is the bearer credential, empty for an unauthenticated
	// standalone.
	Token string
	// Role is "client" or "node".
	Role string
	// Peer is a previously assigned id to reuse, or empty to be assigned one.
	Peer string
	// Caps is the exact capability list to offer. Nil offers only v1.
	Caps []string
	// FrameVersion overrides the version byte of the hello frame so a
	// version-skew requirement can be observed. Zero means FrameVersion.
	FrameVersion uint8
	// SkipHello sends no hello at all, so the "first frame must be hello"
	// rule can be observed.
	SkipHello bool
	// FirstFrame replaces the hello with an arbitrary frame when SkipHello
	// is set.
	FirstFrame *Frame
}

// linkURL turns an http(s) endpoint into the ws(s) /v1/link URL.
func linkURL(endpoint string) string {
	u := strings.TrimSuffix(endpoint, "/")
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	return u + "/v1/link"
}

// Dial opens a connection and performs the handshake unless SkipHello is set.
// A handshake refusal is returned as a *WireError so a check can assert the
// stable code rather than a message.
func Dial(ctx context.Context, opts DialOptions) (*Conn, error) {
	header := http.Header{}
	if opts.Token != "" {
		header.Set("Authorization", "Bearer "+opts.Token)
	}
	ws, _, err := websocket.Dial(ctx, linkURL(opts.Endpoint), &websocket.DialOptions{
		HTTPHeader:      header,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, fmt.Errorf("conformance: dial %s: %w", opts.Endpoint, err)
	}
	ws.SetReadLimit(4 << 20)
	c := &Conn{
		ws:      ws,
		pending: map[uint64]chan *Frame{},
		chunks:  map[string]chan *Frame{},
		events:  make(chan *Frame, 16384),
		done:    make(chan struct{}),
	}
	go c.readLoop()

	first := &Frame{V: FrameVersion, T: KindHello, ID: 1}
	if opts.FrameVersion != 0 {
		first.V = opts.FrameVersion
	}
	if opts.SkipHello {
		if opts.FirstFrame != nil {
			first = opts.FirstFrame
		} else {
			first = &Frame{V: FrameVersion, T: KindReq, ID: 1, To: PeerControl, Op: "ws.list"}
		}
	} else {
		caps := opts.Caps
		if caps == nil {
			caps = []string{CapabilityV1}
		}
		role := opts.Role
		if role == "" {
			role = "client"
		}
		body, err := Marshal(Hello{Peer: opts.Peer, Role: role, Token: opts.Token, Caps: caps})
		if err != nil {
			c.Close()
			return nil, err
		}
		first.Body = body
	}
	res, err := c.roundTrip(ctx, first)
	if err != nil {
		c.Close()
		return nil, err
	}
	if res.Err != nil {
		c.Close()
		return nil, res.Err
	}
	if !opts.SkipHello {
		if err := Unmarshal(res.Body, &c.HelloOK); err != nil {
			c.Close()
			return nil, err
		}
		c.Peer = c.HelloOK.Peer
		c.Negotiated = c.HelloOK.Caps
	}
	return c, nil
}

func (c *Conn) readLoop() {
	defer close(c.done)
	for {
		typ, b, err := c.ws.Read(context.Background())
		if err != nil {
			c.fail(err)
			return
		}
		if typ != websocket.MessageBinary {
			c.fail(errors.New("conformance: non-binary websocket message"))
			return
		}
		f, err := DecodeFrame(b)
		if err != nil {
			c.fail(err)
			return
		}
		c.deliver(f)
	}
}

func (c *Conn) deliver(f *Frame) {
	switch f.T {
	case KindRes:
		c.mu.Lock()
		ch := c.pending[f.ID]
		delete(c.pending, f.ID)
		c.mu.Unlock()
		if ch != nil {
			ch <- f
		}
	case KindChunk:
		c.mu.Lock()
		ch, ok := c.chunks[f.S]
		if !ok {
			ch = make(chan *Frame, 4096)
			c.chunks[f.S] = ch
		}
		c.mu.Unlock()
		select {
		case ch <- f:
		default: // a check that stopped reading must not wedge the reader
		}
	case KindEvent:
		select {
		case c.events <- f:
		default:
		}
	case KindPing:
		_ = c.send(context.Background(), &Frame{V: FrameVersion, T: KindPong, ID: f.ID, To: f.From})
	}
}

func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.closed = true
	pending := c.pending
	c.pending = map[uint64]chan *Frame{}
	c.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
}

func (c *Conn) send(ctx context.Context, f *Frame) error {
	b, err := EncodeFrame(f)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.ws.Write(ctx, websocket.MessageBinary, b)
}

func (c *Conn) roundTrip(ctx context.Context, f *Frame) (*Frame, error) {
	ch := make(chan *Frame, 1)
	c.mu.Lock()
	if c.closed {
		err := c.readErr
		c.mu.Unlock()
		if err == nil {
			err = ErrConnClosed
		}
		return nil, err
	}
	c.pending[f.ID] = ch
	c.mu.Unlock()
	if err := c.send(ctx, f); err != nil {
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case res, ok := <-ch:
		if !ok {
			c.mu.Lock()
			err := c.readErr
			c.mu.Unlock()
			if err == nil {
				err = ErrConnClosed
			}
			return nil, err
		}
		return res, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// nextRequestID returns the next correlation id.
func (c *Conn) nextRequestID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID + 1 // 1 is the hello
}

// Request sends one req frame and decodes the response body into out. A
// protocol-level failure is returned as a *WireError.
func (c *Conn) Request(ctx context.Context, to, op string, body, out any) error {
	f := &Frame{V: FrameVersion, T: KindReq, ID: c.nextRequestID(), To: to, Op: op}
	if body != nil {
		b, err := Marshal(body)
		if err != nil {
			return err
		}
		f.Body = b
	}
	res, err := c.roundTrip(ctx, f)
	if err != nil {
		return err
	}
	if res.Err != nil {
		return res.Err
	}
	if out != nil && len(res.Body) > 0 {
		return Unmarshal(res.Body, out)
	}
	return nil
}

// RequestWS is Request with the frame's ws field set, which node operations
// route on.
func (c *Conn) RequestWS(ctx context.Context, to, op, ws string, body, out any) error {
	f := &Frame{V: FrameVersion, T: KindReq, ID: c.nextRequestID(), To: to, Op: op, WS: ws}
	if body != nil {
		b, err := Marshal(body)
		if err != nil {
			return err
		}
		f.Body = b
	}
	res, err := c.roundTrip(ctx, f)
	if err != nil {
		return err
	}
	if res.Err != nil {
		return res.Err
	}
	if out != nil && len(res.Body) > 0 {
		return Unmarshal(res.Body, out)
	}
	return nil
}

// SendRaw writes an arbitrary frame without waiting for a response. It exists
// so a check can send something a compliant client never would.
func (c *Conn) SendRaw(ctx context.Context, f *Frame) error { return c.send(ctx, f) }

// Chunks returns the delivery channel for one session id, creating it before
// the session is opened so no chunk is missed while the open response is in
// flight (§8 note: the node streams before the response arrives).
func (c *Conn) Chunks(session string) <-chan *Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok := c.chunks[session]
	if !ok {
		ch = make(chan *Frame, 4096)
		c.chunks[session] = ch
	}
	return ch
}

// Events returns the one-way event/offer channel.
func (c *Conn) Events() <-chan *Frame { return c.events }

// Wait blocks until the reader goroutine stops and returns why.
func (c *Conn) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.readErr == nil {
			return ErrConnClosed
		}
		return c.readErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close tears the connection down.
func (c *Conn) Close() {
	c.closeOne.Do(func() { _ = c.ws.CloseNow() })
}

// CollectSession drains a session's chunks until the exit chunk or the
// deadline, and returns them in arrival order.
func (c *Conn) CollectSession(ctx context.Context, session string) ([]*Frame, error) {
	ch := c.Chunks(session)
	var out []*Frame
	for {
		select {
		case f := <-ch:
			out = append(out, f)
			var body ChunkBody
			if err := Unmarshal(f.Body, &body); err == nil && body.St == ChunkExit {
				return out, nil
			}
		case <-ctx.Done():
			return out, ctx.Err()
		}
	}
}

// SortedCopy returns a sorted copy, used where a check compares sets rather
// than sequences.
func SortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// IsCode reports whether err is a wire error carrying exactly code.
func IsCode(err error, code string) bool {
	var we *WireError
	if errors.As(err, &we) {
		return we.Code == code
	}
	return false
}

// CodeOf returns the stable code of a wire error, or "" for anything else.
func CodeOf(err error) string {
	var we *WireError
	if errors.As(err, &we) {
		return we.Code
	}
	return ""
}

// KnownCode reports whether code is one of the §2 stable codes.
func KnownCode(code string) bool {
	for _, c := range StableErrorCodes {
		if c == code {
			return true
		}
	}
	return false
}
