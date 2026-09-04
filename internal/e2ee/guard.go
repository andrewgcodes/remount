package e2ee

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// Config configures one guarded connection.
type Config struct {
	// Policy decides what happens when the far peer cannot negotiate.
	Policy Policy
	// Bind returns this peer's e2ee identity once the peer id is known. A
	// client's id is assigned at hello, so the identity cannot be built
	// before the connection exists; in a deployment this is the call that
	// asks the control plane to sign a binding. It is invoked at most once
	// per connection and its result is reused.
	Bind func(ctx context.Context, self string) (*Identity, error)
	// ControlKey verifies peer bindings. When it is nil the key the control
	// plane returned in HelloOK is used, which is the same ed25519 key nodes
	// already verify grants with.
	ControlKey ed25519.PublicKey
	// HandshakeTimeout bounds one key agreement.
	HandshakeTimeout time.Duration
	// IdleTimeout discards a session that has gone unused, so a peer that
	// disappears without a peer.gone cannot pin key material forever.
	IdleTimeout time.Duration
	// MaxSessions bounds concurrently held peer sessions.
	MaxSessions int
	// MaxOutstanding bounds the request ids remembered so a relay-generated
	// failure for a sealed request can still name the operation it failed.
	MaxOutstanding int
	// MaxAuthFailures closes the connection after this many refused inbound
	// records. A relay that mutates or replays frames does not get to keep
	// the connection.
	MaxAuthFailures int
	// FailureTTL is how long a failed negotiation with one peer is
	// remembered, so every frame does not pay the handshake timeout.
	FailureTTL time.Duration
	// Now and Observe are test seams.
	Now     func() time.Time
	Observe func(Event)
}

func (c Config) handshakeTimeout() time.Duration {
	if c.HandshakeTimeout > 0 {
		return c.HandshakeTimeout
	}
	return defaultHandshakeTimeout
}

func (c Config) idleTimeout() time.Duration {
	if c.IdleTimeout > 0 {
		return c.IdleTimeout
	}
	return defaultIdleTimeout
}

func (c Config) maxSessions() int {
	if c.MaxSessions > 0 {
		return c.MaxSessions
	}
	return defaultMaxSessions
}

func (c Config) maxOutstanding() int {
	if c.MaxOutstanding > 0 {
		return c.MaxOutstanding
	}
	return defaultMaxOutstanding
}

func (c Config) maxAuthFailures() int {
	if c.MaxAuthFailures > 0 {
		return c.MaxAuthFailures
	}
	return defaultMaxAuthFailures
}

func (c Config) failureTTL() time.Duration {
	if c.FailureTTL > 0 {
		return c.FailureTTL
	}
	return defaultFailureTTL
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Guard is a transport.Conn that seals peer-to-peer payloads. Frames to and
// from the control plane pass through untouched: the control plane is the
// party that must read them, and hiding them would only break the protocol
// without hiding anything from the relay, which is the same process.
type Guard struct {
	conn transport.Conn
	cfg  Config

	helloDone chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	// idMu serializes identity resolution so one connection performs at most
	// one binding round trip even when several sends race.
	idMu        sync.Mutex
	identity    *Identity
	identityErr error

	mu         sync.Mutex
	helloID    uint64
	helloSent  bool
	helloSeen  bool
	self       string
	caps       []string
	controlKey ed25519.PublicKey
	sessions   map[string]*Session
	offers     map[string]*pendingOffer
	failed     map[string]time.Time
	ops        *opRing
	failures   int
}

// Wrap returns a Conn that seals peer-addressed payloads on cfg's terms. The
// returned Conn owns conn and closes it.
func Wrap(conn transport.Conn, cfg Config) *Guard {
	return &Guard{
		conn: conn, cfg: cfg,
		helloDone: make(chan struct{}), closed: make(chan struct{}),
		sessions: map[string]*Session{}, offers: map[string]*pendingOffer{},
		failed: map[string]time.Time{}, ops: newOpRing(cfg.maxOutstanding()),
	}
}

// Dialer wraps d so every connection it opens is guarded on cfg's terms. This
// is how a peer opts in without changing anything about how it speaks the
// protocol.
func Dialer(d transport.Dialer, cfg Config) transport.Dialer {
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		conn, err := d.Dial(ctx)
		if err != nil {
			return nil, err
		}
		return Wrap(conn, cfg), nil
	})
}

// Self is the peer id the control plane assigned this connection, once hello
// has completed.
func (g *Guard) Self() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.self
}

// Negotiated is the capability list the control plane echoed at hello.
func (g *Guard) Negotiated() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.caps...)
}

// Session returns the live session with peer, or nil. Tests use it to observe
// that a reconnect produced a different key agreement.
func (g *Guard) Session(peer string) *Session {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sessions[peer]
}

func (g *Guard) observe(kind EventKind, peer, session, reason string) {
	if g.cfg.Observe != nil {
		g.cfg.Observe(Event{Kind: kind, Peer: peer, Session: session, Reason: reason})
	}
}

// Close tears down the connection and wakes every waiting handshake.
func (g *Guard) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })
	return g.conn.Close()
}

// ---------------------------------------------------------------------------
// outbound
// ---------------------------------------------------------------------------

// Send seals f when it is an operation payload for another peer.
func (g *Guard) Send(ctx context.Context, f *proto.Frame) error {
	if f.T == proto.KindHello {
		g.mu.Lock()
		g.helloID, g.helloSent = f.ID, true
		g.mu.Unlock()
		return g.conn.Send(ctx, f)
	}
	if !peerAddressed(f.To) || f.T == proto.KindPing || f.T == proto.KindPong {
		// Control-plane traffic and liveness probes carry no operation
		// payload the relay could read that it does not already route on.
		return g.conn.Send(ctx, f)
	}
	if g.cfg.Policy == PolicyDisabled {
		return g.conn.Send(ctx, f)
	}
	session, err := g.sessionFor(ctx, f.To)
	if err != nil {
		if isContextError(err) {
			// The caller went away mid-negotiation. Nothing was transmitted,
			// and the connection is still healthy.
			return fmt.Errorf("%w: %w", err, transport.ErrNotSent)
		}
		if g.cfg.Policy == PolicyRequired {
			// Fail closed, and fail before the frame reaches the wire: the
			// destination cannot have applied an operation it never saw.
			// ErrNotSent says exactly that, and keeps one refused operation
			// from tearing down a connection every other caller is using.
			g.observe(EventRefused, f.To, "", err.Error())
			return fmt.Errorf("%w: %w", proto.Err(proto.CodeUnsupported,
				"e2ee: %s requires end-to-end encryption and peer %s did not negotiate it: %v", g.Self(), f.To, err), transport.ErrNotSent)
		}
		g.observe(EventFallback, f.To, "", err.Error())
		return g.conn.Send(ctx, f)
	}
	sealed, err := session.Seal(f, g.cfg.now())
	if err != nil {
		return err
	}
	if f.T == proto.KindReq {
		// A relay-generated failure for this request arrives in plaintext and
		// names only the marker op, so remember what was asked.
		g.mu.Lock()
		g.ops.put(f.ID, f.Op)
		g.mu.Unlock()
	}
	return g.conn.Send(ctx, sealed)
}

// sessionFor returns the session for peer, negotiating one if needed. It
// never holds the guard mutex across a key agreement or a network write.
func (g *Guard) sessionFor(ctx context.Context, peer string) (*Session, error) {
	if err := g.waitHello(ctx); err != nil {
		return nil, err
	}
	now := g.cfg.now()
	g.mu.Lock()
	dropped := g.sweepLocked(now)
	if session := g.sessions[peer]; session != nil {
		g.mu.Unlock()
		g.emit(dropped)
		return session, nil
	}
	if until, ok := g.failed[peer]; ok && now.Before(until) {
		g.mu.Unlock()
		g.emit(dropped)
		return nil, ErrNotNegotiated
	}
	if offer := g.offers[peer]; offer != nil {
		g.mu.Unlock()
		g.emit(dropped)
		return g.await(ctx, offer)
	}
	g.mu.Unlock()
	g.emit(dropped)

	identity, err := g.resolveIdentity(ctx)
	if err != nil {
		return nil, err
	}
	self := g.Self()
	offer, kx, err := buildOffer(self, peer, identity, now)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	if session := g.sessions[peer]; session != nil { // installed while we were building
		g.mu.Unlock()
		return session, nil
	}
	if existing := g.offers[peer]; existing != nil {
		g.mu.Unlock()
		return g.await(ctx, existing)
	}
	g.offers[peer] = offer
	g.mu.Unlock()
	if err := g.sendKX(ctx, peer, kx); err != nil {
		g.resolveOffer(offer, nil, err)
		return nil, err
	}
	return g.await(ctx, offer)
}

// await blocks until the key agreement finishes, the handshake bound expires,
// the caller gives up or the connection closes.
func (g *Guard) await(ctx context.Context, offer *pendingOffer) (*Session, error) {
	timer := time.NewTimer(g.cfg.handshakeTimeout())
	defer timer.Stop()
	select {
	case <-offer.done:
		return offer.session, offer.err
	case <-timer.C:
		g.resolveOffer(offer, nil, ErrNotNegotiated)
		return offer.session, offer.err
	case <-g.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// resolveOffer completes a pending offer exactly once and wakes its waiters.
// A failed negotiation is remembered briefly so the next frame to the same
// peer does not pay the handshake bound again.
func (g *Guard) resolveOffer(offer *pendingOffer, session *Session, err error) {
	g.mu.Lock()
	if g.offers[offer.peer] == offer {
		delete(g.offers, offer.peer)
	}
	select {
	case <-offer.done:
		g.mu.Unlock()
		return
	default:
	}
	offer.session, offer.err = session, err
	if err != nil {
		g.failed[offer.peer] = g.cfg.now().Add(g.cfg.failureTTL())
	} else {
		delete(g.failed, offer.peer)
	}
	close(offer.done)
	g.mu.Unlock()
}

func (g *Guard) resolveIdentity(ctx context.Context) (*Identity, error) {
	g.idMu.Lock()
	defer g.idMu.Unlock()
	if g.identity != nil || g.identityErr != nil {
		return g.identity, g.identityErr
	}
	if g.cfg.Bind == nil {
		g.identityErr = proto.Err(proto.CodeUnsupported, "e2ee: no identity source configured")
		return nil, g.identityErr
	}
	bctx, cancel := context.WithTimeout(ctx, g.cfg.handshakeTimeout())
	defer cancel()
	identity, err := g.cfg.Bind(bctx, g.Self())
	if err != nil {
		// A failed binding is not cached as permanent: the control plane may
		// simply have been unreachable, and a connection that outlives that
		// must be able to encrypt again.
		return nil, err
	}
	if identity == nil || len(identity.Key) != ed25519.PrivateKeySize {
		g.identityErr = proto.Err(proto.CodeUnsupported, "e2ee: identity source returned no usable key")
		return nil, g.identityErr
	}
	g.identity = identity
	return identity, nil
}

// waitHello blocks until the peer id is known, because every transcript and
// every AAD names it.
func (g *Guard) waitHello(ctx context.Context) error {
	select {
	case <-g.helloDone:
		return nil
	default:
	}
	timer := time.NewTimer(g.cfg.handshakeTimeout())
	defer timer.Stop()
	select {
	case <-g.helloDone:
		return nil
	case <-timer.C:
		return proto.Err(proto.CodeTimeout, "e2ee: hello has not completed")
	case <-g.closed:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendKX writes one key-exchange frame under a bound of its own, so a stalled
// write can never hold the read loop that produced it.
func (g *Guard) sendKX(ctx context.Context, to string, kx *proto.E2EEKeyExchange) error {
	sctx, cancel := context.WithTimeout(ctx, g.cfg.handshakeTimeout())
	defer cancel()
	return g.conn.Send(sctx, proto.NewEvent(to, proto.OpE2EEKeyExchange, kx))
}

// ---------------------------------------------------------------------------
// inbound
// ---------------------------------------------------------------------------

// Recv returns the next frame the peer above this guard should see. Key
// exchange frames and records that fail authentication are consumed here.
func (g *Guard) Recv(ctx context.Context) (*proto.Frame, error) {
	for {
		f, err := g.conn.Recv(ctx)
		if err != nil {
			return nil, err
		}
		out, deliver := g.inbound(ctx, f)
		if deliver {
			return out, nil
		}
	}
}

func (g *Guard) inbound(ctx context.Context, f *proto.Frame) (*proto.Frame, bool) {
	if g.noteHelloOK(f) {
		return f, true
	}
	if g.cfg.Policy == PolicyDisabled {
		// A disabled guard is transparent. It still swallows this layer's own
		// key-exchange frames, because they are addressed to the layer rather
		// than to the peer above it, and a peer built before this capability
		// would ignore them anyway.
		return f, f.Op != proto.OpE2EEKeyExchange
	}
	switch {
	case f.T == proto.KindEvent && f.Op == proto.OpE2EEKeyExchange:
		g.handleKX(ctx, f)
		return nil, false
	case f.Op == proto.OpE2EESealed:
		return g.openSealed(ctx, f)
	}
	if f.T == proto.KindEvent && f.Op == proto.EvPeerGone {
		g.forgetGonePeer(f)
		return f, true
	}
	if peerAddressed(f.From) && f.T != proto.KindPing && f.T != proto.KindPong && g.cfg.Policy == PolicyRequired {
		// Fail closed in both directions: under a required policy a peer
		// payload that arrived in the clear is refused, not accepted because
		// it happens to parse.
		g.observe(EventDropped, f.From, "", "plaintext payload under a required policy")
		g.countFailure()
		return nil, false
	}
	return f, true
}

// noteHelloOK captures the peer id, the negotiated capabilities and the
// control plane's key from the response to our own hello.
func (g *Guard) noteHelloOK(f *proto.Frame) bool {
	g.mu.Lock()
	if !g.helloSent || g.helloSeen || f.T != proto.KindRes || f.ID != g.helloID {
		g.mu.Unlock()
		return false
	}
	g.helloSeen = true
	g.mu.Unlock()
	var ok proto.HelloOK
	if f.Err == nil {
		if err := f.Decode(&ok); err != nil {
			ok = proto.HelloOK{}
		}
	}
	g.mu.Lock()
	g.self, g.caps = ok.Peer, append([]string(nil), ok.Caps...)
	if g.controlKey = g.cfg.ControlKey; len(g.controlKey) == 0 {
		g.controlKey = ed25519.PublicKey(ok.PubKey)
	}
	g.mu.Unlock()
	if ok.Peer != "" {
		close(g.helloDone)
	}
	return true
}

// openSealed authenticates one record. Every refusal is silent to the sender
// and counted here: a relay that mutates, replays or redirects frames spends
// its budget and loses the connection.
func (g *Guard) openSealed(ctx context.Context, f *proto.Frame) (*proto.Frame, bool) {
	if f.T == proto.KindRes && f.Err != nil && !peerAddressed(f.From) {
		// The relay could not deliver a sealed request. It is a routing
		// report, not an operation result, so the operation name is restored
		// from our own record and the relay's message is discarded rather
		// than handed to the caller.
		g.mu.Lock()
		op, ok := g.ops.take(f.ID)
		g.mu.Unlock()
		if !ok {
			return nil, false
		}
		return &proto.Frame{V: proto.Version, T: proto.KindRes, ID: f.ID, From: f.From, To: f.To, Op: op,
			ControllerEpoch: f.ControllerEpoch,
			Err:             proto.Err(f.Err.Code, "relay reported the destination unreachable")}, true
	}
	session := g.Session(f.From)
	if session == nil {
		g.observe(EventDropped, f.From, "", "no session")
		g.hintRestart(ctx, f)
		g.countFailure()
		return nil, false
	}
	out, err := session.Open(f, g.cfg.now())
	if err != nil {
		g.observe(EventDropped, f.From, session.ID(), err.Error())
		if errors.Is(err, ErrUnknownSession) {
			g.hintRestart(ctx, f)
		}
		g.countFailure()
		return nil, false
	}
	if out.T == proto.KindRes {
		g.mu.Lock()
		g.ops.take(out.ID)
		g.mu.Unlock()
	}
	return out, true
}

// hintRestart tells a peer that sealed under a key we do not hold to
// negotiate again. It is unauthenticated and purely a liveness aid; the
// receiver bounds how often it acts on one.
func (g *Guard) hintRestart(ctx context.Context, f *proto.Frame) {
	if !peerAddressed(f.From) {
		return
	}
	var sealed proto.E2EESealed
	if err := f.Decode(&sealed); err != nil || len(sealed.Session) != sidLen {
		return
	}
	_ = g.sendKX(ctx, f.From, &proto.E2EEKeyExchange{
		Type: proto.E2EERestart, Session: sealed.Session, From: g.Self(), To: f.From,
		Reason: "no key for that session",
	})
}

// countFailure closes the connection once a peer has spent its budget of
// refused records.
func (g *Guard) countFailure() {
	g.mu.Lock()
	g.failures++
	spent := g.failures >= g.cfg.maxAuthFailures()
	g.mu.Unlock()
	if spent {
		_ = g.Close()
	}
}

// forgetGonePeer drops the keys agreed with a peer the control plane says is
// gone. Its next connection negotiates fresh keys, and nothing is retained
// for a peer that will never use it.
func (g *Guard) forgetGonePeer(f *proto.Frame) {
	var body map[string]string
	if err := f.Decode(&body); err != nil {
		return
	}
	peer := body["peer"]
	if peer == "" {
		return
	}
	g.mu.Lock()
	session := g.sessions[peer]
	delete(g.sessions, peer)
	delete(g.failed, peer)
	g.mu.Unlock()
	if session != nil {
		g.observe(EventDiscarded, peer, session.ID(), "peer gone")
	}
}

// ---------------------------------------------------------------------------
// key exchange
// ---------------------------------------------------------------------------

func (g *Guard) handleKX(ctx context.Context, f *proto.Frame) {
	var kx proto.E2EEKeyExchange
	if err := f.Decode(&kx); err != nil || !peerAddressed(f.From) {
		return
	}
	switch kx.Type {
	case proto.E2EEOffer:
		g.handleOffer(ctx, f.From, &kx)
	case proto.E2EEAccept:
		g.handleAccept(f.From, &kx)
	case proto.E2EEReject:
		g.mu.Lock()
		offer := g.offers[f.From]
		g.mu.Unlock()
		if offer != nil && sameSID(offer, kx.Session) {
			g.resolveOffer(offer, nil, ErrNotNegotiated)
		}
	case proto.E2EERestart:
		g.handleRestart(f.From, &kx)
	}
}

func (g *Guard) handleOffer(ctx context.Context, from string, kx *proto.E2EEKeyExchange) {
	self := g.Self()
	if self == "" {
		return
	}
	g.mu.Lock()
	pending := g.offers[from]
	existing := g.sessions[from]
	g.mu.Unlock()
	// Two peers that need each other at the same instant both offer. The
	// smaller peer id wins so both sides converge on one session without a
	// round trip: the larger id abandons its own offer and answers.
	if pending != nil && self < from {
		_ = g.sendKX(ctx, from, &proto.E2EEKeyExchange{Type: proto.E2EEReject, Session: kx.Session, From: self, To: from, Reason: "crossed offers"})
		return
	}
	// An offer may replace a session, which is how a reconnecting peer
	// rekeys. It may not repeat or predate the session it would replace,
	// which is what stops a captured offer from displacing a live key.
	if existing != nil && (existing.ID() == hexSID(kx.Session) || kx.TS < existing.TS) {
		_ = g.sendKX(ctx, from, &proto.E2EEKeyExchange{Type: proto.E2EEReject, Session: kx.Session, From: self, To: from, Reason: "stale offer"})
		return
	}
	identity, err := g.resolveIdentity(ctx)
	if err != nil {
		_ = g.sendKX(ctx, from, &proto.E2EEKeyExchange{Type: proto.E2EEReject, Session: kx.Session, From: self, To: from, Reason: "no identity"})
		return
	}
	g.mu.Lock()
	controlKey := g.controlKey
	g.mu.Unlock()
	session, reply, err := acceptOffer(self, identity, controlKey, kx, from, g.cfg.now())
	if err != nil {
		g.observe(EventDropped, from, hexSID(kx.Session), err.Error())
		g.countFailure()
		_ = g.sendKX(ctx, from, &proto.E2EEKeyExchange{Type: proto.E2EEReject, Session: kx.Session, From: self, To: from, Reason: "offer refused"})
		return
	}
	if err := g.sendKX(ctx, from, reply); err != nil {
		return
	}
	g.install(session, existing != nil)
	if pending != nil {
		// We abandoned our own offer for theirs; whoever was waiting on it
		// gets the session that was actually established.
		g.resolveOffer(pending, session, nil)
	}
}

func (g *Guard) handleAccept(from string, kx *proto.E2EEKeyExchange) {
	g.mu.Lock()
	offer := g.offers[from]
	existing := g.sessions[from]
	controlKey := g.controlKey
	g.mu.Unlock()
	if offer == nil || !sameSID(offer, kx.Session) {
		return // not ours, or already resolved
	}
	identity, err := g.resolveIdentity(context.Background())
	if err != nil {
		g.resolveOffer(offer, nil, err)
		return
	}
	session, err := completeOffer(offer, g.Self(), identity, controlKey, kx, from, g.cfg.now())
	if err != nil {
		g.observe(EventDropped, from, hexSID(kx.Session), err.Error())
		g.countFailure()
		g.resolveOffer(offer, nil, err)
		return
	}
	g.install(session, existing != nil)
	g.resolveOffer(offer, session, nil)
}

// handleRestart acts on a peer that says it holds no key for our session.
func (g *Guard) handleRestart(from string, kx *proto.E2EEKeyExchange) {
	g.mu.Lock()
	session := g.sessions[from]
	g.mu.Unlock()
	if session == nil || session.ID() != hexSID(kx.Session) || !session.used() || !session.allowRestart() {
		return
	}
	g.mu.Lock()
	if g.sessions[from] == session {
		delete(g.sessions, from)
		delete(g.failed, from)
	}
	g.mu.Unlock()
	g.observe(EventDiscarded, from, session.ID(), "peer restarted")
}

// install publishes a session, evicting the least recently used one if the
// bound is reached.
func (g *Guard) install(session *Session, replaced bool) {
	now := g.cfg.now()
	g.mu.Lock()
	dropped := g.sweepLocked(now)
	if _, exists := g.sessions[session.Peer]; !exists && len(g.sessions) >= g.cfg.maxSessions() {
		dropped = append(dropped, g.evictOldestLocked()...)
	}
	g.sessions[session.Peer] = session
	delete(g.failed, session.Peer)
	g.mu.Unlock()
	g.emit(dropped)
	kind := EventEstablished
	if replaced {
		kind = EventReplaced
	}
	g.observe(kind, session.Peer, session.ID(), "")
}

// sweepLocked discards sessions no one has used and returns what it dropped
// so the caller can report it without holding the mutex. It runs on every
// lookup; the map is bounded by maxSessions, so the scan is bounded too.
func (g *Guard) sweepLocked(now time.Time) []Event {
	var dropped []Event
	idle := g.cfg.idleTimeout()
	for peer, session := range g.sessions {
		if session.idle(now, idle) {
			delete(g.sessions, peer)
			dropped = append(dropped, Event{Kind: EventDiscarded, Peer: peer, Session: session.ID(), Reason: "idle"})
		}
	}
	for peer, until := range g.failed {
		if now.After(until) {
			delete(g.failed, peer)
		}
	}
	return dropped
}

// evictOldestLocked drops the least recently used session so the bound on
// held key material is a bound, not a target.
func (g *Guard) evictOldestLocked() []Event {
	var oldestPeer string
	var oldest *Session
	for peer, session := range g.sessions {
		if oldest == nil || session.lastUse().Before(oldest.lastUse()) {
			oldestPeer, oldest = peer, session
		}
	}
	if oldest == nil {
		return nil
	}
	delete(g.sessions, oldestPeer)
	return []Event{{Kind: EventDiscarded, Peer: oldestPeer, Session: oldest.ID(), Reason: "session bound reached"}}
}

// emit reports collected events after the mutex is released. Observe is a
// diagnostic seam and must never re-enter the guard.
func (g *Guard) emit(events []Event) {
	if g.cfg.Observe == nil {
		return
	}
	for _, e := range events {
		g.cfg.Observe(e)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// isContextError reports whether the caller went away rather than the peer
// being unable to negotiate. Either way the frame was not transmitted.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// peerAddressed reports whether an id names another peer rather than the
// control plane. Only those frames are sealed.
func peerAddressed(id string) bool { return id != "" && id != proto.PeerControl }

func sameSID(offer *pendingOffer, sid []byte) bool {
	return len(sid) == sidLen && string(sid) == string(offer.sid[:])
}

func hexSID(sid []byte) string {
	var fixed [sidLen]byte
	if len(sid) != sidLen {
		return ""
	}
	copy(fixed[:], sid)
	return (&Session{SID: fixed}).ID()
}

// opRing remembers the operation of an in-flight sealed request so a
// relay-generated failure can be reported as a failure of that operation. It
// is a fixed-size ring: a peer that never answers cannot grow it.
type opRing struct {
	max  int
	ids  []uint64
	head int
	byID map[uint64]string
}

func newOpRing(max int) *opRing {
	return &opRing{max: max, byID: make(map[uint64]string, max)}
}

func (r *opRing) put(id uint64, op string) {
	if _, exists := r.byID[id]; exists {
		r.byID[id] = op
		return
	}
	if len(r.ids) < r.max {
		r.ids = append(r.ids, id)
	} else {
		delete(r.byID, r.ids[r.head])
		r.ids[r.head] = id
		r.head = (r.head + 1) % r.max
	}
	r.byID[id] = op
}

func (r *opRing) take(id uint64) (string, bool) {
	op, ok := r.byID[id]
	if ok {
		delete(r.byID, id)
	}
	return op, ok
}
