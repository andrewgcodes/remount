package server

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// clientPool turns HTTP bearer credentials into in-process SDK clients. The
// HTTP API is a client of the relay like any other: every request it makes
// carries the caller's own identity through the same hello, the same
// authorizer and the same idempotency records the CLI uses, so there is no
// second authorization path to keep in step. One client is kept per
// credential and reused across requests; an idle one is closed after
// idleAfter, and the pool holds at most max of them.
type clientPool struct {
	srv       *Server
	max       int
	idleAfter time.Duration

	mu      sync.Mutex
	entries map[string]*poolEntry
	lru     *list.List // least recently released at the front
	closed  bool
}

type poolEntry struct {
	key      string
	client   *client.Client
	refs     int
	lastUsed time.Time
	elem     *list.Element
	// ready is closed once the first Connect has settled; err records its
	// outcome so concurrent first users share one hello.
	ready chan struct{}
	err   error
}

func newClientPool(srv *Server, max int, idleAfter time.Duration) *clientPool {
	return &clientPool{srv: srv, max: max, idleAfter: idleAfter, entries: map[string]*poolEntry{}, lru: list.New()}
}

func credentialKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// acquire returns the client for token, connecting (and so authenticating)
// it on first use. The caller must call release when its request is done;
// a client with outstanding references is never evicted.
func (p *clientPool) acquire(ctx context.Context, token string) (*client.Client, func(), error) {
	key := credentialKey(token)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, proto.Err(proto.CodeClosed, "server is shutting down")
	}
	e := p.entries[key]
	first := e == nil
	if first {
		if len(p.entries) >= p.max && !p.evictIdleLocked() {
			p.mu.Unlock()
			metrics.HTTPClientsRejected.Inc()
			return nil, nil, proto.Err(proto.CodeResourceExhausted, "too many distinct API credentials in use")
		}
		e = &poolEntry{key: key, ready: make(chan struct{})}
		e.client = client.New(client.Options{
			Dialer:    transport.DialFunc(p.dial),
			Token:     token,
			Principal: "http",
		})
		p.entries[key] = e
		metrics.HTTPClients.Set(int64(len(p.entries)))
	}
	e.refs++
	if e.elem != nil {
		p.lru.Remove(e.elem)
		e.elem = nil
	}
	p.mu.Unlock()
	if first {
		hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := e.client.Connect(hctx)
		cancel()
		e.err = err
		close(e.ready)
	} else {
		select {
		case <-e.ready:
		case <-ctx.Done():
			p.release(e)
			return nil, nil, ctx.Err()
		}
	}
	if e.err != nil {
		p.mu.Lock()
		// A credential that failed its hello is not cached: the next attempt
		// dials again and sees the current answer.
		if p.entries[key] == e {
			delete(p.entries, key)
			metrics.HTTPClients.Set(int64(len(p.entries)))
		}
		e.refs--
		last := e.refs <= 0
		p.mu.Unlock()
		if last {
			_ = e.client.Close()
		}
		return nil, nil, e.err
	}
	return e.client, func() { p.release(e) }, nil
}

func (p *clientPool) release(e *poolEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e.refs--
	e.lastUsed = time.Now()
	if e.refs == 0 && p.entries[e.key] == e && e.elem == nil {
		e.elem = p.lru.PushBack(e)
	}
}

// evictIdleLocked closes the least recently used unreferenced client to make
// room. It reports false when every client is in use.
func (p *clientPool) evictIdleLocked() bool {
	front := p.lru.Front()
	if front == nil {
		return false
	}
	e := front.Value.(*poolEntry)
	p.removeLocked(e)
	go func() { _ = e.client.Close() }()
	return true
}

func (p *clientPool) removeLocked(e *poolEntry) {
	if e.elem != nil {
		p.lru.Remove(e.elem)
		e.elem = nil
	}
	delete(p.entries, e.key)
	metrics.HTTPClients.Set(int64(len(p.entries)))
}

// sweep closes clients idle for longer than idleAfter.
func (p *clientPool) sweep(now time.Time) int {
	p.mu.Lock()
	var stale []*poolEntry
	for el := p.lru.Front(); el != nil; {
		next := el.Next()
		e := el.Value.(*poolEntry)
		if now.Sub(e.lastUsed) >= p.idleAfter {
			p.removeLocked(e)
			stale = append(stale, e)
		}
		el = next
	}
	p.mu.Unlock()
	for _, e := range stale {
		_ = e.client.Close()
	}
	return len(stale)
}

// close disconnects every client. Requests in flight fail with closed.
func (p *clientPool) close() {
	p.mu.Lock()
	p.closed = true
	all := make([]*poolEntry, 0, len(p.entries))
	for _, e := range p.entries {
		all = append(all, e)
	}
	p.entries = map[string]*poolEntry{}
	p.lru.Init()
	p.mu.Unlock()
	for _, e := range all {
		_ = e.client.Close()
	}
	metrics.HTTPClients.Set(0)
}

// dial connects a pooled client to the relay over an in-memory pipe. The
// server side is served for the server's lifetime, not the request's: the
// client outlives any one HTTP call.
func (p *clientPool) dial(context.Context) (transport.Conn, error) {
	p.srv.mu.RLock()
	closed := p.srv.closed
	ctx := p.srv.lifetime
	p.srv.mu.RUnlock()
	if closed {
		return nil, errors.New("server: closed")
	}
	a, b := transport.Pipe(256)
	go func() { _ = p.srv.AcceptConn(ctx, b) }()
	return a, nil
}
