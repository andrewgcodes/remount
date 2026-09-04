// Package shim is a deliberately broken implementation of the Remount
// protocol, used to prove that the conformance suite fails things.
//
// A conformance suite that has never failed anything is indistinguishable
// from one that asserts nothing. This package is the counter-evidence: it
// implements just enough of spec/PROTOCOL.md for one requirement in each of
// the nine semantic categories to hold, and then offers a switch that
// violates exactly one of them at a time. The suite must catch each
// violation, in its own category, and must not catch anything else.
//
// It is not a Remount implementation and must never be used as one. It has no
// authentication, no persistence, no isolation and no security properties
// whatsoever; every value it returns is invented in memory.
package shim

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/fxamacker/cbor/v2"
)

// Defect names the one semantic rule a shim instance breaks.
type Defect string

// The defects, one per semantic category of Plan B §12.1, plus the two the
// version-skew scenario needs.
const (
	// DefectNone is a shim that holds every rule it implements.
	DefectNone Defect = ""
	// DefectNegotiation echoes back capability identifiers it does not
	// implement, and accepts a hello that never offered the v1 baseline.
	DefectNegotiation Defect = "negotiation"
	// DefectWorkspace ignores the idempotency key, so a retry creates a
	// second workspace.
	DefectWorkspace Defect = "workspace"
	// DefectAuthority accepts a grant bound to a generation the workspace no
	// longer has.
	DefectAuthority Defect = "authority"
	// DefectSession applies an input whose iseq was already applied.
	DefectSession Defect = "session"
	// DefectStdout produces a structurally perfect log that carries nothing the
	// process wrote: seq 0 is the info chunk, sequences are dense, the exit
	// chunk is last, and a replay is byte-identical to the live tail because
	// both are empty. Every session row except CONF-SESS-009 holds for it.
	DefectStdout Defect = "stdout"
	// DefectSnapshot returns a fresh artifact id for an unchanged tree.
	DefectSnapshot Defect = "snapshot"
	// DefectBinding forwards a placeholder aimed at a host its binding does
	// not cover.
	DefectBinding Defect = "binding"
	// DefectApproval reports settled approvals as pending.
	DefectApproval Defect = "approval"
	// DefectAgent lets the inbox grow without limit.
	DefectAgent Defect = "agent"
	// DefectEvent assigns a sequence that goes backwards.
	DefectEvent Defect = "event"
	// DefectVersion accepts a frame carrying a version it never negotiated.
	DefectVersion Defect = "version"
	// DefectVersionMutates refuses that frame, but only after it has already
	// changed durable state. It exists to prove the version requirement's
	// ordering claim is load-bearing rather than decorative.
	DefectVersionMutates Defect = "version-mutates"
)

// Defects lists every defect a caller may install, excluding DefectNone.
var Defects = []Defect{
	DefectNegotiation, DefectWorkspace, DefectAuthority, DefectSession,
	DefectStdout, DefectSnapshot, DefectBinding, DefectApproval, DefectAgent,
	DefectEvent, DefectVersion, DefectVersionMutates,
}

// CategoryOf names the semantic category each defect breaks, as the string
// the conformance manifest uses. DefectVersion and DefectVersionMutates break
// negotiation, which is where the version rule lives.
func CategoryOf(d Defect) string {
	switch d {
	case DefectNegotiation, DefectVersion, DefectVersionMutates:
		return "negotiation"
	case DefectStdout:
		return "session"
	default:
		return string(d)
	}
}

// BindingID, Placeholder, Secret and BoundHost describe the one binding the
// shim pretends to hold. A conformance target must be told the same values.
const (
	BindingID   = "b_conformance"
	Placeholder = "remount-placeholder-b_conformance"
	Secret      = "conformance-canary-2b7f4a1c9e5d0000"
	BoundHost   = "bound.conformance.invalid"
	UnboundHost = "unbound.conformance.invalid"
	// NodeID is the peer the shim answers node operations as.
	NodeID = "n_shim0000000000000000000000"
)

// Server is one running shim.
type Server struct {
	defect Defect
	http   *httptest.Server

	mu         sync.Mutex
	seq        uint64
	brokeSeq   bool
	events     []event
	workspaces map[string]*workspace
	idem       map[string]string
	agents     map[string]*agent
	sessions   map[string]*session
	artifacts  map[string][]byte
	trees      map[string]map[string][]byte
	approvals  []approval
	ids        atomic.Uint64
}

type workspace struct {
	ID    string
	State string
	Node  string
	Gen   uint64
	Spec  map[string]any
	Files map[string][]byte
	Made  int64
}

type agent struct {
	ID     string
	WS     string
	Status string
	Mode   string
	Inbox  []map[string]any
	Made   int64
}

type session struct {
	ID       string
	WS       string
	Seq      uint64
	LastISeq uint64
	Log      []logged
	Exit     *int
	conn     *websocket.Conn
	writeMu  *sync.Mutex
}

// logged is one retained chunk, which is what makes replay possible.
type logged struct {
	Seq  uint64
	St   uint8
	Data []byte
}

type event struct {
	Seq       uint64
	At        int64
	Stream    string
	Workspace string
	Type      string
	Payload   []byte
}

type approval struct {
	ID     string
	Kind   string
	Status string
	Made   int64
}

// Start launches a shim with the named defect on a loopback port.
func Start(d Defect) *Server {
	s := &Server{
		defect:     d,
		workspaces: map[string]*workspace{},
		idem:       map[string]string{},
		agents:     map[string]*agent{},
		sessions:   map[string]*session{},
		artifacts:  map[string][]byte{},
		trees:      map[string]map[string][]byte{},
	}
	s.approvals = []approval{
		{ID: "ap_shim_pending", Kind: "egress", Status: "pending", Made: now()},
		{ID: "ap_shim_decided", Kind: "egress", Status: "decided", Made: now()},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "serving": true, "security_mode": "shim", "security_ready": true})
	})
	mux.HandleFunc("/v1/link", s.handleLink)
	mux.HandleFunc("/broker/", s.handleBroker)
	mux.HandleFunc("/v1/artifacts/", s.handleArtifact)
	mux.HandleFunc("/v1/events", s.handleEventPost)
	s.http = httptest.NewServer(mux)
	s.append("node.online", NodeID, "", nil)
	return s
}

// URL is the endpoint a conformance target points at.
func (s *Server) URL() string { return s.http.URL }

// Close stops the shim.
func (s *Server) Close() { s.http.Close() }

// ---- frame plumbing ------------------------------------------------------

type frame struct {
	V    uint8      `cbor:"v"`
	T    string     `cbor:"t"`
	ID   uint64     `cbor:"id,omitempty"`
	Seq  uint64     `cbor:"seq,omitempty"`
	S    string     `cbor:"s,omitempty"`
	WS   string     `cbor:"ws,omitempty"`
	To   string     `cbor:"to,omitempty"`
	From string     `cbor:"from,omitempty"`
	Op   string     `cbor:"op,omitempty"`
	Body []byte     `cbor:"body,omitempty"`
	Err  *wireError `cbor:"err,omitempty"`
}

type wireError struct {
	Code   string `cbor:"code"`
	Msg    string `cbor:"msg,omitempty"`
	Oldest uint64 `cbor:"oldest,omitempty"`
}

func (e *wireError) Error() string { return e.Code + ": " + e.Msg }

func errf(code, format string, a ...any) *wireError {
	return &wireError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

var (
	enc cbor.EncMode
	dec cbor.DecMode
)

func init() {
	var err error
	if enc, err = cbor.CoreDetEncOptions().EncMode(); err != nil {
		panic(err)
	}
	if dec, err = (cbor.DecOptions{}).DecMode(); err != nil {
		panic(err)
	}
}

// implemented is what an honest shim offers at hello: the baseline plus two
// of §3.1's named capabilities.
var implemented = []string{"v1", "authz-push", "session-cap"}

func (s *Server) handleLink(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled, InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	c.SetReadLimit(4 << 20)
	defer func() { _ = c.CloseNow() }()
	ctx := r.Context()
	writeMu := &sync.Mutex{}

	typ, b, err := c.Read(ctx)
	if err != nil || typ != websocket.MessageBinary {
		return
	}
	var first frame
	if err := dec.Unmarshal(b, &first); err != nil {
		return
	}
	if !s.acceptVersion(first.V) {
		// §2 rule 1: fail closed before the kind or body is interpreted. The
		// mutating defect does the opposite, on purpose.
		if s.defect == DefectVersionMutates {
			id := s.newID("ws")
			s.mu.Lock()
			s.workspaces[id] = &workspace{ID: id, State: "claimed", Node: NodeID, Gen: 1, Files: map[string][]byte{}, Made: now()}
			s.mu.Unlock()
			s.append("ws.created", id, id, map[string]any{"by": "a frame version nobody negotiated"})
		}
		return
	}
	if first.T != "hello" {
		s.send(ctx, c, writeMu, &frame{V: 1, T: "res", ID: first.ID, Err: errf("bad_request", "first frame must be hello")})
		return
	}
	var hello struct {
		Peer string   `cbor:"peer,omitempty"`
		Role string   `cbor:"role,omitempty"`
		Caps []string `cbor:"caps,omitempty"`
	}
	_ = dec.Unmarshal(first.Body, &hello)

	caps, cerr := s.negotiate(hello.Caps)
	if cerr != nil {
		s.send(ctx, c, writeMu, &frame{V: 1, T: "res", ID: first.ID, Err: cerr})
		return
	}
	peer := hello.Peer
	if peer == "" {
		peer = s.newID("c")
	}
	ok, _ := enc.Marshal(map[string]any{
		"peer": peer, "caps": caps, "server": "remount-conformance-shim",
		"now": now(), "lease_sec": 30, "pubkey": []byte("shim-grant-signing-key-not-real!"),
	})
	s.send(ctx, c, writeMu, &frame{V: 1, T: "res", ID: first.ID, Body: ok})

	for {
		typ, b, err := c.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			return
		}
		var f frame
		if err := dec.Unmarshal(b, &f); err != nil {
			return
		}
		if !s.acceptVersion(f.V) {
			return
		}
		switch f.T {
		case "req":
			body, rerr := s.dispatch(ctx, c, writeMu, peer, &f)
			res := &frame{V: 1, T: "res", ID: f.ID, From: f.To, Op: f.Op}
			if rerr != nil {
				res.Err = rerr
			} else if body != nil {
				res.Body, _ = enc.Marshal(body)
			}
			s.send(ctx, c, writeMu, res)
		case "ping":
			s.send(ctx, c, writeMu, &frame{V: 1, T: "pong", ID: f.ID})
		}
	}
}

func (s *Server) acceptVersion(v uint8) bool {
	if s.defect == DefectVersion {
		return true // the defect: any version is fine
	}
	return v == 1
}

func (s *Server) negotiate(offered []string) ([]string, *wireError) {
	if s.defect == DefectNegotiation {
		// Two violations in one: the baseline is not required, and whatever
		// the peer offered is echoed back as though it were implemented.
		return append([]string(nil), offered...), nil
	}
	if !contains(offered, "v1") {
		return nil, errf("unsupported", "peer does not offer required capability %q", "v1")
	}
	var out []string
	for _, c := range implemented {
		if contains(offered, c) {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *Server) send(ctx context.Context, c *websocket.Conn, mu *sync.Mutex, f *frame) {
	b, err := enc.Marshal(f)
	if err != nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	_ = c.Write(ctx, websocket.MessageBinary, b)
}

// ---- operations ----------------------------------------------------------

func (s *Server) dispatch(ctx context.Context, c *websocket.Conn, mu *sync.Mutex, peer string, f *frame) (any, *wireError) {
	switch f.Op {
	case "ws.create":
		return s.wsCreate(f)
	case "ws.get":
		return s.wsGet(f)
	case "ws.list":
		return s.wsList()
	case "ws.destroy":
		return s.wsDestroy(f)
	case "ws.move":
		return s.wsMove(f)
	case "ws.wake":
		return s.wsWake(f)
	case "grant":
		return s.grant(peer, f)
	case "events.tail":
		return s.eventsTail(f)
	case "approval.list":
		return s.approvalList()
	case "approval.get":
		return nil, errf("not_found", "no such approval")
	case "agent.create":
		return s.agentCreate(f)
	case "agent.get":
		return s.agentGet(f)
	case "agent.message":
		return s.agentMessage(f)
	case "agent.destroy":
		return s.agentDestroy(f)
	case "agent.sleep":
		return s.agentSleep(f)
	case "agent.wake":
		return s.agentWake(f)
	case "agent.transcript":
		return map[string]any{"records": []any{}, "next": uint64(0)}, nil
	case "ws.info":
		return s.wsInfo(f)
	case "fs.list":
		return s.fsList(f)
	case "fs.read":
		return s.fsRead(f)
	case "fs.write":
		return s.fsWrite(f)
	case "ws.snapshot":
		return s.snapshot(f)
	case "s.open":
		return s.sessionOpen(ctx, c, mu, f)
	case "s.input":
		return s.sessionInput(ctx, f)
	case "s.attach":
		return s.sessionAttach(ctx, f)
	case "s.wait":
		return s.sessionWait(f)
	default:
		return nil, errf("unsupported", "unknown operation %q", f.Op)
	}
}

func (s *Server) wsCreate(f *frame) (any, *wireError) {
	var req struct {
		Spec map[string]any `cbor:"spec"`
		Idem string         `cbor:"idem,omitempty"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	if path, _ := req.Spec["mount_path"].(string); path != "" && !validMountPath(path) {
		return nil, errf("bad_request", "mount_path %q is not absolute, clean and outside the system directories", path)
	}
	s.mu.Lock()
	if req.Idem != "" && s.defect != DefectWorkspace {
		if id, ok := s.idem[req.Idem]; ok {
			ws := s.workspaces[id]
			s.mu.Unlock()
			return ws.public(), nil
		}
	}
	id := s.newID("ws")
	ws := &workspace{ID: id, State: "claimed", Node: NodeID, Gen: 1, Spec: req.Spec, Files: map[string][]byte{}, Made: now()}
	if from, _ := req.Spec["restore_from"].(string); from != "" {
		tree, ok := s.trees[from]
		if !ok {
			s.mu.Unlock()
			return nil, errf("not_found", "no such artifact %q", from)
		}
		for path, data := range tree {
			ws.Files[path] = append([]byte(nil), data...)
		}
	}
	ws.Files[".remount/env"] = []byte(s.workspaceEnv(id, req.Spec))
	s.workspaces[id] = ws
	if req.Idem != "" {
		s.idem[req.Idem] = id
	}
	s.mu.Unlock()
	s.append("ws.created", id, id, map[string]any{"name": req.Spec["name"]})
	s.append("ws.claiming", id, id, nil)
	s.append("ws.claimed", id, id, nil)
	return ws.public(), nil
}

// workspaceEnv is the §9 file a node writes: the broker address and a
// placeholder, never the secret.
func (s *Server) workspaceEnv(id string, spec map[string]any) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "REMOUNT_WORKSPACE=%s\n", id)
	fmt.Fprintf(b, "REMOUNT_BROKER=%s/broker/%s\n", s.http.URL, id)
	if bindings, ok := spec["bindings"].([]any); ok && len(bindings) > 0 {
		fmt.Fprintf(b, "REMOUNT_REF_CONFORMANCE=%s\n", Placeholder)
	}
	return b.String()
}

func (s *Server) wsGet(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	ws, ok := s.workspaces[req.ID]
	if !ok {
		return nil, errf("not_found", "no such workspace %q", req.ID)
	}
	return ws.public(), nil
}

func (s *Server) wsList() (any, *wireError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, ws := range s.workspaces {
		out = append(out, ws.public())
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["id"].(string) < out[j]["id"].(string) })
	return map[string]any{"workspaces": out}, nil
}

func (s *Server) wsDestroy(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	_, ok := s.workspaces[req.ID]
	delete(s.workspaces, req.ID)
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such workspace %q", req.ID)
	}
	s.append("ws.destroyed", req.ID, req.ID, nil)
	return map[string]any{}, nil
}

func (s *Server) wsInfo(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaces[req.ID]; !ok {
		return nil, errf("not_found", "no such workspace %q", req.ID)
	}
	return map[string]any{
		"ws": req.ID, "backend": "shim", "root": "/shim/" + req.ID,
		"broker": s.http.URL + "/broker/" + req.ID,
	}, nil
}

func (s *Server) grant(peer string, f *frame) (any, *wireError) {
	var req struct {
		WS string `cbor:"ws"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	ws, ok := s.workspaces[req.WS]
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such workspace %q", req.WS)
	}
	return map[string]any{
		"claims": map[string]any{
			"client": peer, "ws": ws.ID, "node": NodeID,
			"exp": now() + 600_000, "gen": ws.Gen,
		},
		"sig":  []byte("shim-signature-not-a-real-signature"),
		"node": NodeID,
	}, nil
}

// authorize is §4 reduced to what the shim can check: the grant must name
// this workspace and the generation the workspace currently has.
func (s *Server) authorize(body []byte, wsID string) (*workspace, *wireError) {
	var req struct {
		Grant *struct {
			Claims struct {
				WS  string `cbor:"ws"`
				Gen uint64 `cbor:"gen"`
			} `cbor:"claims"`
		} `cbor:"grant,omitempty"`
	}
	_ = dec.Unmarshal(body, &req)
	s.mu.Lock()
	ws, ok := s.workspaces[wsID]
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such workspace %q", wsID)
	}
	if req.Grant == nil {
		return nil, errf("unauthorized", "missing grant")
	}
	if s.defect == DefectAuthority {
		return ws, nil // the defect: any grant will do
	}
	if req.Grant.Claims.WS != wsID {
		return nil, errf("unauthorized", "grant is for workspace %q", req.Grant.Claims.WS)
	}
	if req.Grant.Claims.Gen != ws.Gen {
		return nil, errf("unauthorized", "grant is bound to generation %d, workspace is at %d", req.Grant.Claims.Gen, ws.Gen)
	}
	return ws, nil
}

func (s *Server) fsList(f *frame) (any, *wireError) {
	var req struct {
		WS string `cbor:"ws"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	ws, err := s.authorize(f.Body, req.WS)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := []map[string]any{}
	for path := range ws.Files {
		entries = append(entries, map[string]any{"name": path})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i]["name"].(string) < entries[j]["name"].(string) })
	return map[string]any{"entries": entries}, nil
}

func (s *Server) fsRead(f *frame) (any, *wireError) {
	var req struct {
		WS   string `cbor:"ws"`
		Path string `cbor:"path"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	ws, err := s.authorize(f.Body, req.WS)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := ws.Files[req.Path]
	if !ok {
		return nil, errf("not_found", "no such path %q", req.Path)
	}
	return map[string]any{"d": data, "size": int64(len(data)), "eof": true}, nil
}

func (s *Server) fsWrite(f *frame) (any, *wireError) {
	var req struct {
		WS   string `cbor:"ws"`
		Path string `cbor:"path"`
		Data []byte `cbor:"d"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	ws, err := s.authorize(f.Body, req.WS)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	ws.Files[req.Path] = append([]byte(nil), req.Data...)
	// Hoist the id before unlocking: reading a guarded field afterwards is the
	// pattern scripts/lint-locks.sh exists to catch, and a shim that models the
	// protocol should model its discipline too.
	id := ws.ID
	s.mu.Unlock()
	s.append("fs.write", id, id, map[string]any{"path": req.Path})
	return map[string]any{}, nil
}

func (s *Server) snapshot(f *frame) (any, *wireError) {
	var req struct {
		WS string `cbor:"ws"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	ws, err := s.authorize(f.Body, req.WS)
	if err != nil {
		return nil, err
	}
	body, tree := s.pack(ws)
	if s.defect == DefectSnapshot {
		// The defect: an id that identifies the moment, not the tree.
		var salt [16]byte
		_, _ = rand.Read(salt[:])
		body = append(body, salt[:]...)
	}
	sum := sha256.Sum256(body)
	id := "art_sha256:" + hex.EncodeToString(sum[:])
	s.mu.Lock()
	s.artifacts[id] = body
	s.trees[id] = tree
	wsID := ws.ID
	s.mu.Unlock()
	s.append("ws.snapshot", wsID, wsID, map[string]any{"artifact": id})
	return map[string]any{"artifact": id, "bytes": int64(len(body)), "consistency": "live", "authoritative": false}, nil
}

// pack is §10's promise reduced to a map: a deterministic serialization of
// the tree, so the same contents always produce the same id and changing a
// byte changes it.
func (s *Server) pack(ws *workspace) ([]byte, map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := make([]string, 0, len(ws.Files))
	for p := range ws.Files {
		if p == ".remount/env" {
			continue // §10.2: .remount never travels in a snapshot
		}
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var body []byte
	tree := map[string][]byte{}
	for _, p := range paths {
		body = append(body, fmt.Sprintf("%s\x00%d\x00", p, len(ws.Files[p]))...)
		body = append(body, ws.Files[p]...)
		tree[p] = append([]byte(nil), ws.Files[p]...)
	}
	return body, tree
}

// ---- sessions ------------------------------------------------------------

func (s *Server) sessionOpen(ctx context.Context, c *websocket.Conn, mu *sync.Mutex, f *frame) (any, *wireError) {
	var req struct {
		WS      string   `cbor:"ws"`
		Kind    string   `cbor:"kind"`
		Program []string `cbor:"program"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	if _, err := s.authorize(f.Body, req.WS); err != nil {
		return nil, err
	}
	id := s.newID("s")
	sess := &session{ID: id, WS: req.WS, conn: c, writeMu: mu}
	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()
	// §8 guarantee 1: seq 0 is the info chunk.
	info, _ := enc.Marshal(map[string]any{"id": id, "ws": req.WS, "kind": req.Kind, "program": req.Program, "opened_at": now()})
	s.chunk(ctx, sess, 4, info)
	s.append("s.opened", req.WS, req.WS, map[string]any{"s": id})
	stdout, code, interactive := interpret(req.Program)
	if !interactive {
		if len(stdout) > 0 && s.defect != DefectStdout {
			s.chunk(ctx, sess, 1, stdout)
		}
		s.exit(ctx, sess, code)
	}
	return map[string]any{"s": id, "next": sess.Seq}, nil
}

// interpret is the shim's whole process model: enough of a shell to make the
// session requirements observable, and nothing more.
func interpret(program []string) (stdout []byte, code int, interactive bool) {
	switch {
	case len(program) == 0:
		return nil, 0, false
	case strings.HasSuffix(program[0], "/cat"):
		return nil, 0, true
	case strings.HasSuffix(program[0], "/echo"):
		return []byte(strings.Join(program[1:], " ") + "\n"), 0, false
	case strings.HasSuffix(program[0], "/sh") && len(program) == 3 && strings.HasPrefix(program[2], "exit "):
		n := 0
		_, _ = fmt.Sscanf(program[2], "exit %d", &n)
		return nil, n, false
	case strings.EqualFold(program[0], "powershell.exe") && strings.Contains(strings.Join(program[1:], " "), "$input"):
		return nil, 0, true
	case strings.EqualFold(program[0], "cmd.exe") && len(program) >= 6 && strings.EqualFold(program[4], "echo"):
		return []byte(strings.Join(program[5:], " ") + "\n"), 0, false
	case strings.EqualFold(program[0], "cmd.exe") && len(program) == 6 && strings.EqualFold(program[4], "exit"):
		n := 0
		_, _ = fmt.Sscanf(program[5], "%d", &n)
		return nil, n, false
	default:
		return nil, 0, false
	}
}

func (s *Server) exit(ctx context.Context, sess *session, code int) {
	body, _ := enc.Marshal(map[string]any{"code": code})
	s.chunk(ctx, sess, 3, body)
	s.mu.Lock()
	sess.Exit = &code
	s.mu.Unlock()
	s.append("s.exited", sess.WS, sess.WS, map[string]any{"s": sess.ID, "exit": code})
}

func (s *Server) sessionAttach(ctx context.Context, f *frame) (any, *wireError) {
	var req struct {
		S    string `cbor:"s"`
		From uint64 `cbor:"from"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	sess, ok := s.sessions[req.S]
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such session %q", req.S)
	}
	if _, err := s.authorize(f.Body, sess.WS); err != nil {
		return nil, err
	}
	s.mu.Lock()
	log := append([]logged(nil), sess.Log...)
	next := sess.Seq
	s.mu.Unlock()
	// §8 guarantee 3: replay and live tail are the same path, byte for byte.
	for _, c := range log {
		if c.Seq < req.From {
			continue
		}
		body, _ := enc.Marshal(map[string]any{"st": c.St, "d": c.Data})
		s.send(ctx, sess.conn, sess.writeMu, &frame{V: 1, T: "chunk", S: sess.ID, Seq: c.Seq, WS: sess.WS, Body: body})
	}
	return map[string]any{"s": sess.ID, "next": next}, nil
}

func (s *Server) sessionWait(f *frame) (any, *wireError) {
	var req struct {
		S string `cbor:"s"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[req.S]
	if !ok {
		return nil, errf("not_found", "no such session %q", req.S)
	}
	if sess.Exit == nil {
		return map[string]any{"exited": false}, nil
	}
	return map[string]any{"exited": true, "exit": map[string]any{"code": *sess.Exit}}, nil
}

func (s *Server) sessionInput(ctx context.Context, f *frame) (any, *wireError) {
	var req struct {
		S    string `cbor:"s"`
		ISeq uint64 `cbor:"iseq"`
		Data []byte `cbor:"d,omitempty"`
		EOF  bool   `cbor:"eof,omitempty"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	sess, ok := s.sessions[req.S]
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such session %q", req.S)
	}
	apply := true
	s.mu.Lock()
	if req.ISeq != 0 && req.ISeq <= sess.LastISeq && s.defect != DefectSession {
		apply = false // §8: an iseq at or below the last applied one is a retry
	}
	if apply && req.ISeq > sess.LastISeq {
		sess.LastISeq = req.ISeq
	}
	s.mu.Unlock()
	if apply && len(req.Data) > 0 {
		s.chunk(ctx, sess, 1, req.Data) // the shim's process is a cat
	}
	if req.EOF {
		s.exit(ctx, sess, 0)
	}
	return map[string]any{}, nil
}

func (s *Server) chunk(ctx context.Context, sess *session, st uint8, data []byte) {
	body, _ := enc.Marshal(map[string]any{"st": st, "d": data})
	s.mu.Lock()
	seq := sess.Seq
	sess.Seq++
	sess.Log = append(sess.Log, logged{Seq: seq, St: st, Data: append([]byte(nil), data...)})
	s.mu.Unlock()
	s.send(ctx, sess.conn, sess.writeMu, &frame{V: 1, T: "chunk", S: sess.ID, Seq: seq, WS: sess.WS, Body: body})
}

// ---- agents and approvals ------------------------------------------------

// inboxBound is §6.1's bound. The defect removes it.
const inboxBound = 64

func (s *Server) agentCreate(f *frame) (any, *wireError) {
	var req struct {
		Name      string         `cbor:"name,omitempty"`
		Workspace map[string]any `cbor:"workspace,omitempty"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	wsID := s.newID("ws")
	s.mu.Lock()
	s.workspaces[wsID] = &workspace{ID: wsID, State: "claimed", Node: NodeID, Gen: 1, Spec: req.Workspace, Files: map[string][]byte{}, Made: now()}
	a := &agent{ID: s.newID("ag"), WS: wsID, Status: "scheduled", Mode: "acp", Made: now()}
	s.agents[a.ID] = a
	s.mu.Unlock()
	s.append("ws.created", wsID, wsID, nil)
	s.append("agent.created", wsID, wsID, map[string]any{"agent": a.ID})
	return a.public(), nil
}

func (s *Server) agentGet(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[req.ID]
	if !ok {
		return nil, errf("not_found", "no such agent %q", req.ID)
	}
	return a.public(), nil
}

func (s *Server) agentMessage(f *frame) (any, *wireError) {
	var req struct {
		ID   string `cbor:"id"`
		Text string `cbor:"text"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[req.ID]
	if !ok {
		return nil, errf("not_found", "no such agent %q", req.ID)
	}
	if len(a.Inbox) >= inboxBound && s.defect != DefectAgent {
		return nil, errf("resource_exhausted", "inbox is full at %d messages", inboxBound)
	}
	// §6.1: a message to a sleeping agent wakes the workspace and says so.
	woken := s.wakeLocked(a)
	msg := map[string]any{"id": s.newID("m"), "kind": "follow_up", "text": req.Text, "at": now()}
	a.Inbox = append(a.Inbox, msg)
	return map[string]any{"agent": a.public(), "message": msg, "woken": woken}, nil
}

func (s *Server) agentDestroy(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	a, ok := s.agents[req.ID]
	if ok {
		delete(s.agents, req.ID)
		delete(s.workspaces, a.WS)
	}
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such agent %q", req.ID)
	}
	s.append("agent.destroyed", a.WS, a.WS, map[string]any{"agent": a.ID})
	return map[string]any{}, nil
}

func (s *Server) approvalList() (any, *wireError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, a := range s.approvals {
		if a.Status != "pending" && s.defect != DefectApproval {
			continue
		}
		out = append(out, map[string]any{"id": a.ID, "kind": a.Kind, "status": a.Status, "created_at": a.Made})
	}
	return map[string]any{"approvals": out}, nil
}

// ---- events --------------------------------------------------------------

func (s *Server) append(typ, stream, ws string, payload map[string]any) {
	var body []byte
	if payload != nil {
		body, _ = enc.Marshal(payload)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	seq := s.seq
	if s.defect == DefectEvent && !s.brokeSeq && len(s.events) > 2 {
		// The defect, once: a sequence that repeats, so seq is neither a
		// total order nor dense. Doing it once rather than forever keeps the
		// rest of the shim usable, so the seed can prove the other
		// categories still pass.
		seq = s.events[len(s.events)-1].Seq
		s.brokeSeq = true
	}
	s.events = append(s.events, event{Seq: seq, At: now(), Stream: stream, Workspace: ws, Type: typ, Payload: body})
}

func (s *Server) eventsTail(f *frame) (any, *wireError) {
	var req struct {
		From uint64 `cbor:"from"`
		WS   string `cbor:"ws,omitempty"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, e := range s.events {
		if e.Seq < req.From {
			continue
		}
		if req.WS != "" && e.Stream != req.WS {
			continue
		}
		out = append(out, map[string]any{
			"seq": e.Seq, "at": e.At, "stream": e.Stream, "workspace": e.Workspace,
			"type": e.Type, "payload": e.Payload, "origin": "control",
		})
	}
	return map[string]any{"events": out}, nil
}

// ---- broker --------------------------------------------------------------

// handleBroker is §9's reverse-proxy path: /broker/{ws}/d/{host}/{path}.
func (s *Server) handleBroker(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/broker/")
	wsID, rest, ok := strings.Cut(rest, "/d/")
	if !ok {
		http.Error(w, "not a reverse-proxy path", http.StatusNotFound)
		return
	}
	host, _, _ := strings.Cut(rest, "/")
	carriesPlaceholder := strings.Contains(r.Header.Get("Authorization"), Placeholder)

	switch {
	case carriesPlaceholder && host != BoundHost:
		if s.defect == DefectBinding {
			// The defect: the placeholder is forwarded to a host the binding
			// does not cover, which is exfiltration with extra steps.
			writeJSON(w, http.StatusOK, map[string]any{"forwarded": true, "host": host})
			return
		}
		s.append("egress.denied", wsID, wsID, map[string]any{"decision": "leak_blocked", "host": host, "rule": BindingID})
		http.Error(w, "leak blocked", http.StatusForbidden)
	case isPrivateHost(host):
		s.append("egress.denied", wsID, wsID, map[string]any{"decision": "non_public_address", "host": host})
		http.Error(w, "non-public address", http.StatusForbidden)
	case !carriesPlaceholder && host != BoundHost:
		s.append("egress.denied", wsID, wsID, map[string]any{"decision": "no_rule", "host": host})
		http.Error(w, "no rule covers this destination", http.StatusForbidden)
	default:
		s.append("egress.allowed", wsID, wsID, map[string]any{"host": host})
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
}

func isPrivateHost(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast()
}

// ---- helpers -------------------------------------------------------------

func (w *workspace) public() map[string]any {
	return map[string]any{
		"id": w.ID, "state": w.State, "node": w.Node, "gen": w.Gen,
		"created_at": w.Made, "updated_at": w.Made, "spec": w.Spec,
	}
}

func (a *agent) public() map[string]any {
	inbox := a.Inbox
	if inbox == nil {
		inbox = []map[string]any{}
	}
	return map[string]any{
		"id": a.ID, "ws": a.WS, "status": a.Status, "mode": a.Mode,
		"tenant": "shim", "owner": "shim", "inbox": inbox, "turns": 0,
		"created_at": a.Made, "updated_at": a.Made,
	}
}

func (s *Server) newID(prefix string) string {
	return fmt.Sprintf("%s_shim%012d", prefix, s.ids.Add(1))
}

func now() int64 { return time.Now().UnixMilli() }

func contains(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}

// systemDirs is §5.2's refusal list.
var systemDirs = []string{"/", "/proc", "/sys", "/dev", "/etc", "/bin", "/sbin", "/lib", "/usr", "/var", "/run", "/boot"}

func validMountPath(p string) bool {
	if !strings.HasPrefix(p, "/") || strings.Contains(p, "//") || strings.Contains(p, "/..") {
		return false
	}
	for _, d := range systemDirs {
		if p == d || (d != "/" && strings.HasPrefix(p, d+"/")) {
			return false
		}
	}
	return p != "/"
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// ---- lifecycle operations the manifest also reaches ----------------------

func (s *Server) wsMove(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	ws, ok := s.workspaces[req.ID]
	if ok {
		// §5: a new claim increments the generation, which is what makes
		// every grant minted under the old one useless.
		ws.Gen++
	}
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such workspace %q", req.ID)
	}
	s.append("ws.moved", ws.ID, ws.ID, map[string]any{"processes": "restarted"})
	s.append("ws.claiming", ws.ID, ws.ID, nil)
	s.append("ws.claimed", ws.ID, ws.ID, nil)
	return ws.public(), nil
}

func (s *Server) wsWake(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	ws, ok := s.workspaces[req.ID]
	if ok {
		ws.State = "claimed"
	}
	s.mu.Unlock()
	if !ok {
		// §5.1: destroyed is absorbing, so a wake never resurrects one.
		return nil, errf("not_found", "no such workspace %q", req.ID)
	}
	s.append("ws.resumed", ws.ID, ws.ID, nil)
	return ws.public(), nil
}

func (s *Server) agentSleep(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	a, ok := s.agents[req.ID]
	if ok {
		a.Status = "sleeping"
		if ws := s.workspaces[a.WS]; ws != nil {
			ws.State = "paused"
		}
	}
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such agent %q", req.ID)
	}
	s.append("agent.slept", a.WS, a.WS, map[string]any{"agent": a.ID})
	s.append("ws.paused", a.WS, a.WS, nil)
	return a.public(), nil
}

func (s *Server) agentWake(f *frame) (any, *wireError) {
	var req struct {
		ID string `cbor:"id"`
	}
	_ = dec.Unmarshal(f.Body, &req)
	s.mu.Lock()
	a, ok := s.agents[req.ID]
	if ok {
		s.wakeLocked(a)
	}
	s.mu.Unlock()
	if !ok {
		return nil, errf("not_found", "no such agent %q", req.ID)
	}
	s.append("agent.woken", a.WS, a.WS, map[string]any{"agent": a.ID})
	return a.public(), nil
}

// wakeLocked returns a sleeping agent's workspace to service. The caller
// holds the lock.
func (s *Server) wakeLocked(a *agent) bool {
	if a.Status != "sleeping" {
		return false
	}
	a.Status = "scheduled"
	if ws := s.workspaces[a.WS]; ws != nil {
		ws.State = "claimed"
	}
	return true
}

// ---- artifacts over HTTP -------------------------------------------------

func (s *Server) handleArtifact(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.mu.Lock()
		body, ok := s.artifacts[id]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write(body)
	case http.MethodPut:
		body, err := readAll(r)
		if err != nil {
			http.Error(w, "unreadable body", http.StatusBadRequest)
			return
		}
		// §10: publish only after the digest matches the id it was stored
		// under.
		sum := sha256.Sum256(body)
		if id != "art_sha256:"+hex.EncodeToString(sum[:]) {
			http.Error(w, "digest does not match the artifact id", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.artifacts[id] = body
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "bytes": len(body)})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ---- out-of-band event append -------------------------------------------

func (s *Server) handleEventPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": map[string]any{"code": "unsupported", "message": "method not allowed"}})
		return
	}
	body, err := readAll(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "bad_request", "message": "unreadable body"}})
		return
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "bad_request", "message": "body is not a JSON object"}})
		return
	}
	// §11: a field the implementation does not know is a 400, never a
	// half-applied append.
	for key := range raw {
		switch key {
		case "type", "stream", "payload", "agent":
		default:
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "bad_request", "message": "unknown field " + key}})
			return
		}
	}
	var typ, stream string
	_ = json.Unmarshal(raw["type"], &typ)
	_ = json.Unmarshal(raw["stream"], &stream)
	if typ == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "bad_request", "message": "type is required"}})
		return
	}
	s.append(typ, stream, "", map[string]any{"out_of_band": true})
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func readAll(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	return io.ReadAll(io.LimitReader(r.Body, 1<<20))
}
