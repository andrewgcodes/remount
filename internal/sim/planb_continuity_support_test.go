package sim

// Plan B phase B3 (docs/engineering/plan-b-repository-executable-2026-09-03.md
// §11) is the continuity and authority failure matrix. Every scenario in this
// lane injects its failure through a real peer — the world harness's cut,
// stopNode, and frame hooks — because forcing a state through Control's
// internals proves only that the state exists, not that the system reaches it.
//
// §11.3 requires each recovery claim to name the authoritative row, the
// generation, the checkpoint digest, the session cursor, the event, and the
// cleanup result. These helpers exist so those facts land in the failure
// message: "it recovered" is not evidence, and neither is "it did not".

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// continuityRow renders the authoritative workspace row. Every B3 assertion
// reports through this so a failure names the fact that was wrong.
func continuityRow(ws *proto.Workspace) string {
	if ws == nil {
		return "row{<none>}"
	}
	return fmt.Sprintf("row{ws=%s state=%s node=%s gen=%d lease_until=%d snapshot=%q authz=%d}",
		ws.ID, ws.State, ws.Node, ws.Generation, ws.LeaseUntil, ws.LastSnapshot, ws.AuthzRevision)
}

// awaitWorkspace polls the authoritative row until pred holds, and fails
// naming the last row it actually read rather than the one it wanted.
func awaitWorkspace(t *testing.T, ctx context.Context, c *client.Client, id, what string, within time.Duration, pred func(*proto.Workspace) bool) *proto.Workspace {
	t.Helper()
	deadline := time.Now().Add(within)
	var last *proto.Workspace
	var lastErr error
	for {
		ws, err := c.GetWorkspace(ctx, id)
		if err == nil {
			last, lastErr = ws, nil
			if pred(ws) {
				return ws
			}
		} else {
			lastErr = err
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			t.Fatalf("workspace never reached %s within %s: %s last_error=%v", what, within, continuityRow(last), lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// requireStableWorkspace asserts the authoritative row does not move for d.
// A fence is only proven by nothing happening while something could have: the
// replacement node is online and eligible the whole time.
func requireStableWorkspace(t *testing.T, ctx context.Context, c *client.Client, why string, d time.Duration, want *proto.Workspace) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		got, err := c.GetWorkspace(ctx, want.ID)
		if err != nil {
			t.Fatalf("%s: read authoritative row: %v", why, err)
		}
		if got.State != want.State || got.Node != want.Node || got.Generation != want.Generation || got.LastSnapshot != want.LastSnapshot {
			t.Fatalf("%s: authoritative row moved: %s want %s", why, continuityRow(got), continuityRow(want))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// continuityStateChange is one decoded ws.state_changed event: the central
// transition table's own record of who moved the row, from where, and at
// which generation.
type continuityStateChange struct {
	Seq        uint64
	From       string
	To         string
	Operation  string
	Actor      string
	Generation uint64
	Node       string
}

func (s continuityStateChange) String() string {
	return fmt.Sprintf("state_changed{seq=%d %s->%s op=%s actor=%s gen=%d node=%s}",
		s.Seq, s.From, s.To, s.Operation, s.Actor, s.Generation, s.Node)
}

// continuityStateChanges reads the workspace's transition history.
func continuityStateChanges(t *testing.T, ctx context.Context, c *client.Client, ws string) []continuityStateChange {
	t.Helper()
	evs, err := c.ReadEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	var out []continuityStateChange
	for _, e := range evs {
		if e.Type != proto.EvWSStateChanged {
			continue
		}
		var payload struct {
			From       string `cbor:"from"`
			To         string `cbor:"to"`
			Operation  string `cbor:"operation"`
			Actor      string `cbor:"actor"`
			Generation uint64 `cbor:"generation"`
			Node       string `cbor:"node"`
		}
		if err := proto.Unmarshal(e.Payload, &payload); err != nil {
			t.Fatalf("decode %s at seq %d: %v", e.Type, e.Seq, err)
		}
		out = append(out, continuityStateChange{
			Seq: e.Seq, From: payload.From, To: payload.To, Operation: payload.Operation,
			Actor: payload.Actor, Generation: payload.Generation, Node: payload.Node,
		})
	}
	return out
}

// continuityCode returns the wire code of err. Assertions match on the code,
// never on message text.
func continuityCode(err error) string {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// continuityRawClient dials a client peer that manages its own grants. The
// SDK deliberately refreshes a stale grant and retries once, which is right
// for a user and wrong for a fencing proof: it would turn every refusal into
// a silent success. The peer id is fixed by the caller so a redial keeps the
// identity a grant is bound to.
func continuityRawClient(t *testing.T, ctx context.Context, dial transport.Dialer, token, peer string) *transport.Peer {
	t.Helper()
	conn, err := dial.Dial(ctx)
	if err != nil {
		t.Fatalf("dial raw client %s: %v", peer, err)
	}
	p := transport.NewPeer(conn, nil)
	ok, err := transport.Hello(ctx, p, proto.Hello{Peer: peer, Role: proto.RoleClient, Token: token, Caps: []string{proto.CapabilityV1}})
	if err != nil {
		t.Fatalf("raw client hello: %v", err)
	}
	if ok.Peer != peer {
		t.Fatalf("raw client was assigned %s, want the requested id %s", ok.Peer, peer)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// continuityRawNode dials a node peer directly so a test can send a claim,
// ready or renew that a healthy node would never send. It advertises no
// backend, so the control plane can never place work on it; its only purpose
// is to speak stale authority messages over the real protocol.
func continuityRawNode(t *testing.T, ctx context.Context, dial transport.Dialer, token, id string) *transport.Peer {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	hello := proto.Hello{
		Peer: id, Role: proto.RoleNode, Token: token, Caps: []string{proto.CapabilityV1},
		PubKey: public, IssuedAt: time.Now().UnixMilli(), Nonce: nonce,
	}
	hello.Proof = ed25519.Sign(private, proto.HelloProofBytes(hello))
	conn, err := dial.Dial(ctx)
	if err != nil {
		t.Fatalf("dial raw node: %v", err)
	}
	p := transport.NewPeer(conn, nil)
	if _, err := transport.Hello(ctx, p, hello); err != nil {
		t.Fatalf("raw node hello: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// continuityGrant fetches a grant bound to this raw peer at the workspace's
// current generation, authorization revision and holder.
func continuityGrant(t *testing.T, ctx context.Context, p *transport.Peer, ws string) *proto.Grant {
	t.Helper()
	var g proto.Grant
	if err := p.Call(ctx, proto.PeerControl, proto.OpGrant, proto.GrantReq{WS: ws}, &g); err != nil {
		t.Fatalf("grant for %s: %v", ws, err)
	}
	return &g
}

// continuityReadWithGrant performs one node read with exactly the grant given.
func continuityReadWithGrant(ctx context.Context, p *transport.Peer, node, ws, path string, g *proto.Grant) ([]byte, error) {
	var res proto.FSReadRes
	err := p.Call(ctx, node, proto.OpFSRead, proto.FSReadReq{WS: ws, Path: path, Grant: g}, &res)
	return res.Data, err
}
