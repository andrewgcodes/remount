package conformance

import (
	"context"
	"strings"
)

// ---- protocol negotiation and stable errors -----------------------------

func checkNegBaselineAccepted(ctx context.Context, s *Session) error {
	c, err := Dial(ctx, DialOptions{Endpoint: s.Target.Endpoint, Token: s.Target.Token, Caps: []string{CapabilityV1}})
	if err != nil {
		return failf("a hello offering only the v1 baseline was refused: %v", err)
	}
	defer c.Close()
	if !containsString(c.HelloOK.Caps, CapabilityV1) {
		return failf("the handshake response did not echo %q; negotiated %v", CapabilityV1, c.HelloOK.Caps)
	}
	if c.HelloOK.Peer == "" {
		return failf("the handshake response assigned no peer id")
	}
	return nil
}

// unknownExtension is an identifier no implementation may ever implement, so
// echoing it back is proof the implementation echoes what it was offered
// rather than what it supports. That is the failure §3.1's "never echoes an
// identifier it does not implement" exists to prevent.
const unknownExtension = "x-conformance-unimplemented-extension"

func checkNegOrderedIntersection(ctx context.Context, s *Session) error {
	// Offered out of canonical order, with an unknown identifier in the
	// middle, so a naive implementation that echoes its input fails here.
	offered := []string{"session-cap", unknownExtension, CapabilityV1, "authz-push"}
	c, err := Dial(ctx, DialOptions{Endpoint: s.Target.Endpoint, Token: s.Target.Token, Caps: offered})
	if err != nil {
		return failf("a hello offering the baseline plus an unknown extension was refused: %v", err)
	}
	defer c.Close()
	got := c.HelloOK.Caps
	if len(got) == 0 || got[0] != CapabilityV1 {
		return failf("negotiated capabilities must start with %q; got %v", CapabilityV1, got)
	}
	if containsString(got, unknownExtension) {
		return failf("the implementation echoed %q, an identifier it cannot implement", unknownExtension)
	}
	for _, c := range got {
		if !containsString(offered, c) {
			return failf("negotiated %q, which was never offered", c)
		}
	}
	// Canonical order: the returned list must be a subsequence of §3.1's
	// table.
	i := 0
	for _, want := range KnownCapabilities {
		if i < len(got) && got[i] == want {
			i++
		}
	}
	if i != len(got) {
		return failf("negotiated capabilities %v are not in the canonical order %v", got, KnownCapabilities)
	}
	return nil
}

func checkNegBaselineRequired(ctx context.Context, s *Session) error {
	for _, caps := range [][]string{{}, {unknownExtension}} {
		c, err := Dial(ctx, DialOptions{Endpoint: s.Target.Endpoint, Token: s.Target.Token, Caps: caps})
		if err == nil {
			c.Close()
			return failf("a hello offering caps=%v was accepted; an empty or baseline-free capability list is not an implicit wildcard", caps)
		}
		if code := CodeOf(err); code != "unsupported" {
			return failf("a hello offering caps=%v was refused with %q, not the stable code %q (%v)", caps, code, "unsupported", err)
		}
	}
	return nil
}

func checkNegVersionFailsClosed(ctx context.Context, s *Session) error {
	before, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	var beforeList WSListRes
	if err := s.Call(ctx, "ws.list", nil, &beforeList); err != nil {
		return err
	}

	c, err := Dial(ctx, DialOptions{
		Endpoint: s.Target.Endpoint, Token: s.Target.Token,
		Caps: []string{CapabilityV1}, FrameVersion: FrameVersion + 1,
	})
	if err == nil {
		c.Close()
		return failf("a hello carrying frame version %d was accepted; §2 rule 1 requires a receiver to reject a version it did not negotiate", FrameVersion+1)
	}

	// "Fails closed" is an ordering claim, not merely an error: nothing may
	// have changed. The event log's head and the workspace inventory are the
	// two observable records of a lifecycle mutation.
	after, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	if after != before {
		return failf("the rejected version-%d connection advanced the event log from seq %d to %d, so it mutated state before failing", FrameVersion+1, before, after)
	}
	var afterList WSListRes
	if err := s.Call(ctx, "ws.list", nil, &afterList); err != nil {
		return err
	}
	if len(afterList.Workspaces) != len(beforeList.Workspaces) {
		return failf("the rejected version-%d connection changed the workspace inventory from %d to %d", FrameVersion+1, len(beforeList.Workspaces), len(afterList.Workspaces))
	}
	return nil
}

func checkNegUnknownOp(ctx context.Context, s *Session) error {
	err := s.Call(ctx, "conformance.no.such.operation", map[string]string{}, nil)
	if err == nil {
		return failf("an unknown operation succeeded")
	}
	if code := CodeOf(err); code != "unsupported" {
		return failf("an unknown operation answered %q, not %q (%v)", code, "unsupported", err)
	}
	return nil
}

func checkNegHelloFirst(ctx context.Context, s *Session) error {
	c, err := Dial(ctx, DialOptions{Endpoint: s.Target.Endpoint, Token: s.Target.Token, SkipHello: true})
	if err == nil {
		c.Close()
		return failf("a connection whose first frame was a request rather than a hello was accepted")
	}
	if code := CodeOf(err); code != "" && !KnownCode(code) {
		return failf("a non-hello first frame was refused with the non-stable code %q", code)
	}
	return nil
}

func checkNegStableCodes(ctx context.Context, s *Session) error {
	probes := []struct {
		what string
		call func() error
	}{
		{"ws.get of an unknown id", func() error { return s.Call(ctx, "ws.get", WSGetReq{ID: "ws_conformance_absent"}, nil) }},
		{"an unknown operation", func() error { return s.Call(ctx, "conformance.no.such.operation", map[string]string{}, nil) }},
		{"agent.get of an unknown id", func() error { return s.Call(ctx, "agent.get", AgentGetReq{ID: "a_conformance_absent"}, nil) }},
		{"ws.create with a system mount path", func() error {
			return s.Call(ctx, "ws.create", WSCreateReq{Spec: WorkspaceSpec{MountPath: "/etc/conformance"}, Idem: s.Idem("codes")}, nil)
		}},
	}
	for _, p := range probes {
		err := p.call()
		if err == nil {
			return failf("%s did not fail, so no error code was produced", p.what)
		}
		code := CodeOf(err)
		if code == "" {
			return failf("%s failed without a protocol error: %v", p.what, err)
		}
		if !KnownCode(code) {
			return failf("%s answered with code %q, which is not one of the stable codes %v", p.what, code, StableErrorCodes)
		}
	}
	return nil
}

func checkNegHandshakeCarriesLeaseAndKey(_ context.Context, s *Session) error {
	ok := s.Control.HelloOK
	if ok.LeaseSec <= 0 {
		return failf("the handshake reported lease_sec=%d; a node cannot honour the one-third renew rule without it", ok.LeaseSec)
	}
	if len(ok.PubKey) == 0 {
		return failf("the handshake carried no grant-signing key, so a node could not verify a grant offline")
	}
	if ok.Now == 0 {
		return failf("the handshake reported no server clock")
	}
	return nil
}

// ---- workspace generation and readiness ---------------------------------

func checkWSCreateShape(ctx context.Context, s *Session) error {
	ws, err := s.CreateWorkspace(ctx, WorkspaceSpec{Name: "conformance-create"})
	if err != nil {
		return err
	}
	if !strings.HasPrefix(ws.ID, "ws_") {
		return failf("ws.create returned id %q, which does not carry the ws_ prefix §1 assigns to workspaces", ws.ID)
	}
	if !containsString(WorkspaceStates, ws.State) {
		return failf("ws.create returned state %q, which is not in the lifecycle table %v", ws.State, WorkspaceStates)
	}
	if ws.CreatedAt == 0 {
		return failf("ws.create returned no creation time")
	}
	claimed, err := s.WaitClaimed(ctx, ws.ID)
	if err != nil {
		return err
	}
	if claimed.Generation == 0 {
		return failf("a claimed workspace reported generation 0; every claim increments the generation")
	}
	return nil
}

func checkWSIdempotentCreate(ctx context.Context, s *Session) error {
	key := s.Idem("idem")
	spec := WorkspaceSpec{Name: "conformance-idem", Requires: Requires{Backend: s.Target.Backend}}
	var first, second Workspace
	if err := s.Call(ctx, "ws.create", WSCreateReq{Spec: spec, Idem: key}, &first); err != nil {
		return err
	}
	s.track(first.ID)
	if err := s.Call(ctx, "ws.create", WSCreateReq{Spec: spec, Idem: key}, &second); err != nil {
		return failf("replaying ws.create with the same idempotency key failed: %v", err)
	}
	if second.ID != first.ID {
		return failf("replaying ws.create with key %q created a second workspace: %s then %s", key, first.ID, second.ID)
	}
	return nil
}

func checkWSReadyGatesClaimed(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	var ws Workspace
	if err := s.Call(ctx, "ws.get", WSGetReq{ID: f.WS.ID}, &ws); err != nil {
		return err
	}
	if ws.State != WSClaimed {
		return failf("the fixture workspace reports state %q, not %q", ws.State, WSClaimed)
	}
	if ws.Node == "" {
		return failf("a claimed workspace named no node; claimed means held and answering")
	}
	if ws.Generation == 0 {
		return failf("a claimed workspace reported generation 0")
	}
	// A claimed workspace must be reachable on the node it names: that is
	// what "a client never talks to a node that is still restoring" means in
	// observable terms.
	var info WSInfoRes
	if err := s.CallNode(ctx, ws.Node, "ws.info", WSGetReq{ID: ws.ID, Grant: &f.Grant}, &info); err != nil {
		return failf("the workspace is claimed but its node would not serve ws.info: %v", err)
	}
	if info.WS != ws.ID {
		return failf("ws.info answered for %q when asked about %q", info.WS, ws.ID)
	}
	return nil
}

func checkWSUnknownIsNotFound(ctx context.Context, s *Session) error {
	var ws Workspace
	err := s.Call(ctx, "ws.get", WSGetReq{ID: "ws_conformance_absent"}, &ws)
	if err == nil {
		return failf("ws.get of an absent id succeeded and returned %+v", ws)
	}
	if code := CodeOf(err); code != "not_found" {
		return failf("ws.get of an absent id answered %q, not %q (%v)", code, "not_found", err)
	}
	return nil
}

func checkWSSystemMountPathRefused(ctx context.Context, s *Session) error {
	for _, path := range []string{"/etc/conformance", "/proc/conformance", "relative/path", "/"} {
		spec := WorkspaceSpec{Name: "conformance-mount", MountPath: path, Requires: Requires{Backend: s.Target.Backend}}
		var ws Workspace
		err := s.Call(ctx, "ws.create", WSCreateReq{Spec: spec, Idem: s.Idem("mount")}, &ws)
		if err == nil {
			s.track(ws.ID)
			return failf("ws.create accepted mount_path %q; §5.2 refuses anything not absolute, clean and outside the system directories", path)
		}
		if code := CodeOf(err); code != "bad_request" {
			return failf("ws.create with mount_path %q answered %q, not %q (%v)", path, code, "bad_request", err)
		}
	}
	return nil
}

func checkWSListAgrees(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	var list WSListRes
	if err := s.Call(ctx, "ws.list", nil, &list); err != nil {
		return err
	}
	for _, ws := range list.Workspaces {
		if ws.ID != f.WS.ID {
			continue
		}
		var direct Workspace
		if err := s.Call(ctx, "ws.get", WSGetReq{ID: f.WS.ID}, &direct); err != nil {
			return err
		}
		if ws.Generation != direct.Generation {
			return failf("ws.list reports generation %d for %s while ws.get reports %d", ws.Generation, ws.ID, direct.Generation)
		}
		return nil
	}
	return failf("ws.list did not contain %s, a workspace this run created and ws.get serves", f.WS.ID)
}

func checkWSDestroyIsAbsorbing(ctx context.Context, s *Session) error {
	ws, err := s.CreateWorkspace(ctx, WorkspaceSpec{Name: "conformance-destroy"})
	if err != nil {
		return err
	}
	if _, err := s.WaitClaimed(ctx, ws.ID); err != nil {
		return err
	}
	if err := s.Call(ctx, "ws.destroy", WSGetReq{ID: ws.ID, Idem: s.Idem("destroy")}, nil); err != nil {
		return err
	}
	var after Workspace
	err = s.Call(ctx, "ws.get", WSGetReq{ID: ws.ID}, &after)
	switch {
	case IsCode(err, "not_found"):
	case err != nil:
		return failf("ws.get after destroy answered %q rather than not_found or a terminal state: %v", CodeOf(err), err)
	case after.State != "destroyed":
		return failf("ws.get after destroy reports state %q; destroyed is absorbing", after.State)
	}
	// A destroyed workspace must not be revivable.
	if err := s.Call(ctx, "ws.wake", WSGetReq{ID: ws.ID, Idem: s.Idem("wake")}, nil); err == nil {
		return failf("ws.wake revived a destroyed workspace")
	}
	return nil
}

// ---- claims, leases and stale-authority rejection ------------------------

func checkAuthGrantBinds(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	g, err := s.Grant(ctx, f.WS.ID)
	if err != nil {
		return err
	}
	switch {
	case len(g.Signature) == 0:
		return failf("the grant for %s carried no signature; §4 says a node verifies it offline", f.WS.ID)
	case g.Claims.WS != f.WS.ID:
		return failf("the grant binds workspace %q, not the requested %q", g.Claims.WS, f.WS.ID)
	case g.Claims.Client == "":
		return failf("the grant binds no client")
	case g.Claims.Node == "" && g.Node == "":
		return failf("the grant names no node")
	case g.Claims.Gen != f.WS.Generation:
		return failf("the grant binds generation %d while the workspace is at %d", g.Claims.Gen, f.WS.Generation)
	case g.Claims.ExpiresAt == 0:
		return failf("the grant has no expiry; §4 requires a node to reject one whose exp has passed")
	}
	return nil
}

func checkAuthGrantUnknownWorkspace(ctx context.Context, s *Session) error {
	var g Grant
	err := s.Call(ctx, "grant", GrantReq{WS: "ws_conformance_absent"}, &g)
	if err == nil {
		return failf("a grant was minted for a workspace that does not exist")
	}
	if code := CodeOf(err); !KnownCode(code) {
		return failf("a grant request for an absent workspace answered with the non-stable code %q (%v)", code, err)
	}
	return nil
}

func checkAuthStaleGenerationRefused(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	stale := f.Grant
	stale.Claims.Gen = f.WS.Generation + 7
	err = s.CallNode(ctx, f.Node, "fs.list", map[string]any{"ws": f.WS.ID, "path": ".", "grant": stale}, nil)
	if err == nil {
		return failf("the node accepted a grant bound to generation %d while holding generation %d", stale.Claims.Gen, f.WS.Generation)
	}
	if code := CodeOf(err); !KnownCode(code) {
		return failf("a stale-generation grant was refused with the non-stable code %q (%v)", code, err)
	}
	return nil
}

func checkAuthWrongWorkspaceRefused(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	other, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-other"})
	if err != nil {
		return err
	}
	// A validly signed grant for `other`, presented on a request naming the
	// shared fixture, must not authorize anything.
	err = s.CallNode(ctx, f.Node, "fs.list", map[string]any{"ws": f.WS.ID, "path": ".", "grant": other.Grant}, nil)
	if err == nil {
		return failf("the node accepted a grant for %s on a request naming %s", other.WS.ID, f.WS.ID)
	}
	if code := CodeOf(err); !KnownCode(code) {
		return failf("a mismatched-workspace grant was refused with the non-stable code %q (%v)", code, err)
	}
	return nil
}

func checkAuthNoGrantRefused(ctx context.Context, s *Session) error {
	f, err := s.Fixture(ctx)
	if err != nil {
		return err
	}
	// A fresh connection, so no grant is cached for this client on the node.
	c, err := Dial(ctx, DialOptions{Endpoint: s.Target.Endpoint, Token: s.Target.Token, Caps: []string{CapabilityV1}})
	if err != nil {
		return err
	}
	defer c.Close()
	err = c.Request(ctx, f.Node, "fs.list", map[string]any{"ws": f.WS.ID, "path": "."}, nil)
	if err == nil {
		return failf("the node served fs.list for %s to a client that presented no grant", f.WS.ID)
	}
	if code := CodeOf(err); !KnownCode(code) {
		return failf("an ungranted node request was refused with the non-stable code %q (%v)", code, err)
	}
	return nil
}

func checkAuthMoveInvalidatesGrant(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-move-grant"})
	if err != nil {
		return err
	}
	before := f.Grant
	beforeGen := f.WS.Generation
	if err := s.Call(ctx, "ws.move", WSMoveReq{ID: f.WS.ID, Idem: s.Idem("move")}, nil); err != nil {
		return err
	}
	if err := s.Refresh(ctx, f); err != nil {
		return err
	}
	if f.WS.Generation <= beforeGen {
		return failf("a move left the generation at %d; §5 says every claim increments it", f.WS.Generation)
	}
	err = s.CallNode(ctx, f.Node, "fs.list", map[string]any{"ws": f.WS.ID, "path": ".", "grant": before}, nil)
	if err == nil {
		return failf("a grant minted at generation %d still worked after the workspace moved to generation %d", beforeGen, f.WS.Generation)
	}
	if code := CodeOf(err); !KnownCode(code) {
		return failf("a post-move stale grant was refused with the non-stable code %q (%v)", code, err)
	}
	return nil
}

// checkAuthLeaseExpiry is reached only when the target declares that the
// runner may stop one of its nodes. Nothing this release can launch satisfies
// that -- a standalone's node and control plane are one process, so stopping
// the node stops the observer too -- and saying so is the honest outcome.
func checkAuthLeaseExpiry(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares node-fault injection available, but this runner can only stop a node by stopping the control plane that would have to observe the expiry", s.Target.Name)
}

func containsString(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}

// checkNegControllerEpochIsFenced is the capability-gated half of
// negotiation: §3.2 says a peer that negotiated controller-epoch is told a
// nonzero epoch and that every event persists it, which is what lets a reader
// tell a superseded controller's decisions from the live one's.
func checkNegControllerEpochIsFenced(ctx context.Context, s *Session) error {
	epoch := s.Control.HelloOK.ControllerEpoch
	if epoch == 0 {
		return failf("controller-epoch was negotiated but the handshake reported epoch 0; there is nothing to fence against")
	}
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-epoch"})
	if err != nil {
		return err
	}
	events, err := s.Tail(ctx, 1, f.WS.ID)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("creating and claiming %s produced no events to carry the epoch", f.WS.ID)
	}
	for _, e := range events {
		if e.ControllerEpoch != epoch {
			return failf("event %d of type %s carries controller epoch %d while the handshake reported %d", e.Seq, e.Type, e.ControllerEpoch, epoch)
		}
	}
	return nil
}
