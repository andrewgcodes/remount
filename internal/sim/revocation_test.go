package sim

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// TestRevocationClosesSessionsWithinOneRenew is the authz-push proof: an
// owner removes a principal from the ACL mid-session, the node learns the new
// revision on its next renew (at most a third of the two-second lease used
// here), the revoked principal's live PTY ends with exit{reason: revoked}, its
// next call is refused, and the other principal's session and grant carry on.
func TestRevocationClosesSessionsWithinOneRenew(t *testing.T) {
	w := newWorldWith(t, func(o *server.Options) {
		o.LeaseSec = expiringLeaseSec // the test waits one renewal
		o.Authenticator = control.StaticAuthenticator{
			"owner-tok": {ID: "owner", Tenant: "team"},
			"guest-tok": {ID: "guest", Tenant: "team"},
			"other-tok": {ID: "other", Tenant: "team"},
		}
	})
	w.node("n1", nil)
	owner := w.clientWithToken("owner", "owner-tok")
	guest := w.clientWithToken("guest", "guest-tok")
	other := w.clientWithToken("other", "other-tok")
	ctx := ctxT(t, 60*time.Second)

	ws := mustWS(t, owner, proto.WorkspaceSpec{ACL: proto.WorkspaceACL{Writers: []string{"guest", "other"}}})
	kind := proto.SessionPTY
	program := []string{"sleep", "60"}
	if runtime.GOOS == "windows" {
		kind = proto.SessionExec
		program = []string{"ping.exe", "-n", "60", "127.0.0.1"}
	}
	guestPTY, err := guest.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: kind, Program: program, Rows: 20, Cols: 80})
	if err != nil {
		t.Fatalf("guest exec: %v", err)
	}
	otherPTY, err := other.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: kind, Program: program, Rows: 20, Cols: 80})
	if err != nil {
		t.Fatalf("other exec: %v", err)
	}
	// A plain exec session by the revoked principal must go too: closure is
	// per principal, not per session kind.
	guestExec, err := guest.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: program})
	if err != nil {
		t.Fatalf("guest exec: %v", err)
	}
	if _, err := guest.ReadFile(ctx, ws.ID, "missing"); err == nil || codeOf(err) != proto.CodeNotFound {
		t.Fatalf("guest should be authorized before revocation, got %v", err)
	}
	before, _ := owner.GetWorkspace(ctx, ws.ID)

	revokedAt := time.Now()
	updated, err := owner.SetWorkspaceACL(ctx, ws.ID, proto.WorkspaceACL{Writers: []string{"other"}})
	if err != nil {
		t.Fatalf("acl: %v", err)
	}
	if updated.AuthzRevision != before.AuthzRevision+1 || len(updated.Revocations) != 1 || updated.Revocations[0].Principal != "guest" {
		t.Fatalf("acl result %+v", updated)
	}

	// Control refuses to mint a grant for the revoked principal immediately,
	// before any node has heard about the change.
	if _, err := guest.ReadFile(ctx, ws.ID, "missing"); err == nil || codeOf(err) != proto.CodeDenied {
		t.Fatalf("revoked principal should be refused a grant, got %v", err)
	}

	// The node closes the revoked principal's sessions within one renew
	// interval (lease/3 = 667 ms here); allow the race detector some slack.
	for _, s := range []*client.Session{guestPTY, guestExec} {
		exit, err := s.Wait(ctxT(t, 10*time.Second))
		if err != nil {
			t.Fatalf("revoked session wait: %v", err)
		}
		if exit.Reason != proto.ExitReasonRevoked {
			t.Fatalf("revoked session exit %+v", exit)
		}
	}
	if elapsed := time.Since(revokedAt); elapsed > 5*time.Second {
		t.Fatalf("revocation took %v, bound is one renew interval", elapsed)
	}

	// The other principal is unaffected: its PTY is still running and it can
	// keep calling, on a fresh grant minted at the new revision.
	select {
	case <-otherPTY.Done():
		t.Fatalf("unaffected session closed: %+v", otherPTY.Exit())
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := other.ReadFile(ctx, ws.ID, "missing"); err == nil || codeOf(err) != proto.CodeNotFound {
		t.Fatalf("other should still be authorized, got %v", err)
	}
	if err := otherPTY.Signal(ctx, "TERM"); err != nil {
		t.Fatalf("other signal: %v", err)
	}
	if exit, err := otherPTY.Wait(ctx); err != nil || exit.Reason == proto.ExitReasonRevoked {
		t.Fatalf("other exit %v %+v", err, exit)
	}

	// A writer may not change the ACL; only the owner or an administrator.
	if _, err := other.SetWorkspaceACL(ctx, ws.ID, proto.WorkspaceACL{}); err == nil || codeOf(err) != proto.CodeDenied {
		t.Fatalf("writer changing acl: %v", err)
	}

	// The event log records the change and the node-side closure.
	events, err := owner.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sawACL, sawRevoked bool
	for _, e := range events {
		switch e.Type {
		case proto.EvWSACL:
			sawACL = true
		case proto.EvAuthzRevoked:
			sawRevoked = true
		}
	}
	if !sawACL || !sawRevoked {
		t.Fatalf("events acl=%v revoked=%v", sawACL, sawRevoked)
	}
}

func codeOf(err error) string {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}
