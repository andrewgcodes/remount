package sim

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

func TestCrossTenantCannotForgeSessionOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture runs the POSIX cat program")
	}
	w := newWorldWith(t, func(o *server.Options) {
		o.Authenticator = control.StaticAuthenticator{
			"owner-test-token":    {ID: "owner", Tenant: "team"},
			"attacker-test-token": {ID: "attacker", Tenant: "rivals"},
		}
	})
	w.node("n1", nil)
	owner := w.clientWithToken("owner", "owner-test-token")
	attacker := w.clientWithToken("attacker", "attacker-test-token")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, owner, proto.WorkspaceSpec{})
	s, err := owner.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"cat"}, Stdin: true})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case chunk := <-s.Chunks():
		if chunk.Stream != proto.StreamInfo || chunk.Seq != 0 {
			t.Fatalf("first chunk: %+v", chunk)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := attacker.GetWorkspace(ctx, ws.ID); err == nil {
		t.Fatal("attacker can read victim workspace")
	}
	peer, err := attacker.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, body := range []proto.ChunkBody{
		{Stream: proto.StreamStdout, Data: []byte("FORGED")},
		{Stream: proto.StreamExit, Data: proto.MustMarshal(proto.ExitInfo{Code: 99})},
		{Stream: proto.StreamGap, Data: proto.MustMarshal(proto.Gap{From: 1, To: 100})},
	} {
		if err := peer.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindChunk, To: owner.ID(), From: "n1", S: s.ID, WS: ws.ID, Seq: uint64(i + 1), Body: proto.MustMarshal(body)}); err != nil {
			t.Fatal(err)
		}
	}
	// This round trip orders every injected frame before the real node's
	// output, without a timing-based "nothing arrived" assertion.
	if _, err := attacker.ListWorkspaces(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Input(ctx, []byte("genuine output"), true); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	for chunk := range s.Chunks() {
		if chunk.Stream == proto.StreamGap {
			t.Fatal("forged gap was delivered")
		}
		if chunk.Stream == proto.StreamStdout {
			stdout.Write(chunk.Data)
		}
	}
	exit, err := s.Wait(ctx)
	if err != nil || exit.Code != 0 || stdout.String() != "genuine output" {
		t.Fatalf("output=%q exit=%+v err=%v", stdout.String(), exit, err)
	}
	// Reconnect and replay must keep the grant-derived producer binding.
	w.cut("owner")
	replayed, err := owner.Attach(ctx, ws.ID, s.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	for chunk := range replayed.Chunks() {
		if chunk.Stream == proto.StreamStdout {
			stdout.Write(chunk.Data)
		}
	}
	if stdout.String() != "genuine output" {
		t.Fatalf("replay=%q", stdout.String())
	}
}

func TestReusedWorkspaceHonorsRequestedSecurity(t *testing.T) {
	w := newWorld(t, control.Binding{ID: "b_openai", Secret: "synthetic-test-secret", Destinations: []string{"api.openai.com"}, Placeholder: "synthetic-test-placeholder"})
	w.node("n1", nil)
	c := w.client("owner")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{Bindings: []string{"b_openai"}, Requires: proto.Requires{Backend: "process"}})
	r, err := launch.Load("custom")
	if err != nil {
		t.Fatal(err)
	}
	b, err := launch.ParseBinding("b_openai")
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{proto.SecurityIsolated, proto.SecurityMultiTenant} {
		called := false
		result, err := launch.Start(ctx, c, launch.Options{
			Recipe: r, WS: ws.ID, Args: []string{"true"}, Bindings: []launch.Binding{b}, Security: profile,
			BeforeOpen: func(context.Context, *proto.Workspace) error { called = true; return nil },
		})
		if !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) || result != nil || called {
			t.Fatalf("%s launch was not refused before side effects: result=%+v callback=%v err=%v", profile, result, called, err)
		}
	}
	sessions, err := c.ListSessions(ctx, ws.ID)
	if err != nil || len(sessions) != 0 {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
	if _, err := c.Stat(ctx, ws.ID, r.LauncherPath()); !errors.Is(err, &proto.Error{Code: proto.CodeNotFound}) {
		t.Fatalf("launcher should not exist: %v", err)
	}
}
