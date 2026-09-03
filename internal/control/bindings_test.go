package control

import (
	"context"
	"sync"
	"testing"

	"remount.dev/remount/internal/proto"
)

type blockingSecretResolver struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	value   string
}

func (r *blockingSecretResolver) Resolve(ctx context.Context, _ string) (string, error) {
	r.once.Do(func() { close(r.started) })
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.release:
		return r.value, nil
	}
}

func TestBindingLeaseRevalidatesGenerationAfterSourceResolution(t *testing.T) {
	resolver := &blockingSecretResolver{started: make(chan struct{}), release: make(chan struct{}), value: "resolved-secret"}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.SecretResolver = resolver
		opts.Bindings = []Binding{{ID: "b_external", Source: "env://TOKEN", Destinations: []string{"api.example"}}}
	})
	connectNode(t, f.c, "n_one", processNodeInfo(1024))
	created := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Bindings: []string{"b_external"}})
	claim, err := f.c.wsClaim(context.Background(), "n_one", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: created.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.c.bindingLease(context.Background(), "n_one", created.ID, claim.Workspace.Generation)
		done <- err
	}()
	<-resolver.started
	// Model a committed authority transfer while the provider is blocked. The
	// regression assertion is that the old node cannot receive the resolved
	// credential after this generation fence changes.
	f.c.mu.Lock()
	f.c.workspaces[created.ID].Generation++
	f.c.mu.Unlock()
	close(resolver.release)
	if err := <-done; err == nil {
		t.Fatal("stale node received a resolved secret after generation changed")
	}
	if stored := f.c.bindings["b_external"]; stored.Secret != "" || stored.Source != "env://TOKEN" {
		t.Fatalf("resolved value entered durable binding state: %+v", stored)
	}
}

type staticSecretResolver string

func (r staticSecretResolver) Resolve(context.Context, string) (string, error) { return string(r), nil }

func TestBindingLeaseEnforcesWorkspaceScope(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) {
		opts.SecretResolver = staticSecretResolver("secret")
		opts.Bindings = []Binding{{ID: "b_scoped", Source: "env://TOKEN", Destinations: []string{"api.example"}, Workspaces: []string{"ws_other"}}}
	})
	connectNode(t, f.c, "n_one", processNodeInfo(1024))
	created := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Bindings: []string{"b_scoped"}})
	claim, err := f.c.wsClaim(context.Background(), "n_one", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.c.bindingLease(context.Background(), "n_one", created.ID, claim.Workspace.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Leases) != 0 {
		t.Fatalf("workspace outside binding scope received lease: %+v", res.Leases)
	}
}
