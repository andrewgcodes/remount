package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	if stored := f.c.bindings[bindingKey("", "b_external")]; stored.Secret != "" || stored.Source != "env://TOKEN" {
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

// operatorSubject is the tenant operator that owns binding administration.
func operatorSubject() Subject {
	return Subject{ID: "op", Tenant: "tenant-a", Roles: []string{"tenant_admin"}}
}

func sampleBindingSpec(id string) proto.BindingSpec {
	return proto.BindingSpec{
		ID: id, Kind: proto.BindingKindAPIKey, Secret: "sk-real-value",
		Destinations: []string{"api.example"}, Placeholder: "sk-placeholder",
		Methods: []string{"POST"}, PathPrefixes: []string{"/v1/"},
		Retention: proto.BindingRetention{NoLog: true, Note: "zdr"},
	}
}

func reasonOf(err error) string {
	var protocol *proto.Error
	if errors.As(err, &protocol) {
		return protocol.Reason
	}
	return ""
}

// TestBindingLifecycleCreateListGetRotateRevoke walks the whole durable
// lifecycle and asserts the fingerprint the lease path reports moves with it.
func TestBindingLifecycleCreateListGetRotateRevoke(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()

	created, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{
		Binding: sampleBindingSpec("b_model"), IdempotencyKey: "idem-create",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != 1 || created.Tenant != "tenant-a" || created.Kind != proto.BindingKindAPIKey {
		t.Fatalf("created binding = %+v", created)
	}
	if created.Secret != "" {
		t.Fatal("create response carried the secret")
	}

	listed, err := f.c.bindingList(ctx, actor, &proto.BindingListReq{})
	if err != nil || len(listed.Bindings) != 1 || listed.Bindings[0].ID != "b_model" {
		t.Fatalf("list = %+v, %v", listed, err)
	}
	got, err := f.c.bindingGet(ctx, actor, &proto.BindingGetReq{ID: "b_model"})
	if err != nil || got.Secret != "" || !got.Retention.NoLog {
		t.Fatalf("get = %+v, %v", got, err)
	}

	f.c.mu.Lock()
	before := f.c.bindingSetRevisionLocked("tenant-a", []string{"b_model"})
	f.c.mu.Unlock()

	rotated, err := f.c.bindingRotate(ctx, actor, &proto.BindingRotateReq{
		ID: "b_model", Secret: "sk-rotated", IdempotencyKey: "idem-rotate",
	})
	if err != nil || rotated.Revision != 2 || rotated.RotatedAt == 0 || rotated.Secret != "" {
		t.Fatalf("rotate = %+v, %v", rotated, err)
	}
	f.c.mu.Lock()
	afterRotate := f.c.bindingSetRevisionLocked("tenant-a", []string{"b_model"})
	stored := f.c.bindings[bindingKey("tenant-a", "b_model")]
	f.c.mu.Unlock()
	if afterRotate == before {
		t.Fatal("rotation did not move the binding-set fingerprint")
	}
	if stored.Secret != "sk-rotated" {
		t.Fatalf("rotation did not replace the credential: %q", stored.Secret)
	}

	revoked, err := f.c.bindingRevoke(ctx, actor, &proto.BindingRevokeReq{
		ID: "b_model", Reason: "leaked", IdempotencyKey: "idem-revoke",
	})
	if err != nil || revoked.RevokedAt == 0 || revoked.RevokedReason != "leaked" {
		t.Fatalf("revoke = %+v, %v", revoked, err)
	}
	f.c.mu.Lock()
	afterRevoke := f.c.bindingSetRevisionLocked("tenant-a", []string{"b_model"})
	cleared := f.c.bindings[bindingKey("tenant-a", "b_model")]
	f.c.mu.Unlock()
	if afterRevoke == afterRotate {
		t.Fatal("revocation did not move the binding-set fingerprint")
	}
	if cleared.Secret != "" || cleared.Source != "" {
		t.Fatal("revoked binding retained a credential")
	}
	// A revoked binding is retained for audit but is not offered as usable.
	visible, err := f.c.bindingList(ctx, actor, &proto.BindingListReq{})
	if err != nil || len(visible.Bindings) != 0 {
		t.Fatalf("revoked binding still listed: %+v, %v", visible, err)
	}
	retained, err := f.c.bindingList(ctx, actor, &proto.BindingListReq{IncludeRevoked: true})
	if err != nil || len(retained.Bindings) != 1 {
		t.Fatalf("revoked binding was not retained for audit: %+v, %v", retained, err)
	}
	if ids := f.c.Bindings(); len(ids) != 0 {
		t.Fatalf("revoked binding still offered to the CLI: %v", ids)
	}
	if _, err := f.c.bindingRotate(ctx, actor, &proto.BindingRotateReq{
		ID: "b_model", Secret: "sk-again", IdempotencyKey: "idem-rotate-after-revoke",
	}); reasonOf(err) != proto.ReasonRevoked {
		t.Fatalf("rotate after revoke = %v", err)
	}
}

// TestBindingSecretNeverEntersAResponseOrEvent is the leak proof: every API
// response and the whole event log are scanned for the credential value.
func TestBindingSecretNeverEntersAResponseOrEvent(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()
	const canary = "sk-canary-9f3ac1"

	spec := sampleBindingSpec("b_secretive")
	spec.Secret = canary
	created, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{Binding: spec, IdempotencyKey: "idem-secret"})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := f.c.bindingRotate(ctx, actor, &proto.BindingRotateReq{
		ID: "b_secretive", Secret: canary + "-rotated", IdempotencyKey: "idem-secret-rotate",
	})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := f.c.bindingList(ctx, actor, &proto.BindingListReq{})
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := f.c.bindingRevoke(ctx, actor, &proto.BindingRevokeReq{ID: "b_secretive", IdempotencyKey: "idem-secret-revoke"})
	if err != nil {
		t.Fatal(err)
	}
	responses := fmt.Sprintf("%+v%+v%+v%+v", created, rotated, listed, revoked)
	if strings.Contains(responses, canary) {
		t.Fatal("a binding response carried the credential")
	}

	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, event := range events {
		if strings.Contains(fmt.Sprintf("%+v", event), canary) || bytes.Contains(event.Payload, []byte(canary)) {
			t.Fatalf("event %s carried the credential", event.Type)
		}
		switch event.Type {
		case proto.EvBindingCreated, proto.EvBindingRotated, proto.EvBindingRevoked:
			found++
		}
	}
	if found != 3 {
		t.Fatalf("binding lifecycle emitted %d of 3 events", found)
	}
	// Prove the scan can see the value at all, so a clean result is evidence
	// rather than a broken instrument.
	if !bytes.Contains(proto.MustMarshal(map[string]any{"secret": canary}), []byte(canary)) {
		t.Fatal("credential scan cannot detect a planted value")
	}
}

// TestBindingsSurviveControlRestart proves the store is the authority: a
// binding created through the API and one seeded from --bindings both come
// back, and a revocation is not undone by restarting with the original file.
func TestBindingsSurviveControlRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings-restart.db")
	seed := []Binding{{ID: "b_seeded", Secret: "file-secret", Destinations: []string{"seed.example"}}}
	f1 := newControlFixture(t, path, func(opts *Options) { opts.Bindings = seed })
	ctx := context.Background()
	actor := operatorSubject()
	if _, err := f1.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{
		Binding: sampleBindingSpec("b_api"), IdempotencyKey: "idem-restart-create",
	}); err != nil {
		t.Fatal(err)
	}
	if err := f1.log.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newControlFixture(t, path, func(opts *Options) { opts.Bindings = seed })
	f2.c.mu.Lock()
	seeded, hasSeed := f2.c.bindings[bindingKey("", "b_seeded")]
	fromAPI, hasAPI := f2.c.bindings[bindingKey("tenant-a", "b_api")]
	f2.c.mu.Unlock()
	if !hasSeed || seeded.Secret != "file-secret" || seeded.Revision != 1 {
		t.Fatalf("seeded binding after restart = %+v (present=%v)", seeded, hasSeed)
	}
	if !hasAPI || fromAPI.Secret != "sk-real-value" || fromAPI.Revision != 1 {
		t.Fatalf("api binding after restart = %+v (present=%v)", fromAPI, hasAPI)
	}
	revoked, err := f2.c.bindingRevoke(ctx, actor, &proto.BindingRevokeReq{
		ID: "b_api", Reason: "rotation drill", IdempotencyKey: "idem-restart-revoke",
	})
	if err != nil || revoked.RevokedAt == 0 {
		t.Fatalf("revoke after restart = %+v, %v", revoked, err)
	}
	if err := f2.log.Close(); err != nil {
		t.Fatal(err)
	}

	f3 := newControlFixture(t, path, func(opts *Options) { opts.Bindings = seed })
	f3.c.mu.Lock()
	stillRevoked := f3.c.bindings[bindingKey("tenant-a", "b_api")]
	seedCount := 0
	for _, b := range f3.c.bindings {
		if b.ID == "b_seeded" {
			seedCount++
		}
	}
	f3.c.mu.Unlock()
	if stillRevoked.RevokedAt == 0 || stillRevoked.Secret != "" {
		t.Fatalf("restart resurrected a revoked binding: %+v", stillRevoked)
	}
	if seedCount != 1 {
		t.Fatalf("seeding duplicated the file binding %d times", seedCount)
	}
}

// TestBindingMutationsReplayIdempotently proves a retried create, rotate or
// revoke returns the original result and emits no second event.
func TestBindingMutationsReplayIdempotently(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()
	req := &proto.BindingCreateReq{Binding: sampleBindingSpec("b_replay"), IdempotencyKey: "idem-replay"}
	first, err := f.c.bindingCreate(ctx, actor, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.c.bindingCreate(ctx, actor, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != second.Revision || first.CreatedAt != second.CreatedAt {
		t.Fatalf("create replay diverged: %+v vs %+v", first, second)
	}

	rotate := &proto.BindingRotateReq{ID: "b_replay", Secret: "sk-rot", IdempotencyKey: "idem-replay-rotate"}
	r1, err := f.c.bindingRotate(ctx, actor, rotate)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := f.c.bindingRotate(ctx, actor, rotate)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Revision != r2.Revision || r2.Revision != 2 {
		t.Fatalf("rotate replay diverged: %+v vs %+v", r1, r2)
	}

	revoke := &proto.BindingRevokeReq{ID: "b_replay", Reason: "done", IdempotencyKey: "idem-replay-revoke"}
	v1, err := f.c.bindingRevoke(ctx, actor, revoke)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := f.c.bindingRevoke(ctx, actor, revoke)
	if err != nil {
		t.Fatal(err)
	}
	if v1.RevokedAt != v2.RevokedAt || v1.Revision != v2.Revision {
		t.Fatalf("revoke replay diverged: %+v vs %+v", v1, v2)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts[proto.EvBindingCreated] != 1 || counts[proto.EvBindingRotated] != 1 || counts[proto.EvBindingRevoked] != 1 {
		t.Fatalf("idempotent replay emitted duplicate events: %v", counts)
	}
}

// TestBindingLeaseRefusesRevokedAndMissing pins the reasons a node receives.
func TestBindingLeaseRefusesRevokedAndMissing(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()
	if _, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{
		Binding: sampleBindingSpec("b_lease"), IdempotencyKey: "idem-lease-create",
	}); err != nil {
		t.Fatal(err)
	}
	connectNode(t, f.c, "n_one", processNodeInfo(1024))
	ws := createWorkspace(t, f.c, actor, proto.WorkspaceSpec{Bindings: []string{"b_lease"}})
	claim, err := f.c.wsClaim(ctx, "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	gen := claim.Workspace.Generation
	lease, err := f.c.bindingLease(ctx, "n_one", ws.ID, gen)
	if err != nil || len(lease.Leases) != 1 {
		t.Fatalf("lease = %+v, %v", lease, err)
	}
	if lease.Leases[0].Secret != "sk-real-value" || lease.Leases[0].Revision != 1 || lease.Leases[0].Generation != gen {
		t.Fatalf("lease did not carry the credential, revision and generation: %+v", lease.Leases[0])
	}
	if lease.Revision == 0 {
		t.Fatal("lease response carried no binding-set fingerprint")
	}
	if lease.Leases[0].Kind != proto.BindingKindAPIKey || len(lease.Leases[0].PathPrefixes) != 1 {
		t.Fatalf("lease dropped binding policy: %+v", lease.Leases[0])
	}

	if _, err := f.c.bindingRevoke(ctx, actor, &proto.BindingRevokeReq{ID: "b_lease", IdempotencyKey: "idem-lease-revoke"}); err != nil {
		t.Fatal(err)
	}
	_, err = f.c.bindingLease(ctx, "n_one", ws.ID, gen)
	if codeOfError(err) != proto.CodeUnauthorized || reasonOf(err) != proto.ReasonRevoked {
		t.Fatalf("lease after revoke = %v", err)
	}

	f.c.mu.Lock()
	delete(f.c.bindings, bindingKey("tenant-a", "b_lease"))
	f.c.mu.Unlock()
	_, err = f.c.bindingLease(ctx, "n_one", ws.ID, gen)
	if codeOfError(err) != proto.CodeNotFound || reasonOf(err) != proto.ReasonBindingMissing {
		t.Fatalf("lease with a missing binding = %v", err)
	}
}

// TestBindingTenantIsolation proves one tenant cannot read, rotate or revoke
// another tenant's binding, nor name it in its own workspace.
func TestBindingTenantIsolation(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	owner := operatorSubject()
	stranger := Subject{ID: "mallory", Tenant: "tenant-b", Roles: []string{"tenant_admin"}}
	if _, err := f.c.bindingCreate(ctx, owner, &proto.BindingCreateReq{
		Binding: sampleBindingSpec("b_private"), IdempotencyKey: "idem-private",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.bindingGet(ctx, stranger, &proto.BindingGetReq{ID: "b_private"}); reasonOf(err) != proto.ReasonBindingMissing {
		t.Fatalf("cross-tenant get = %v", err)
	}
	if _, err := f.c.bindingRevoke(ctx, stranger, &proto.BindingRevokeReq{ID: "b_private", IdempotencyKey: "idem-x"}); err == nil {
		t.Fatal("cross-tenant revoke succeeded")
	}
	listed, err := f.c.bindingList(ctx, stranger, &proto.BindingListReq{})
	if err != nil || len(listed.Bindings) != 0 {
		t.Fatalf("cross-tenant list = %+v, %v", listed, err)
	}
	if _, err := f.c.wsCreate(ctx, stranger, &proto.WSCreateReq{
		Spec: proto.WorkspaceSpec{Bindings: []string{"b_private"}},
	}); reasonOf(err) != proto.ReasonBindingMissing {
		t.Fatalf("cross-tenant workspace bound another tenant's credential: %v", err)
	}
}

// TestBindingCreateRejectsMalformedSpecs keeps the validation contract honest.
func TestBindingCreateRejectsMalformedSpecs(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()
	cases := map[string]proto.BindingSpec{
		"no id":            {Secret: "s", Destinations: []string{"h"}},
		"no credential":    {ID: "b_x", Destinations: []string{"h"}},
		"both credentials": {ID: "b_x", Secret: "s", Source: "env://X", Destinations: []string{"h"}},
		"no destination":   {ID: "b_x", Secret: "s"},
		"bad kind":         {ID: "b_x", Secret: "s", Destinations: []string{"h"}, Kind: "oauth"},
		"lowercase method": {ID: "b_x", Secret: "s", Destinations: []string{"h"}, Methods: []string{"post"}},
		"relative path":    {ID: "b_x", Secret: "s", Destinations: []string{"h"}, PathPrefixes: []string{"v1/"}},
	}
	for name, spec := range cases {
		if _, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{Binding: spec, IdempotencyKey: "idem-" + name}); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	if _, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{Binding: sampleBindingSpec("b_ok")}); err == nil {
		t.Fatal("create without an idempotency key was accepted")
	}
}

// A cookie is a distinct binding kind by design: browser session state must
// never be mixed into an API-key binding's destination scope.
func TestCookieBindingIsADistinctKind(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	actor := operatorSubject()
	spec := proto.BindingSpec{
		ID: "b_login", Kind: proto.BindingKindCookie, Secret: "session=abc",
		Destinations: []string{"app.example"}, Placeholder: "session=placeholder",
	}
	created, err := f.c.bindingCreate(ctx, actor, &proto.BindingCreateReq{Binding: spec, IdempotencyKey: "idem-cookie"})
	if err != nil || created.Kind != proto.BindingKindCookie {
		t.Fatalf("cookie binding = %+v, %v", created, err)
	}
	connectNode(t, f.c, "n_one", processNodeInfo(1024))
	ws := createWorkspace(t, f.c, actor, proto.WorkspaceSpec{Bindings: []string{"b_login"}})
	claim, err := f.c.wsClaim(ctx, "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.c.bindingLease(ctx, "n_one", ws.ID, claim.Workspace.Generation)
	if err != nil || len(lease.Leases) != 1 || lease.Leases[0].Kind != proto.BindingKindCookie {
		t.Fatalf("cookie lease = %+v, %v", lease, err)
	}
}
