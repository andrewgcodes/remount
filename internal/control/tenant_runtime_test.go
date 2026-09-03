package control

import (
	"context"
	"testing"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/tenant"
)

func tenantControlFixture(t *testing.T) *controlFixture {
	t.Helper()
	return newControlFixture(t, "", func(options *Options) {
		store, err := tenant.NewStore(options.DB, options.Log, tenant.Options{})
		if err != nil {
			t.Fatal(err)
		}
		options.Tenants = store
	})
}

func TestTenantRuntimeWorkspaceAdmissionIsAtomicAndScoped(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	global := Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}}
	policy := proto.TenantPolicy{Quotas: proto.TenantQuotas{MaxWorkspaces: 1}}
	if _, err := f.c.tenantCreate(ctx, global, &proto.TenantCreateReq{ID: "tenant-a", Policy: policy, IdempotencyKey: "create-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.tenantCreate(ctx, global, &proto.TenantCreateReq{ID: "tenant-b", Policy: policy, IdempotencyKey: "create-b"}); err != nil {
		t.Fatal(err)
	}
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}
	bob := Subject{ID: "bob", Tenant: "tenant-b", Roles: []string{"admin"}}
	request := &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "a"}, IdempotencyKey: "workspace-a"}
	first, err := f.c.wsCreate(ctx, alice, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.c.wsCreate(ctx, alice, request)
	if err != nil || replay.ID != first.ID {
		t.Fatalf("idempotent create=%+v err=%v", replay, err)
	}
	if _, err := f.c.wsCreate(ctx, alice, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "over"}, IdempotencyKey: "workspace-over"}); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("over-quota workspace=%v", err)
	}
	if _, err := f.c.wsCreate(ctx, bob, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "b"}, IdempotencyKey: "workspace-b"}); err != nil {
		t.Fatalf("other tenant admission shared quota: %v", err)
	}
	usage, err := f.c.tenantUsage(ctx, alice, &proto.TenantUsageReq{})
	if err != nil || usage.Workspaces != 1 {
		t.Fatalf("usage=%+v err=%v", usage, err)
	}

	f.c.mu.Lock()
	f.c.subjects["c_alice"] = alice
	f.c.subjects["c_bob"] = bob
	f.c.mu.Unlock()
	aliceList, err := f.c.wsListAuthorized(ctx, "c_alice")
	if err != nil || len(aliceList.Workspaces) != 1 || aliceList.Workspaces[0].Tenant != "tenant-a" {
		t.Fatalf("Alice list=%+v err=%v", aliceList, err)
	}
	bobList, err := f.c.wsListAuthorized(ctx, "c_bob")
	if err != nil || len(bobList.Workspaces) != 1 || bobList.Workspaces[0].Tenant != "tenant-b" {
		t.Fatalf("Bob list=%+v err=%v", bobList, err)
	}
	if _, err := f.c.tenantGet(ctx, alice, "tenant-b"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant policy read=%v", err)
	}

	events, err := f.log.Read(ctx, 0, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var created, reserved, exceeded bool
	for _, event := range events {
		if event.Tenant != "tenant-a" {
			continue
		}
		switch event.Type {
		case proto.EvWSCreated:
			created = true
		case "quota.reserved":
			reserved = true
		case "quota.exceeded":
			exceeded = true
		}
	}
	if !created || !reserved || !exceeded {
		t.Fatalf("workspace/quota evidence created=%v reserved=%v exceeded=%v", created, reserved, exceeded)
	}
}

func TestTenantResidencyAppliedAtClaimBoundary(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	global := Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}}
	policy := proto.TenantPolicy{Residency: proto.TenantResidency{AllowedRegions: []string{"us-east-1"}, RequiredLabels: map[string]string{"tier": "confidential"}}}
	if _, err := f.c.tenantCreate(ctx, global, &proto.TenantCreateReq{ID: "tenant-a", Policy: policy, IdempotencyKey: "create-a"}); err != nil {
		t.Fatal(err)
	}
	connectNode(t, f.c, "n_wrong", processNodeInfo(4096))
	connectNode(t, f.c, "n_right", processNodeInfo(4096))
	f.c.mu.Lock()
	f.c.nodes["n_wrong"].Status.Labels = map[string]string{"region": "us-west-2", "tier": "confidential"}
	f.c.nodes["n_right"].Status.Labels = map[string]string{"region": "us-east-1", "tier": "confidential"}
	f.c.mu.Unlock()
	workspace, err := f.c.wsCreate(ctx, Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}, &proto.WSCreateReq{IdempotencyKey: "ws-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.wsClaim(ctx, "n_wrong", workspace.ID); codeOf(err) != proto.CodeDenied {
		t.Fatalf("wrong residency claim=%v", err)
	}
	if _, err := f.c.wsClaim(ctx, "n_right", workspace.ID); err != nil {
		t.Fatalf("matching residency claim=%v", err)
	}
}
