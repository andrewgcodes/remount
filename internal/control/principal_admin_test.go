package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

type fakePrincipalAuthority struct {
	values map[string]proto.Principal
	issued int
}

func (f *fakePrincipalAuthority) CreatePrincipal(_ context.Context, tenant, subject string, roles []string, _ string) (proto.Principal, error) {
	key := tenant + "\x00" + subject
	if _, ok := f.values[key]; ok {
		return proto.Principal{}, errors.New("duplicate")
	}
	value := proto.Principal{ID: subject, Tenant: tenant, Roles: append([]string(nil), roles...)}
	f.values[key] = value
	return value, nil
}
func (f *fakePrincipalAuthority) ListPrincipals(_ context.Context, tenant string) ([]proto.Principal, error) {
	var out []proto.Principal
	for _, value := range f.values {
		if value.Tenant == tenant {
			out = append(out, value)
		}
	}
	return out, nil
}
func (f *fakePrincipalAuthority) IssueAccessToken(_ context.Context, tenant, subject, role string, ttl time.Duration) (string, time.Time, error) {
	value, ok := f.values[tenant+"\x00"+subject]
	if !ok {
		return "", time.Time{}, errors.New("missing")
	}
	found := false
	for _, assigned := range value.Roles {
		found = found || assigned == role
	}
	if !found {
		return "", time.Time{}, errors.New("role")
	}
	f.issued++
	return "ephemeral", time.Unix(2_000_000_000, 0).Add(ttl), nil
}

func TestPrincipalAdminTenantBoundaryAndInvite(t *testing.T) {
	authority := &fakePrincipalAuthority{values: map[string]proto.Principal{}}
	f := newControlFixture(t, "", func(options *Options) { options.Principals = authority })
	ctx := context.Background()
	operator := Subject{ID: "op", Tenant: "tenant-a", Roles: []string{"admin"}}
	created, err := f.c.principalCreate(ctx, operator, &proto.PrincipalCreateReq{Principal: "alice", Roles: []string{"agent"}, IdempotencyKey: "one"})
	if err != nil || created.Tenant != "tenant-a" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	if _, err := f.c.principalList(ctx, operator, &proto.PrincipalListReq{Tenant: "tenant-b"}); err == nil {
		t.Fatal("cross-tenant principal list allowed")
	}
	if _, err := f.c.principalTokenIssue(ctx, operator, &proto.PrincipalTokenIssueReq{Principal: "alice", Role: "operator", TTLMS: 60_000, IdempotencyKey: "two"}); err == nil {
		t.Fatal("unassigned role issued")
	}
	global := Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}}
	invited, err := f.c.principalInvite(ctx, global, &proto.PrincipalInviteReq{Tenant: "tenant-b", Principal: "bob", TTLMS: 60_000, IdempotencyKey: "three"})
	if err != nil || invited.AccessToken == "" || authority.values["tenant-b\x00bob"].Roles[0] != "operator" {
		t.Fatalf("invite=%+v err=%v", invited, err)
	}
}
