package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// oldNodeHello is a node built before named capabilities existed: it offers
// only v1 and would silently ignore every field a named capability adds.
func oldNodeHello(t *testing.T, id, token string, info proto.NodeInfo) proto.Hello {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h := proto.Hello{
		Peer: id, Role: proto.RoleNode, Token: token, Caps: []string{proto.CapabilityV1}, PubKey: key.Public().(ed25519.PublicKey),
		Node: &info, IssuedAt: time.Now().UnixMilli(), Nonce: make([]byte, 32),
	}
	if _, err := rand.Read(h.Nonce); err != nil {
		t.Fatal(err)
	}
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	return h
}

func secureNodeInfo() proto.NodeInfo {
	info := processNodeInfo(4096)
	info.Backends = append(info.Backends, "micro")
	info.BackendDescriptors = append(info.BackendDescriptors, proto.BackendDescriptor{
		Name: "micro", Security: proto.BackendSecurityCaps{
			Isolation: "microvm", MultiTenant: true, SiblingIsolation: true,
			EgressMode: "enforced_gateway", BrokerIdentity: "workload_identity",
			FilesystemBoundary: "block_device", NetworkNamespace: true, DeviceIsolation: true,
		}, Runtime: proto.RuntimeCaps{Snapshots: "fs"},
	})
	return info
}

// A deployment whose security floor requires a capability refuses, at hello,
// every peer that did not negotiate it; the error names the capability and
// the profile. Under local the same peer is accepted.
func TestDeploymentFloorRefusesPeerLackingRequiredCapability(t *testing.T) {
	for _, profile := range []string{proto.SecurityIsolated, proto.SecurityMultiTenant} {
		f := newControlFixture(t, "", func(opts *Options) { opts.SecurityProfileFloor = profile })
		old := oldNodeHello(t, "n_old", "node-token", secureNodeInfo())
		_, _, err := f.c.Authenticate(context.Background(), &old)
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeUnsupported || !strings.Contains(pe.Msg, proto.CapabilityAuthzPush) || !strings.Contains(pe.Msg, profile) {
			t.Fatalf("%s: old node accepted: %v", profile, err)
		}
		oldClient := proto.Hello{Role: proto.RoleClient, Token: "node-token", Caps: []string{proto.CapabilityV1}}
		if _, _, err := f.c.Authenticate(context.Background(), &oldClient); !errors.As(err, &pe) || pe.Code != proto.CodeUnsupported {
			t.Fatalf("%s: old client accepted: %v", profile, err)
		}
		fresh, _ := signedNodeHello(t, "n_new", "node-token", secureNodeInfo())
		id, ok, err := f.c.Authenticate(context.Background(), &fresh)
		if err != nil || id != "n_new" || !proto.HasCapability(ok.Caps, proto.CapabilityAuthzPush) {
			t.Fatalf("%s: current node refused: %v caps=%v", profile, err, ok)
		}
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.SecurityProfileFloor = proto.SecurityLocal })
	old := oldNodeHello(t, "n_old", "node-token", processNodeInfo(1024))
	if _, _, err := f.c.Authenticate(context.Background(), &old); err != nil {
		t.Fatalf("local floor refused an old node: %v", err)
	}
}

// Without a deployment floor an old node may join, but it is eligible only
// for local workspaces: a workspace whose profile requires a capability the
// node never negotiated is placed on a current node or not at all.
func TestOldNodeServesLocalWorkspacesOnly(t *testing.T) {
	f := newControlFixture(t, "", nil)
	old := oldNodeHello(t, "n_old", "node-token", secureNodeInfo())
	id, _, err := f.c.Authenticate(context.Background(), &old)
	if err != nil || id != "n_old" {
		t.Fatalf("old node refused under no floor: %v", err)
	}
	f.c.PeerConnected(context.Background(), id, &old)
	status := f.c.nodeList()
	if len(status.Nodes) != 1 || len(proto.MissingCapabilities(status.Nodes[0].Protocol, []string{proto.CapabilityV1})) != 0 || proto.HasCapability(status.Nodes[0].Protocol, proto.CapabilityAuthzPush) {
		t.Fatalf("node status protocol = %#v", status)
	}

	local := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	if _, err := f.c.wsClaim(context.Background(), "n_old", local.ID); err != nil {
		t.Fatalf("old node could not serve a local workspace: %v", err)
	}
	isolated := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Security: proto.SecuritySpec{Profile: proto.SecurityIsolated}})
	if _, err := f.c.wsClaim(context.Background(), "n_old", isolated.ID); err == nil {
		t.Fatal("old node claimed an isolated workspace")
	}
	multi := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Security: proto.SecuritySpec{Profile: proto.SecurityMultiTenant}})
	if _, err := f.c.wsClaim(context.Background(), "n_old", multi.ID); err == nil {
		t.Fatal("old node claimed a multi-tenant workspace")
	}

	connectNode(t, f.c, "n_new", secureNodeInfo())
	for _, ws := range []*proto.Workspace{isolated, multi} {
		claim, err := f.c.wsClaim(context.Background(), "n_new", ws.ID)
		if err != nil || claim.Workspace.ID != ws.ID {
			t.Fatalf("current node could not claim %s: %v", ws.Spec.Security.Profile, err)
		}
	}
}
