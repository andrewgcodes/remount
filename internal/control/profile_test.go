package control

import (
	"testing"

	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
)

func processNodeState(online bool) *nodeState {
	n := &nodeState{}
	n.Status.ID = "n_process"
	n.Status.Online = online
	n.Status.Info = proto.NodeInfo{
		Backends: []string{"process"},
		BackendDescriptors: []proto.BackendDescriptor{{
			Name: "process",
			Security: proto.BackendSecurityCaps{
				Isolation: "none", EgressMode: "cooperative_proxy", BrokerIdentity: "token",
			},
		}},
		Profile: string(profile.MultiTenantIsolated),
	}
	return n
}

// A node's own report of its host is accepted only to downgrade. Every check
// it can name may say "pass" — including the very checks whose names the
// evaluator uses — and a process backend still fails the descriptor
// predicates, because the descriptors are the evidence and the node's words
// are not.
func TestNodeSelfReportCannotUpgradeADescriptor(t *testing.T) {
	n := processNodeState(true)
	n.Status.Info.RuntimeChecks = []proto.Finding{
		{Status: proto.CheckPass, Check: profile.CheckBackendIsolated, Detail: "trust me"},
		{Status: proto.CheckPass, Check: profile.CheckBackendSiblingIsolated, Detail: "trust me"},
		{Status: proto.CheckPass, Check: profile.CheckBackendEnforcedEgress, Detail: "trust me"},
		{Status: proto.CheckPass, Check: profile.CheckBackendUntrustedAbsent, Detail: "trust me"},
		{Status: proto.CheckPass, Check: profile.CheckRuntimeHealthy, Detail: "trust me"},
		{Status: proto.CheckPass, Check: profile.CheckHostMicroVMCompat, Detail: "trust me"},
	}
	c := &Control{nodes: map[string]*nodeState{"n_process": n}}
	if c.nodeSatisfiesProfileLocked(n, string(profile.MultiTenantIsolated)) {
		t.Fatal("a process node upgraded itself into multi-tenant-isolated by self-report")
	}
	if c.anyNodeSatisfiesProfileLocked(string(profile.MultiTenantIsolated)) {
		t.Fatal("the fleet reported a multi-tenant-isolated node that does not exist")
	}
	// The same self-reports cannot fabricate a microVM either.
	if c.nodeSatisfiesProfileLocked(n, string(profile.MicroVM)) {
		t.Fatal("a process node upgraded itself into microvm by self-report")
	}
	// It is still a perfectly good dev node.
	if !c.nodeSatisfiesProfileLocked(n, string(profile.Dev)) {
		t.Fatal("a process node should satisfy dev")
	}
}

// A failing self-report is believed immediately: downgrading is the one
// direction a node is trusted in.
func TestNodeSelfReportDowngrades(t *testing.T) {
	n := &nodeState{}
	n.Status.ID = "n_iso"
	n.Status.Online = true
	n.Status.Info = proto.NodeInfo{
		BackendDescriptors: []proto.BackendDescriptor{{
			Name: "isolated",
			Security: proto.BackendSecurityCaps{
				Isolation: "container", SiblingIsolation: true, EgressMode: "enforced_gateway",
				BrokerIdentity: "per_session_capability", NetworkNamespace: true, DeviceIsolation: true,
			},
		}},
		Profile: string(profile.MultiTenantIsolated),
	}
	c := &Control{nodes: map[string]*nodeState{"n_iso": n}}
	if !c.nodeSatisfiesProfileLocked(n, string(profile.MultiTenantIsolated)) {
		t.Fatal("an isolated node should satisfy multi-tenant-isolated")
	}
	n.Status.Info.RuntimeChecks = []proto.Finding{{Status: proto.CheckFail, Check: "iso.network", Detail: "rules removed"}}
	if c.nodeSatisfiesProfileLocked(n, string(profile.MultiTenantIsolated)) {
		t.Fatal("a failing host check did not downgrade the node")
	}
	// An unavailable check is not a passing check either.
	n.Status.Info.RuntimeChecks = []proto.Finding{{Status: proto.CheckUnavailable, Check: "iso.network", Detail: "probe timed out"}}
	if c.nodeSatisfiesProfileLocked(n, string(profile.MultiTenantIsolated)) {
		t.Fatal("an unavailable host check was treated as healthy")
	}
}

// An offline node's evidence cannot be re-proved, so it satisfies nothing and
// its report says unavailable rather than repeating a stale pass.
func TestOfflineNodeSatisfiesNothing(t *testing.T) {
	n := processNodeState(false)
	n.Status.Info.BackendDescriptors[0].Security = proto.BackendSecurityCaps{
		Isolation: "microvm", MultiTenant: true, SiblingIsolation: true, EgressMode: "enforced_gateway",
		BrokerIdentity: "per_session_capability", NetworkNamespace: true, DeviceIsolation: true,
	}
	c := &Control{nodes: map[string]*nodeState{"n_process": n}}
	if c.nodeSatisfiesProfileLocked(n, string(profile.MultiTenantIsolated)) {
		t.Fatal("an offline node satisfied a profile")
	}
	report := nodeProfileReportLocked("n_process", n, string(profile.MultiTenantIsolated), 0)
	if report.Status != proto.CheckUnavailable {
		t.Fatalf("offline report status %q, want unavailable", report.Status)
	}
}

// A v1 peer that sent no descriptors is credited only with a local,
// non-isolating backend: absence is evidence for weakness, never for strength.
func TestLegacyPeerWithoutDescriptorsIsWeak(t *testing.T) {
	info := proto.NodeInfo{Backends: []string{"gvisor", "firecracker"}, Snapshots: "fs"}
	got := nodeDescriptorsLocked(info)
	if len(got) != 2 {
		t.Fatalf("%d synthesized descriptors, want 2", len(got))
	}
	for _, d := range got {
		if d.Security.Isolation != "none" || d.Security.EgressMode != "open" || d.Security.BrokerIdentity != "none" {
			t.Fatalf("descriptor %q inherited strength from a name: %+v", d.Name, d.Security)
		}
	}
	if profile.Evaluate(profile.MultiTenantIsolated, got, nil).OK() {
		t.Fatal("a name-only backend list satisfied multi-tenant-isolated")
	}
}
