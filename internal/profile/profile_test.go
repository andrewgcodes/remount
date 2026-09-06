package profile

import (
	"testing"

	"remount.dev/remount/internal/proto"
)

// The descriptors below are the exact values each backend's Caps method
// produces after workspace.Registry.Descriptors normalizes EgressMode, so a
// change to a backend's advertised capabilities breaks this test rather than
// silently changing what a profile means.
//
//	process              internal/workspace/workspace.go  (*Process).Caps
//	docker               internal/workspace/workspace.go  (*Docker).Caps
//	gvisor               internal/workspace/gvisor         (*Backend).Caps
//	firecracker verified internal/workspace/firecracker    (*Backend).Caps, verified
//	firecracker unverif. internal/workspace/firecracker    (*Backend).Caps, !verified
func processDescriptor() proto.BackendDescriptor {
	return proto.BackendDescriptor{
		Name: "process",
		Security: proto.BackendSecurityCaps{
			Isolation: "none", EgressMode: "cooperative_proxy", BrokerIdentity: "token",
			FilesystemBoundary: "root_handle",
		},
		Runtime: proto.RuntimeCaps{Snapshots: "fs"},
	}
}

func dockerDescriptor() proto.BackendDescriptor {
	return proto.BackendDescriptor{
		Name: "docker",
		Security: proto.BackendSecurityCaps{
			Isolation: "container", EgressMode: "cooperative_proxy", BrokerIdentity: "token",
			FilesystemBoundary: "bind_mount", NetworkNamespace: true, DeviceIsolation: true,
		},
		Runtime: proto.RuntimeCaps{Snapshots: "fs", MountPath: true},
	}
}

func gvisorDescriptor() proto.BackendDescriptor {
	return proto.BackendDescriptor{
		Name: "gvisor",
		Security: proto.BackendSecurityCaps{
			Isolation: "container", SiblingIsolation: true, EgressMode: "enforced_gateway",
			BrokerIdentity: "per_session_capability", FilesystemBoundary: "bind_mount",
			NetworkNamespace: true, DeviceIsolation: true,
		},
		Runtime: proto.RuntimeCaps{Snapshots: "fs", MountPath: true},
	}
}

func firecrackerDescriptor() proto.BackendDescriptor {
	return proto.BackendDescriptor{
		Name: "firecracker",
		Security: proto.BackendSecurityCaps{
			Isolation: "microvm", MultiTenant: true, SiblingIsolation: true,
			EgressMode: "enforced_gateway", BrokerIdentity: "per_session_capability",
			FilesystemBoundary: "block_device", NetworkNamespace: true, DeviceIsolation: true,
		},
		Runtime: proto.RuntimeCaps{Snapshots: "fs+mem", MountPath: true},
	}
}

func unverifiedFirecrackerDescriptor() proto.BackendDescriptor {
	return proto.BackendDescriptor{
		Name:     "firecracker",
		Security: proto.BackendSecurityCaps{Isolation: "none", EgressMode: "open", BrokerIdentity: "none"},
		Runtime:  proto.RuntimeCaps{Snapshots: "fs"},
	}
}

func pass(check string) proto.Finding {
	return proto.Finding{Severity: "info", Status: proto.CheckPass, Check: check, Detail: "ok"}
}

func fail(check string) proto.Finding {
	return proto.Finding{Severity: "error", Status: proto.CheckFail, Check: check, Detail: "broken"}
}

func unavailable(check string) proto.Finding {
	return proto.Finding{Severity: "warn", Status: proto.CheckUnavailable, Check: check, Detail: "could not run"}
}

func TestEvaluateBackendMatrix(t *testing.T) {
	healthy := []proto.Finding{pass("gvisor.runsc"), pass("gvisor.network")}
	microvmHealthy := []proto.Finding{pass("firecracker.machine"), pass(CheckHostMicroVMCompat)}

	cases := []struct {
		name        string
		descriptors []proto.BackendDescriptor
		runtime     []proto.Finding
		want        map[Profile]string
	}{
		{
			name:        "process only",
			descriptors: []proto.BackendDescriptor{processDescriptor()},
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckFail,
				MultiTenantIsolated: proto.CheckFail, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "docker only",
			descriptors: []proto.BackendDescriptor{dockerDescriptor()},
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckPass,
				MultiTenantIsolated: proto.CheckFail, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "gvisor only",
			descriptors: []proto.BackendDescriptor{gvisorDescriptor()},
			runtime:     healthy,
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckPass,
				MultiTenantIsolated: proto.CheckPass, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "verified firecracker",
			descriptors: []proto.BackendDescriptor{firecrackerDescriptor()},
			runtime:     microvmHealthy,
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckPass,
				MultiTenantIsolated: proto.CheckPass, MicroVM: proto.CheckPass,
			},
		},
		{
			name:        "unverified firecracker",
			descriptors: []proto.BackendDescriptor{unverifiedFirecrackerDescriptor()},
			runtime:     []proto.Finding{fail("firecracker.verified"), fail(CheckHostMicroVMCompat)},
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckFail,
				MultiTenantIsolated: proto.CheckFail, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "gvisor beside process weakens the whole node",
			descriptors: []proto.BackendDescriptor{gvisorDescriptor(), processDescriptor()},
			runtime:     healthy,
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckFail,
				MultiTenantIsolated: proto.CheckFail, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "no backend at all",
			descriptors: nil,
			want: map[Profile]string{
				Dev: proto.CheckFail, TrustedSingleTenant: proto.CheckFail,
				MultiTenantIsolated: proto.CheckFail, MicroVM: proto.CheckFail,
			},
		},
		{
			name:        "microvm without a host compatibility result is unavailable",
			descriptors: []proto.BackendDescriptor{firecrackerDescriptor()},
			runtime:     []proto.Finding{pass("firecracker.machine")},
			want: map[Profile]string{
				Dev: proto.CheckPass, TrustedSingleTenant: proto.CheckPass,
				MultiTenantIsolated: proto.CheckPass, MicroVM: proto.CheckUnavailable,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for p, want := range tc.want {
				got := Evaluate(p, tc.descriptors, tc.runtime)
				if got.Status != want {
					t.Fatalf("%s: status %q, want %q (failed: %v)", p, got.Status, want, got.Failed())
				}
				if len(got.Checks) != len(Requirements(p)) {
					t.Fatalf("%s: %d checks for %d requirements", p, len(got.Checks), len(Requirements(p)))
				}
				for _, c := range got.Checks {
					if c.Status == "" {
						t.Fatalf("%s: check %q has no status; an unstated status could be read as a pass", p, c.Check)
					}
				}
			}
		})
	}
}

// An unavailable host check must never yield a pass, for any profile that
// folds host checks at all. This is the invariant a diagnostic exists for.
func TestUnavailableRuntimeCheckIsNeverPass(t *testing.T) {
	descriptors := []proto.BackendDescriptor{firecrackerDescriptor()}
	runtime := []proto.Finding{pass("firecracker.machine"), unavailable("firecracker.network"), pass(CheckHostMicroVMCompat)}
	for _, p := range []Profile{MultiTenantIsolated, MicroVM} {
		got := Evaluate(p, descriptors, runtime)
		if got.OK() {
			t.Fatalf("%s passed with an unavailable host check", p)
		}
		if got.Status != proto.CheckUnavailable {
			t.Fatalf("%s: status %q, want unavailable", p, got.Status)
		}
	}
	// Dev claims nothing, so the same unavailable check does not sink it.
	if !Evaluate(Dev, descriptors, runtime).OK() {
		t.Fatal("dev must not be sunk by a host check it makes no claim about")
	}
}

// A check reported with no status at all is a result nobody recorded. It must
// be read as unavailable, never folded in as a pass.
func TestMissingStatusIsUnavailable(t *testing.T) {
	runtime := []proto.Finding{{Severity: "info", Check: "gvisor.network", Detail: "no status"}}
	got := Evaluate(MultiTenantIsolated, []proto.BackendDescriptor{gvisorDescriptor()}, runtime)
	if got.Status != proto.CheckUnavailable {
		t.Fatalf("status %q, want unavailable", got.Status)
	}
}

// A failing host check downgrades; it can never upgrade. Symmetrically, a
// node reporting nothing but passes cannot make a weak descriptor strong.
func TestRuntimeChecksOnlyDowngrade(t *testing.T) {
	weak := []proto.BackendDescriptor{processDescriptor()}
	allPass := []proto.Finding{
		pass(CheckBackendIsolated), pass(CheckBackendSiblingIsolated),
		pass(CheckBackendEnforcedEgress), pass(CheckHostMicroVMCompat),
		pass("profile.runtime.healthy"),
	}
	got := Evaluate(MultiTenantIsolated, weak, allPass)
	if got.OK() {
		t.Fatal("a node's self-reported passes upgraded a process backend")
	}
	strong := []proto.BackendDescriptor{gvisorDescriptor()}
	if !Evaluate(MultiTenantIsolated, strong, nil).OK() {
		t.Fatal("gvisor with no reported host checks should satisfy multi-tenant-isolated")
	}
	if Evaluate(MultiTenantIsolated, strong, []proto.Finding{fail("gvisor.network")}).OK() {
		t.Fatal("a failing host check must downgrade")
	}
}

func TestSatisfiedIsMonotone(t *testing.T) {
	got := Satisfied([]proto.BackendDescriptor{firecrackerDescriptor()},
		[]proto.Finding{pass("firecracker.machine"), pass(CheckHostMicroVMCompat)})
	want := []Profile{Dev, TrustedSingleTenant, MultiTenantIsolated, MicroVM}
	if len(got) != len(want) {
		t.Fatalf("satisfied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("satisfied %v, want %v", got, want)
		}
	}
}

func TestParse(t *testing.T) {
	if p, err := Parse(""); err != nil || p != Dev {
		t.Fatalf(`Parse("") = %q, %v; want dev, nil`, p, err)
	}
	for _, name := range Names() {
		if p, err := Parse(name); err != nil || string(p) != name {
			t.Fatalf("Parse(%q) = %q, %v", name, p, err)
		}
	}
	if _, err := Parse("prod"); err == nil {
		t.Fatal("an unknown profile must not parse")
	}
	if Profile("prod").Rank() != -1 {
		t.Fatal("an unknown profile must rank below every known one")
	}
	if Dev.MakesSecurityClaim() {
		t.Fatal("dev must not claim a security boundary")
	}
	for _, p := range []Profile{TrustedSingleTenant, MultiTenantIsolated, MicroVM} {
		if !p.MakesSecurityClaim() {
			t.Fatalf("%s must claim a security boundary", p)
		}
	}
}

func TestUnknownProfileNeverPasses(t *testing.T) {
	got := Evaluate(Profile("prod"), []proto.BackendDescriptor{firecrackerDescriptor()}, nil)
	if got.OK() {
		t.Fatal("an unknown profile must never evaluate as satisfied")
	}
}
