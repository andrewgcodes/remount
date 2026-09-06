package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
)

// backendOS records which operating systems each backend can actually run a
// workspace on, as opposed to which it compiles for. Every backend compiles
// for every GOOS the release matrix builds; what gates a backend at run time
// is named beside it:
//
//	process      no OS gate; the jail is os.Root and the runners are portable
//	docker       no OS gate; needs a reachable docker/podman CLI and daemon
//	             (internal/workspace/workspace.go, exec.LookPath(d.Binary))
//	gvisor       needs runsc plus a node-owned network namespace, and
//	             internal/netns refuses off Linux (netns_stub.go)
//	firecracker  refuses off Linux explicitly and opens /dev/kvm
//	             (firecracker/compat.go, jailer.go, reprobe.go)
//
// Generation fails when a registered backend is missing from this map, so a
// new backend cannot reach the documentation without someone stating where it
// runs.
var backendOS = map[string][]string{
	"process":     {"Linux", "macOS", "Windows"},
	"docker":      {"Linux", "macOS", "Windows"},
	"gvisor":      {"Linux"},
	"firecracker": {"Linux"},
}

// osRoles records what a host of each operating system can be. "Client" is the
// CLI and the SDKs; "Node" is a process that hosts workspaces. The reductions
// are facts about this tree, each with the code or lane that establishes it.
var osRoles = []struct {
	OS     string
	Client string
	Node   string
	Note   string
}{
	{
		OS: "Linux", Client: "yes", Node: "yes",
		Note: "the only host for `gvisor` and `firecracker`; the E4 isolation and KVM lanes run here",
	},
	{
		OS: "macOS", Client: "yes", Node: "yes",
		Note: "no `runsc` and no network namespaces, so isolation backends need a Linux VM; read-only volume mounts are unavailable (`internal/volume/mount_other.go`)",
	},
	{
		OS: "Windows", Client: "yes", Node: "yes, reduced",
		Note: "no PTY sessions (no ConPTY runner), no POSIX-shell recipe/ACP launchers, no read-only volume mounts, no parent-directory fsync, and no host memory figure; the CI Windows lane names each one",
	},
}

// feature is one column of the availability matrix. Each predicate reads the
// backend's own advertised Caps, normalized exactly as a live node advertises
// them, so a backend cannot gain a documented feature without advertising it.
var features = []struct {
	Name string
	Has  func(proto.BackendDescriptor) bool
}{
	{"Sleep/wake", func(d proto.BackendDescriptor) bool { return d.Runtime.Snapshots != "" }},
	{"Move", func(d proto.BackendDescriptor) bool { return d.Runtime.Snapshots != "" }},
	{"Memory snapshot", func(d proto.BackendDescriptor) bool { return d.Runtime.Snapshots == "fs+mem" }},
	// A computer session needs a path the browser profile can live at, which
	// is what node.computerCreate requires of workspace.MountPathOf: either a
	// mount namespace the backend can name, or a host-rooted jail.
	{"Computer session", func(d proto.BackendDescriptor) bool {
		return d.Runtime.MountPath || d.Security.FilesystemBoundary == "root_handle"
	}},
	{"Brokered credentials", func(d proto.BackendDescriptor) bool {
		return d.Security.BrokerIdentity != "" && d.Security.BrokerIdentity != "none"
	}},
	{"Enforced egress", func(d proto.BackendDescriptor) bool {
		return d.Security.EgressMode == "enforced_gateway"
	}},
	// Artifacts are the node's content-addressed blob store: snapshot upload
	// and download run above the backend, so every backend has them.
	{"Artifacts", func(proto.BackendDescriptor) bool { return true }},
}

// checkOSCoverage fails generation when a registered backend has no recorded
// operating-system support, mirroring the registry check in main.go.
func checkOSCoverage(descriptors []proto.BackendDescriptor) error {
	for _, d := range descriptors {
		if len(backendOS[d.Name]) == 0 {
			return fmt.Errorf("backend %q has no recorded operating-system support; add it to backendOS", d.Name)
		}
	}
	for name := range backendOS {
		found := false
		for _, d := range descriptors {
			if d.Name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("backendOS names %q, which is not a registered backend", name)
		}
	}
	return nil
}

// writeSupportMatrix renders the operating-system rows and the per-backend
// feature and production-suitability columns.
func writeSupportMatrix(out *bytes.Buffer, descriptors []proto.BackendDescriptor) {
	fmt.Fprint(out, `
## Operating systems

Which role a host can fill, and which backends can run a workspace there. A
backend compiles for every platform `+"`make dist`"+` builds; this table is about
what actually runs.

| OS | Client | Node | Backends that run there | Notes |
|---|---|---|---|---|
`)
	for _, row := range osRoles {
		var names []string
		for name, oses := range backendOS {
			for _, os := range oses {
				if os == row.OS {
					names = append(names, "`"+name+"`")
				}
			}
		}
		sort.Strings(names)
		fmt.Fprintf(out, "| %s | %s | %s | %s | %s |\n",
			row.OS, row.Client, row.Node, strings.Join(names, ", "), row.Note)
	}
	fmt.Fprint(out, `
The installer (`+"`install.sh`"+`) serves Linux and macOS; a Windows host takes a
binary from `+"`make dist`"+`, which builds linux, darwin and windows on amd64 and
arm64. gVisor and Firecracker are Linux kernel mechanisms: on a macOS or
Windows machine they run inside a Linux VM with nested virtualization, not on
the host GOOS.

## Feature availability by backend

Every cell is derived from the backend's own advertised capabilities. The four
right-hand columns are `+"`internal/profile`"+` evaluated against that backend alone
with no host evidence, which is what documentation can honestly prove: a
`+"`pass`"+` here means the capabilities satisfy the profile, and
`+"`unavailable`"+` means a host check has to run before anything may be
claimed. An unavailable profile is never a pass, and only
`+"`remount doctor --profile`"+` against a live node settles it.

`)
	fmt.Fprint(out, "| Backend |")
	for _, f := range features {
		fmt.Fprintf(out, " %s |", f.Name)
	}
	for _, p := range profile.All() {
		fmt.Fprintf(out, " %s |", string(p))
	}
	fmt.Fprint(out, "\n|---|")
	for range features {
		fmt.Fprint(out, "---:|")
	}
	for range profile.All() {
		fmt.Fprint(out, "---|")
	}
	fmt.Fprintln(out)
	for _, d := range matrixDescriptors(descriptors) {
		fmt.Fprintf(out, "| %s |", d.label)
		for _, f := range features {
			fmt.Fprintf(out, " %s |", yesNo(f.Has(d.descriptor)))
		}
		for _, p := range profile.All() {
			status := profile.Evaluate(p, []proto.BackendDescriptor{d.descriptor}, nil).Status
			fmt.Fprintf(out, " `%s` |", status)
		}
		fmt.Fprintln(out)
	}
}

type labelledDescriptor struct {
	label      string
	descriptor proto.BackendDescriptor
}

// matrixDescriptors is the registry's descriptors plus the capabilities a
// verified Firecracker node advertises. The registry can only produce the
// unverified zero value on a host without KVM, and a matrix that showed only
// that would understate the backend as badly as showing only the verified row
// would overstate it, so both appear and each says which it is.
func matrixDescriptors(descriptors []proto.BackendDescriptor) []labelledDescriptor {
	out := make([]labelledDescriptor, 0, len(descriptors)+1)
	for _, d := range descriptors {
		label := "`" + d.Name + "`"
		if d.Name == "firecracker" {
			label += " (unverified)"
		}
		out = append(out, labelledDescriptor{label: label, descriptor: d})
	}
	out = append(out, labelledDescriptor{
		label:      "`firecracker` (verified)",
		descriptor: verifiedFirecracker(),
	})
	return out
}
