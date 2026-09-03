package firecracker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
)

// Compatibility is the exact restore fence persisted with a full checkpoint.
// Firecracker documents CPU features, architecture, host kernel and snapshot
// format as compatibility inputs; Remount also fences the VMM and guest RPC.
type Compatibility struct {
	Architecture   string `cbor:"arch" json:"architecture"`
	CPUFingerprint string `cbor:"cpu" json:"cpu_fingerprint"`
	HostKernel     string `cbor:"kernel" json:"host_kernel"`
	Firecracker    string `cbor:"firecracker" json:"firecracker"`
	Snapshot       string `cbor:"snapshot" json:"snapshot"`
	GuestProtocol  uint16 `cbor:"guest" json:"guest_protocol"`
}

// DetectCompatibility records only stable, equality-comparable host inputs.
func DetectCompatibility(ctx context.Context, firecracker string) (Compatibility, error) {
	if runtime.GOOS != "linux" {
		return Compatibility{}, fmt.Errorf("firecracker compatibility requires Linux, current OS is %s", runtime.GOOS)
	}
	version, err := boundedCommand(ctx, firecracker, "--version")
	if err != nil {
		return Compatibility{}, fmt.Errorf("firecracker version: %w", err)
	}
	snapshot, err := boundedCommand(ctx, firecracker, "--snapshot-version")
	if err != nil {
		return Compatibility{}, fmt.Errorf("firecracker snapshot version: %w", err)
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return Compatibility{}, fmt.Errorf("host kernel: %w", err)
	}
	cpu, err := stableCPUFingerprint()
	if err != nil {
		return Compatibility{}, err
	}
	return Compatibility{
		Architecture: runtime.GOARCH, CPUFingerprint: cpu,
		HostKernel: strings.TrimSpace(string(kernel)), Firecracker: strings.TrimSpace(version),
		Snapshot: strings.TrimSpace(snapshot), GuestProtocol: GuestProtocolVersion,
	}, nil
}

func stableCPUFingerprint() (string, error) {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "", fmt.Errorf("cpu features: %w", err)
	}
	var stable []string
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(strings.ToLower(key)) {
		case "vendor_id", "model name", "cpu family", "model", "stepping", "flags",
			"features", "cpu implementer", "cpu architecture", "cpu variant", "cpu part", "cpu revision":
			stable = append(stable, strings.TrimSpace(key)+":"+strings.Join(strings.Fields(value), " "))
		}
	}
	if len(stable) == 0 {
		return "", fmt.Errorf("cpu features: no stable identity fields")
	}
	sort.Strings(stable)
	stable = compactStrings(stable)
	sum := sha256.Sum256([]byte(strings.Join(stable, "\n")))
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func (c Compatibility) validate() error {
	if c.Architecture == "" || c.CPUFingerprint == "" || c.HostKernel == "" || c.Firecracker == "" || c.Snapshot == "" || c.GuestProtocol == 0 {
		return fmt.Errorf("incomplete Firecracker compatibility metadata")
	}
	return nil
}
