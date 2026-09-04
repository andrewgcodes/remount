package evidence

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// Gate is a Phase B0 required gate. Until every gate passes against the exact
// candidate, no scenario claim about that candidate means anything.
type Gate struct {
	ID    string
	Title string
	Layer Layer
	// Argv is the command to run from the repository root. It is empty for the
	// gates the reporter performs itself.
	Argv []string
}

// Gates lists the B0 required gates in the order the plan states them, plus the
// two the aggregate gate adds: generated-output drift and the planted-canary
// leak scan.
func Gates() []Gate {
	return []Gate{
		{ID: "B0.lint", Title: "vet, gofmt, lock discipline, llms.txt freshness and acpgen drift", Layer: LayerCode, Argv: []string{"make", "lint"}},
		{ID: "B0.test", Title: "the whole suite plus the out-of-module public API", Layer: LayerCode, Argv: []string{"make", "test"}},
		{ID: "B0.race", Title: "the whole suite under the race detector", Layer: LayerCode, Argv: []string{"make", "race"}},
		{ID: "B0.conformance", Title: "hostile-input and compromised-workspace packages under race", Layer: LayerCode, Argv: []string{"make", "conformance"}},
		{ID: "B0.fuzz", Title: "every fuzz target for 30s", Layer: LayerCode, Argv: []string{"make", "fuzz", "FUZZTIME=30s"}},
		{ID: "B0.dist", Title: "static binaries for every supported platform", Layer: LayerArtifact, Argv: []string{"make", "dist"}},
		{ID: "B0.mod-verify", Title: "module dependencies match their recorded hashes", Layer: LayerCode, Argv: []string{"go", "mod", "verify"}},
		{ID: "B0.mod-tidy", Title: "go.mod and go.sum are already tidy", Layer: LayerCode, Argv: []string{"go", "mod", "tidy", "-diff"}},
		{ID: "B0.generated", Title: "generated protocol and SDK types are not stale", Layer: LayerCode, Argv: []string{"go", "run", "./cmd/protogen", "--check"}},
		{ID: "B0.leak-canary", Title: "the leak scan finds a planted canary and nothing else in the evidence", Layer: LayerCode},
	}
}

// Probe is the verdict of an existing host gate script. The scripts already
// speak `status=available` / `status=unavailable reason=…`; consuming them
// keeps one probe per capability instead of a second, divergent one here.
type Probe struct {
	Name      string `json:"name"`
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
	Script    string `json:"script"`
}

var probeLine = regexp.MustCompile(`^(\S+) status=(available|unavailable)(?: (.*))?$`)

// backendGates is the checked-in docker/gVisor probe.
const backendGatesScript = "integration/chaos/backend-gates.sh"

// firecrackerGate is the checked-in exact-host Firecracker probe. It reports
// unavailability through exit 77 and a leading UNAVAILABLE: line.
const firecrackerGateScript = "integration/firecracker/host-gate.sh"

// ProbeBackends asks the checked-in backend gate what this host can run. A
// missing script is itself an unavailable verdict, never an assumed pass.
func ProbeBackends(ctx context.Context, root string) []Probe {
	path := filepath.Join(root, backendGatesScript)
	if _, err := os.Stat(path); err != nil {
		return []Probe{
			{Name: "docker", Script: backendGatesScript, Reason: "gate script is absent from this checkout"},
			{Name: "gvisor", Script: backendGatesScript, Reason: "gate script is absent from this checkout"},
		}
	}
	cmd := exec.CommandContext(ctx, "sh", path, "--probe")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	probes := map[string]Probe{}
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		m := probeLine.FindStringSubmatch(strings.TrimSpace(scanner.Text()))
		if m == nil {
			continue
		}
		p := Probe{Name: m[1], Available: m[2] == "available", Script: backendGatesScript}
		if detail := strings.TrimPrefix(m[3], "reason="); detail != m[3] {
			p.Reason = detail
		} else if m[3] != "" && !p.Available {
			p.Reason = m[3]
		}
		probes[p.Name] = p
	}
	for _, name := range []string{"docker", "gvisor"} {
		if _, ok := probes[name]; !ok {
			reason := "the gate script reported no verdict for this backend"
			if err != nil {
				reason = "the gate script exited without a verdict for this backend"
			}
			probes[name] = Probe{Name: name, Script: backendGatesScript, Reason: reason}
		}
	}
	return []Probe{probes["docker"], probes["gvisor"]}
}

// ProbeFirecracker asks the checked-in exact-host gate. Unit and simulation
// tests may never upgrade this verdict.
func ProbeFirecracker(ctx context.Context, root string) Probe {
	p := Probe{Name: "firecracker", Script: firecrackerGateScript}
	path := filepath.Join(root, firecrackerGateScript)
	if _, err := os.Stat(path); err != nil {
		p.Reason = "gate script is absent from this checkout"
		return p
	}
	cmd := exec.CommandContext(ctx, "sh", path)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err == nil {
		p.Available = true
		return p
	}
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "UNAVAILABLE:"); ok {
			p.Reason = strings.TrimSpace(rest)
			break
		}
	}
	if p.Reason == "" {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			p.Reason = "the host gate exited " + exit.Error() + " without naming a reason"
		} else {
			p.Reason = err.Error()
		}
	}
	return p
}
