package policy

import (
	"regexp"
	"strings"
	"testing"
)

var (
	digestPinned = regexp.MustCompile(`@sha256:[0-9a-f]{64}\b`)
	dockerFrom   = regexp.MustCompile(`^\s*FROM\s+(\S+)`)
	yamlImage    = regexp.MustCompile(`^\s*-?\s*image:\s*(\S+)`)
	// A compose interpolation with a default: ${REMOUNT_IMAGE:-remount:local}.
	composeDefault = regexp.MustCompile(`^\$\{[A-Za-z0-9_]+:-([^}]*)\}$`)
)

// builtFromThisTree reports whether an image reference names an image this
// repository builds locally rather than pulls.
//
// `remount:local` cannot be digest-pinned in the file that references it: its
// digest is whatever the operator's `docker build` just produced, and writing
// one down would pin the reference to somebody else's build. The exemption is
// therefore narrow: no registry host, and the tag `local`.
func builtFromThisTree(reference string) bool {
	if match := composeDefault.FindStringSubmatch(reference); match != nil {
		reference = match[1]
	}
	name, tag, ok := strings.Cut(reference, ":")
	if !ok || tag != "local" {
		return false
	}
	host, _, hasSlash := strings.Cut(name, "/")
	return !hasSlash || (!strings.Contains(host, ".") && host != "localhost")
}

// TestEveryImageReferenceIsPinnedByDigest is the rule.
//
// A tag is a name someone else can repoint. The image that was reviewed, the
// image the conformance suite ran against, and the image a node starts three
// months later are then three different artifacts wearing one label, and
// nothing in the deployment records that they diverged.
func TestEveryImageReferenceIsPinnedByDigest(t *testing.T) {
	for _, f := range unpinnedImages(collect(t, infrastructureScopes...)) {
		t.Errorf("floating image reference: %s", f)
	}
}

// TestTheDigestScanCatchesItsCanary is the control.
func TestTheDigestScanCatchesItsCanary(t *testing.T) {
	found := unpinnedImages(collect(t, "integration/policy/testdata/canary"))
	var sawDockerfile, sawYAML bool
	for _, f := range found {
		if strings.Contains(f.path, "Dockerfile") {
			sawDockerfile = true
		}
		if strings.HasSuffix(f.path, ".yaml") {
			sawYAML = true
		}
	}
	if !sawDockerfile {
		t.Error("the digest scan found no floating FROM in the canary Dockerfile")
	}
	if !sawYAML {
		t.Error("the digest scan found no floating image: in the canary manifest")
	}
}

// TestALocalBuildIsNotAFloatingTag pins the one exemption, in both directions.
func TestALocalBuildIsNotAFloatingTag(t *testing.T) {
	for _, reference := range []string{"remount:local", "remount-node:local", "${REMOUNT_IMAGE:-remount:local}"} {
		if !builtFromThisTree(reference) {
			t.Errorf("%q is built by this repository and should be exempt", reference)
		}
	}
	for _, reference := range []string{"minio/minio:latest", "ghcr.io/x/remount:local", "debian:bookworm-slim", "remount:v1"} {
		if builtFromThisTree(reference) {
			t.Errorf("%q is pulled from a registry and must be pinned", reference)
		}
	}
}

// unpinnedImages returns every image reference that is neither digest-pinned,
// nor `scratch`, nor a build stage, nor built from this tree.
func unpinnedImages(files []configFile) []finding {
	var found []finding
	for _, file := range files {
		stages := map[string]bool{}
		for index, line := range strings.Split(file.body, "\n") {
			var reference, kind string
			switch {
			case dockerFrom.MatchString(line):
				reference, kind = dockerFrom.FindStringSubmatch(line)[1], "dockerfile-from"
				if fields := strings.Fields(line); len(fields) >= 4 && strings.EqualFold(fields[2], "AS") {
					stages[fields[3]] = true
				}
			case yamlImage.MatchString(line):
				reference, kind = yamlImage.FindStringSubmatch(line)[1], "manifest-image"
			default:
				continue
			}
			reference = strings.Trim(reference, `"'`)
			switch {
			// scratch is the empty image: there is nothing to pin, and pinning
			// it would be pinning nothing.
			// An unrendered Helm action. What it produces is checked where it
			// can be checked: golden/default.yaml is scanned by this same rule,
			// and the chart itself refuses a digest-less value at render time.
			case strings.HasPrefix(reference, "{{"):
			case reference == "scratch":
			case stages[reference]:
			case digestPinned.MatchString(reference):
			case builtFromThisTree(reference):
			default:
				found = append(found, finding{path: file.path, line: index + 1, rule: kind, text: line})
			}
		}
	}
	return found
}

// TestTheHelmChartRefusesTheScratchImage guards the packaging fact this
// repository has already paid for once.
//
// packaging/container/Dockerfile is FROM scratch, which is right for the
// control plane and the CLI and impossible for a process-backend node: its
// workspaces have no userland, so a session dies with
// `"sh": executable file not found in $PATH`. The chart must therefore refuse
// the control-plane image, and the rendered manifest must not carry it.
func TestTheHelmChartRefusesTheScratchImage(t *testing.T) {
	for _, file := range collect(t, "deploy/helm/remount-node/golden/default.yaml") {
		for index, line := range strings.Split(file.body, "\n") {
			match := yamlImage.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			repository, _, _ := strings.Cut(match[1], "@")
			if strings.HasSuffix(repository, "/remount") || repository == "remount" {
				t.Errorf("%s:%d: the node workload is scheduled with the control-plane image, which is FROM scratch and cannot host a process-backend workspace", file.path, index+1)
			}
		}
	}
}
