package installs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dockerArch maps what the daemon calls its architecture onto the name the
// dist artifacts use. A mismatch here would silently build an image around a
// binary the daemon cannot execute.
func dockerArch(t *testing.T, reported string) string {
	t.Helper()
	switch reported {
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	default:
		t.Skipf("unavailable: no dist artifact for the daemon's architecture %q", reported)
		return ""
	}
}

// requireDocker reports the daemon's architecture, or names why this lane
// cannot run. A daemon that is installed but not running is `unavailable`
// exactly like one that is absent.
func requireDocker(t *testing.T) string {
	t.Helper()
	docker := requireTool(t, "docker")
	out, err := runCleanErr(t, t.TempDir(), cleanEnv(t), docker, "info", "--format", "{{.OSType}}/{{.Architecture}}")
	if err != nil {
		t.Skipf("unavailable: no docker daemon: %v\n%s", err, out)
	}
	ostype, arch, ok := strings.Cut(strings.TrimSpace(out), "/")
	if !ok || ostype != "linux" {
		t.Skipf("unavailable: the daemon serves %q; the release images are linux images", strings.TrimSpace(out))
	}
	return dockerArch(t, arch)
}

// TestB32TheOCIArchivesInstallAndPassTheBlackBoxSmoke is B32's image lane.
//
// It builds both release images from a context that holds nothing but the dist
// binary and the two Dockerfiles, saves them to archives, deletes the local
// images so a load has something to do, loads the archives back, and judges the
// stack that comes up. Two images and not one: packaging/container/Dockerfile
// is FROM scratch, which is right for the control plane and impossible for a
// process-backend node, whose workspaces need a userland to run in.
// deploy/compose/node.Dockerfile exists for that asymmetry and says so. A
// single-image stack fails CONF-SESS-004 and CONF-SESS-006 with "executable
// file not found", which is the documented shape of that limitation rather
// than a defect the smoke discovered.
func TestB32TheOCIArchivesInstallAndPassTheBlackBoxSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: the image lane builds, saves and runs real containers; skipped under -short")
	}
	arch := requireDocker(t)
	root := repoRoot(t)
	dist := distDir(t)

	// The build context: one binary and two Dockerfiles. Nothing else is even
	// visible to the build, so the images cannot depend on the checkout.
	ctx := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ctx, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	binary := distName("linux", arch)
	copyFile(t, filepath.Join(dist, binary), filepath.Join(ctx, "dist", binary), 0o755)
	copyFile(t, filepath.Join(root, "packaging", "container", "Dockerfile"), filepath.Join(ctx, "Dockerfile"), 0o644)
	copyFile(t, filepath.Join(root, "deploy", "compose", "node.Dockerfile"), filepath.Join(ctx, "node.Dockerfile"), 0o644)

	run := "b32-" + fmt.Sprint(time.Now().UnixNano())
	control := "remount-b32-control:" + run
	node := "remount-b32-node:" + run
	env := cleanEnv(t)
	docker := func(args ...string) string { return runClean(t, ctx, env, "docker", args...) }
	dockerErr := func(args ...string) (string, error) { return runCleanErr(t, ctx, env, "docker", args...) }

	t.Cleanup(func() {
		_, _ = dockerErr("rm", "-f", run+"-control", run+"-node")
		_, _ = dockerErr("network", "rm", run+"-net")
		_, _ = dockerErr("rmi", "-f", control, node)
		// Cleanup that is not verified is a claim, not a fact.
		for _, name := range []string{control, node} {
			if _, err := dockerErr("image", "inspect", name); err == nil {
				t.Errorf("%s is still in the local image store after cleanup", name)
			}
		}
		if left := strings.TrimSpace(runClean(t, ctx, env, "docker", "ps", "-aq", "--filter", "name="+run)); left != "" {
			t.Errorf("containers survived cleanup: %s", left)
		}
	})

	docker("build", "--build-arg", "TARGETOS=linux", "--build-arg", "TARGETARCH="+arch,
		"--build-arg", "VERSION="+run, "-f", "Dockerfile", "-t", control, ".")
	docker("build", "--build-arg", "TARGETOS=linux", "--build-arg", "TARGETARCH="+arch,
		"--build-arg", "VERSION="+run, "-f", "node.Dockerfile", "-t", node, ".")

	archives := t.TempDir()
	controlArchive := filepath.Join(archives, "control.tar")
	nodeArchive := filepath.Join(archives, "node.tar")
	docker("save", "-o", controlArchive, control)
	docker("save", "-o", nodeArchive, node)

	// Deleting the images first is what makes the next step an installation.
	// Loading an image that is already present proves nothing about the
	// archive.
	docker("rmi", "-f", control, node)
	for _, name := range []string{control, node} {
		if _, err := dockerErr("image", "inspect", name); err == nil {
			t.Fatalf("%s survived removal, so the load below would not install anything", name)
		}
	}
	docker("load", "-i", controlArchive)
	docker("load", "-i", nodeArchive)

	addr := freeLoopbackAddr(t)
	_, port, _ := strings.Cut(addr, ":")
	token := "b32-" + run
	docker("network", "create", run+"-net")
	// No bind mount and no host network: the containers cannot see this
	// machine's filesystem at all, which is the strongest form of the clean
	// environment these lanes are built around.
	docker("run", "-d", "--name", run+"-control", "--network", run+"-net",
		"-p", "127.0.0.1:"+port+":7443", control,
		"server", "--listen", "0.0.0.0:7443", "--data", "/var/lib/remount", "--token", token)
	docker("run", "-d", "--name", run+"-node", "--network", run+"-net", node,
		"up", "--server", "http://"+run+"-control:7443", "--token", token,
		"--data", "/var/lib/remount-node", "--backend", "process")

	endpoint := "http://" + addr
	waitHealthy(t, endpoint, token, 90*time.Second)
	conformanceSmoke(t, endpoint, token)
}

func copyFile(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()
	body, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, body, mode); err != nil {
		t.Fatal(err)
	}
}
