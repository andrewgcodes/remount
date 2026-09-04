package installs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// copyTree copies a package's sources into a scratch directory. The build and
// pack steps write node_modules/ and dist/ next to the sources, and doing that
// inside the checkout would make this lane edit the tree it is judging.
func copyTree(t *testing.T, from, to string, skip map[string]bool) {
	t.Helper()
	entries, err := os.ReadDir(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(to, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if skip[entry.Name()] {
			continue
		}
		src, dst := filepath.Join(from, entry.Name()), filepath.Join(to, entry.Name())
		if entry.IsDir() {
			copyTree(t, src, dst, skip)
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		copyFile(t, src, dst, info.Mode().Perm())
	}
}

// installedPackageDir is the source-tree detector for a Node install. npm
// installs a local directory as a symlink into that directory, so the question
// "is this package a real directory under the install root" is exactly the
// question "did the tarball carry the code, or is the checkout still here".
func installedPackageDir(t *testing.T, installDir string) string {
	t.Helper()
	return realPath(t, filepath.Join(installDir, "node_modules", "@remount", "sdk"))
}

// npmSmoke reports where the runtime resolved the package from, then uses it
// for a whole workspace lifecycle against the installed server.
const npmSmoke = `import { fileURLToPath } from "node:url";
import { Client } from "@remount/sdk";

const endpoint = process.argv[2];
process.stdout.write(fileURLToPath(import.meta.resolve("@remount/sdk")) + "\n");

const client = new Client(endpoint, "");
await client.connect();
const ws = await client.createWorkspace({ name: "b32-npm-consumer", security: { profile: "local" } });
for (;;) {
  const current = await client.getWorkspace(ws.id);
  if (current.state === "claimed") break;
  if (current.state === "failed" || current.state === "destroyed") throw new Error("workspace reached " + current.state);
  await new Promise((resolve) => setTimeout(resolve, 100));
}
const session = await client.exec(ws.id, ["/bin/echo", "installed-from-the-tarball"]);
let out = "";
for await (const chunk of session) {
  if (chunk.stream === 1) out += Buffer.from(chunk.data).toString();
}
await client.destroyWorkspace(ws.id);
await client.close();
process.stdout.write(out);
`

// TestB32TheNpmTarballInstallsIntoAnEmptyDirAndDrivesTheInstalledServer is
// B32's Node lane: pack the SDK exactly as a publish would, install that
// tarball into an empty directory, and use it.
func TestB32TheNpmTarballInstallsIntoAnEmptyDirAndDrivesTheInstalledServer(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	npm := requireTool(t, "npm")
	node := requireTool(t, "node")
	root := repoRoot(t)
	sandbox := t.TempDir()
	env := cleanEnv(t)

	pkg := filepath.Join(sandbox, "package")
	copyTree(t, filepath.Join(root, "sdk", "typescript"), pkg, map[string]bool{"node_modules": true, "dist": true})

	out, err := runCleanErr(t, pkg, env, npm, "ci", "--no-audit", "--no-fund")
	skipIfOffline(t, "npm ci", out, err)
	// `npm pack` ships the files the package.json `files` list names, and dist
	// is generated, so the pack has to follow a build or it would ship an
	// empty package that installs and then fails to import.
	out, err = runCleanErr(t, pkg, env, npm, "run", "build")
	skipIfOffline(t, "npm run build", out, err)

	packed := filepath.Join(sandbox, "pack")
	if err := os.MkdirAll(packed, 0o755); err != nil {
		t.Fatal(err)
	}
	runClean(t, pkg, env, npm, "pack", "--pack-destination", packed)
	tarballs, err := filepath.Glob(filepath.Join(packed, "*.tgz"))
	if err != nil || len(tarballs) != 1 {
		t.Fatalf("expected exactly one tarball in %s, got %v (%v)", packed, tarballs, err)
	}
	tarball := tarballs[0]

	// An empty directory with a private manifest: nothing but the tarball can
	// supply the package.
	install := filepath.Join(sandbox, "install")
	if err := os.MkdirAll(install, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(install, "package.json"),
		[]byte(`{"name":"b32-consumer","version":"0.0.0","private":true,"type":"module"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err = runCleanErr(t, install, env, npm, "install", "--no-audit", "--no-fund", "--omit=dev", tarball)
	skipIfOffline(t, "npm install", out, err)

	if dir := installedPackageDir(t, install); !strings.HasPrefix(dir, realPath(t, install)) {
		t.Fatalf("the installed package lives at %s, outside the directory it was installed into", dir)
	}

	// The script lives in the install directory because Node resolves bare
	// specifiers from the importing file, not from the working directory.
	script := filepath.Join(install, "smoke.mjs")
	if err := os.WriteFile(script, []byte(npmSmoke), 0o644); err != nil {
		t.Fatal(err)
	}

	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	endpoint := startInstalled(t, installBinary(t, prefix, goos, goarch))

	// Run from the install directory: Node resolves bare specifiers upward
	// from the entry point, so this is where the installed package is the only
	// candidate.
	out = runClean(t, install, env, node, script, endpoint)
	resolved, echoed, ok := strings.Cut(strings.TrimSpace(out), "\n")
	if !ok {
		t.Fatalf("the smoke printed no output beyond %q", out)
	}
	if strings.HasPrefix(realPath(t, resolved), root+string(os.PathSeparator)) {
		t.Fatalf("the runtime resolved @remount/sdk to %s, inside the checkout", resolved)
	}
	if strings.TrimSpace(echoed) != "installed-from-the-tarball" {
		t.Fatalf("the workspace produced %q", echoed)
	}
	t.Logf("tarball %s installed at %s drove %s", filepath.Base(tarball), resolved, endpoint)
}

// TestB32ALinkedNpmInstallIsCaught is the control for installedPackageDir.
// Installing the package directory rather than the tarball is the everyday way
// a Node project keeps depending on a checkout — npm links it rather than
// copying it — and the same detector must see that.
func TestB32ALinkedNpmInstallIsCaught(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	npm := requireTool(t, "npm")
	root := repoRoot(t)
	install := t.TempDir()
	if err := os.WriteFile(filepath.Join(install, "package.json"),
		[]byte(`{"name":"b32-linked","version":"0.0.0","private":true,"type":"module"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := runCleanErr(t, install, cleanEnv(t), npm, "install", "--no-audit", "--no-fund", "--omit=dev",
		filepath.Join(root, "sdk", "typescript"))
	skipIfOffline(t, "npm install of the package directory", out, err)

	if dir := installedPackageDir(t, install); !strings.HasPrefix(dir, root+string(os.PathSeparator)) {
		t.Fatalf("a directory install resolved to %s, outside the checkout %s; the detector cannot tell a tarball from a link", dir, root)
	}
}
