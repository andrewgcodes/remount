// Command artifact-transfer moves a directory of files between two workspaces
// through Remount's content-addressed artifact store, and proves the round
// trip byte for byte.
//
// It uploads a local directory into a workspace, snapshots that workspace,
// downloads the snapshot object over the server's artifact endpoint and
// checks its digest, restores a second workspace from the same artifact, and
// compares every file's bytes against the originals.
//
// It imports only the supported public packages
// (remount.dev/remount/api and remount.dev/remount/client) plus net/http for
// the artifact download, needs no credentials, and reaches no network but the
// Remount server itself.
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/artifact-transfer ./docs/adr
//
// With no argument it generates a small tree in a temporary directory so the
// example runs anywhere.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

// artifactPrefix is the only shape a Remount artifact id takes: the SHA-256 of
// the object's plaintext bytes. It is stable across nodes, which is what lets
// a second workspace restore from an object a first workspace produced.
const artifactPrefix = "art_sha256:"

func main() {
	source := ""
	if len(os.Args) > 1 {
		source = os.Args[1]
	}
	if err := run(context.Background(), os.Stdout, source); err != nil {
		fmt.Fprintln(os.Stderr, "artifact-transfer:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer, source string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if source == "" {
		generated, err := generateTree()
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(generated) }()
		source = generated
	}
	local, err := readTree(source)
	if err != nil {
		return err
	}
	if len(local) == 0 {
		return fmt.Errorf("%s holds no regular files", source)
	}
	fmt.Fprintf(out, "source %s: %d files\n", source, len(local))

	server := envOr("REMOUNT_SERVER", "http://127.0.0.1:7443")
	token := os.Getenv("REMOUNT_TOKEN")
	c, err := client.New(client.Options{Server: server, Token: token, Principal: "a_artifact_transfer"})
	if err != nil {
		return err
	}
	defer c.Close()

	origin, destroyOrigin, err := claim(ctx, c, "artifact-transfer-origin", api.WorkspaceSpec{Name: "artifact-transfer-origin"})
	if err != nil {
		return err
	}
	defer destroyOrigin(out)

	// Upload: one Mkdir per directory, one WriteFile per file. Both are
	// jailed against the workspace root, so a path that escapes is refused
	// rather than written outside the tree.
	for _, name := range sortedNames(local) {
		if dir := path.Dir(name); dir != "." {
			if err := c.Mkdir(ctx, origin, dir); err != nil {
				return fmt.Errorf("mkdir %s: %w", dir, err)
			}
		}
		if err := c.WriteFile(ctx, origin, name, local[name], 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	fmt.Fprintf(out, "uploaded %d files into %s\n", len(local), origin)

	// Snapshot with upload=true publishes the object to the control plane's
	// artifact store, which is what makes it reachable from another node.
	snapshot, err := c.Snapshot(ctx, origin, true)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "snapshot %s format=%s bytes=%d\n", snapshot.Artifact, snapshot.Format, snapshot.Bytes)

	// Download the object the snapshot names. For format "tar" it is the
	// deterministic archive of the whole tree; for "chunked-v1" it is the
	// manifest that names the tree's chunks. Either way the id is the
	// SHA-256 of the bytes served, so a corrupted transfer is detectable
	// without trusting the server.
	archive := filepath.Join(os.TempDir(), strings.TrimPrefix(snapshot.Artifact, artifactPrefix)+".bin")
	downloaded, err := download(ctx, server, token, snapshot.Artifact, archive)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(archive) }()
	fmt.Fprintf(out, "downloaded %d bytes to %s, digest verified\n", downloaded, archive)

	// Restore: the control plane resolves the artifact's format and its
	// transitive object closure itself. A caller supplies only the id.
	restored, destroyRestored, err := claim(ctx, c, "artifact-transfer-restored", api.WorkspaceSpec{
		Name: "artifact-transfer-restored", RestoreFrom: snapshot.Artifact,
	})
	if err != nil {
		return err
	}
	defer destroyRestored(out)

	for _, name := range sortedNames(local) {
		got, err := c.ReadFile(ctx, restored, name)
		if err != nil {
			return fmt.Errorf("read %s from %s: %w", name, restored, err)
		}
		if string(got) != string(local[name]) {
			return fmt.Errorf("%s differs after restore: %d bytes in, %d bytes out", name, len(local[name]), len(got))
		}
	}
	fmt.Fprintf(out, "verified %d files byte for byte in %s\n", len(local), restored)
	return nil
}

// claim creates a workspace, waits for a node to restore it, and returns a
// destroy function that runs even when the caller's context is already done.
func claim(ctx context.Context, c *client.Client, label string, spec api.WorkspaceSpec) (string, func(io.Writer), error) {
	ws, err := c.CreateWorkspace(ctx, spec)
	if err != nil {
		return "", nil, fmt.Errorf("create %s: %w", label, err)
	}
	if ws, err = c.WaitClaimed(ctx, ws.ID); err != nil {
		return "", nil, fmt.Errorf("wait %s: %w", label, err)
	}
	id := ws.ID
	return id, func(out io.Writer) {
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := c.DestroyWorkspace(stop, id); err != nil {
			fmt.Fprintf(out, "destroy %s: %v\n", id, err)
			return
		}
		fmt.Fprintf(out, "workspace %s destroyed\n", id)
	}, nil
}

// download fetches one artifact and refuses to keep bytes whose SHA-256 does
// not equal the id that named them.
func download(ctx context.Context, server, token, id, dest string) (int64, error) {
	digest, ok := strings.CutPrefix(id, artifactPrefix)
	if !ok {
		return 0, fmt.Errorf("artifact id %q is not %s<hex>", id, artifactPrefix)
	}
	url := strings.TrimSuffix(server, "/") + "/v1/artifacts/" + id
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s: %s", url, response.Status)
	}
	file, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	sum := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, sum), response.Body)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, err
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != digest {
		return 0, fmt.Errorf("artifact %s hashed to %s: the bytes are not the object that was named", id, got)
	}
	return written, nil
}

// readTree reads every regular file under root, keyed by its slash-separated
// path relative to root.
func readTree(root string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(root, func(p string, entry os.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		relative, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = data
		return nil
	})
	return files, err
}

// generateTree writes a small nested tree so the example runs with no
// argument and no assumptions about the checkout.
func generateTree() (string, error) {
	dir, err := os.MkdirTemp("", "artifact-transfer-")
	if err != nil {
		return "", err
	}
	contents := map[string]string{
		"README.md":         "# transferred by remount\n",
		"src/main.go":       "package main\n\nfunc main() {}\n",
		"src/data/rows.csv": "id,value\n1,alpha\n2,beta\n",
	}
	for name, body := range contents {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func sortedNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
