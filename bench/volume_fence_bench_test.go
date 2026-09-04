package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/volume"
)

// Every workspace claim and every move calls
// node.attachWorkspaceVolumesScoped -> volume.LocalBackend.SetWorkspaceGeneration
// before it checks whether the workspace has any volumes at all, and that call
// commits through mutateLocked -> saveState, which marshals the node's entire
// volume catalog, writes it to a temp file, fsyncs the file and fsyncs the
// parent directory. The cost of one claim therefore grows with the number of
// workspaces the node has ever fenced (bounded only by MaxWorkspaceFences,
// default 65536), which makes claiming n workspaces O(n^2) work.
//
// These benchmarks hold the catalog size fixed and re-fence one workspace at an
// advancing generation, which is exactly what a claim or a move does. Comparing
// ns/op across the catalog sizes shows the per-claim cost, and the growth in
// that cost is the defect. The backend needs no resolver or mount engine for
// this path: a fence with no attachments never mounts anything.
func benchmarkVolumeFence(b *testing.B, catalog int) {
	b.Helper()
	ctx := context.Background()
	backend, err := volume.OpenLocalBackend(ctx, filepath.Join(b.TempDir(), "volumes.json"), nil, nil, volume.Options{})
	if err != nil {
		b.Fatalf("open local volume backend: %v", err)
	}
	for i := range catalog {
		if err := backend.SetWorkspaceGeneration(ctx, "t_bench", fmt.Sprintf("ws-fill-%06d", i), 1); err != nil {
			b.Fatalf("preload fence %d: %v", i, err)
		}
	}
	generation := uint64(1)
	b.ResetTimer()
	for range b.N {
		generation++
		if err := backend.SetWorkspaceGeneration(ctx, "t_bench", "ws-subject", generation); err != nil {
			b.Fatalf("fence at generation %d: %v", generation, err)
		}
	}
	b.StopTimer()
}

func BenchmarkVolumeFenceCatalog1(b *testing.B)     { benchmarkVolumeFence(b, 1) }
func BenchmarkVolumeFenceCatalog100(b *testing.B)   { benchmarkVolumeFence(b, 100) }
func BenchmarkVolumeFenceCatalog1000(b *testing.B)  { benchmarkVolumeFence(b, 1000) }
func BenchmarkVolumeFenceCatalog10000(b *testing.B) { benchmarkVolumeFence(b, 10000) }
