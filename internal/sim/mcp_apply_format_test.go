package sim

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/mcp"
	"remount.dev/remount/internal/proto"
)

// TestMCPGatewayCarriesApplyArtifactFormat pins a regression: the MCP gateway
// decodes an FSApplyTarReq and must forward its representation. Dropping the
// field applied a chunked manifest as though it were a tar archive. The node
// fails closed on the header rather than corrupting the tree, so the damage is
// an operation that can never succeed through MCP rather than a bad write —
// which is exactly the kind of silent uselessness a test has to catch.
func TestMCPGatewayCarriesApplyArtifactFormat(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)

	src := t.TempDir()
	writeTree(t, src, map[string]string{
		"main.go":      "package main\n",
		"lib/util.go":  "package lib\n",
		"docs/read.md": "hello from the chunked representation\n",
	})
	result, err := c.UploadChunkedSnapshot(ctx, src, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestID == "" {
		t.Fatal("chunked upload produced no manifest")
	}

	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "mcp-apply"})

	// The manifest advertises fs.apply_tar as op_fs_apply_tar. It is the only
	// protocol op whose name contains an underscore, so resolving the tool name
	// by replacing underscores with dots produced "fs.apply.tar" and made the
	// advertised tool permanently unreachable.
	if op, ok := mcp.ProtocolOpForTool("op_fs_apply_tar"); !ok || op != proto.OpFSApplyTar {
		t.Fatalf("op_fs_apply_tar resolved to %q (found=%v), want %q", op, ok, proto.OpFSApplyTar)
	}

	gateway := &mcp.ClientGateway{Client: c}
	invoke := func(format string) (any, error) {
		raw, err := json.Marshal(proto.FSApplyTarReq{
			WS: ws.ID, Artifact: result.ManifestID, Format: format,
			IdempotencyKey: "apply-" + format,
		})
		if err != nil {
			t.Fatal(err)
		}
		return gateway.Invoke(ctx, "op_fs_apply_tar", json.RawMessage(raw))
	}

	// Asking for tar with a chunked manifest must fail closed, and prove the
	// format is honored rather than guessed from the id.
	if _, err := invoke(proto.ArtifactFormatTar); err == nil {
		t.Fatal("a chunked manifest applied as tar succeeded; the node guessed a representation")
	}

	if _, err := invoke(proto.ArtifactFormatChunkedV1); err != nil {
		t.Fatalf("chunked apply through the MCP gateway: %v", err)
	}
	for path, want := range map[string]string{
		"main.go":      "package main\n",
		"lib/util.go":  "package lib\n",
		"docs/read.md": "hello from the chunked representation\n",
	} {
		got, err := c.ReadFile(context.Background(), ws.ID, path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", path, got, want)
		}
	}
}
