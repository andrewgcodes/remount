package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/volume"
)

type recordingVolumes struct {
	mu          sync.Mutex
	attachments []volume.Attachment
	versions    map[string]volume.Version
	sourceRoot  string
}

func newRecordingVolumes(sourceRoot string) *recordingVolumes {
	return &recordingVolumes{versions: make(map[string]volume.Version), sourceRoot: sourceRoot}
}

func (r *recordingVolumes) EnsureVersion(_ context.Context, tenant, id string, number uint64, artifactID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s\x00%s\x00%d", tenant, id, number)
	if prior, ok := r.versions[key]; ok && prior.Artifact != artifactID {
		return volume.ErrConflict
	}
	r.versions[key] = volume.Version{Number: number, Artifact: artifactID}
	return nil
}

func (r *recordingVolumes) Create(context.Context, volume.CreateRequest) (volume.Volume, error) {
	return volume.Volume{}, volume.ErrUnsupported
}
func (r *recordingVolumes) Delete(context.Context, volume.DeleteRequest) error {
	return volume.ErrUnsupported
}
func (r *recordingVolumes) Publish(context.Context, volume.PublishRequest) (volume.Volume, error) {
	return volume.Volume{}, volume.ErrUnsupported
}
func (r *recordingVolumes) Attach(_ context.Context, req volume.AttachRequest) (volume.Attachment, error) {
	target := filepath.Join(req.WorkspaceRoot, filepath.FromSlash(req.Path[1:]))
	if err := os.MkdirAll(target, 0o755); err != nil {
		return volume.Attachment{}, err
	}
	data, err := os.ReadFile(filepath.Join(r.sourceRoot, volume.SourceRelativePath(req.Tenant, req.Artifact), "dataset.txt"))
	if err != nil {
		return volume.Attachment{}, err
	}
	if err := os.WriteFile(filepath.Join(target, "dataset.txt"), data, 0o444); err != nil {
		return volume.Attachment{}, err
	}
	a := volume.Attachment{Tenant: req.Tenant, Workspace: req.Workspace, Generation: req.Generation, VolumeID: req.ID, VolumeVersion: req.Version, Artifact: req.Artifact, Path: req.Path, WorkspaceRoot: req.WorkspaceRoot, ReadOnly: true, State: "attached"}
	r.mu.Lock()
	r.attachments = append(r.attachments, a)
	r.mu.Unlock()
	return a, nil
}
func (r *recordingVolumes) Detach(_ context.Context, req volume.DetachRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.attachments {
		a := r.attachments[i]
		if a.Tenant == req.Tenant && a.Workspace == req.Workspace && a.Generation == req.Generation && a.Path == req.Path {
			_ = os.RemoveAll(filepath.Join(a.WorkspaceRoot, filepath.FromSlash(a.Path[1:])))
			r.attachments = append(r.attachments[:i], r.attachments[i+1:]...)
			return nil
		}
	}
	return volume.ErrNotFound
}
func (r *recordingVolumes) List(context.Context, string) ([]volume.Volume, error) { return nil, nil }
func (r *recordingVolumes) Inspect(context.Context, string, string) (volume.Detail, error) {
	return volume.Detail{}, volume.ErrNotFound
}
func (r *recordingVolumes) SetWorkspaceGeneration(context.Context, string, string, uint64) error {
	return nil
}
func (r *recordingVolumes) Stats() volume.Stats { return volume.Stats{} }
func (r *recordingVolumes) Close() error        { return nil }

func (r *recordingVolumes) attachment(workspace string) (volume.Attachment, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, attachment := range r.attachments {
		if attachment.Workspace == workspace {
			return attachment, true
		}
	}
	return volume.Attachment{}, false
}

// TestSharedVolumeSurvivesPublishAndMove proves the composed boundary: ready
// waits for mount materialization, publishing creates a new immutable version
// without changing a live pin, and a move remounts that same pin while the
// ordinary workspace checkpoint excludes the shared tree.
func TestSharedVolumeSurvivesPublishAndMove(t *testing.T) {
	w := newWorld(t)
	var firstMounts, secondMounts *recordingVolumes
	n1 := w.nodeWith("volume-n1", func(options *node.Options) {
		firstMounts = newRecordingVolumes(filepath.Join(options.DataDir, "volumes", "sources"))
		options.Volumes, options.VolumeCapability = firstMounts, true
	})
	n2 := w.nodeWith("volume-n2", func(options *node.Options) {
		secondMounts = newRecordingVolumes(filepath.Join(options.DataDir, "volumes", "sources"))
		options.Volumes, options.VolumeCapability = secondMounts, true
	})
	c := w.client("volume-client")

	seed := t.TempDir()
	if err := os.WriteFile(filepath.Join(seed, "dataset.txt"), []byte("version one"), 0o644); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := artifact.Snapshot(seed, nil, &archive); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 90*time.Second)
	artifactID, _, err := c.UploadArtifact(ctx, bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateVolume(ctx, proto.VolumeCreateReq{ID: "dataset", Artifact: artifactID})
	if err != nil {
		t.Fatal(err)
	}
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Placement: proto.Placement{Node: n1.ID()},
		Volumes:   []proto.VolumeMount{{ID: "dataset", Path: "/shared"}},
	})
	if mounted, ok := firstMounts.attachment(ws.ID); !ok || mounted.VolumeVersion != 1 || mounted.Artifact != artifactID || !mounted.ReadOnly {
		t.Fatalf("first node mount = %+v, present=%v", mounted, ok)
	}
	got, err := c.ReadFile(ctx, ws.ID, "/shared/dataset.txt")
	if err != nil || string(got) != "version one" {
		t.Fatalf("initial mounted bytes = %q, %v", got, err)
	}
	if err := c.WriteFile(ctx, ws.ID, "/output/dataset.txt", []byte("version two"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFile(ctx, ws.ID, "/src/shared/keep", []byte("ordinary workspace bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	published, err := c.PublishVolumePath(ctx, ws.ID, "/output", "dataset", created.Version)
	if err != nil || published.Version != 2 {
		t.Fatalf("publish = %+v, %v", published, err)
	}
	current, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := current.Spec.Volumes[0]; got.Version != 1 || got.Artifact != artifactID {
		t.Fatalf("publish changed existing workspace pin: %+v", got)
	}
	consumer := mustWS(t, c, proto.WorkspaceSpec{
		Placement: proto.Placement{Node: n1.ID()},
		Volumes:   []proto.VolumeMount{{ID: "dataset", Path: "/shared"}},
	})
	consumerBytes, err := c.ReadFile(ctx, consumer.ID, "/shared/dataset.txt")
	if err != nil || string(consumerBytes) != "version two" {
		t.Fatalf("new consumer mounted bytes = %q, %v", consumerBytes, err)
	}
	if mounted, ok := firstMounts.attachment(consumer.ID); !ok || mounted.VolumeVersion != 2 || mounted.Artifact != published.Artifact {
		t.Fatalf("new consumer mount = %+v, present=%v", mounted, ok)
	}

	if _, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: n2.ID()}); err != nil {
		t.Fatal(err)
	}
	moved, err := c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Node != n2.ID() {
		t.Fatalf("moved node = %s, want %s", moved.Node, n2.ID())
	}
	if mounted, ok := secondMounts.attachment(ws.ID); !ok || mounted.VolumeVersion != 1 || mounted.Artifact != artifactID {
		t.Fatalf("second node mount = %+v, present=%v", mounted, ok)
	}
	if _, ok := firstMounts.attachment(ws.ID); ok {
		t.Fatal("source mount remained after durable move commit")
	}
	movedBytes, err := c.ReadFile(ctx, ws.ID, "/shared/dataset.txt")
	if err != nil || string(movedBytes) != "version one" {
		t.Fatalf("moved pinned bytes = %q, %v", movedBytes, err)
	}

	// The moved workspace checkpoint must not have absorbed volume bytes. If
	// it had, restoring then mounting would find a non-empty target and the
	// real bind-mount backend would refuse it.
	if moved.LastSnapshot == "" {
		t.Fatal("move did not commit a workspace checkpoint")
	}
	rc, err := c.DownloadSnapshot(ctx, moved.LastSnapshot, moved.LastSnapshotFormat)
	if err != nil {
		t.Fatal(err)
	}
	restored := t.TempDir()
	err = artifact.Restore(restored, rc)
	closeErr := rc.Close()
	if err := errors.Join(err, closeErr); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restored, "shared")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shared volume leaked into workspace snapshot: %v", err)
	}
	nested, err := os.ReadFile(filepath.Join(restored, "src", "shared", "keep"))
	if err != nil || string(nested) != "ordinary workspace bytes" {
		t.Fatalf("root-anchored exclusion dropped nested same-name path: %q, %v", nested, err)
	}
}
