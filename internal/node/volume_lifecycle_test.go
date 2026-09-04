package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/volume"
)

func lifecycleArtifact(t *testing.T, n *Node) string {
	t.Helper()
	var archive bytes.Buffer
	if err := artifact.Snapshot(t.TempDir(), nil, &archive); err != nil {
		t.Fatal(err)
	}
	id, _, err := n.store.Put(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type lifecycleVolumes struct {
	attachCalls int
	detachCalls int
	detachErr   error
	lastAttach  volume.AttachRequest
}

func (*lifecycleVolumes) EnsureVersion(context.Context, string, string, uint64, string) error {
	return nil
}
func (*lifecycleVolumes) Create(context.Context, volume.CreateRequest) (volume.Volume, error) {
	return volume.Volume{}, nil
}
func (*lifecycleVolumes) Delete(context.Context, volume.DeleteRequest) error { return nil }
func (*lifecycleVolumes) Publish(context.Context, volume.PublishRequest) (volume.Volume, error) {
	return volume.Volume{}, nil
}
func (v *lifecycleVolumes) Attach(_ context.Context, req volume.AttachRequest) (volume.Attachment, error) {
	v.attachCalls++
	v.lastAttach = req
	return volume.Attachment{Tenant: req.Tenant, Workspace: req.Workspace, Generation: req.Generation, Path: req.Path}, nil
}
func (v *lifecycleVolumes) Detach(context.Context, volume.DetachRequest) error {
	v.detachCalls++
	return v.detachErr
}
func (*lifecycleVolumes) List(context.Context, string) ([]volume.Volume, error) { return nil, nil }
func (*lifecycleVolumes) Inspect(context.Context, string, string) (volume.Detail, error) {
	return volume.Detail{}, nil
}
func (*lifecycleVolumes) SetWorkspaceGeneration(context.Context, string, string, uint64) error {
	return nil
}
func (*lifecycleVolumes) Stats() volume.Stats { return volume.Stats{} }
func (*lifecycleVolumes) Close() error        { return nil }

func releaseTestWorkspace(t *testing.T, n *Node, id string, mounts []proto.VolumeMount) (*ws, string) {
	t.Helper()
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), id, proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := mustHostFS(t, handle).Root()
	w := &ws{Workspace: proto.Workspace{
		ID: id, Tenant: "tenant-a", Generation: 7, State: proto.WSClaimed,
		Spec: proto.WorkspaceSpec{Requires: proto.Requires{Backend: "process"}, Volumes: mounts},
	}, handle: handle}
	n.mu.Lock()
	n.workspaces[id] = w
	n.mu.Unlock()
	return w, root
}

func TestReleaseDetachFailureCannotProducePreparedProof(t *testing.T) {
	volumes := &lifecycleVolumes{detachErr: errors.New("unmount still present")}
	n := newTestNode(t, func(opts *Options) { opts.Volumes = volumes })
	mount := proto.VolumeMount{ID: "dataset", Path: "/data", Version: 1, Artifact: lifecycleArtifact(t, n)}
	w, _ := releaseTestWorkspace(t, n, "ws_release_detach_failure", []proto.VolumeMount{mount})

	if _, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}); err == nil {
		t.Fatal("release acknowledged despite failed exact volume detach")
	}
	n.mu.Lock()
	serving := n.workspaces[w.ID] == w
	_, prepared := n.prepared[w.ID]
	n.mu.Unlock()
	if serving || !prepared || volumes.detachCalls != 2 || volumes.attachCalls != 0 {
		t.Fatalf("rollback serving=%v prepared=%v detach=%d attach=%d", serving, prepared, volumes.detachCalls, volumes.attachCalls)
	}
	if record, ok := n.releaseRecord(w.ID); !ok || record.State != releaseAborting {
		t.Fatalf("failed detach did not retain abort authority: %+v present=%v", record, ok)
	}
}

func TestReleaseAbortReattachesExactPinsBeforeServing(t *testing.T) {
	volumes := &lifecycleVolumes{}
	n := newTestNode(t, func(opts *Options) { opts.Volumes = volumes })
	mount := proto.VolumeMount{ID: "dataset", Path: "/data", Version: 3, Artifact: lifecycleArtifact(t, n)}
	w, _ := releaseTestWorkspace(t, n, "ws_release_abort_mounts", []proto.VolumeMount{mount})
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}); err != nil {
		t.Fatal(err)
	}
	record, _ := n.releaseRecord(w.ID)
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, OperationID: record.OperationID}
	if err := n.releaseAbort(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	serving := n.workspaces[w.ID] == w
	n.mu.Unlock()
	if serving || volumes.detachCalls != 2 || volumes.attachCalls != 1 {
		t.Fatalf("abort serving=%v detach=%d attach=%d", serving, volumes.detachCalls, volumes.attachCalls)
	}
	if err := n.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	serving = n.workspaces[w.ID] == w
	n.mu.Unlock()
	if !serving {
		t.Fatal("abort commit did not publish restored source")
	}
	if volumes.lastAttach.ID != mount.ID || volumes.lastAttach.Version != mount.Version ||
		volumes.lastAttach.Artifact != mount.Artifact || volumes.lastAttach.Path != mount.Path ||
		volumes.lastAttach.Generation != w.Generation || volumes.lastAttach.Tenant != w.Tenant {
		t.Fatalf("abort reattached wrong pin: %+v", volumes.lastAttach)
	}
}

func TestReleaseLifecycleFenceResumesOnlyAfterAbortCommit(t *testing.T) {
	n := newTestNode(t, nil)
	fs, err := fsops.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &failingHandle{id: "ws_release_fenced_abort", fs: fs}
	w := &ws{Workspace: proto.Workspace{ID: h.id, Tenant: "tenant-a", Generation: 7, State: proto.WSClaimed}, handle: h}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.mu.Unlock()
	result, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation, Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	response := result.(proto.WSReleasedReq)
	if h.fenced.Load() != 1 || h.resumed.Load() != 0 {
		t.Fatalf("prepare fence=%d resume=%d", h.fenced.Load(), h.resumed.Load())
	}
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, OperationID: response.OperationID, Snapshot: response.Snapshot, SnapshotFormat: response.SnapshotFormat}
	if err := n.releaseAbort(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if h.resumed.Load() != 0 {
		t.Fatal("abort preparation resumed before durable publication authorization")
	}
	if err := n.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if err := n.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if h.resumed.Load() != 1 {
		t.Fatalf("abort publication resumed %d times", h.resumed.Load())
	}
}

func TestReleaseLifecycleFenceCommitNeverResumesSource(t *testing.T) {
	n := newTestNode(t, nil)
	fs, err := fsops.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &failingHandle{id: "ws_release_fenced_commit", fs: fs}
	w := &ws{Workspace: proto.Workspace{ID: h.id, Tenant: "tenant-a", Generation: 7, State: proto.WSClaimed}, handle: h}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.mu.Unlock()
	result, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation, Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	response := result.(proto.WSReleasedReq)
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, OperationID: response.OperationID, Snapshot: response.Snapshot, SnapshotFormat: response.SnapshotFormat}
	if err := n.releaseCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if h.resumed.Load() != 0 || h.destroyed.Load() != 1 {
		t.Fatalf("commit resume=%d destroy=%d", h.resumed.Load(), h.destroyed.Load())
	}
}

func TestReleasePreparedProofAndCommitTombstoneSurviveRestart(t *testing.T) {
	directory := t.TempDir()
	n, err := New(Options{DataDir: directory})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(n) })
	w, root := releaseTestWorkspace(t, n, "ws_release_restart", nil)
	req := &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}
	prepared, err := n.release(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.(proto.WSReleasedReq)

	closeNodeRuntimeForTest(n)
	restarted, err := New(Options{DataDir: directory})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(restarted) })
	if _, err := restarted.release(context.Background(), &proto.WSReleaseReq{
		WS: w.ID, Gen: w.Generation, Tenant: "other-tenant",
	}); err == nil {
		t.Fatal("restart retry accepted changed control-derived tenant")
	}
	freshRetry := &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}
	replayed, err := restarted.release(context.Background(), freshRetry)
	if err != nil || replayed.(proto.WSReleasedReq) != result {
		t.Fatalf("replayed prepare=%#v err=%v", replayed, err)
	}
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation}
	if err := restarted.releaseCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source survived committed destruction: %v", err)
	}
	if err := restarted.releaseCommit(context.Background(), commit); err != nil {
		t.Fatalf("lost commit acknowledgement replay: %v", err)
	}
	record, ok := restarted.releaseRecord(w.ID)
	if !ok || record.State != releaseCommitted {
		t.Fatalf("durable release tombstone=%+v present=%v", record, ok)
	}
	newGeneration, _ := releaseTestWorkspace(t, restarted, w.ID, nil)
	newGeneration.Generation = 8
	if _, err := restarted.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: 8}); err != nil {
		t.Fatalf("higher generation could not replace old commit tombstone: %v", err)
	}
}

func TestReleaseAbortAfterRestartRebuildsRuntimeAndExactPins(t *testing.T) {
	directory := t.TempDir()
	volumes := &lifecycleVolumes{}
	n, err := New(Options{DataDir: directory, Volumes: volumes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(n) })
	mount := proto.VolumeMount{ID: "dataset", Path: "/data", Version: 6, Artifact: lifecycleArtifact(t, n)}
	w, _ := releaseTestWorkspace(t, n, "ws_release_abort_restart", []proto.VolumeMount{mount})
	if _, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation}); err != nil {
		t.Fatal(err)
	}

	closeNodeRuntimeForTest(n)
	restarted, err := New(Options{DataDir: directory, Volumes: volumes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(restarted) })
	record, _ := restarted.releaseRecord(w.ID)
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, OperationID: record.OperationID}
	if err := restarted.releaseAbort(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	restored := restarted.workspaces[w.ID]
	restarted.mu.Unlock()
	if restored != nil {
		t.Fatalf("abort published runtime before control authorization: %+v", restored)
	}
	if err := restarted.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	restored = restarted.workspaces[w.ID]
	restarted.mu.Unlock()
	if restored == nil || restored.Generation != w.Generation || restored.broker == nil {
		t.Fatalf("abort commit did not publish rebuilt runtime: %+v", restored)
	}
	_ = restored.broker.Close()

	// A crash after the durable published tombstone loses the in-memory
	// runtime. The same handshake must reconstruct it before acknowledging the
	// control-plane retry; a published journal is not by itself serviceability.
	closeNodeRuntimeForTest(restarted)
	again, err := New(Options{DataDir: directory, Volumes: volumes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(again) })
	if err := again.releaseAbort(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	again.mu.Lock()
	servingBeforeCommit := again.workspaces[w.ID] != nil
	again.mu.Unlock()
	if servingBeforeCommit {
		t.Fatal("published tombstone recovery served before abort commit retry")
	}
	if err := again.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	again.mu.Lock()
	republished := again.workspaces[w.ID]
	again.mu.Unlock()
	if republished == nil || republished.broker == nil {
		t.Fatalf("published tombstone recovery did not rebuild serviceability: %+v", republished)
	}
	t.Cleanup(func() { _ = republished.broker.Close() })
	if volumes.lastAttach.ID != mount.ID || volumes.lastAttach.Version != mount.Version ||
		volumes.lastAttach.Artifact != mount.Artifact || volumes.lastAttach.Path != mount.Path ||
		volumes.lastAttach.Generation != w.Generation || volumes.lastAttach.Tenant != w.Tenant {
		t.Fatalf("restart abort reattached wrong pin: %+v", volumes.lastAttach)
	}
}

func TestRecoveredQuarantineDetachesDeclaredPinsBeforeProof(t *testing.T) {
	volumes := &lifecycleVolumes{}
	n := newTestNode(t, func(opts *Options) { opts.Volumes = volumes })
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_quarantine_recovered", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = handle.FS().Close()
	mount := proto.VolumeMount{ID: "dataset", Path: "/data", Version: 4,
		Artifact: "art_sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	res, err := n.quarantine(context.Background(), &proto.WSQuarantineReq{
		OperationID: "fleet-recovered", WS: "ws_quarantine_recovered", Gen: 9,
		Action: proto.FleetActionDestroy, Backend: "process", Tenant: "tenant-a", Volumes: []proto.VolumeMount{mount},
	})
	if err != nil || !res.Fenced || res.Snapshot == "" || volumes.detachCalls != 1 {
		t.Fatalf("recovered quarantine=%+v err=%v detaches=%d", res, err, volumes.detachCalls)
	}
}

func TestRecoveredQuarantineDetachFailureRetainsSourceAndNoProof(t *testing.T) {
	volumes := &lifecycleVolumes{detachErr: errors.New("mount remains visible")}
	n := newTestNode(t, func(opts *Options) { opts.Volumes = volumes })
	backend, err := n.opts.Backends.Get("process")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := backend.Create(context.Background(), "ws_quarantine_detach_failure", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := mustHostFS(t, handle).Root()
	_ = handle.FS().Close()
	req := &proto.WSQuarantineReq{
		OperationID: "fleet-detach-failure", WS: "ws_quarantine_detach_failure", Gen: 5,
		Action: proto.FleetActionDestroy, Backend: "process", Tenant: "tenant-a",
		Volumes: []proto.VolumeMount{{ID: "dataset", Path: "/data", Version: 2,
			Artifact: "art_sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}},
	}
	if _, err := n.quarantine(context.Background(), req); err == nil {
		t.Fatal("quarantine acknowledged despite failed exact volume detach")
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("failed detach discarded retained source: %v", err)
	}
	key := "fleet:" + req.OperationID + ":" + req.WS
	n.mutationMu.Lock()
	_, hasProof := n.mutations[key]
	n.mutationMu.Unlock()
	if hasProof {
		t.Fatal("failed detach left a durable quarantine proof")
	}
}

func TestRestartRefusesQuarantineWithoutDurableQuiescenceProof(t *testing.T) {
	directory := t.TempDir()
	volumes := &lifecycleVolumes{}
	n, err := New(Options{DataDir: directory, Volumes: volumes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(n) })
	w, root := releaseTestWorkspace(t, n, "ws_quarantine_ambiguous", nil)
	req := proto.WSQuarantineReq{
		OperationID: "fleet-ambiguous", WS: w.ID, Gen: w.Generation,
		Action: proto.FleetActionDestroy, Backend: "process", Tenant: w.Tenant,
	}
	key := "fleet:" + req.OperationID + ":" + req.WS
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := n.runMutation(context.Background(), key, req, func() ([]byte, error) {
			close(entered)
			<-release
			return proto.Marshal(proto.WSQuarantineRes{Fenced: true})
		})
		done <- err
	}()
	<-entered

	restarted, err := New(Options{DataDir: directory, Volumes: volumes})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(restarted) })
	if _, err := restarted.quarantine(context.Background(), &req); err == nil {
		close(release)
		t.Fatal("restart acknowledged quarantine without durable producer-join proof")
	}
	if volumes.detachCalls != 0 {
		close(release)
		t.Fatalf("ambiguous quarantine detached retained volumes %d times", volumes.detachCalls)
	}
	if _, err := os.Stat(root); err != nil {
		close(release)
		t.Fatalf("ambiguous quarantine discarded retained tree: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestQuarantineJournalRetainsNonterminalAuthorityAtCapacity(t *testing.T) {
	n := newTestNode(t, func(opts *Options) { opts.MaxMutationRecords = 1 })
	first := proto.WSQuarantineReq{
		OperationID: "fleet-retained-proof", WS: "ws_retained_proof", Gen: 4,
		Action: proto.FleetActionDestroy, Backend: "process", Tenant: "tenant-a",
	}
	n.quarantineMu.Lock()
	n.quarantines[quarantineKey(first.OperationID, first.WS)] = durableQuarantine{
		Request: first, State: quarantineCheckpointed,
		Response:    proto.WSQuarantineRes{Fenced: true, Generation: first.Gen, Action: first.Action, Backend: first.Backend},
		CompletedAt: time.Now().Add(-90 * 24 * time.Hour).UnixMilli(),
	}
	if err := n.persistQuarantinesLocked(); err != nil {
		n.quarantineMu.Unlock()
		t.Fatal(err)
	}
	n.quarantineMu.Unlock()

	_, _, err := n.beginQuarantine(proto.WSQuarantineReq{
		OperationID: "fleet-new", WS: "ws_new", Gen: 1, Action: proto.FleetActionFreeze,
	})
	var protocolErr *proto.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != proto.CodeResourceExhausted {
		t.Fatalf("journal admission error=%v", err)
	}
	restarted, err := New(Options{DataDir: n.opts.DataDir, Volumes: n.volumes, MaxMutationRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNodeRuntimeForTest(restarted) })
	record, ok := restarted.quarantineRecord(first.OperationID, first.WS)
	if !ok || record.State != quarantineCheckpointed {
		t.Fatalf("nonterminal quarantine proof was pruned: %+v present=%v", record, ok)
	}
}

func TestDestroyQuarantineBlocksNewOperationUntilCommit(t *testing.T) {
	n := newTestNode(t, nil)
	w, _ := releaseTestWorkspace(t, n, "ws_quarantine_epoch", nil)
	first := &proto.WSQuarantineReq{
		OperationID: "fleet-destroy-old", WS: w.ID, Gen: w.Generation,
		Action: proto.FleetActionDestroy, Tenant: w.Tenant, Backend: "process",
	}
	res, err := n.quarantine(context.Background(), first)
	if err != nil || res.Snapshot == "" {
		t.Fatalf("first quarantine=%+v err=%v", res, err)
	}
	second := *first
	second.OperationID = "fleet-new-cycle"
	second.Action = proto.FleetActionFreeze
	if _, err := n.quarantine(context.Background(), &second); err == nil {
		t.Fatal("new quarantine cycle bypassed an outstanding destroy proof")
	}
	old, ok := n.quarantineRecord(first.OperationID, first.WS)
	if !ok || old.State != quarantinePrepared {
		t.Fatalf("old destroy proof changed: %+v present=%v", old, ok)
	}
}

func TestReleaseCommitAndAbortWaitsAreContextBounded(t *testing.T) {
	n := newTestNode(t, nil)
	w, _ := releaseTestWorkspace(t, n, "ws_release_serialized", nil)
	prepared, err := n.release(context.Background(), &proto.WSReleaseReq{WS: w.ID, Gen: w.Generation})
	if err != nil {
		t.Fatal(err)
	}
	result := prepared.(proto.WSReleasedReq)
	n.releaseReconcileMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = n.releaseCommit(ctx, &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, Snapshot: result.Snapshot})
	cancel()
	if err == nil {
		n.releaseReconcileMu.Unlock()
		t.Fatal("release commit crossed an active reconciliation boundary")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = n.releaseAbort(ctx, &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation})
	cancel()
	n.releaseReconcileMu.Unlock()
	if err == nil {
		t.Fatal("release abort crossed an active reconciliation boundary")
	}
	record, _ := n.releaseRecord(w.ID)
	commit := &proto.WSReleaseCommitReq{ID: w.ID, Gen: w.Generation, OperationID: record.OperationID}
	if err := n.releaseAbort(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	serving := n.workspaces[w.ID] == w
	n.mu.Unlock()
	if serving {
		t.Fatal("serialized abort published before authorization")
	}
	if err := n.releaseAbortCommit(context.Background(), commit); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	serving = n.workspaces[w.ID] == w
	n.mu.Unlock()
	if !serving {
		t.Fatal("serialized abort commit did not publish retained source")
	}
}
