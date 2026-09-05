package failover_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact/s3"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/control/replicate"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

type memoryObject struct {
	data []byte
	info s3.ObjectInfo
}

type memoryStore struct {
	mu      sync.Mutex
	objects map[string]memoryObject
	next    uint64
}

func newMemoryStore() *memoryStore                                { return &memoryStore{objects: map[string]memoryObject{}} }
func (*memoryStore) CheckConditionalWrites(context.Context) error { return nil }
func (s *memoryStore) PutObject(_ context.Context, key string, reader io.Reader, size int64, options s3.PutOptions) (s3.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prior, exists := s.objects[key]
	if options.IfNoneMatch == "*" && exists || options.IfMatch != "" && (!exists || prior.info.ETag != options.IfMatch) {
		return s3.ObjectInfo{}, s3.ErrPreconditionFailed
	}
	data, err := io.ReadAll(io.LimitReader(reader, size+1))
	if err != nil || int64(len(data)) != size {
		return s3.ObjectInfo{}, errors.Join(err, io.ErrUnexpectedEOF)
	}
	digest := sha256.Sum256(data)
	if options.ContentSHA256 != "" && options.ContentSHA256 != hex.EncodeToString(digest[:]) {
		return s3.ObjectInfo{}, errors.New("content digest mismatch")
	}
	s.next++
	info := s3.ObjectInfo{Key: key, Size: size, ETag: strconv.FormatUint(s.next, 10), LastModified: time.Now(), Metadata: options.Metadata}
	s.objects[key] = memoryObject{data: data, info: info}
	return info, nil
}
func (s *memoryStore) OpenObject(_ context.Context, key string) (io.ReadCloser, s3.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return nil, s3.ObjectInfo{}, &s3.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), object.data...))), object.info, nil
}
func (s *memoryStore) HeadObject(_ context.Context, key string) (s3.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return s3.ObjectInfo{}, &s3.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}
	}
	return object.info, nil
}
func (s *memoryStore) ListObjects(_ context.Context, prefix string) ([]s3.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []s3.ObjectInfo
	for key, object := range s.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, object.info)
		}
	}
	return result, nil
}
func (s *memoryStore) DeleteObject(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

type recoveryNode struct{ report proto.ControllerNodeState }

func (*recoveryNode) Send(context.Context, *proto.Frame) error { return nil }
func (n *recoveryNode) Online(id string) bool                  { return id == n.report.Node }
func (n *recoveryNode) Peers() []string                        { return []string{n.report.Node} }
func (n *recoveryNode) Request(_ context.Context, to, operation string, _ any, out any) error {
	if to != n.report.Node {
		return proto.Err(proto.CodeUnreachable, "node is offline")
	}
	switch operation {
	case proto.OpControllerState:
		state, ok := out.(*proto.ControllerNodeState)
		if !ok {
			return errors.New("unexpected recovery response type")
		}
		*state = n.report
		return nil
	case proto.OpWSReleaseAbort, proto.OpWSReleaseAbortCommit:
		return nil
	default:
		return proto.Err(proto.CodeUnsupported, "unexpected recovery operation %s", operation)
	}
}

// TestWarmStandbyFailoverE10 proves the composed recovery contract: only a
// committed recovery point is restored, the epoch advances exactly once, and
// node-authoritative evidence rolls an unshipped move back without duplicating
// a workspace generation.
func TestWarmStandbyFailoverE10(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	activePath := filepath.Join(directory, "active.db")
	store := newMemoryStore()
	now := time.Unix(1_800_000_000, 0)
	options := replicate.Options{Prefix: "e10", LeaseTTL: 3 * time.Second, RenewInterval: time.Second, ShipInterval: time.Second, SnapshotInterval: time.Hour, OrphanGrace: 3 * time.Second, MaxSnapshotBytes: 16 << 20, MaxWALBytes: 16 << 20, Now: func() time.Time { return now }}
	activeLease, err := replicate.NewCoordinator(store, "active", options)
	if err != nil {
		t.Fatal(err)
	}
	if epoch, _, err := activeLease.Acquire(ctx); err != nil || epoch != 1 {
		t.Fatalf("active acquire=(%d,%v)", epoch, err)
	}
	activeSQLite, err := eventlog.OpenSQLite(activePath)
	if err != nil {
		t.Fatal(err)
	}
	activeLog := eventlog.New(activeSQLite)
	activeControl, err := control.New(control.Options{DB: activeSQLite.DB(), Log: activeLog, LeaseSec: 10, Now: func() time.Time { return now }, ControllerAuthority: activeLease})
	if err != nil {
		t.Fatal(err)
	}
	workspace := proto.Workspace{ID: "ws_e10", Tenant: "local", Owner: "operator", AuthzRevision: 1, Generation: 7, Node: "n_e10", State: proto.WSClaimed, LeaseUntil: now.Add(time.Minute).UnixMilli(), Spec: proto.WorkspaceSpec{Principal: "operator"}}
	if _, err := activeSQLite.DB().Exec(`INSERT INTO workspaces(id,data) VALUES(?,?)`, workspace.ID, proto.MustMarshal(workspace)); err != nil {
		t.Fatal(err)
	}
	if err := activeSQLite.Append(ctx, &proto.Event{At: now.UnixMilli(), Type: proto.EvWSClaimed, Workspace: workspace.ID, Generation: workspace.Generation, Node: workspace.Node, ControllerEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	source, err := replicate.NewSQLiteSource(activeSQLite.DB(), activePath, directory, 16<<20, 16<<20, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	shipper, err := replicate.NewShipper(store, source, activeLease, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shipper.Ship(ctx, 1, true); err != nil {
		t.Fatal(err)
	}
	// The active begins a move after the last published manifest. The retained
	// node has prepared it, but the control transaction is deliberately lost.
	now = now.Add(500 * time.Millisecond)
	unshippedAt := now
	operationID := "op_e10_move"
	workspace.State, workspace.ReleaseOperation = proto.WSQuiescing, operationID
	if _, err := activeSQLite.DB().Exec(`UPDATE workspaces SET data=? WHERE id=?`, proto.MustMarshal(workspace), workspace.ID); err != nil {
		t.Fatal(err)
	}
	activeControl.Stop()
	if err := activeLog.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(3500 * time.Millisecond)
	standbyLease, _ := replicate.NewCoordinator(store, "standby", options)
	restorer, _ := replicate.NewRestorer(store, standbyLease, options)
	promotedPath := filepath.Join(directory, "promoted.db")
	started := time.Now()
	recovery, err := restorer.Promote(ctx, promotedPath)
	if err != nil {
		t.Fatal(err)
	}
	promotedSQLite, err := eventlog.OpenSQLite(promotedPath)
	if err != nil {
		t.Fatal(err)
	}
	promotedLog := eventlog.New(promotedSQLite)
	promoted, err := control.New(control.Options{DB: promotedSQLite.DB(), Log: promotedLog, LeaseSec: 10, Now: func() time.Time { return now }, ControllerAuthority: standbyLease, ControllerRole: "promoted", Recovery: &control.RecoveryState{PreviousEpoch: recovery.PreviousEpoch, RestoredEventSeq: recovery.Manifest.SourceEventSeq, LastReplicatedAt: recovery.LastReplicatedAt, PromotedAt: recovery.PromotedAt, LostWindow: recovery.LostWindow}})
	if err != nil {
		t.Fatal(err)
	}
	defer promoted.Stop()
	defer promotedLog.Close()
	const releaseEpoch = 7
	node := &recoveryNode{report: proto.ControllerNodeState{Node: workspace.Node, Epoch: recovery.Epoch, Releases: []proto.ControllerReleaseState{{Request: proto.WSReleaseReq{WS: workspace.ID, Gen: workspace.Generation, ReleaseEpoch: releaseEpoch, OperationID: operationID, Tenant: workspace.Tenant, Spec: workspace.Spec}, OperationID: operationID, State: "prepared"}}}}
	promoted.Attach(node)
	if err := promoted.ReconcileRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("promotion RTO=%s, want <5s", elapsed)
	}
	var encoded []byte
	if err := promotedSQLite.DB().QueryRow(`SELECT data FROM workspaces WHERE id=?`, workspace.ID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var got proto.Workspace
	if err := proto.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSClaimed || got.Generation != workspace.Generation || got.Node != workspace.Node ||
		got.ReleaseEpoch != releaseEpoch || got.ReleaseOperation != "" {
		t.Fatalf("reconciled workspace=%+v", got)
	}
	var assignments int
	if err := promotedSQLite.DB().QueryRow(`SELECT count(*) FROM assignments WHERE workspace=? AND generation=?`, workspace.ID, workspace.Generation).Scan(&assignments); err != nil || assignments != 1 {
		t.Fatalf("assignments=%d err=%v, want exactly one", assignments, err)
	}
	events, err := promotedLog.Read(ctx, 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var reconciled, recovered bool
	for _, event := range events {
		if (event.Type == proto.EvControlReconciled || event.Type == proto.EvControlRecovered) && event.ControllerEpoch != recovery.Epoch {
			t.Fatalf("event %s epoch=%d, want %d", event.Type, event.ControllerEpoch, recovery.Epoch)
		}
		reconciled = reconciled || event.Type == proto.EvControlReconciled
		recovered = recovered || event.Type == proto.EvControlRecovered
	}
	if !reconciled || !recovered || recovery.Epoch != 2 || !recovery.NeedsReconcile {
		t.Fatalf("reconciliation evidence missing: reconciled=%v recovered=%v recovery=%+v", reconciled, recovered, recovery)
	}
	if rpo := unshippedAt.Sub(recovery.LastReplicatedAt); rpo < 0 || rpo > options.ShipInterval {
		t.Fatalf("deliberately lost move was %s beyond the recovery point, want <= ship interval %s", rpo, options.ShipInterval)
	}
}

var _ replicate.Store = (*memoryStore)(nil)
