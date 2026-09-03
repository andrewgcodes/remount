package control

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

func TestExportCursorCASIsDurableTenantScopedAndEvented(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	f := newControlFixture(t, path, nil)
	storeA, err := f.c.ExportCursors("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := storeA.Load(context.Background(), "otlp-primary")
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := storeA.CompareAndSwap(context.Background(), "otlp-primary", initial, 42)
	if err != nil || advanced.Next != 42 || advanced.Revision != 1 {
		t.Fatalf("advance = %+v, %v", advanced, err)
	}
	storeB, _ := f.c.ExportCursors("tenant-b")
	other, err := storeB.Load(context.Background(), "otlp-primary")
	if err != nil || other.Next != 0 || other.Revision != 0 {
		t.Fatalf("cross-tenant cursor = %+v, %v", other, err)
	}
	events, err := f.log.Read(context.Background(), 0, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != proto.EvExportAdvanced || events[0].Tenant != "tenant-a" {
		t.Fatalf("cursor events = %+v", events)
	}

	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newControlFixture(t, path, nil)
	restartedStore, _ := restarted.c.ExportCursors("tenant-a")
	got, err := restartedStore.Load(context.Background(), "otlp-primary")
	if err != nil || got.Next != 42 || got.Revision != 1 {
		t.Fatalf("restarted cursor = %+v, %v", got, err)
	}
}

func TestExportCursorCASFencesConcurrentExportersAndCapacity(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) { opts.MaxExportCursorsPerTenant = 1 })
	store, _ := f.c.ExportCursors("tenant-a")
	initial, _ := store.Load(context.Background(), "one")
	var successes atomic.Int64
	var conflicts atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 24; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := store.CompareAndSwap(context.Background(), "one", initial, 2); err == nil {
				successes.Add(1)
			} else if errors.Is(err, eventlog.ErrCursorConflict) {
				conflicts.Add(1)
			}
		}()
	}
	wait.Wait()
	if successes.Load() != 1 || conflicts.Load() != 23 {
		t.Fatalf("successes=%d conflicts=%d", successes.Load(), conflicts.Load())
	}
	second, _ := store.Load(context.Background(), "two")
	if _, err := store.CompareAndSwap(context.Background(), "two", second, 3); err == nil {
		t.Fatal("cursor capacity admitted a second tenant cursor")
	} else {
		var protocolError *proto.Error
		if !errors.As(err, &protocolError) || protocolError.Code != proto.CodeResourceExhausted {
			t.Fatalf("capacity error = %v", err)
		}
	}
}

func TestFollowedExportCursorSettlesPastOwnAuditEvent(t *testing.T) {
	f := newControlFixture(t, "", nil)
	if err := f.log.Append(context.Background(), &proto.Event{Type: "source", Tenant: "tenant-a"}); err != nil {
		t.Fatal(err)
	}
	cursors, err := f.c.ExportCursors("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &countingExportSink{}
	done := make(chan error, 1)
	go func() {
		_, err := eventlog.RunExport(ctx, f.log, sink, cursors, eventlog.RunOptions{
			Name: "follow-no-spin", From: 1, Follow: true, PollInterval: time.Millisecond,
		})
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cursor, loadErr := cursors.Load(context.Background(), "follow-no-spin")
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if cursor.Next >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cursor did not advance")
		}
		time.Sleep(time.Millisecond)
	}
	// Several follow polls must observe a quiescent source: the audit event
	// was atomically covered by the cursor rather than becoming new work.
	time.Sleep(20 * time.Millisecond)
	if sink.events.Load() != 1 {
		t.Fatalf("delivered %d source events, want one", sink.events.Load())
	}
	last, err := f.log.Last(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := cursors.Load(context.Background(), "follow-no-spin")
	if err != nil || cursor.Next != last+1 {
		t.Fatalf("cursor=%+v last=%d err=%v", cursor, last, err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunExport = %v", err)
	}
}

func TestExportCursorDoesNotSkipAppendBetweenDeliveryAndCommit(t *testing.T) {
	f := newControlFixture(t, "", nil)
	if err := f.log.Append(context.Background(), &proto.Event{Type: "first", Tenant: "tenant-a"}); err != nil {
		t.Fatal(err)
	}
	inner, err := f.c.ExportCursors("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	cursors := &appendBeforeCursorCAS{inner: inner, log: f.log}
	firstSink := &recordingControlExportSink{}
	if _, err := eventlog.RunExport(context.Background(), f.log, firstSink, cursors, eventlog.RunOptions{
		Name: "interleaved", From: 1, To: 1,
	}); err != nil {
		t.Fatal(err)
	}
	cursor, err := inner.Load(context.Background(), "interleaved")
	if err != nil || cursor.Next != 2 {
		t.Fatalf("cursor skipped interleaved seq: %+v, %v", cursor, err)
	}
	secondSink := &recordingControlExportSink{}
	if _, err := eventlog.RunExport(context.Background(), f.log, secondSink, inner, eventlog.RunOptions{
		Name: "interleaved", From: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(secondSink.types) < 1 || secondSink.types[0] != "interleaved" {
		t.Fatalf("interleaved event was not replayed: %v", secondSink.types)
	}
}

type appendBeforeCursorCAS struct {
	inner eventlog.CursorStore
	log   *eventlog.Log
	once  sync.Once
}

func (s *appendBeforeCursorCAS) Load(ctx context.Context, name string) (eventlog.Cursor, error) {
	return s.inner.Load(ctx, name)
}

func (s *appendBeforeCursorCAS) CompareAndSwap(ctx context.Context, name string, previous eventlog.Cursor, next uint64) (eventlog.Cursor, error) {
	var appendErr error
	s.once.Do(func() {
		appendErr = s.log.Append(ctx, &proto.Event{Type: "interleaved", Tenant: "tenant-a"})
	})
	if appendErr != nil {
		return previous, appendErr
	}
	return s.inner.CompareAndSwap(ctx, name, previous, next)
}

type recordingControlExportSink struct{ types []string }

func (s *recordingControlExportSink) Send(_ context.Context, events []proto.Event) error {
	for _, event := range events {
		s.types = append(s.types, event.Type)
	}
	return nil
}

type countingExportSink struct{ events atomic.Int64 }

func (s *countingExportSink) Send(_ context.Context, events []proto.Event) error {
	s.events.Add(int64(len(events)))
	return nil
}
