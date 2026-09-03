package control

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

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
