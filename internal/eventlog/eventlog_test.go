package eventlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

func stores(t *testing.T) map[string]Store {
	t.Helper()
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "ev.db"))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]Store{"memory": NewMemory(0), "sqlite": sq}
}

func TestAppendReadLast(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			l := New(st)
			defer l.Close()
			ctx := context.Background()
			if last, _ := l.Last(ctx); last != 0 {
				t.Fatal(last)
			}
			for i := 0; i < 5; i++ {
				stream := "ws_a"
				if i%2 == 1 {
					stream = "ws_b"
				}
				e, err := l.Emit(ctx, "t.x", stream, "p", "n", map[string]int{"i": i}, 0)
				if err != nil {
					t.Fatal(err)
				}
				if e.Seq != uint64(i+1) || e.At == 0 {
					t.Fatalf("%+v", e)
				}
			}
			evs, _ := l.Read(ctx, 1, "", 0)
			if len(evs) != 5 || evs[4].Seq != 5 {
				t.Fatalf("%d", len(evs))
			}
			evs, _ = l.Read(ctx, 3, "", 0)
			if len(evs) != 3 || evs[0].Seq != 3 {
				t.Fatalf("%+v", evs)
			}
			evs, _ = l.Read(ctx, 1, "ws_b", 0)
			if len(evs) != 2 || evs[0].Seq != 2 || evs[1].Seq != 4 {
				t.Fatalf("%+v", evs)
			}
			evs, _ = l.Read(ctx, 1, "", 2)
			if len(evs) != 2 {
				t.Fatal(len(evs))
			}
			var p map[string]int
			proto.Unmarshal(evs[1].Payload, &p)
			if p["i"] != 1 {
				t.Fatal(p)
			}
			if last, _ := l.Last(ctx); last != 5 {
				t.Fatal(last)
			}
		})
	}
}

func TestSubscribeHistoryThenLive(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			l := New(st)
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l.Emit(ctx, "a", "", "", "", nil, 0)
			l.Emit(ctx, "b", "", "", "", nil, 0)
			sub := l.Subscribe(1, "")
			defer sub.Close()
			evs, err := sub.Next(ctx)
			if err != nil || len(evs) != 2 || evs[0].Type != "a" {
				t.Fatalf("%v %+v", err, evs)
			}
			go func() {
				time.Sleep(10 * time.Millisecond)
				l.Emit(ctx, "c", "", "", "", nil, 0)
			}()
			evs, err = sub.Next(ctx)
			if err != nil || len(evs) != 1 || evs[0].Type != "c" || evs[0].Seq != 3 {
				t.Fatalf("%v %+v", err, evs)
			}
			// Only-new subscription.
			last, _ := l.Last(ctx)
			sub2 := l.Subscribe(last+1, "ws_z")
			defer sub2.Close()
			l.Emit(ctx, "ignored", "ws_other", "", "", nil, 0)
			l.Emit(ctx, "mine", "ws_z", "", "", nil, 0)
			evs, err = sub2.Next(ctx)
			if err != nil || len(evs) != 1 || evs[0].Type != "mine" {
				t.Fatalf("%v %+v", err, evs)
			}
		})
	}
}

func TestSubscribeSlowConsumerCatchesUp(t *testing.T) {
	l := New(NewMemory(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := l.Subscribe(1, "")
	defer sub.Close()
	// Overflow the 256-deep live channel.
	for i := 0; i < 1000; i++ {
		l.Emit(ctx, "e", "", "", "", nil, 0)
	}
	var got uint64
	for got < 1000 {
		evs, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range evs {
			if e.Seq != got+1 {
				t.Fatalf("out of order or gap: got %d after %d", e.Seq, got)
			}
			got = e.Seq
		}
	}
}

func TestMemoryBounded(t *testing.T) {
	m := NewMemory(3)
	l := New(m)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		l.Emit(ctx, "e", "", "", "", nil, 0)
	}
	if _, err := l.Read(ctx, 1, "", 0); !errors.Is(err, &proto.Error{Code: proto.CodeEvicted}) {
		t.Fatalf("retention gap error = %v", err)
	}
	evs, _ := l.Read(ctx, 0, "", 0)
	if len(evs) != 3 || evs[0].Seq != 3 {
		t.Fatalf("%+v", evs)
	}
}

func TestSQLitePersistsAcrossOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.db")
	s, _ := OpenSQLite(path)
	l := New(s)
	l.Emit(context.Background(), "x", "", "", "", nil, 0)
	l.Close()
	s2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	last, _ := s2.Last(context.Background())
	if last != 1 {
		t.Fatal(last)
	}
	e := &proto.Event{Type: "y", At: 1}
	s2.Append(context.Background(), e)
	if e.Seq != 2 {
		t.Fatal(e.Seq)
	}
}

func TestPruneReportsRetentionGapAndNeverPunchesMiddleHole(t *testing.T) {
	for name, st := range stores(t) {
		t.Run(name, func(t *testing.T) {
			defer st.Close()
			l := New(st)
			ctx := context.Background()
			for _, at := range []int64{1, 100, 2} {
				if err := l.Append(ctx, &proto.Event{Type: "test", At: at}); err != nil {
					t.Fatal(err)
				}
			}
			removed, err := l.Prune(ctx, 50, 10)
			if err != nil || removed != 1 {
				t.Fatalf("first prune = (%d, %v)", removed, err)
			}
			first, _ := l.First(ctx)
			if first != 2 {
				t.Fatalf("first retained = %d, want 2", first)
			}
			if _, err := l.Read(ctx, 1, "", 10); !errors.Is(err, &proto.Error{Code: proto.CodeEvicted}) {
				t.Fatalf("read below retention = %v", err)
			} else {
				var protocolErr *proto.Error
				if !errors.As(err, &protocolErr) || protocolErr.Oldest != 2 {
					t.Fatalf("retention error = %+v", protocolErr)
				}
			}
			events, err := l.Read(ctx, 0, "", 10)
			if err != nil || len(events) != 2 || events[0].Seq != 2 || events[1].Seq != 3 {
				t.Fatalf("retained events = %+v, %v", events, err)
			}
			// Bounded passes are resumable and preserve the highest assigned seq
			// even after every row has been removed.
			if n, err := l.Prune(ctx, 200, 1); err != nil || n != 1 {
				t.Fatalf("second prune = (%d, %v)", n, err)
			}
			if n, err := l.Prune(ctx, 200, 1); err != nil || n != 1 {
				t.Fatalf("third prune = (%d, %v)", n, err)
			}
			if first, _ := l.First(ctx); first != 4 {
				t.Fatalf("first after full prune = %d, want 4", first)
			}
			if last, _ := l.Last(ctx); last != 3 {
				t.Fatalf("last assigned after full prune = %d, want 3", last)
			}
		})
	}
}

func TestSQLitePrunePersistsNodeProducerHighWatermark(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "producer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := &proto.Event{Type: "node", At: 1, Origin: "node", Node: "n_one", ProducerSeq: 7}
	if err := s.Append(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Prune(context.Background(), 2, 10); err != nil || n != 1 {
		t.Fatalf("Prune = (%d, %v)", n, err)
	}
	var seq uint64
	if err := s.DB().QueryRow(`SELECT producer_seq FROM event_producers WHERE node='n_one'`).Scan(&seq); err != nil || seq != 7 {
		t.Fatalf("producer watermark = (%d, %v)", seq, err)
	}
}

func TestPruneSizeBoundsRowsAndReportsWatermark(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			defer store.Close()
			log := New(store)
			for index := 0; index < 7; index++ {
				if err := log.Append(context.Background(), &proto.Event{Type: "sized", At: int64(index + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			if removed, err := log.PruneSize(context.Background(), 3, 2); err != nil || removed != 2 {
				t.Fatalf("first PruneSize = (%d, %v)", removed, err)
			}
			if removed, err := log.PruneSize(context.Background(), 3, 10); err != nil || removed != 2 {
				t.Fatalf("second PruneSize = (%d, %v)", removed, err)
			}
			if first, err := log.First(context.Background()); err != nil || first != 5 {
				t.Fatalf("first retained = (%d, %v), want 5", first, err)
			}
			events, err := log.Read(context.Background(), 0, "", 10)
			if err != nil || len(events) != 3 || events[0].Seq != 5 || events[2].Seq != 7 {
				t.Fatalf("retained = %+v, %v", events, err)
			}
			if _, err := log.Read(context.Background(), 4, "", 10); !errors.Is(err, &proto.Error{Code: proto.CodeEvicted}) {
				t.Fatalf("read below size watermark = %v", err)
			}
		})
	}
}

func TestSubscriptionCloseWakesBlockedNext(t *testing.T) {
	l := New(NewMemory(0))
	defer l.Close()
	sub := l.Subscribe(1, "")
	done := make(chan error, 1)
	go func() {
		_, err := sub.Next(context.Background())
		done <- err
	}()
	sub.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed subscription returned no error")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake Next")
	}
	// Idempotent close must not panic.
	sub.Close()
}
