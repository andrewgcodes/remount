package eventlog

import (
	"context"
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
	evs, _ := l.Read(ctx, 1, "", 0)
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
