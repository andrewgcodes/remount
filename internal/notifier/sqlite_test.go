package notifier

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"remount.dev/remount/internal/proto"
)

func openDeadLetterDB(t *testing.T, path string, max int) (*sql.DB, *SQLiteDeadLetterStore) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	store, err := NewSQLiteDeadLetterStore(db, max)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, store
}

func testDeadLetter(tenant string, seq uint64, failedAt int64) DeadLetter {
	return DeadLetter{
		SubscriptionID: "sub_1", Tenant: tenant, FirstSeq: seq, LastSeq: seq,
		EventTypes: []string{proto.EvEgressPending}, Attempts: 3,
		Reason: "delivery_failed", FailedAt: failedAt,
	}
}

func TestSQLiteDeadLettersSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifier.db")
	db, store := openDeadLetterDB(t, path, 4)
	want := testDeadLetter("tenant-a", 7, 1234)
	if err := store.Store(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, store = openDeadLetterDB(t, path, 4)
	defer db.Close()
	records, err := store.List(context.Background(), "tenant-a", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID == 0 || records[0].DeadLetter.SubscriptionID != want.SubscriptionID ||
		records[0].FirstSeq != want.FirstSeq || records[0].FailedAt != want.FailedAt {
		t.Fatalf("records = %#v", records)
	}
}

func TestSQLiteDeadLetterCapacityIsPerTenantAndAtomic(t *testing.T) {
	db, store := openDeadLetterDB(t, ":memory:", 2)
	defer db.Close()
	ctx := context.Background()
	if err := store.Store(ctx, testDeadLetter("tenant-a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.Store(ctx, testDeadLetter("tenant-a", 2, 2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Store(ctx, testDeadLetter("tenant-a", 3, 3)); !errors.Is(err, ErrDeadLetterCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := store.Store(ctx, testDeadLetter("tenant-b", 4, 4)); err != nil {
		t.Fatalf("other tenant admission: %v", err)
	}
	records, err := store.List(ctx, "tenant-a", 0, 10)
	if err != nil || len(records) != 2 {
		t.Fatalf("tenant-a records=%#v err=%v", records, err)
	}
}

func TestSQLiteDeadLetterPruneIsBoundedAndRestoresCapacity(t *testing.T) {
	db, store := openDeadLetterDB(t, ":memory:", 3)
	defer db.Close()
	ctx := context.Background()
	for seq := uint64(1); seq <= 3; seq++ {
		if err := store.Store(ctx, testDeadLetter("tenant-a", seq, int64(seq))); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := store.Prune(ctx, "tenant-a", 4, 2)
	if err != nil || removed != 2 {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	remaining, err := store.List(ctx, "tenant-a", 0, 10)
	if err != nil || len(remaining) != 1 || remaining[0].FirstSeq != 3 {
		t.Fatalf("remaining=%#v err=%v", remaining, err)
	}
	if err := store.Store(ctx, testDeadLetter("tenant-a", 4, 4)); err != nil {
		t.Fatalf("admission after prune: %v", err)
	}
}

func TestSQLiteDeadLetterRejectsUnsanitizedMetadata(t *testing.T) {
	db, store := openDeadLetterDB(t, ":memory:", 3)
	defer db.Close()
	letter := testDeadLetter("tenant-a", 1, 1)
	letter.Reason = "request failed: Authorization: Bearer secret"
	if err := store.Store(context.Background(), letter); !errors.Is(err, ErrInvalidDeadLetter) {
		t.Fatalf("error = %v", err)
	}
	letter = testDeadLetter("tenant-a", 1, 1)
	letter.SubscriptionID = "secret\nvalue"
	if err := store.Store(context.Background(), letter); !errors.Is(err, ErrInvalidDeadLetter) {
		t.Fatalf("error = %v", err)
	}
}
