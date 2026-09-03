package failover_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	"remount.dev/remount/internal/artifact/s3"
	"remount.dev/remount/internal/control/replicate"
)

func TestMinIOControlFailoverE10Core(t *testing.T) {
	endpoint := os.Getenv("REMOUNT_S3_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("REMOUNT_S3_INTEGRATION_ENDPOINT is unset; MinIO failover lane is unavailable")
	}
	store, err := s3.New(s3.Config{Endpoint: endpoint, Region: os.Getenv("REMOUNT_S3_INTEGRATION_REGION"), Bucket: os.Getenv("REMOUNT_S3_INTEGRATION_BUCKET"), Prefix: fmt.Sprintf("failover-e10/%d", time.Now().UnixNano()), AccessKeyID: os.Getenv("REMOUNT_S3_INTEGRATION_ACCESS_KEY"), SecretAccessKey: os.Getenv("REMOUNT_S3_INTEGRATION_SECRET_KEY"), PathStyle: true, MaxObjectBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "active.db")
	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE moves (generation INTEGER PRIMARY KEY, state TEXT); INSERT INTO moves VALUES (1, 'committed')`); err != nil {
		t.Fatal(err)
	}
	opts := replicate.Options{Prefix: "control", LeaseTTL: 600 * time.Millisecond, RenewInterval: 200 * time.Millisecond, ShipInterval: 100 * time.Millisecond, SnapshotInterval: time.Hour, MaxClockSkew: 50 * time.Millisecond, MaxSnapshotBytes: 16 << 20, MaxWALBytes: 16 << 20}
	active, err := replicate.NewCoordinator(store, "active", opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := active.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := replicate.NewSQLiteSource(db, sourcePath, dir, 16<<20, 16<<20, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	shipper, _ := replicate.NewShipper(store, source, active, opts)
	if _, err := shipper.Ship(ctx, 1, true); err != nil {
		t.Fatal(err)
	}
	// This transaction is the deliberately delayed, unshipped tail at failure.
	if _, err := db.Exec(`INSERT INTO moves VALUES (2, 'unshipped')`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	standby, _ := replicate.NewCoordinator(store, "standby", opts)
	restorer, _ := replicate.NewRestorer(store, standby, opts)
	destination := filepath.Join(dir, "promoted.db")
	started := time.Now()
	recovery, err := restorer.Promote(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	rto := time.Since(started)
	restored, err := sql.Open("sqlite", destination)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var count, maxGeneration int
	if err := restored.QueryRow(`SELECT count(*), max(generation) FROM moves`).Scan(&count, &maxGeneration); err != nil {
		t.Fatal(err)
	}
	if count != 1 || maxGeneration != 1 {
		t.Fatalf("restored count=%d generation=%d; duplicated or admitted uncommitted tail", count, maxGeneration)
	}
	if recovery.LostWindow <= 0 || !recovery.NeedsReconcile {
		t.Fatalf("recovery did not expose lost window: %+v", recovery)
	}
	t.Logf("measured core promotion RTO=%s lost-window-estimate=%s epoch=%d", rto, recovery.LostWindow, recovery.Epoch)
}
