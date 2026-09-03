package compliance_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/compliance"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

func TestSQLiteRestartBundleRemainsVerifiableAfterRetention(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "events.db")
	bundlePath := filepath.Join(directory, "tenant-a.audit")
	store, err := eventlog.OpenSQLite(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(store)
	for index := 0; index < 40; index++ {
		tenant := "tenant-a"
		if index%2 != 0 {
			tenant = "tenant-b"
		}
		event := &proto.Event{
			At:        int64(index + 1),
			Type:      "integration.audit",
			Tenant:    tenant,
			Stream:    "tenant:" + tenant,
			Principal: "integration-subject",
			Payload:   proto.MustMarshal(map[string]int{"index": index}),
		}
		if err := log.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	seed := sha256.Sum256([]byte("restart integration signing key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	exporter, err := compliance.NewExporter(log, "integration-key", privateKey, compliance.Options{
		Now: func() time.Time { return time.Date(2026, 9, 3, 18, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := exporter.Export(ctx, compliance.Request{Tenant: "tenant-a", From: 1, To: 40}, &compliance.AtomicFileSink{Path: bundlePath})
	if err != nil {
		t.Fatal(err)
	}
	if signed.Manifest.EventCount != 20 || signed.Manifest.FirstSeq != 1 || signed.Manifest.LastSeq != 39 {
		t.Fatalf("manifest = %+v", signed.Manifest)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart the canonical store, then evict the source range. Verification of
	// the already committed bundle must remain independent of source retention.
	reopenedStore, err := eventlog.OpenSQLite(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	reopened := eventlog.New(reopenedStore)
	defer reopened.Close()
	if removed, err := reopened.PruneSize(ctx, 0, 100); err != nil || removed != 40 {
		t.Fatalf("source retention after restart = (%d, %v)", removed, err)
	}
	file, err := os.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	keyring, err := compliance.NewKeyring(map[string]ed25519.PublicKey{
		"integration-key": privateKey.Public().(ed25519.PublicKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := compliance.Verify(ctx, file, keyring, compliance.VerifyOptions{ExpectedTenant: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if verified != signed.Manifest {
		t.Fatalf("verified manifest = %+v, want %+v", verified, signed.Manifest)
	}
}
