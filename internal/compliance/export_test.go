package compliance

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

type memorySink struct {
	mu   sync.Mutex
	data []byte
}

func (s *memorySink) Commit(_ context.Context, produce func(io.Writer) error) error {
	var staging bytes.Buffer
	if err := produce(&staging); err != nil {
		return err
	}
	s.mu.Lock()
	s.data = append([]byte(nil), staging.Bytes()...)
	s.mu.Unlock()
	return nil
}

func (s *memorySink) bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func testKeys(t testing.TB) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seed := sha256.Sum256([]byte("remount compliance test signing key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	return append(ed25519.PublicKey(nil), private.Public().(ed25519.PublicKey)...), private
}

func mixedLog(t testing.TB, count int) (*eventlog.Log, [][]byte) {
	t.Helper()
	log := eventlog.New(eventlog.NewMemory(0))
	payloads := make([][]byte, count)
	for index := 0; index < count; index++ {
		tenant := "tenant-a"
		marker := fmt.Sprintf("a-%d", index)
		if index%2 == 1 {
			tenant, marker = "tenant-b", fmt.Sprintf("b-private-%d", index)
		}
		payload := proto.MustMarshal(map[string]any{"marker": marker, "nested": map[string]any{"z": 1, "a": "two"}})
		payloads[index] = append([]byte(nil), payload...)
		if err := log.Append(context.Background(), &proto.Event{At: int64(index+1) * 1000, Type: "audit.event", Tenant: tenant,
			Stream: "tenant:" + tenant, Principal: "agent:alice", Workspace: fmt.Sprintf("ws_%d", index), Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	return log, payloads
}

func newTestExporter(t testing.TB, source eventlog.Exporter, now time.Time, options Options) (*Exporter, *Keyring) {
	t.Helper()
	public, private := testKeys(t)
	exporter, err := NewExporter(source, "audit-key-2026-09", private, Options{
		Now: func() time.Time { return now }, MaxRangeEvents: options.MaxRangeEvents,
		MaxEventBytes: options.MaxEventBytes, MaxPayloadBytes: options.MaxPayloadBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewKeyring(map[string]ed25519.PublicKey{"audit-key-2026-09": public})
	if err != nil {
		t.Fatal(err)
	}
	return exporter, keys
}

func TestExportFiltersTenantPreservesPayloadAndIsDeterministic(t *testing.T) {
	log, originalPayloads := mixedLog(t, 6)
	defer log.Close()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	exporter, keys := newTestExporter(t, log, now, Options{})
	first, second := &memorySink{}, &memorySink{}
	signed, err := exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 6}, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 6}, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.bytes(), second.bytes()) {
		t.Fatal("fixed-clock export was not byte deterministic")
	}
	verified, err := Verify(context.Background(), bytes.NewReader(first.bytes()), keys, VerifyOptions{ExpectedTenant: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if signed.Manifest != verified || signed.Manifest.EventCount != 3 || signed.Manifest.RangeFrom != 1 || signed.Manifest.RangeTo != 6 || signed.Manifest.FirstSeq != 1 || signed.Manifest.LastSeq != 5 {
		t.Fatalf("manifest = %+v, verified = %+v", signed.Manifest, verified)
	}
	if bytes.Contains(first.bytes(), []byte("b-private")) || !bytes.Contains(first.bytes(), []byte("a-0")) {
		t.Fatalf("tenant filter failed: %s", first.bytes())
	}
	events, err := log.Read(context.Background(), 1, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for index := range events {
		if !bytes.Equal(events[index].Payload, originalPayloads[index]) {
			t.Fatalf("source payload %d was mutated", index)
		}
	}
}

func TestExportRejectsCrossTenantAndInvalidRanges(t *testing.T) {
	log, _ := mixedLog(t, 2)
	defer log.Close()
	exporter, _ := newTestExporter(t, log, time.Unix(1_800_000_000, 0), Options{MaxRangeEvents: 2})
	for _, request := range []Request{{Tenant: "*", From: 1, To: 2}, {Tenant: "", From: 1, To: 2}, {Tenant: "tenant-a", From: 0, To: 2}, {Tenant: "tenant-a", From: 2, To: 1}} {
		if _, err := exporter.Export(context.Background(), request, &memorySink{}); err == nil {
			t.Fatalf("invalid request accepted: %+v", request)
		}
	}
	if _, err := exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 3}, &memorySink{}); !errors.Is(err, ErrLimit) {
		t.Fatalf("range bound = %v", err)
	}
}

func TestVerifyRejectsPayloadManifestAndKeyTampering(t *testing.T) {
	log, _ := mixedLog(t, 4)
	defer log.Close()
	exporter, keys := newTestExporter(t, log, time.Unix(1_800_000_000, 0), Options{})
	sink := &memorySink{}
	if _, err := exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 4}, sink); err != nil {
		t.Fatal(err)
	}
	original := sink.bytes()
	mutations := [][]byte{
		bytes.Replace(append([]byte(nil), original...), []byte("a-0"), []byte("a-X"), 1),
		bytes.Replace(append([]byte(nil), original...), []byte(`"event_count":2`), []byte(`"event_count":9`), 1),
		append([]byte(nil), original[:bytes.LastIndexByte(original[:len(original)-1], '\n')+1]...),
		append(append([]byte(nil), original...), []byte(`{"seq":999,"at":1,"tenant":"tenant-a","type":"late"}`+"\n")...),
	}
	for index, mutated := range mutations {
		if _, err := Verify(context.Background(), bytes.NewReader(mutated), keys, VerifyOptions{ExpectedTenant: "tenant-a"}); !errors.Is(err, ErrTampered) {
			t.Fatalf("tamper %d accepted: %v", index, err)
		}
	}
	otherPublic, _ := testKeysWithSeed("other")
	wrongKeys, _ := NewKeyring(map[string]ed25519.PublicKey{"audit-key-2026-09": otherPublic})
	if _, err := Verify(context.Background(), bytes.NewReader(original), wrongKeys, VerifyOptions{}); !errors.Is(err, ErrTampered) {
		t.Fatalf("wrong key accepted: %v", err)
	}
	if _, err := Verify(context.Background(), bytes.NewReader(original), keys, VerifyOptions{ExpectedTenant: "tenant-b"}); !errors.Is(err, ErrTampered) {
		t.Fatalf("wrong expected tenant accepted: %v", err)
	}
}

func TestVerifyRejectsTrustedSignatureOverMixedTenants(t *testing.T) {
	public, private := testKeys(t)
	keys, err := NewKeyring(map[string]ed25519.PublicKey{"key": public})
	if err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	for sequence, tenant := range []string{"tenant-a", "tenant-b"} {
		line, err := eventlog.MarshalEventJSONLine(proto.Event{
			Seq: uint64(sequence + 1), At: int64(sequence + 1), Tenant: tenant, Type: "audit.event",
		})
		if err != nil {
			t.Fatal(err)
		}
		bundle.Write(line)
	}
	payload := bundle.Bytes()
	digest := sha256.Sum256(payload)
	manifest := Manifest{
		Schema: bundleSchema, Tenant: "tenant-a", RangeFrom: 1, RangeTo: 2, CreatedAt: 1,
		EventCount: 2, FirstSeq: 1, LastSeq: 2, PayloadBytes: uint64(len(payload)),
		PayloadSHA256: hex.EncodeToString(digest[:]), HashAlgorithm: "sha256", SignatureAlgorithm: "ed25519", KeyID: "key",
	}
	signed := SignedManifest{Manifest: manifest, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, manifestSigningBytes(manifest)))}
	trailer, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Write(trailer)
	bundle.WriteByte('\n')
	if _, err := Verify(context.Background(), bytes.NewReader(bundle.Bytes()), keys, VerifyOptions{}); !errors.Is(err, ErrTampered) {
		t.Fatalf("trusted mixed-tenant bundle accepted: %v", err)
	}
}

func testKeysWithSeed(value string) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte(value))
	private := ed25519.NewKeyFromSeed(seed[:])
	return private.Public().(ed25519.PublicKey), private
}

type pruningExporter struct {
	log  *eventlog.Log
	once sync.Once
}

func (p *pruningExporter) Export(ctx context.Context, from, to uint64, sink func(proto.Event) error) (uint64, error) {
	return p.log.Export(ctx, from, to, func(event proto.Event) error {
		if err := sink(event); err != nil {
			return err
		}
		p.once.Do(func() { _, _ = p.log.PruneSize(ctx, 10, 10_000) })
		return nil
	})
}

func TestEvictionAndRetentionRaceLeaveDestinationUntouched(t *testing.T) {
	log, _ := mixedLog(t, 1025)
	defer log.Close()
	_, private := testKeys(t)
	exporter, err := NewExporter(&pruningExporter{log: log}, "key", private, Options{Now: func() time.Time { return time.Unix(1_800_000_000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "audit.bundle")
	if err := os.WriteFile(path, []byte("previous-good"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 1025}, &AtomicFileSink{Path: path, Replace: true})
	var gap *GapError
	if !errors.As(err, &gap) || gap.Kind != GapIncomplete || gap.Expected != 513 {
		t.Fatalf("retention race = %#v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != "previous-good" {
		t.Fatalf("failed export changed destination: %q", data)
	}

	pruned, _ := mixedLog(t, 8)
	defer pruned.Close()
	if _, err := pruned.PruneSize(context.Background(), 2, 20); err != nil {
		t.Fatal(err)
	}
	exporter, _ = NewExporter(pruned, "key", private, Options{Now: func() time.Time { return time.Unix(1_800_000_000, 0) }})
	missing := filepath.Join(t.TempDir(), "missing.bundle")
	_, err = exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 8}, &AtomicFileSink{Path: missing})
	if !errors.As(err, &gap) || gap.Kind != GapEvicted || gap.Oldest != 7 {
		t.Fatalf("pre-export eviction = %#v", err)
	}
	if _, statErr := os.Stat(missing); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("evicted export published a file: %v", statErr)
	}
}

func TestLargeExportUsesBoundedRangeAndVerifies(t *testing.T) {
	log, _ := mixedLog(t, 3000)
	defer log.Close()
	exporter, keys := newTestExporter(t, log, time.Unix(1_800_000_000, 0), Options{MaxRangeEvents: 3000, MaxEventBytes: 4096, MaxPayloadBytes: 4 << 20})
	path := filepath.Join(t.TempDir(), "large.bundle")
	signed, err := exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 3000}, &AtomicFileSink{Path: path, MaxBytes: 5 << 20})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	verified, err := Verify(context.Background(), file, keys, VerifyOptions{ExpectedTenant: "tenant-a", MaxEvents: 1500, MaxEventBytes: 4096, MaxBundleBytes: 5 << 20})
	if err != nil || verified != signed.Manifest || verified.EventCount != 1500 {
		t.Fatalf("large verify = %+v, %v", verified, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle mode = %v, %v", info.Mode(), err)
	}
}

func TestAtomicFileNoClobberAndCleanup(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "audit.bundle")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	sink := &AtomicFileSink{Path: path}
	if err := sink.Commit(context.Background(), func(writer io.Writer) error { _, err := writer.Write([]byte("replacement")); return err }); err == nil {
		t.Fatal("no-clobber sink replaced existing destination")
	}
	if data, _ := os.ReadFile(path); string(data) != "original" {
		t.Fatalf("destination changed: %q", data)
	}
	old := filepath.Join(directory, stagingPrefix+"old")
	recent := filepath.Join(directory, stagingPrefix+"recent")
	unrelated := filepath.Join(directory, "unrelated")
	for _, candidate := range []string{old, recent, unrelated} {
		if err := os.WriteFile(candidate, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, stagingPrefix+"link")
	if err := os.Symlink(unrelated, symlink); err != nil {
		t.Fatal(err)
	}
	removed, err := sink.CleanupStaging(context.Background(), time.Now().Add(-time.Hour), 1)
	if err != nil || removed != 1 {
		t.Fatalf("cleanup = %d, %v", removed, err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old staging survived: %v", err)
	}
	for _, preserved := range []string{recent, unrelated, symlink} {
		if _, err := os.Lstat(preserved); err != nil {
			t.Fatalf("cleanup removed %s: %v", filepath.Base(preserved), err)
		}
	}
}

func FuzzVerifyNeverAcceptsCorruptionOrPanics(f *testing.F) {
	public, private := testKeys(f)
	log := eventlog.New(eventlog.NewMemory(0))
	defer log.Close()
	_ = log.Append(context.Background(), &proto.Event{At: 1, Type: "seed", Tenant: "tenant-a", Payload: proto.MustMarshal(map[string]string{"a": "b"})})
	exporter, _ := NewExporter(log, "key", private, Options{Now: func() time.Time { return time.Unix(1, 0) }, MaxEventBytes: 4096, MaxPayloadBytes: 8192})
	sink := &memorySink{}
	_, _ = exporter.Export(context.Background(), Request{Tenant: "tenant-a", From: 1, To: 1}, sink)
	f.Add(sink.bytes())
	f.Add([]byte("not a bundle\n"))
	keys, _ := NewKeyring(map[string]ed25519.PublicKey{"key": public})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Verify(context.Background(), bytes.NewReader(data), keys, VerifyOptions{MaxEvents: 32, MaxEventBytes: 4096, MaxBundleBytes: 64 << 10})
	})
}

var _ eventlog.Exporter = (*pruningExporter)(nil)
