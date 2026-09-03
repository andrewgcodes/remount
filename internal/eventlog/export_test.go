package eventlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

func exportEvents(t *testing.T, count int) *Log {
	t.Helper()
	log := New(NewMemory(0))
	for index := 0; index < count; index++ {
		event := &proto.Event{
			At: int64(index+1) * 1000, Type: "s.opened", Stream: "ws_one",
			Workspace: "ws_one", Session: "s_one", Principal: "agent:alice",
			Tenant: "t_one", Payload: proto.MustMarshal(map[string]any{"index": index}),
		}
		if err := log.Append(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	return log
}

type recordingSink struct {
	mu      sync.Mutex
	batches [][]uint64
	errAt   int
}

func (s *recordingSink) Send(_ context.Context, events []proto.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := make([]uint64, len(events))
	for index, event := range events {
		batch[index] = event.Seq
	}
	s.batches = append(s.batches, batch)
	if s.errAt > 0 && len(s.batches) == s.errAt {
		return errors.New("destination unavailable")
	}
	return nil
}

func TestRunExportBatchesAndAdvancesCursorAfterAcceptance(t *testing.T) {
	log := exportEvents(t, 5)
	defer log.Close()
	cursors, err := NewMemoryCursorStore(2)
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	last, err := RunExport(context.Background(), log, sink, cursors, RunOptions{Name: "otlp-main", From: 1, BatchEvents: 2})
	if err != nil || last != 5 {
		t.Fatalf("RunExport = (%d, %v)", last, err)
	}
	if got := sink.batches; len(got) != 3 || !equalSeqs(got[0], []uint64{1, 2}) || !equalSeqs(got[2], []uint64{5}) {
		t.Fatalf("batches = %v", got)
	}
	cursor, err := cursors.Load(context.Background(), "otlp-main")
	if err != nil || cursor.Next != 6 || cursor.Revision != 3 {
		t.Fatalf("cursor = %+v, %v", cursor, err)
	}

	second := &recordingSink{}
	if last, err := RunExport(context.Background(), log, second, cursors, RunOptions{Name: "otlp-main", From: 1}); err != nil || last != 0 || len(second.batches) != 0 {
		t.Fatalf("resumed export = (%d, %v), batches %v", last, err, second.batches)
	}
}

func TestRunExportDoesNotAdvanceRejectedBatch(t *testing.T) {
	log := exportEvents(t, 4)
	defer log.Close()
	cursors, _ := NewMemoryCursorStore(1)
	sink := &recordingSink{errAt: 2}
	last, err := RunExport(context.Background(), log, sink, cursors, RunOptions{Name: "failed", From: 1, BatchEvents: 2})
	if err == nil || last != 2 {
		t.Fatalf("RunExport = (%d, %v)", last, err)
	}
	cursor, _ := cursors.Load(context.Background(), "failed")
	if cursor.Next != 3 {
		t.Fatalf("cursor advanced over rejected batch: %+v", cursor)
	}

	retry := &recordingSink{}
	last, err = RunExport(context.Background(), log, retry, cursors, RunOptions{Name: "failed", From: 1, BatchEvents: 2})
	if err != nil || last != 4 || len(retry.batches) != 1 || !equalSeqs(retry.batches[0], []uint64{3, 4}) {
		t.Fatalf("retry = (%d, %v), batches %v", last, err, retry.batches)
	}
}

func TestRunExportRejectsGapAndOversize(t *testing.T) {
	source := exporterFunc(func(_ context.Context, _, _ uint64, sink func(proto.Event) error) (uint64, error) {
		if err := sink(proto.Event{Seq: 1, Type: "one"}); err != nil {
			return 0, err
		}
		if err := sink(proto.Event{Seq: 3, Type: "three"}); err != nil {
			return 1, err
		}
		return 3, nil
	})
	if _, err := RunExport(context.Background(), source, &recordingSink{}, nil, RunOptions{From: 1}); err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("gap error = %v", err)
	}
	large := exportEvents(t, 1)
	defer large.Close()
	events, _ := large.Read(context.Background(), 1, "", 1)
	events[0].Payload = proto.MustMarshal(map[string]string{"large": strings.Repeat("x", 100)})
	oversize := exporterFunc(func(_ context.Context, _, _ uint64, sink func(proto.Event) error) (uint64, error) {
		return 0, sink(events[0])
	})
	if _, err := RunExport(context.Background(), oversize, &recordingSink{}, nil, RunOptions{From: 1, BatchBytes: 32}); !errors.Is(err, ErrExportBatchTooLarge) {
		t.Fatalf("oversize error = %v", err)
	}
}

func TestMemoryCursorStoreFencesConcurrencyAndBoundsNames(t *testing.T) {
	store, _ := NewMemoryCursorStore(1)
	initial, _ := store.Load(context.Background(), "one")
	var successes atomic.Int64
	var conflicts atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 20; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := store.CompareAndSwap(context.Background(), "one", initial, 2); err == nil {
				successes.Add(1)
			} else if errors.Is(err, ErrCursorConflict) {
				conflicts.Add(1)
			}
		}()
	}
	wait.Wait()
	if successes.Load() != 1 || conflicts.Load() != 19 {
		t.Fatalf("successes=%d conflicts=%d", successes.Load(), conflicts.Load())
	}
	missing, _ := store.Load(context.Background(), "two")
	if _, err := store.CompareAndSwap(context.Background(), "two", missing, 1); err == nil || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("capacity error = %v", err)
	}
	current, _ := store.Load(context.Background(), "one")
	if _, err := store.CompareAndSwap(context.Background(), "one", current, 1); err == nil || !strings.Contains(err.Error(), "backward") {
		t.Fatalf("backward error = %v", err)
	}
	current.Revision = math.MaxUint64
	store.mu.Lock()
	store.cursors["one"] = current
	store.mu.Unlock()
	if _, err := store.CompareAndSwap(context.Background(), "one", current, 3); err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("revision exhaustion = %v", err)
	}
}

func TestMarshalEventJSONLineIsCanonicalAndDecodesPayload(t *testing.T) {
	event := proto.Event{Seq: 7, At: 8, Type: "cred.used", Payload: proto.MustMarshal(map[string]any{"z": 1, "a": "two"})}
	first, err := MarshalEventJSONLine(event)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalEventJSONLine(event)
	if err != nil || !bytes.Equal(first, second) || first[len(first)-1] != '\n' {
		t.Fatalf("canonical line mismatch: %q %q %v", first, second, err)
	}
	if string(first) != "{\"seq\":7,\"at\":8,\"type\":\"cred.used\",\"payload\":{\"a\":\"two\",\"z\":1}}\n" {
		t.Fatalf("line = %s", first)
	}
}

func TestJSONLSinkHandlesShortWrites(t *testing.T) {
	writer := &shortWriter{limit: 3}
	sink := &JSONLSink{Writer: writer}
	if err := sink.Send(context.Background(), []proto.Event{{Seq: 1, Type: "x"}}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(writer.String(), "\n") {
		t.Fatalf("output = %q", writer.String())
	}
}

type shortWriter struct {
	bytes.Buffer
	limit int
}

func (w *shortWriter) Write(payload []byte) (int, error) {
	if len(payload) > w.limit {
		payload = payload[:w.limit]
	}
	return w.Buffer.Write(payload)
}

type exporterFunc func(context.Context, uint64, uint64, func(proto.Event) error) (uint64, error)

func (f exporterFunc) Export(ctx context.Context, from, to uint64, sink func(proto.Event) error) (uint64, error) {
	return f(ctx, from, to, sink)
}

func equalSeqs(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestE16OTLPHTTPJSONStreamsSessionsAsTraces(t *testing.T) {
	var requests atomic.Int64
	var captured []byte
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %s %s", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer collector.Close()

	sink, err := NewOTLPSink(OTLPOptions{Endpoint: collector.URL, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	events := []proto.Event{
		{Seq: 1, At: 1000, Type: "s.opened", Session: "s_trace", Principal: "agent:alice", Payload: proto.MustMarshal(map[string]string{"secret": "must-not-enter-span"})},
		{Seq: 2, At: 2000, Type: "egress.denied", Session: "s_trace", Cause: 1},
	}
	if err := sink.Send(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || bytes.Contains(captured, []byte("must-not-enter-span")) {
		t.Fatalf("requests=%d body=%s", requests.Load(), captured)
	}
	var request otlpTraceRequest
	if err := json.Unmarshal(captured, &request); err != nil {
		t.Fatal(err)
	}
	spans := request.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 || spans[0].TraceID != spans[1].TraceID || spans[0].SpanID != spans[1].ParentSpanID {
		t.Fatalf("session trace mapping = %+v", spans)
	}
	if !hasOTLPAttribute(spans[0].Attributes, "gen_ai.operation.name", "execute_tool") || !hasOTLPAttribute(spans[0].Attributes, "gen_ai.agent.id", "alice") {
		t.Fatalf("gen_ai attributes = %+v", spans[0].Attributes)
	}
}

func TestOTLPRetriesIdenticalBodyAndRejectsPartialSuccess(t *testing.T) {
	var attempts atomic.Int64
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"partialSuccess":{"rejectedSpans":"1"}}`)
	}))
	defer server.Close()
	sink, err := NewOTLPSink(OTLPOptions{Endpoint: server.URL, Retries: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = sink.Send(context.Background(), []proto.Event{{Seq: 1, At: 1, Type: "x"}})
	if err == nil || !strings.Contains(err.Error(), "partially rejected") || attempts.Load() != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatalf("attempts=%d err=%v bodiesEqual=%v", attempts.Load(), err, len(bodies) == 2 && bytes.Equal(bodies[0], bodies[1]))
	}
}

func TestHTTPExporterErrorsNeverIncludeCredentialsOrResponseBody(t *testing.T) {
	const secret = "super-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, secret)
	}))
	defer server.Close()
	sink, err := NewOTLPSink(OTLPOptions{Endpoint: server.URL, Headers: http.Header{"Authorization": {"Bearer " + secret}}, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	err = sink.Send(context.Background(), []proto.Event{{Seq: 1, Type: "x"}})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe error = %v", err)
	}
	if _, err := NewOTLPSink(OTLPOptions{Endpoint: "https://user:" + secret + "@example.com"}); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe endpoint error = %v", err)
	}
}

func hasOTLPAttribute(attributes []otlpAttribute, key, value string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key && attribute.Value.StringValue == value {
			return true
		}
	}
	return false
}

type objectRecorder struct {
	objects []ExportObject
}

func (r *objectRecorder) RecordExportObject(_ context.Context, object ExportObject) error {
	r.objects = append(r.objects, object)
	return nil
}

func TestE16S3SinkWritesHourlyContentHashedJSONL(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	recorder := &objectRecorder{}
	sink := &S3Sink{Store: store, Recorder: recorder}
	events := []proto.Event{
		{Seq: 1, At: time.Date(2026, 9, 3, 1, 10, 0, 0, time.UTC).UnixMilli(), Type: "one"},
		{Seq: 2, At: time.Date(2026, 9, 3, 1, 20, 0, 0, time.UTC).UnixMilli(), Type: "two"},
		{Seq: 3, At: time.Date(2026, 9, 3, 2, 0, 0, 0, time.UTC).UnixMilli(), Type: "three"},
	}
	if err := sink.Send(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if len(recorder.objects) != 2 || recorder.objects[0].First != 1 || recorder.objects[0].Last != 2 {
		t.Fatalf("objects = %+v", recorder.objects)
	}
	want := appendEventLines(t, events[:2])
	sum := sha256.Sum256(want)
	if recorder.objects[0].ID != artifact.ID(sum[:]) || recorder.objects[0].SHA256 != strings.TrimPrefix(artifact.ID(sum[:]), artifact.Prefix) {
		t.Fatalf("content hash = %+v want %s", recorder.objects[0], artifact.ID(sum[:]))
	}
	reader, size, err := store.Open(recorder.objects[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, _ := io.ReadAll(reader)
	if size != int64(len(want)) || !bytes.Equal(got, want) {
		t.Fatalf("stored JSONL mismatch: size=%d got=%q want=%q", size, got, want)
	}
}

func appendEventLines(t *testing.T, events []proto.Event) []byte {
	t.Helper()
	var output []byte
	for _, event := range events {
		line, err := MarshalEventJSONLine(event)
		if err != nil {
			t.Fatal(err)
		}
		output = append(output, line...)
	}
	return output
}

func TestSIEMRequiresHTTPSAndEmitsCanonicalJSONOrEscapedCEF(t *testing.T) {
	if _, err := NewSIEMSink(SIEMOptions{Endpoint: "http://siem.example", Format: SIEMJSON}); err == nil {
		t.Fatal("HTTP SIEM endpoint accepted")
	}
	var bodies [][]byte
	var contentTypes []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		contentTypes = append(contentTypes, r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	event := proto.Event{Seq: 1, At: 2, Type: "egress.denied|line\n", Principal: "a=b\\c", Tenant: "t_one"}
	jsonSink, err := NewSIEMSink(SIEMOptions{Endpoint: server.URL, Format: SIEMJSON, HTTPClient: server.Client(), Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := jsonSink.Send(context.Background(), []proto.Event{event}); err != nil {
		t.Fatal(err)
	}
	cefSink, err := NewSIEMSink(SIEMOptions{Endpoint: server.URL, Format: SIEMCEF, HTTPClient: server.Client(), Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cefSink.Send(context.Background(), []proto.Event{event}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], appendEventLines(t, []proto.Event{event})) || !strings.Contains(string(bodies[1]), `a\=b\\c`) || strings.Contains(string(bodies[1]), "line\n") {
		t.Fatalf("SIEM bodies = %q", bodies)
	}
	if contentTypes[0] != "application/x-ndjson" || !strings.HasPrefix(contentTypes[1], "text/plain") {
		t.Fatalf("content types = %v", contentTypes)
	}
}
