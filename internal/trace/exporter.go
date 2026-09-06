package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/metrics"
)

// The exported document is OTLP's ExportTraceServiceRequest, encoded with the
// protobuf JSON mapping:
//
//   - opentelemetry/proto/collector/trace/v1/trace_service.proto
//     (ExportTraceServiceRequest.resource_spans)
//   - opentelemetry/proto/trace/v1/trace.proto
//     (ResourceSpans, ScopeSpans, Span, Status, SpanKind, StatusCode)
//   - opentelemetry/proto/common/v1/common.proto (KeyValue, AnyValue)
//
// Two encoding rules that the generic protobuf JSON mapping does not give you:
// OTLP/HTTP specifies that trace_id and span_id, although `bytes` in the
// schema, are hex-encoded strings in JSON rather than base64; and 64-bit
// fields (the fixed64 timestamps) are JSON strings. Both are honoured below.
// See docs/adr/0093-typed-errors-support-matrix-and-dependency-free-tracing.md.

const (
	// defaultPath is appended when the operator's endpoint names only a host.
	// OTLP/HTTP defines /v1/traces as the trace signal's path.
	defaultPath = "/v1/traces"
	// scopeName identifies the instrumentation that produced these spans.
	scopeName = "remount.dev/remount/internal/trace"
)

// ExporterOptions configures the OTLP exporter. Every field has a working
// default except Endpoint, which has none: there is no Remount-operated
// collector, so an unset endpoint means tracing is off.
type ExporterOptions struct {
	// Endpoint is the operator's collector. A URL with no path (or "/") gets
	// /v1/traces appended.
	Endpoint string
	// Service names this process in the exported resource.
	Service string
	// Headers are sent with every batch, for a collector behind an
	// authenticating proxy.
	Headers map[string]string
	// MaxBatch is the largest number of spans in one POST.
	MaxBatch int
	// FlushInterval bounds how long a partial batch waits.
	FlushInterval time.Duration
	// QueueDepth bounds spans buffered ahead of the exporter. A full queue
	// drops the span and counts an export failure; telemetry never applies
	// back pressure to a request.
	QueueDepth int
	// Timeout bounds one POST.
	Timeout time.Duration
	// Client, when set, replaces the default HTTP client. Tests use it.
	Client *http.Client
}

// OTLPExporter batches spans and posts them to an operator-configured
// collector as OTLP/HTTP JSON.
type OTLPExporter struct {
	url     string
	service string
	headers map[string]string
	client  *http.Client
	batch   int
	flush   time.Duration
	timeout time.Duration

	queue chan Record
	done  chan struct{}
	stop  sync.Once
	wg    sync.WaitGroup
}

// NewOTLPExporter validates the endpoint and starts the batching goroutine. An
// empty endpoint is an error rather than a silent no-op: a deployment that
// asked for tracing and got none should be told, and a deployment that did not
// ask never constructs one.
func NewOTLPExporter(opts ExporterOptions) (*OTLPExporter, error) {
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("trace: an OTLP endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("trace: invalid OTLP endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("trace: OTLP endpoint %q must be http or https", endpoint)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("trace: OTLP endpoint %q has no host", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = defaultPath
	}
	e := &OTLPExporter{
		url:     u.String(),
		service: orDefault(opts.Service, "remount"),
		headers: opts.Headers,
		client:  opts.Client,
		batch:   opts.MaxBatch,
		flush:   opts.FlushInterval,
		timeout: opts.Timeout,
		done:    make(chan struct{}),
	}
	if e.batch <= 0 {
		e.batch = 256
	}
	if e.flush <= 0 {
		e.flush = 5 * time.Second
	}
	if e.timeout <= 0 {
		e.timeout = 10 * time.Second
	}
	if e.client == nil {
		e.client = &http.Client{Timeout: e.timeout}
	}
	depth := opts.QueueDepth
	if depth <= 0 {
		depth = 4096
	}
	e.queue = make(chan Record, depth)
	e.wg.Add(1)
	go e.run()
	return e, nil
}

// Endpoint reports the URL batches are posted to, after path defaulting.
func (e *OTLPExporter) Endpoint() string { return e.url }

// Enqueue buffers a finished span. A full queue drops it and counts an export
// failure, because a span is never worth stalling the request that produced it.
func (e *OTLPExporter) Enqueue(r Record) {
	// Shutdown is checked first and on its own: a select with both a closed
	// done channel and a free queue slot would pick between them at random,
	// so a span arriving after Close would sometimes be buffered into a queue
	// nothing drains and sometimes be counted. Either way it is lost; only one
	// of them says so.
	select {
	case <-e.done:
		metrics.TraceExportFailures.Inc()
		return
	default:
	}
	select {
	case e.queue <- r:
	default:
		metrics.TraceExportFailures.Inc()
	}
}

// Close flushes what is buffered and stops the exporter. It is safe to call
// more than once.
func (e *OTLPExporter) Close(ctx context.Context) {
	e.stop.Do(func() { close(e.done) })
	finished := make(chan struct{})
	go func() { e.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
	}
}

func (e *OTLPExporter) run() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.flush)
	defer ticker.Stop()
	pending := make([]Record, 0, e.batch)
	for {
		select {
		case r := <-e.queue:
			pending = append(pending, r)
			if len(pending) >= e.batch {
				e.send(pending)
				pending = pending[:0]
			}
		case <-ticker.C:
			if len(pending) > 0 {
				e.send(pending)
				pending = pending[:0]
			}
		case <-e.done:
			// Drain whatever is already queued so a clean shutdown does not
			// discard the spans of the requests it just finished.
			for {
				select {
				case r := <-e.queue:
					pending = append(pending, r)
					if len(pending) >= e.batch {
						e.send(pending)
						pending = pending[:0]
					}
					continue
				default:
				}
				break
			}
			if len(pending) > 0 {
				e.send(pending)
			}
			return
		}
	}
}

func (e *OTLPExporter) send(records []Record) {
	body, err := Encode(e.service, records)
	if err != nil {
		metrics.TraceExportFailures.Add(uint64(len(records)))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		metrics.TraceExportFailures.Add(uint64(len(records)))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}
	res, err := e.client.Do(req)
	if err != nil {
		metrics.TraceExportFailures.Add(uint64(len(records)))
		return
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		metrics.TraceExportFailures.Add(uint64(len(records)))
		return
	}
	metrics.TraceSpansExported.Add(uint64(len(records)))
}

// ---------------------------------------------------------------------------
// OTLP/HTTP JSON encoding
// ---------------------------------------------------------------------------

type otlpDocument struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeSpans struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpSpan struct {
	TraceID           string         `json:"traceId"`
	SpanID            string         `json:"spanId"`
	ParentSpanID      string         `json:"parentSpanId,omitempty"`
	Name              string         `json:"name"`
	Kind              int            `json:"kind"`
	StartTimeUnixNano string         `json:"startTimeUnixNano"`
	EndTimeUnixNano   string         `json:"endTimeUnixNano"`
	Attributes        []otlpKeyValue `json:"attributes,omitempty"`
	Status            otlpStatus     `json:"status"`
}

type otlpStatus struct {
	Message string `json:"message,omitempty"`
	Code    int    `json:"code"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}

// Encode renders records as one OTLP/HTTP JSON export request. It is exported
// so a test can assert the shape without a collector.
func Encode(service string, records []Record) ([]byte, error) {
	spans := make([]otlpSpan, 0, len(records))
	for _, r := range records {
		attrs := make([]otlpKeyValue, 0, len(r.Attrs))
		for _, a := range r.Attrs {
			attrs = append(attrs, otlpKeyValue{Key: a.Key, Value: otlpAnyValue{StringValue: a.Value}})
		}
		spans = append(spans, otlpSpan{
			TraceID:           r.TraceID,
			SpanID:            r.SpanID,
			ParentSpanID:      r.ParentID,
			Name:              r.Name,
			Kind:              orKind(r.Kind),
			StartTimeUnixNano: strconv.FormatInt(r.Start.UnixNano(), 10),
			EndTimeUnixNano:   strconv.FormatInt(r.End.UnixNano(), 10),
			Attributes:        attrs,
			Status:            otlpStatus{Code: r.Status, Message: r.StatusMsg},
		})
	}
	doc := otlpDocument{ResourceSpans: []otlpResourceSpans{{
		Resource: otlpResource{Attributes: []otlpKeyValue{
			{Key: "service.name", Value: otlpAnyValue{StringValue: orDefault(service, "remount")}},
		}},
		ScopeSpans: []otlpScopeSpans{{Scope: otlpScope{Name: scopeName}, Spans: spans}},
	}}}
	return json.Marshal(doc)
}

func orDefault(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

func orKind(kind int) int {
	if kind == 0 {
		return KindInternal
	}
	return kind
}
