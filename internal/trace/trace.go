// Package trace records one span per request and, when an operator has
// configured a collector, ships those spans as OTLP/HTTP JSON.
//
// It is deliberately dependency-free, for the same reason internal/metrics is:
// the OpenTelemetry SDK is a larger import than the thing it measures, and the
// only shapes Remount needs are a span and a batched POST. The exported
// document follows the OTLP `ExportTraceServiceRequest` encoding — see
// exporter.go for the normative references.
//
// Tracing is off unless an endpoint is configured. Remount runs no hosted
// service, so there is no default collector and no telemetry leaves a
// deployment that did not ask for it.
//
// Attributes are scrubbed through internal/redact on the way in. A span carries
// routing facts — the operation, the workspace, the calling principal — and
// never a token, a secret or a request body.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/redact"
)

// Status codes match OTLP's StatusCode enum.
const (
	StatusUnset = 0
	StatusOK    = 1
	StatusError = 2
)

// Span kinds match OTLP's SpanKind enum. Remount records server spans: every
// span is a request this process accepted.
const (
	KindInternal = 1
	KindServer   = 2
)

// maxAttrBytes bounds one attribute value. A span is routing metadata, so a
// value longer than this is a mistake rather than a long identifier, and
// truncating it keeps one malformed request from inflating every batch.
const maxAttrBytes = 512

// Attr is one key/value pair on a span. Values are strings because every
// attribute Remount records is an identifier.
type Attr struct {
	Key   string
	Value string
}

// Span is one recorded operation. A Span is created by a Tracer, mutated by
// the handler that owns it, and copied into the exporter when it ends; nothing
// reads a live Span after End.
type Span struct {
	tracer *Tracer

	mu        sync.Mutex
	traceID   string
	spanID    string
	parentID  string
	name      string
	kind      int
	start     time.Time
	end       time.Time
	attrs     []Attr
	status    int
	statusMsg string
	ended     bool
}

// Record is the immutable snapshot an exporter receives.
type Record struct {
	TraceID   string
	SpanID    string
	ParentID  string
	Name      string
	Kind      int
	Start     time.Time
	End       time.Time
	Attrs     []Attr
	Status    int
	StatusMsg string
}

// Exporter accepts finished spans. Enqueue must not block its caller: a
// request handler ending a span is on the hot path, and losing telemetry is
// always preferable to stalling the request that produced it.
type Exporter interface {
	Enqueue(Record)
}

// Tracer starts spans. The zero value and a nil *Tracer are disabled: Start
// returns the context unchanged and a nil *Span whose methods do nothing, so a
// deployment without a collector pays one nil check per request.
type Tracer struct {
	exporter Exporter
}

// NewTracer returns a Tracer that hands finished spans to exporter. A nil
// exporter yields a disabled tracer.
func NewTracer(exporter Exporter) *Tracer {
	if exporter == nil {
		return nil
	}
	return &Tracer{exporter: exporter}
}

// Enabled reports whether spans are recorded at all.
func (t *Tracer) Enabled() bool { return t != nil && t.exporter != nil }

var defaultTracer atomic.Pointer[Tracer]

// SetDefault installs the process-wide tracer. cmd/remount calls it once, from
// the flag that names the collector; passing nil disables tracing again.
func SetDefault(t *Tracer) { defaultTracer.Store(t) }

// Default returns the process-wide tracer, which is disabled until SetDefault
// installs one.
func Default() *Tracer { return defaultTracer.Load() }

// Start begins a root span on the default tracer.
func Start(ctx context.Context, name string) (context.Context, *Span) {
	return Default().Start(ctx, name)
}

// StartRemote begins a span on the default tracer that continues a trace a
// peer began.
func StartRemote(ctx context.Context, name, traceID, parentSpanID string) (context.Context, *Span) {
	return Default().StartRemote(ctx, name, traceID, parentSpanID)
}

// Start begins a span. When ctx already carries a span the new one is its
// child, so control-plane work that fans out stays one trace.
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, *Span) {
	if !t.Enabled() {
		return ctx, nil
	}
	traceID, parentID := Context(ctx)
	if traceID == "" {
		traceID = newID(16)
	}
	return t.begin(ctx, name, traceID, parentID)
}

// StartRemote begins a span continuing a trace a peer started. An unusable
// traceID or parentSpanID starts a fresh trace rather than emitting a span
// that points at nothing.
func (t *Tracer) StartRemote(ctx context.Context, name, traceID, parentSpanID string) (context.Context, *Span) {
	if !t.Enabled() {
		return ctx, nil
	}
	if !validID(traceID, 32) {
		return t.begin(ctx, name, newID(16), "")
	}
	if !validID(parentSpanID, 16) {
		parentSpanID = ""
	}
	return t.begin(ctx, name, traceID, parentSpanID)
}

func (t *Tracer) begin(ctx context.Context, name, traceID, parentID string) (context.Context, *Span) {
	s := &Span{
		tracer:   t,
		traceID:  traceID,
		spanID:   newID(8),
		parentID: parentID,
		name:     name,
		kind:     KindServer,
		start:    time.Now(),
		status:   StatusUnset,
	}
	return context.WithValue(ctx, spanKey{}, s), s
}

type spanKey struct{}

// FromContext returns the span ctx carries, or nil.
func FromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(spanKey{}).(*Span)
	return s
}

// Context returns the trace and span ids a caller should stamp on an outgoing
// frame so the peer's span becomes a child of this one. Both are empty when
// ctx carries no span, which is what a deployment without tracing always does.
func Context(ctx context.Context) (traceID, spanID string) {
	s := FromContext(ctx)
	if s == nil {
		return "", ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traceID, s.spanID
}

// TraceID reports the span's trace id, or "" for a nil span.
func (s *Span) TraceID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traceID
}

// SpanID reports the span's own id, or "" for a nil span.
func (s *Span) SpanID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spanID
}

// Set records an attribute. The value is scrubbed through internal/redact and
// truncated, because a span leaves the deployment and a credential-shaped
// string in it would be a leak that no later control could take back. An empty
// key or value records nothing rather than an empty attribute.
func (s *Span) Set(key, value string) {
	if s == nil || key == "" || value == "" {
		return
	}
	value = redact.String(value)
	if len(value) > maxAttrBytes {
		value = value[:maxAttrBytes]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.attrs = append(s.attrs, Attr{Key: key, Value: value})
}

// SetStatus records an explicit status. End derives one from its error, so
// this is only for a handler that wants to mark success early.
func (s *Span) SetStatus(status int, message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.statusMsg = redact.String(message)
}

// End finishes the span and hands it to the exporter. A non-nil err marks the
// span as an error; the error's text is scrubbed like any other attribute.
// Ending twice is a no-op, so a deferred End beside an explicit one is safe.
func (s *Span) End(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.end = time.Now()
	if err != nil {
		s.status = StatusError
		s.statusMsg = redact.String(err.Error())
	} else if s.status == StatusUnset {
		s.status = StatusOK
	}
	record := Record{
		TraceID:   s.traceID,
		SpanID:    s.spanID,
		ParentID:  s.parentID,
		Name:      s.name,
		Kind:      s.kind,
		Start:     s.start,
		End:       s.end,
		Attrs:     append([]Attr(nil), s.attrs...),
		Status:    s.status,
		StatusMsg: s.statusMsg,
	}
	tracer := s.tracer
	s.mu.Unlock()
	if tracer != nil && tracer.exporter != nil {
		tracer.exporter.Enqueue(record)
	}
}

// newID returns n random bytes as lowercase hex. crypto/rand.Read never fails
// on a supported platform; a failure here would silently correlate unrelated
// requests, so it panics rather than emitting a zero id.
func newID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("trace: no randomness for a span id: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// validID reports whether s is exactly n lowercase hex digits and not all
// zeroes. OTLP treats an all-zero id as absent.
func validID(s string, n int) bool {
	if len(s) != n {
		return false
	}
	zero := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			if c != '0' {
				zero = false
			}
		case c >= 'a' && c <= 'f':
			zero = false
		default:
			return false
		}
	}
	return !zero
}
