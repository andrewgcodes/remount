package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"remount.dev/remount/internal/redact"
	"remount.dev/remount/internal/trace"
)

// otlpEndpointUsage is the flag help shared by server, up and standalone. It
// says "operator-configured" because there is no Remount-operated collector
// and no default: unset means no span leaves the process.
const otlpEndpointUsage = "OTLP/HTTP collector to export request spans to (http[s]://host[:port][/v1/traces]); tracing is off when empty"

// configureTracing installs the process-wide tracer when the operator named a
// collector, and returns the function that flushes it on shutdown. An empty
// endpoint installs nothing and returns a no-op, so a deployment that did not
// ask for tracing exports nothing and pays one nil check per request.
func configureTracing(endpoint, service string) (func(), error) {
	if endpoint == "" {
		return func() {}, nil
	}
	exporter, err := trace.NewOTLPExporter(trace.ExporterOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return nil, fmt.Errorf("--otlp-endpoint: %w", err)
	}
	trace.SetDefault(trace.NewTracer(exporter))
	slog.Info("tracing enabled", "endpoint", exporter.Endpoint(), "service", service)
	return func() {
		trace.SetDefault(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exporter.Close(ctx)
	}, nil
}

// redactingHandler scrubs every log message and string attribute through
// internal/redact before the underlying handler formats it.
//
// It exists because a log line is the one caller-facing surface no single site
// owns: a broker rejection, a harness that echoed its environment, or an
// upstream error body can each carry a credential-shaped string into a message
// that some other code wrote. Redaction here is defence in depth, not the
// control that keeps secrets out of logs — the control is that no component
// puts a secret in a log argument in the first place.
type redactingHandler struct{ inner slog.Handler }

// newRedactingHandler wraps h. A nil handler is returned unchanged so a caller
// cannot accidentally build a handler that logs nowhere.
func newRedactingHandler(h slog.Handler) slog.Handler {
	if h == nil {
		return nil
	}
	return redactingHandler{inner: h}
}

func (h redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, redact.String(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, out)
}

func (h redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return redactingHandler{inner: h.inner.WithAttrs(redactAttrs(attrs))}
}

func (h redactingHandler) WithGroup(name string) slog.Handler {
	return redactingHandler{inner: h.inner.WithGroup(name)}
}

func redactAttrs(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, redactAttr(a))
	}
	return out
}

// redactAttr scrubs one attribute. Strings and the string forms of errors and
// Stringers are the shapes a credential actually travels in; groups recurse.
// A number or an opaque struct is left alone, because rewriting it would
// change the document shape without protecting anything a secret is carried in.
func redactAttr(a slog.Attr) slog.Attr {
	value := a.Value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		return slog.String(a.Key, redact.String(value.String()))
	case slog.KindGroup:
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(redactAttrs(value.Group())...)}
	case slog.KindAny:
		switch v := value.Any().(type) {
		case error:
			return slog.String(a.Key, redact.String(v.Error()))
		case fmt.Stringer:
			return slog.String(a.Key, redact.String(v.String()))
		case []byte:
			scrubbed, _ := redact.Bytes(v)
			return slog.Any(a.Key, scrubbed)
		}
	}
	return slog.Attr{Key: a.Key, Value: value}
}
