package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"remount.dev/remount/internal/redact"
	"remount.dev/remount/internal/trace"
)

// logCanary is a synthetic, never-real credential shaped like the tokens
// internal/redact matches. It is planted so the assertion "the log has no
// secret in it" cannot be satisfied by a scan that matches nothing.
const logCanary = "sk-remountloggingcanary0000000000000000"

func TestStructuredLogsAreRedacted(t *testing.T) {
	// Prove the instrument before trusting it: the scan must see the canary in
	// the unwrapped handler's output, and not see it in the wrapped one's.
	plain := &bytes.Buffer{}
	slog.New(slog.NewTextHandler(plain, nil)).Info("upstream rejected the call", "detail", logCanary)
	if !strings.Contains(plain.String(), logCanary) {
		t.Fatalf("the canary is not visible without redaction; the test proves nothing: %q", plain)
	}

	for _, test := range []struct {
		name string
		log  func(l *slog.Logger)
	}{
		{"message", func(l *slog.Logger) { l.Info("token " + logCanary + " was refused") }},
		{"string attribute", func(l *slog.Logger) { l.Info("refused", "detail", logCanary) }},
		{"error attribute", func(l *slog.Logger) { l.Info("refused", "err", errors.New(logCanary)) }},
		{"any attribute", func(l *slog.Logger) { l.Info("refused", "detail", any(logCanary)) }},
		{"group attribute", func(l *slog.Logger) {
			l.Info("refused", slog.Group("egress", slog.String("detail", logCanary)))
		}},
		{"handler attribute", func(l *slog.Logger) { l.With("detail", logCanary).Info("refused") }},
		{"handler group", func(l *slog.Logger) { l.WithGroup("egress").Info("refused", "detail", logCanary) }},
		{"byte attribute", func(l *slog.Logger) { l.Info("refused", "body", []byte(logCanary)) }},
		{"stringer attribute", func(l *slog.Logger) { l.Info("refused", "url", stringerCanary{}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			test.log(slog.New(newRedactingHandler(slog.NewTextHandler(buf, nil))))
			if strings.Contains(buf.String(), logCanary) {
				t.Fatalf("the planted credential reached the log: %q", buf)
			}
			if !strings.Contains(buf.String(), redact.Mark) {
				t.Fatalf("nothing was marked as redacted: %q", buf)
			}
		})
	}
}

// stringerCanary is the shape a URL or a config value takes when it reaches a
// log as an opaque value rather than a string.
type stringerCanary struct{}

func (stringerCanary) String() string { return "https://broker.example/c/" + logCanary }

func TestRedactionKeepsOrdinaryFieldsIntact(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(newRedactingHandler(slog.NewTextHandler(buf, nil)))
	logger.Info("node claimed", "ws", "ws_1", "gen", 7, "ok", true)
	out := buf.String()
	for _, want := range []string{"msg=\"node claimed\"", "ws=ws_1", "gen=7", "ok=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log line lost %q: %q", want, out)
		}
	}
}

func TestRedactingHandlerHonoursLevelAndNilInner(t *testing.T) {
	buf := &bytes.Buffer{}
	h := newRedactingHandler(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("wrapper reported a level the inner handler filters out")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("wrapper suppressed a level the inner handler accepts")
	}
	if newRedactingHandler(nil) != nil {
		t.Fatal("wrapping nil produced a handler that logs nowhere")
	}
}

func TestTracingIsOffUnlessAnEndpointIsConfigured(t *testing.T) {
	trace.SetDefault(nil)
	stop, err := configureTracing("", "remount-test")
	if err != nil {
		t.Fatal(err)
	}
	if trace.Default().Enabled() {
		t.Fatal("an unset endpoint enabled tracing; Remount has no default collector")
	}
	stop()

	stop, err = configureTracing("http://127.0.0.1:1/collector", "remount-test")
	if err != nil {
		t.Fatal(err)
	}
	if !trace.Default().Enabled() {
		t.Fatal("a configured endpoint did not enable tracing")
	}
	stop()
	if trace.Default().Enabled() {
		t.Fatal("shutdown left the tracer installed")
	}

	if _, err := configureTracing("not a url at all", "remount-test"); err == nil {
		t.Fatal("an unusable endpoint was accepted")
	}
}
