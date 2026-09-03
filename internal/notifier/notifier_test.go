package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type recordingDeadLetters struct {
	mu      sync.Mutex
	letters []DeadLetter
	err     error
}

func (s *recordingDeadLetters) Store(_ context.Context, letter DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	letter.EventTypes = append([]string(nil), letter.EventTypes...)
	s.letters = append(s.letters, letter)
	return nil
}

func publicResolver() Resolver {
	return resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	})
}

func response(status int) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("response content must not be surfaced")), Header: make(http.Header)}
}

func newTestLog(t *testing.T) (*eventlog.Log, *eventlog.MemoryCursorStore) {
	t.Helper()
	cursors, err := eventlog.NewMemoryCursorStore(8)
	if err != nil {
		t.Fatal(err)
	}
	return eventlog.New(eventlog.NewMemory(0)), cursors
}

func TestSlackReceivesOnlyCommittedSanitizedEventBeforeCursorAdvances(t *testing.T) {
	log, cursors := newTestLog(t)
	event := &proto.Event{
		Tenant: "tenant-a", Type: proto.EvEgressPending, Stream: "ws_1", Workspace: "ws_1",
		Payload: proto.MustMarshal(map[string]any{"credential": "payload-secret"}),
	}
	if err := log.Append(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if event.Seq == 0 {
		t.Fatal("event was not committed before export")
	}

	var requests int
	runner, err := New(Options{
		Source: log, Cursors: cursors, CursorName: "slack-primary",
		Subscriptions: []Subscription{{
			ID: "approvals", Tenant: "tenant-a", Events: []string{proto.EvEgressPending},
			Destination: Destination{Kind: SlackWebhook, URL: "https://hooks.slack.com/services/T/B/path-secret"},
		}},
		Resolver: publicResolver(),
		RoundTripper: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			cursor, cursorErr := cursors.Load(request.Context(), "slack-primary")
			if cursorErr != nil || cursor.Next != 0 {
				t.Errorf("cursor advanced before delivery: next=%d err=%v", cursor.Next, cursorErr)
			}
			last, readErr := log.Last(request.Context())
			if readErr != nil || last < event.Seq {
				t.Errorf("delivery preceded canonical commit: last=%d err=%v", last, readErr)
			}
			body, readErr := io.ReadAll(request.Body)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(body), "payload-secret") || strings.Contains(string(body), "path-secret") {
				t.Fatalf("notification exposed a configured or payload secret: %s", body)
			}
			var message struct{ Text string }
			if err := json.Unmarshal(body, &message); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(message.Text, "egress.pending (seq 1)") {
				t.Fatalf("unexpected Slack message %q", message.Text)
			}
			if got := request.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q", got)
			}
			return response(http.StatusOK), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	last, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if last != event.Seq || requests != 1 {
		t.Fatalf("last=%d requests=%d", last, requests)
	}
	cursor, err := cursors.Load(context.Background(), "slack-primary")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next != event.Seq+1 {
		t.Fatalf("cursor next = %d, want %d", cursor.Next, event.Seq+1)
	}
}

func TestGenericFiltersTenantAndTypeWhileAdvancingContiguousCursor(t *testing.T) {
	log, cursors := newTestLog(t)
	for _, event := range []proto.Event{
		{Tenant: "other", Type: proto.EvEgressPending},
		{Tenant: "tenant-a", Type: proto.EvRunFinished, Workspace: "ws_1"},
		{Tenant: "tenant-a", Type: "unrelated.event"},
		{Tenant: "tenant-a", Type: proto.EvPoolScaled},
	} {
		copy := event
		if err := log.Append(context.Background(), &copy); err != nil {
			t.Fatal(err)
		}
	}
	var delivered []wireEvent
	runner, err := New(Options{
		Source: log, Cursors: cursors, CursorName: "generic-primary",
		Subscriptions: []Subscription{{
			ID: "lifecycle", Tenant: "tenant-a", Events: []string{proto.EvRunFinished, "pool.*"},
			Destination: Destination{
				Kind: GenericWebhook, URL: "https://notify.example.test/remount", BearerToken: "header-secret",
				AllowedHosts: []string{"notify.example.test"},
			},
		}},
		Resolver: publicResolver(),
		RoundTripper: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if got := request.Header.Get("Authorization"); got != "Bearer header-secret" {
				t.Fatalf("authorization = %q", got)
			}
			var envelope struct {
				Version int         `json:"version"`
				Events  []wireEvent `json:"events"`
			}
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Version != 1 {
				t.Fatalf("version = %d", envelope.Version)
			}
			delivered = append(delivered, envelope.Events...)
			return response(http.StatusNoContent), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 2 || delivered[0].Type != proto.EvRunFinished || delivered[1].Type != proto.EvPoolScaled {
		t.Fatalf("delivered = %#v", delivered)
	}
	cursor, _ := cursors.Load(context.Background(), "generic-primary")
	if cursor.Next != 5 {
		t.Fatalf("cursor next = %d, want 5", cursor.Next)
	}
}

func TestRetryExhaustionCommitsSanitizedDeadLetterBeforeCursor(t *testing.T) {
	log, cursors := newTestLog(t)
	event := &proto.Event{Tenant: "tenant-a", Type: proto.EvWSFenced, Workspace: "ws_1"}
	if err := log.Append(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	letters := &recordingDeadLetters{}
	var attempts int
	var signals []Signal
	runner, err := New(Options{
		Source: log, Cursors: cursors, CursorName: "dead-letter",
		Subscriptions: []Subscription{{
			ID: "fences", Tenant: "tenant-a", Events: []string{proto.EvWSFenced},
			Destination: Destination{Kind: SlackWebhook, URL: "https://hooks.slack.com/services/T/B/super-secret"},
		}},
		DeadLetters: letters, Resolver: publicResolver(), Attempts: 3,
		RetryBase: time.Millisecond, RetryMax: 4 * time.Millisecond,
		Sleep: func(context.Context, time.Duration) error { return nil },
		Signal: func(_ context.Context, signal Signal) error {
			signals = append(signals, signal)
			return nil
		},
		RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return response(http.StatusServiceUnavailable), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || len(letters.letters) != 1 {
		t.Fatalf("attempts=%d letters=%#v", attempts, letters.letters)
	}
	letter := letters.letters[0]
	encoded, _ := json.Marshal(letter)
	if strings.Contains(string(encoded), "super-secret") || letter.FirstSeq != event.Seq || letter.LastSeq != event.Seq || letter.Reason != "delivery_failed" {
		t.Fatalf("unsafe or incorrect dead letter: %s", encoded)
	}
	if len(signals) != 2 || signals[0].Kind != SignalUnavailable || signals[1].Kind != SignalDeadLettered {
		t.Fatalf("signals = %#v", signals)
	}
	cursor, _ := cursors.Load(context.Background(), "dead-letter")
	if cursor.Next != event.Seq+1 {
		t.Fatalf("cursor next = %d", cursor.Next)
	}
}

func TestFailedDeliveryWithoutDurableDeadLetterRetainsCursor(t *testing.T) {
	for _, test := range []struct {
		name  string
		store DeadLetterStore
		want  error
	}{
		{name: "absent", want: ErrDeliveryUnavailable},
		{name: "failed", store: &recordingDeadLetters{err: errors.New("database contains secret")}, want: ErrDeadLetterUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			log, cursors := newTestLog(t)
			if err := log.Append(context.Background(), &proto.Event{Tenant: "tenant-a", Type: proto.EvRunFinished}); err != nil {
				t.Fatal(err)
			}
			var signals []Signal
			runner, err := New(Options{
				Source: log, Cursors: cursors, CursorName: "must-not-skip", DeadLetters: test.store,
				Subscriptions: []Subscription{{
					ID: "runs", Tenant: "tenant-a", Events: []string{proto.EvRunFinished},
					Destination: Destination{Kind: SlackWebhook, URL: "https://hooks.slack.com/services/T/B/path-secret"},
				}},
				Resolver: publicResolver(), Attempts: 1,
				Signal: func(_ context.Context, signal Signal) error {
					signals = append(signals, signal)
					return nil
				},
				RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("transport contains secret")
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, runErr := runner.RunOnce(context.Background())
			if !errors.Is(runErr, test.want) {
				t.Fatalf("error = %v, want %v", runErr, test.want)
			}
			if strings.Contains(runErr.Error(), "secret") {
				t.Fatalf("error leaked a secret: %v", runErr)
			}
			for _, signal := range signals {
				encoded, _ := json.Marshal(signal)
				if strings.Contains(string(encoded), "secret") || signal.Reason == "" {
					t.Fatalf("signal exposed transport details: %s", encoded)
				}
			}
			cursor, _ := cursors.Load(context.Background(), "must-not-skip")
			if cursor.Next != 0 {
				t.Fatalf("cursor advanced to %d", cursor.Next)
			}
		})
	}
}

func TestDestinationPolicyAndRuntimeResolutionFailClosed(t *testing.T) {
	log, cursors := newTestLog(t)
	base := Options{
		Source: log, Cursors: cursors, CursorName: "validation",
		Subscriptions: []Subscription{{
			ID: "s", Tenant: "t", Events: []string{proto.EvEgressPending},
			Destination: Destination{Kind: GenericWebhook, URL: "https://notify.example.test/hook", AllowedHosts: []string{"notify.example.test"}},
		}},
		Resolver: publicResolver(), RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) { return response(200), nil }),
	}
	for _, mutate := range []func(*Options){
		func(options *Options) { options.Subscriptions[0].Destination.URL = "http://notify.example.test/hook" },
		func(options *Options) {
			options.Subscriptions[0].Destination.URL = "https://user:secret@notify.example.test/hook"
		},
		func(options *Options) {
			options.Subscriptions[0].Destination.URL = "https://notify.example.test/hook?token=secret"
		},
		func(options *Options) { options.Subscriptions[0].Destination.AllowedHosts = []string{"*.example.test"} },
		func(options *Options) {
			options.Subscriptions[0].Destination.AllowedHosts = []string{"elsewhere.example.test"}
		},
	} {
		copy := base
		copy.Subscriptions = append([]Subscription(nil), base.Subscriptions...)
		copy.Subscriptions[0].Destination.AllowedHosts = append([]string(nil), base.Subscriptions[0].Destination.AllowedHosts...)
		mutate(&copy)
		if _, err := New(copy); err == nil {
			t.Fatal("unsafe destination was accepted")
		} else if strings.Contains(err.Error(), "secret") {
			t.Fatalf("validation error leaked configured data: %v", err)
		}
	}

	if err := log.Append(context.Background(), &proto.Event{Tenant: "t", Type: proto.EvEgressPending}); err != nil {
		t.Fatal(err)
	}
	var posts int
	base.Resolver = resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("127.0.0.1")}, nil
	})
	base.RoundTripper = roundTripFunc(func(*http.Request) (*http.Response, error) {
		posts++
		return response(200), nil
	})
	base.Attempts = 1
	runner, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background()); !errors.Is(err, ErrDeliveryUnavailable) {
		t.Fatalf("run error = %v", err)
	}
	if posts != 0 {
		t.Fatalf("made %d requests after non-public resolution", posts)
	}
	cursor, _ := cursors.Load(context.Background(), "validation")
	if cursor.Next != 0 {
		t.Fatalf("cursor advanced to %d", cursor.Next)
	}
}

func TestRedirectIsNotAccepted(t *testing.T) {
	log, cursors := newTestLog(t)
	if err := log.Append(context.Background(), &proto.Event{Tenant: "t", Type: proto.EvEgressPending}); err != nil {
		t.Fatal(err)
	}
	runner, err := New(Options{
		Source: log, Cursors: cursors, CursorName: "redirects", Attempts: 1,
		Subscriptions: []Subscription{{
			ID: "s", Tenant: "t", Events: []string{proto.EvEgressPending},
			Destination: Destination{Kind: SlackWebhook, URL: "https://hooks.slack.com/services/T/B/C"},
		}},
		Resolver: publicResolver(), RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) {
			redirect := response(http.StatusFound)
			redirect.Header.Set("Location", "http://127.0.0.1/private")
			return redirect, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background()); !errors.Is(err, ErrDeliveryUnavailable) {
		t.Fatalf("redirect error = %v", err)
	}
}

func TestRunIsSingleWorker(t *testing.T) {
	log, cursors := newTestLog(t)
	runner, err := New(Options{
		Source: log, Cursors: cursors, CursorName: "single-worker",
		Subscriptions: []Subscription{{
			ID: "s", Tenant: "t", Events: []string{proto.EvEgressPending},
			Destination: Destination{Kind: SlackWebhook, URL: "https://hooks.slack.com/services/T/B/C"},
		}},
		Resolver: publicResolver(), RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) { return response(200), nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for !runner.running.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := runner.RunOnce(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second worker error = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
}
