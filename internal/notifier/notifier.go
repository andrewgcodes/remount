// Package notifier delivers selected committed events to preconfigured HTTPS
// destinations. Delivery is at least once: the durable event-export cursor is
// advanced only after every matching destination accepted the batch or after a
// durable dead-letter store accepted sanitized failure metadata.
package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	defaultBatchEvents = 64
	defaultBatchBytes  = 512 << 10
	defaultAttempts    = 4
	defaultRetryBase   = 100 * time.Millisecond
	defaultRetryMax    = 5 * time.Second
	defaultHTTPTimeout = 20 * time.Second
	defaultMaxSubs     = 128
	maximumSubs        = 4096
	maximumAttempts    = 10
	maximumFilters     = 32
	maximumBatchEvents = 4096
	maximumBatchBytes  = 16 << 20
)

var (
	// ErrAlreadyRunning means the same Runner already owns its cursor worker.
	ErrAlreadyRunning = errors.New("notifier: runner is already active")
	// ErrDeliveryUnavailable means delivery and any durable fallback failed.
	ErrDeliveryUnavailable = errors.New("notifier: delivery unavailable; cursor retained")
	// ErrDeadLetterUnavailable means the dead-letter commit failed.
	ErrDeadLetterUnavailable = errors.New("notifier: dead-letter unavailable; cursor retained")
)

// Kind identifies the wire adapter for a preconfigured destination.
type Kind string

const (
	// SlackWebhook sends a Slack incoming-webhook JSON message.
	SlackWebhook Kind = "slack_webhook"
	// GenericWebhook sends a JSON envelope containing canonical event metadata.
	GenericWebhook Kind = "generic_webhook"
)

// Destination is an operator-configured endpoint. BearerToken and the secret
// embedded in a Slack webhook path are never copied into errors, signals, or
// dead-letter records.
type Destination struct {
	Kind         Kind
	URL          string
	BearerToken  string
	AllowedHosts []string
}

// Subscription routes allowed event types for one tenant to one destination.
// Event filters may be egress.pending, run.finished, ws.fenced, or pool.*.
type Subscription struct {
	ID          string
	Tenant      string
	Events      []string
	Destination Destination
}

// SignalKind identifies a sanitized operational state change.
type SignalKind string

const (
	// SignalUnavailable reports exhausted delivery or an unavailable fallback.
	SignalUnavailable SignalKind = "notifier.unavailable"
	// SignalDeadLettered reports that a durable fallback accepted the failure.
	SignalDeadLettered SignalKind = "notifier.dead_lettered"
)

// Signal contains no endpoint, credential, response body, or raw event payload.
type Signal struct {
	Kind           SignalKind
	SubscriptionID string
	Tenant         string
	FirstSeq       uint64
	LastSeq        uint64
	Attempts       int
	Reason         string
}

// DeadLetter is the bounded, sanitized record accepted before a failed batch
// may be skipped. Implementations must durably commit Store before returning.
type DeadLetter struct {
	SubscriptionID string
	Tenant         string
	FirstSeq       uint64
	LastSeq        uint64
	EventTypes     []string
	Attempts       int
	Reason         string
	FailedAt       int64
}

// DeadLetterStore durably retains failed-delivery metadata.
type DeadLetterStore interface {
	Store(context.Context, DeadLetter) error
}

// SignalFunc records an explicit notifier availability event. A failure to
// record a dead-letter signal keeps the export cursor in place.
type SignalFunc func(context.Context, Signal) error

// Options configure one serialized, bounded export worker.
type Options struct {
	Source        eventlog.Exporter
	Cursors       eventlog.CursorStore
	CursorName    string
	Subscriptions []Subscription
	DeadLetters   DeadLetterStore
	Signal        SignalFunc

	BatchEvents int
	BatchBytes  int
	Attempts    int
	RetryBase   time.Duration
	RetryMax    time.Duration
	HTTPTimeout time.Duration
	MaxSubs     int

	// Resolver and RoundTripper are test seams. Resolution authorization still
	// runs before every request. Production callers should leave both nil so the
	// transport pins each connection to the public IP address it validated.
	Resolver     Resolver
	RoundTripper http.RoundTripper
	Now          func() time.Time
	Sleep        func(context.Context, time.Duration) error
}

// Runner owns one durable cursor and deliberately permits only one Run or
// RunOnce call at a time. This makes concurrency one and bounds queued work to
// the configured eventlog batch.
type Runner struct {
	source      eventlog.Exporter
	cursors     eventlog.CursorStore
	cursorName  string
	subs        []subscription
	deadLetters DeadLetterStore
	signal      SignalFunc
	runOptions  eventlog.RunOptions
	attempts    int
	retryBase   time.Duration
	retryMax    time.Duration
	client      *http.Client
	resolver    Resolver
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
	running     atomic.Bool
}

type subscription struct {
	id          string
	tenant      string
	events      map[string]struct{}
	destination destination
}

// New validates and copies configuration. No destination learned from an
// event can be used: every URL and exact generic host allow-list is fixed here.
func New(options Options) (*Runner, error) {
	if options.Source == nil || options.Cursors == nil || options.CursorName == "" {
		return nil, errors.New("notifier: source, durable cursor store, and cursor name are required")
	}
	if options.BatchEvents == 0 {
		options.BatchEvents = defaultBatchEvents
	}
	if options.BatchBytes == 0 {
		options.BatchBytes = defaultBatchBytes
	}
	if options.Attempts == 0 {
		options.Attempts = defaultAttempts
	}
	if options.RetryBase == 0 {
		options.RetryBase = defaultRetryBase
	}
	if options.RetryMax == 0 {
		options.RetryMax = defaultRetryMax
	}
	if options.HTTPTimeout == 0 {
		options.HTTPTimeout = defaultHTTPTimeout
	}
	if options.MaxSubs == 0 {
		options.MaxSubs = defaultMaxSubs
	}
	if options.Attempts < 1 || options.Attempts > maximumAttempts {
		return nil, fmt.Errorf("notifier: attempts must be between 1 and %d", maximumAttempts)
	}
	if options.BatchEvents < 1 || options.BatchEvents > maximumBatchEvents || options.BatchBytes < 1 || options.BatchBytes > maximumBatchBytes {
		return nil, errors.New("notifier: export batch bounds are invalid")
	}
	if options.RetryBase < time.Millisecond || options.RetryMax < options.RetryBase || options.RetryMax > time.Minute {
		return nil, errors.New("notifier: retry bounds are invalid")
	}
	if options.HTTPTimeout < time.Millisecond || options.HTTPTimeout > time.Minute {
		return nil, errors.New("notifier: HTTP timeout must be between 1ms and 1m")
	}
	if options.MaxSubs < 1 || options.MaxSubs > maximumSubs || len(options.Subscriptions) > options.MaxSubs {
		return nil, errors.New("notifier: subscription capacity exceeded")
	}
	if len(options.Subscriptions) == 0 {
		return nil, errors.New("notifier: at least one subscription is required")
	}
	resolver := options.Resolver
	if resolver == nil {
		resolver = netResolver{}
	}
	transport := options.RoundTripper
	if transport == nil {
		transport = newSafeTransport(resolver, options.HTTPTimeout)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepContext
	}

	seen := make(map[string]struct{}, len(options.Subscriptions))
	subs := make([]subscription, 0, len(options.Subscriptions))
	for _, configured := range options.Subscriptions {
		if !safeIdentifier(configured.ID) || !safeIdentifier(configured.Tenant) {
			return nil, errors.New("notifier: subscription id and tenant must be bounded identifiers")
		}
		if _, exists := seen[configured.ID]; exists {
			return nil, errors.New("notifier: subscription ids must be unique")
		}
		seen[configured.ID] = struct{}{}
		if len(configured.Events) == 0 || len(configured.Events) > maximumFilters {
			return nil, errors.New("notifier: subscription event filters are invalid")
		}
		filters := make(map[string]struct{}, len(configured.Events))
		for _, eventType := range configured.Events {
			if !allowedFilter(eventType) {
				return nil, errors.New("notifier: unsupported event filter")
			}
			filters[eventType] = struct{}{}
		}
		destination, err := normalizeDestination(configured.Destination)
		if err != nil {
			return nil, err
		}
		subs = append(subs, subscription{id: configured.ID, tenant: configured.Tenant, events: filters, destination: destination})
	}

	return &Runner{
		source: options.Source, cursors: options.Cursors, cursorName: options.CursorName,
		subs: subs, deadLetters: options.DeadLetters, signal: options.Signal,
		runOptions: eventlog.RunOptions{Name: options.CursorName, Follow: true, BatchEvents: options.BatchEvents, BatchBytes: options.BatchBytes},
		attempts:   options.Attempts, retryBase: options.RetryBase, retryMax: options.RetryMax,
		client:   &http.Client{Transport: transport, Timeout: options.HTTPTimeout, CheckRedirect: refuseRedirect},
		resolver: resolver, now: now, sleep: sleep,
	}, nil
}

// CloseIdleConnections releases pooled outbound connections. A server should
// call it after cancelling and joining Run during shutdown.
func (r *Runner) CloseIdleConnections() {
	r.client.CloseIdleConnections()
}

// Run follows the canonical log until ctx ends.
func (r *Runner) Run(ctx context.Context) error {
	return r.run(ctx, true)
}

// RunOnce drains the currently committed range and returns its last sequence.
func (r *Runner) RunOnce(ctx context.Context) (uint64, error) {
	if !r.running.CompareAndSwap(false, true) {
		return 0, ErrAlreadyRunning
	}
	defer r.running.Store(false)
	options := r.runOptions
	options.Follow = false
	return eventlog.RunExport(ctx, r.source, batchSink{runner: r}, r.cursors, options)
}

func (r *Runner) run(ctx context.Context, follow bool) error {
	if !r.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer r.running.Store(false)
	options := r.runOptions
	options.Follow = follow
	_, err := eventlog.RunExport(ctx, r.source, batchSink{runner: r}, r.cursors, options)
	return err
}

type batchSink struct{ runner *Runner }

func (s batchSink) Send(ctx context.Context, events []proto.Event) error {
	for _, subscription := range s.runner.subs {
		matching := filterEvents(events, subscription)
		if len(matching) == 0 {
			continue
		}
		attempts, reason := s.runner.deliver(ctx, subscription, matching)
		if reason == "" {
			metrics.NotificationDeliveries.Inc()
			continue
		}
		metrics.NotificationDeliveryFailures.Inc()
		first, last := matching[0].Seq, matching[len(matching)-1].Seq
		unavailable := Signal{Kind: SignalUnavailable, SubscriptionID: subscription.id, Tenant: subscription.tenant, FirstSeq: first, LastSeq: last, Attempts: attempts, Reason: reason}
		_ = s.runner.emit(ctx, unavailable)
		if s.runner.deadLetters == nil {
			return ErrDeliveryUnavailable
		}
		letter := DeadLetter{
			SubscriptionID: subscription.id, Tenant: subscription.tenant,
			FirstSeq: first, LastSeq: last, EventTypes: uniqueTypes(matching),
			Attempts: attempts, Reason: reason, FailedAt: s.runner.now().UnixMilli(),
		}
		if err := s.runner.deadLetters.Store(ctx, letter); err != nil {
			_ = s.runner.emit(ctx, Signal{Kind: SignalUnavailable, SubscriptionID: subscription.id, Tenant: subscription.tenant, FirstSeq: first, LastSeq: last, Attempts: attempts, Reason: "dead_letter_store_failed"})
			return ErrDeadLetterUnavailable
		}
		metrics.NotificationDeadLetters.Inc()
		if err := s.runner.emit(ctx, Signal{Kind: SignalDeadLettered, SubscriptionID: subscription.id, Tenant: subscription.tenant, FirstSeq: first, LastSeq: last, Attempts: attempts, Reason: reason}); err != nil {
			return ErrDeadLetterUnavailable
		}
	}
	return nil
}

func (r *Runner) deliver(ctx context.Context, subscription subscription, events []proto.Event) (int, string) {
	payload, err := marshalPayload(subscription.destination.kind, events)
	if err != nil {
		return 0, "encode_failed"
	}
	delay := r.retryBase
	for attempt := 1; attempt <= r.attempts; attempt++ {
		if err := authorizeResolvedHost(ctx, r.resolver, subscription.destination.host); err == nil {
			if r.post(ctx, subscription.destination, payload) {
				return attempt, ""
			}
		} else if ctx.Err() != nil {
			return attempt, "cancelled"
		}
		if attempt == r.attempts {
			return attempt, "delivery_failed"
		}
		if err := r.sleep(ctx, delay); err != nil {
			return attempt, "cancelled"
		}
		if delay < r.retryMax {
			delay *= 2
			if delay > r.retryMax {
				delay = r.retryMax
			}
		}
	}
	return r.attempts, "delivery_failed"
}

func (r *Runner) post(ctx context.Context, destination destination, payload []byte) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, destination.endpoint, bytes.NewReader(payload))
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "remount-notifier/1")
	if destination.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+destination.bearerToken)
	}
	response, err := r.client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	return response.StatusCode >= 200 && response.StatusCode < 300
}

func (r *Runner) emit(ctx context.Context, signal Signal) error {
	if r.signal == nil {
		return nil
	}
	if err := r.signal(ctx, signal); err != nil {
		return ErrDeadLetterUnavailable
	}
	return nil
}

func filterEvents(events []proto.Event, subscription subscription) []proto.Event {
	matching := make([]proto.Event, 0, len(events))
	for _, event := range events {
		if event.Tenant != subscription.tenant || !matchesFilter(subscription.events, event.Type) {
			continue
		}
		matching = append(matching, event)
	}
	return matching
}

func allowedFilter(eventType string) bool {
	switch eventType {
	case proto.EvEgressPending, proto.EvRunFinished, proto.EvWSFenced, "pool.*":
		return true
	default:
		return false
	}
}

func matchesFilter(filters map[string]struct{}, eventType string) bool {
	if _, ok := filters[eventType]; ok {
		return true
	}
	_, pool := filters["pool.*"]
	return pool && strings.HasPrefix(eventType, "pool.")
}

type wireEvent struct {
	Seq         uint64 `json:"seq"`
	At          int64  `json:"at"`
	Principal   string `json:"principal,omitempty"`
	Node        string `json:"node,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	ReceivedAt  int64  `json:"received_at,omitempty"`
	ObservedAt  int64  `json:"observed_at,omitempty"`
	Origin      string `json:"origin,omitempty"`
	Actor       string `json:"actor,omitempty"`
	Tenant      string `json:"tenant"`
	Type        string `json:"type"`
	Stream      string `json:"stream,omitempty"`
	Workspace   string `json:"workspace,omitempty"`
	Generation  uint64 `json:"generation,omitempty"`
	Session     string `json:"session,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
	ProducerSeq uint64 `json:"producer_seq,omitempty"`
	Cause       uint64 `json:"cause,omitempty"`
}

func marshalPayload(kind Kind, events []proto.Event) ([]byte, error) {
	wire := make([]wireEvent, len(events))
	for index, event := range events {
		wire[index] = wireEvent{
			Seq: event.Seq, At: event.At, Principal: event.Principal, Node: event.Node,
			EventID: event.EventID, ReceivedAt: event.ReceivedAt, ObservedAt: event.ObservedAt,
			Origin: event.Origin, Actor: event.Actor, Tenant: event.Tenant, Type: event.Type,
			Stream: event.Stream, Workspace: event.Workspace, Generation: event.Generation,
			Session: event.Session, OperationID: event.OperationID,
			ProducerSeq: event.ProducerSeq, Cause: event.Cause,
		}
	}
	if kind == GenericWebhook {
		return json.Marshal(struct {
			Version int         `json:"version"`
			Events  []wireEvent `json:"events"`
		}{Version: 1, Events: wire})
	}
	parts := make([]string, len(wire))
	for index, event := range wire {
		parts[index] = fmt.Sprintf("%s (seq %d)", event.Type, event.Seq)
	}
	return json.Marshal(struct {
		Text string `json:"text"`
	}{Text: "Remount: " + strings.Join(parts, ", ")})
}

func uniqueTypes(events []proto.Event) []string {
	set := make(map[string]struct{}, len(events))
	for _, event := range events {
		set[event.Type] = struct{}{}
	}
	types := make([]string, 0, len(set))
	for eventType := range set {
		types = append(types, eventType)
	}
	sort.Strings(types)
	return types
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func refuseRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}
