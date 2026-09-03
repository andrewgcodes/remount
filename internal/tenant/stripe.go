package tenant

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const stripeMeterEventsURL = "https://api.stripe.com/v1/billing/meter_events"

// StripeOptions configure the stable v1 Billing Meter Event API adapter.
// APIKey is resolved per request and is never persisted by Store.
type StripeOptions struct {
	APIKey       SecretProvider
	CustomerID   func(context.Context, string) (string, error)
	EventNames   map[MeterKind]string
	Now          func() time.Time
	Timeout      time.Duration
	Resolver     Resolver
	RoundTripper http.RoundTripper
}

// StripeExporter maps canonical meter events to Stripe's v1 form schema.
type StripeExporter struct {
	apiKey     SecretProvider
	customerID func(context.Context, string) (string, error)
	eventNames map[MeterKind]string
	now        func() time.Time
	client     *http.Client
}

// StripeMeterExporter is the hosted configuration name for StripeExporter.
type StripeMeterExporter = StripeExporter

// NewStripeExporter constructs an exporter without making a network call.
func NewStripeExporter(options StripeOptions) (*StripeExporter, error) {
	if options.APIKey == nil || options.CustomerID == nil || len(options.EventNames) == 0 {
		return nil, &Error{Code: CodeBadRequest}
	}
	names := make(map[MeterKind]string, len(options.EventNames))
	for kind, name := range options.EventNames {
		if !validMeterKind(kind) || !stripeField(name, 100) {
			return nil, &Error{Code: CodeBadRequest}
		}
		names[kind] = name
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Timeout <= 0 {
		options.Timeout = 15 * time.Second
	}
	if options.Resolver == nil {
		options.Resolver = netResolver{}
	}
	transport := options.RoundTripper
	if transport == nil {
		transport = safeTransport(options.Resolver, options.Timeout)
	}
	client := &http.Client{Transport: transport, Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("tenant: Stripe redirects are disabled")
	}}
	return &StripeExporter{apiKey: options.APIKey, customerID: options.CustomerID, eventNames: names, now: options.Now, client: client}, nil
}

// NewStripeMeterExporter constructs the stripe_meter implementation.
func NewStripeMeterExporter(options StripeOptions) (*StripeMeterExporter, error) {
	return NewStripeExporter(options)
}

// Export implements BillingExporter. Stripe v1 accepts one meter event per
// request, so a partial failure leaves the store cursor unchanged and the
// whole page is safely replayed using stable identifiers/idempotency keys.
func (e *StripeExporter) Export(ctx context.Context, events []MeterEvent) error {
	if e == nil || len(events) == 0 || len(events) > maxPage {
		return &Error{Code: CodeBadRequest}
	}
	for _, event := range events {
		if err := e.exportOne(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (e *StripeExporter) exportOne(ctx context.Context, event MeterEvent) error {
	if validateMeterEvent(event) != nil {
		return &Error{Code: CodeBadRequest}
	}
	eventName, ok := e.eventNames[event.Kind]
	if !ok {
		return &Error{Code: CodeBadRequest}
	}
	now := e.now().UTC()
	if event.At.Before(now.Add(-35*24*time.Hour)) || event.At.After(now.Add(5*time.Minute)) {
		return &Error{Code: CodeBadRequest}
	}
	customer, err := e.customerID(ctx, event.Tenant)
	if err != nil {
		return &Error{Code: CodeUnavailable}
	}
	if !stripeField(customer, 128) {
		return &Error{Code: CodeBadRequest}
	}
	key, err := e.apiKey(ctx)
	if err != nil {
		return &Error{Code: CodeUnavailable}
	}
	if key == "" || len(key) > 16<<10 || strings.ContainsAny(key, "\r\n") {
		return &Error{Code: CodeBadRequest}
	}
	form := url.Values{}
	form.Set("event_name", eventName)
	form.Set("identifier", event.ID)
	form.Set("timestamp", strconv.FormatInt(event.At.Unix(), 10))
	form.Set("payload[stripe_customer_id]", customer)
	form.Set("payload[value]", strconv.FormatInt(event.Value, 10))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, stripeMeterEventsURL, strings.NewReader(form.Encode()))
	if err != nil {
		return &Error{Code: CodeBadRequest}
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Idempotency-Key", event.ID)
	request.Header.Set("User-Agent", "remount-stripe-meter/1")
	response, err := e.client.Do(request)
	if err != nil {
		return &Error{Code: CodeUnavailable, Retryable: true}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return outboundError(response.StatusCode)
	}
	return nil
}

func stripeField(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !strings.ContainsAny(value, "\r\n\x00")
}

var _ BillingExporter = (*StripeExporter)(nil)
