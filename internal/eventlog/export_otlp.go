package eventlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/proto"
)

// OTLPOptions configures a bounded OTLP/HTTP JSON trace exporter.
type OTLPOptions struct {
	Endpoint       string
	HTTPClient     *http.Client
	Headers        http.Header
	Retries        int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	MaxBodyBytes   int
	ServiceName    string
	ServiceVersion string
}

// OTLPSink maps canonical events to OTLP spans. Events attributed to the same
// Remount session share a deterministic trace ID, so sessions appear as traces.
type OTLPSink struct {
	endpoint string
	http     httpExportOptions
	service  string
	version  string
}

// NewOTLPSink validates options without contacting the collector. Endpoint is
// a base OTLP URL; /v1/traces is appended as required by OTLP/HTTP.
func NewOTLPSink(options OTLPOptions) (*OTLPSink, error) {
	endpoint, err := validateExportURL(options.Endpoint, false, "/v1/traces")
	if err != nil {
		return nil, err
	}
	httpOptions, err := normalizeHTTPOptions(options.HTTPClient, options.Headers, options.Retries, options.BaseBackoff, options.MaxBackoff, options.MaxBodyBytes)
	if err != nil {
		return nil, err
	}
	service := options.ServiceName
	if service == "" {
		service = "remount"
	}
	return &OTLPSink{endpoint: endpoint.String(), http: httpOptions, service: service, version: options.ServiceVersion}, nil
}

// Send implements BatchSink.
func (s *OTLPSink) Send(ctx context.Context, events []proto.Event) error {
	if s == nil {
		return errors.New("eventlog: OTLP sink is required")
	}
	spans := make([]otlpSpan, 0, len(events))
	for _, event := range events {
		spans = append(spans, otlpSpanForEvent(event))
	}
	resourceAttributes := []otlpAttribute{stringAttribute("service.name", s.service)}
	if s.version != "" {
		resourceAttributes = append(resourceAttributes, stringAttribute("service.version", s.version))
	}
	body, err := json.Marshal(otlpTraceRequest{ResourceSpans: []otlpResourceSpans{{
		Resource:   otlpResource{Attributes: resourceAttributes},
		ScopeSpans: []otlpScopeSpans{{Scope: otlpScope{Name: "remount.eventlog"}, Spans: spans}},
	}}})
	if err != nil {
		return errors.New("eventlog: encode OTLP trace request")
	}
	endpoint, err := validateExportURL(s.endpoint, false, "")
	if err != nil {
		return err
	}
	return postExport(ctx, endpoint, s.http, "OTLP", "application/json", body, validateOTLPResponse)
}

func validateOTLPResponse(body []byte) error {
	if len(body) == 0 {
		return nil
	}
	var response struct {
		PartialSuccess struct {
			RejectedSpans json.Number `json:"rejectedSpans"`
		} `json:"partialSuccess"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		return errors.New("eventlog: collector returned malformed OTLP response")
	}
	if response.PartialSuccess.RejectedSpans != "" && response.PartialSuccess.RejectedSpans != "0" {
		return errors.New("eventlog: collector partially rejected OTLP spans")
	}
	return nil
}

type otlpTraceRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpAttribute `json:"attributes"`
}

type otlpScopeSpans struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpSpan struct {
	TraceID           string          `json:"traceId"`
	SpanID            string          `json:"spanId"`
	ParentSpanID      string          `json:"parentSpanId,omitempty"`
	Name              string          `json:"name"`
	Kind              int             `json:"kind"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	EndTimeUnixNano   string          `json:"endTimeUnixNano"`
	Attributes        []otlpAttribute `json:"attributes"`
	Status            otlpStatus      `json:"status"`
}

type otlpAttribute struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue,omitempty"`
	IntValue    string `json:"intValue,omitempty"`
}

type otlpStatus struct {
	Code int `json:"code"`
}

func stringAttribute(key, value string) otlpAttribute {
	return otlpAttribute{Key: key, Value: otlpAnyValue{StringValue: value}}
}

func intAttribute(key string, value uint64) otlpAttribute {
	return otlpAttribute{Key: key, Value: otlpAnyValue{IntValue: formatUint(value)}}
}

func otlpSpanForEvent(event proto.Event) otlpSpan {
	traceSeed := "event:" + formatUint(event.Seq)
	if event.Session != "" {
		traceSeed = "session:" + event.Session
	}
	traceHash := sha256.Sum256([]byte(traceSeed))
	spanHash := sha256.Sum256([]byte("event:" + formatUint(event.Seq)))
	attributes := []otlpAttribute{
		stringAttribute("remount.event.type", event.Type),
		intAttribute("remount.event.seq", event.Seq),
	}
	appendString := func(key, value string) {
		if value != "" {
			attributes = append(attributes, stringAttribute(key, value))
		}
	}
	appendString("remount.stream", event.Stream)
	appendString("remount.principal", event.Principal)
	appendString("remount.node.id", event.Node)
	appendString("remount.event.id", event.EventID)
	appendString("remount.event.origin", event.Origin)
	appendString("remount.event.actor", event.Actor)
	appendString("remount.tenant.id", event.Tenant)
	appendString("remount.workspace.id", event.Workspace)
	appendString("remount.operation.id", event.OperationID)
	if event.Generation != 0 {
		attributes = append(attributes, intAttribute("remount.workspace.generation", event.Generation))
	}
	if event.ProducerSeq != 0 {
		attributes = append(attributes, intAttribute("remount.event.producer_seq", event.ProducerSeq))
	}
	if event.Cause != 0 {
		attributes = append(attributes, intAttribute("remount.event.cause", event.Cause))
	}
	if event.Session != "" {
		appendString("remount.session.id", event.Session)
		appendString("gen_ai.conversation.id", event.Session)
		if strings.HasPrefix(event.Type, "s.") {
			appendString("gen_ai.operation.name", "execute_tool")
		}
	}
	agent := event.Actor
	if agent == "" {
		agent = event.Principal
	}
	if strings.HasPrefix(agent, "agent:") {
		appendString("gen_ai.agent.id", strings.TrimPrefix(agent, "agent:"))
	}
	status := 1
	if strings.HasSuffix(event.Type, ".failed") || strings.HasSuffix(event.Type, ".denied") || strings.HasSuffix(event.Type, ".blocked") {
		status = 2
	}
	span := otlpSpan{
		TraceID: hex.EncodeToString(traceHash[:16]), SpanID: hex.EncodeToString(spanHash[:8]),
		Name: otlpSpanName(event.Type), Kind: 1,
		StartTimeUnixNano: millisNanos(event.At),
		EndTimeUnixNano:   millisNanos(event.At),
		Attributes:        attributes, Status: otlpStatus{Code: status},
	}
	if event.Cause != 0 && event.Session != "" {
		parentHash := sha256.Sum256([]byte("event:" + formatUint(event.Cause)))
		span.ParentSpanID = hex.EncodeToString(parentHash[:8])
	}
	return span
}

func otlpSpanName(eventType string) string {
	prefix := "event"
	switch {
	case strings.HasPrefix(eventType, "s."):
		prefix = "exec"
	case strings.HasPrefix(eventType, "fs."):
		prefix = "fs"
	case strings.HasPrefix(eventType, "cred."):
		prefix = "credential"
	case strings.HasPrefix(eventType, "egress."):
		prefix = "egress"
	}
	return "remount." + prefix + "." + eventType
}

func formatUint(value uint64) string { return strconv.FormatUint(value, 10) }

func millisNanos(value int64) string {
	formatted := strconv.FormatInt(value, 10)
	if strings.HasPrefix(formatted, "-") {
		return "-" + strings.TrimPrefix(formatted, "-") + "000000"
	}
	return formatted + "000000"
}
