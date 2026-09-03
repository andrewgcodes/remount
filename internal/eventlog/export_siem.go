package eventlog

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/proto"
)

// SIEMFormat selects canonical JSONL or Common Event Format output.
type SIEMFormat string

const (
	// SIEMJSON sends the same canonical JSONL used by stdout and object export.
	SIEMJSON SIEMFormat = "json"
	// SIEMCEF sends one CEF record per canonical event without payload content.
	SIEMCEF SIEMFormat = "cef"
)

// SIEMOptions configures one HTTPS SIEM destination.
type SIEMOptions struct {
	Endpoint     string
	Format       SIEMFormat
	HTTPClient   *http.Client
	Headers      http.Header
	Retries      int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxBodyBytes int
}

// SIEMSink exports audit records over HTTPS. Redirects are refused so
// authorization headers cannot leave the configured origin.
type SIEMSink struct {
	endpoint *url.URL
	format   SIEMFormat
	http     httpExportOptions
}

// NewSIEMSink validates options without contacting the destination.
func NewSIEMSink(options SIEMOptions) (*SIEMSink, error) {
	endpoint, err := validateExportURL(options.Endpoint, true, "")
	if err != nil {
		return nil, err
	}
	format := options.Format
	if format == "" {
		format = SIEMJSON
	}
	if format != SIEMJSON && format != SIEMCEF {
		return nil, errors.New("eventlog: SIEM format must be json or cef")
	}
	httpOptions, err := normalizeHTTPOptions(options.HTTPClient, options.Headers, options.Retries, options.BaseBackoff, options.MaxBackoff, options.MaxBodyBytes)
	if err != nil {
		return nil, err
	}
	return &SIEMSink{endpoint: endpoint, format: format, http: httpOptions}, nil
}

// Send implements BatchSink.
func (s *SIEMSink) Send(ctx context.Context, events []proto.Event) error {
	if s == nil {
		return errors.New("eventlog: SIEM sink is required")
	}
	var body bytes.Buffer
	for _, event := range events {
		if s.format == SIEMCEF {
			body.WriteString(marshalCEF(event))
			body.WriteByte('\n')
			continue
		}
		line, err := MarshalEventJSONLine(event)
		if err != nil {
			return err
		}
		body.Write(line)
	}
	contentType := "application/x-ndjson"
	if s.format == SIEMCEF {
		contentType = "text/plain; charset=utf-8"
	}
	return postExport(ctx, s.endpoint, s.http, "SIEM", contentType, body.Bytes(), nil)
}

func marshalCEF(event proto.Event) string {
	severity := "3"
	if strings.HasSuffix(event.Type, ".failed") || strings.HasSuffix(event.Type, ".denied") || strings.HasSuffix(event.Type, ".blocked") {
		severity = "8"
	}
	headerType := escapeCEFHeader(event.Type)
	extensions := []string{
		"rt=" + strconv.FormatInt(event.At, 10),
		"cs1Label=Sequence", "cs1=" + strconv.FormatUint(event.Seq, 10),
	}
	appendExtension := func(label, value string) {
		if value != "" {
			extensions = append(extensions, label+"="+escapeCEFExtension(value))
		}
	}
	appendExtension("suser", event.Principal)
	appendExtension("shost", event.Node)
	if event.Tenant != "" {
		appendExtension("cs2Label", "Tenant")
		appendExtension("cs2", event.Tenant)
	}
	if event.Workspace != "" {
		appendExtension("cs3Label", "Workspace")
		appendExtension("cs3", event.Workspace)
	}
	if event.Session != "" {
		appendExtension("cs4Label", "Session")
		appendExtension("cs4", event.Session)
	}
	return "CEF:0|Remount|Remount|1|" + headerType + "|" + headerType + "|" + severity + "|" + strings.Join(extensions, " ")
}

func escapeCEFHeader(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "|", "\\|")
	value = strings.ReplaceAll(value, "\r", " ")
	return strings.ReplaceAll(value, "\n", " ")
}

func escapeCEFExtension(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "=", "\\=")
	value = strings.ReplaceAll(value, "\r", "\\r")
	return strings.ReplaceAll(value, "\n", "\\n")
}

var _ BatchSink = (*SIEMSink)(nil)
