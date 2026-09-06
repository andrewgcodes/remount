package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/artifact/s3"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

func cmdEventsExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("events export", flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	otlpEndpoint := fs.String("otlp", "", "OTLP/HTTP base URL")
	s3Destination := fs.String("s3", "", "S3 destination (s3://bucket/prefix)")
	jsonl := fs.Bool("jsonl", false, "write canonical JSONL to stdout")
	siemEndpoint := fs.String("siem", "", "HTTPS SIEM endpoint")
	siemFormat := fs.String("siem-format", "json", "SIEM wire format: json or cef")
	follow := fs.Bool("follow", false, "continue exporting new events")
	from := fs.Uint64("from", 1, "first canonical event sequence")
	batchEvents := fs.Int("batch-events", 128, "maximum events per request")
	batchBytes := fs.Int("batch-bytes", 1<<20, "maximum canonical bytes per request")
	parse(fs, args)
	if err := arity(fs, 0, 0, "events export (--otlp URL | --s3 URI | --jsonl | --siem URL) [--from N] [--follow]"); err != nil {
		return err
	}
	selected := 0
	for _, present := range []bool{*otlpEndpoint != "", *s3Destination != "", *jsonl, *siemEndpoint != ""} {
		if present {
			selected++
		}
	}
	if selected != 1 {
		return errors.New("events export: select exactly one of --otlp, --s3, --jsonl, or --siem")
	}
	var sink eventlog.BatchSink
	switch {
	case *otlpEndpoint != "":
		created, err := eventlog.NewOTLPSink(eventlog.OTLPOptions{Endpoint: *otlpEndpoint})
		if err != nil {
			return err
		}
		sink = created
	case *s3Destination != "":
		store, err := newExportS3Store(*s3Destination)
		if err != nil {
			return err
		}
		sink = &eventlog.S3Sink{Store: store}
	case *jsonl:
		sink = &eventlog.JSONLSink{Writer: os.Stdout}
	case *siemEndpoint != "":
		headers := make(http.Header)
		if token := os.Getenv("REMOUNT_SIEM_TOKEN"); token != "" {
			headers.Set("Authorization", "Bearer "+token)
		}
		created, err := eventlog.NewSIEMSink(eventlog.SIEMOptions{
			Endpoint: *siemEndpoint, Format: eventlog.SIEMFormat(*siemFormat), Headers: headers,
		})
		if err != nil {
			return err
		}
		sink = created
	}
	remote := commonFlags.client()
	defer remote.Close()
	last, err := eventlog.RunExport(ctx, clientEventExporter{client: remote}, sink, nil, eventlog.RunOptions{
		From: *from, Follow: *follow, BatchEvents: *batchEvents, BatchBytes: *batchBytes,
	})
	if err != nil {
		return err
	}
	if !*jsonl {
		fmt.Fprintf(os.Stderr, "exported through event %d\n", last)
	}
	return nil
}

type eventReader interface {
	ReadEventPage(ctx context.Context, from uint64, workspace string, filters ...client.EventFilterOption) ([]proto.Event, error)
}

type clientEventExporter struct {
	client eventReader
}

func (e clientEventExporter) Export(ctx context.Context, from, to uint64, sink func(proto.Event) error) (uint64, error) {
	var last uint64
	cursor := from
	for {
		events, err := e.client.ReadEventPage(ctx, cursor, "")
		if err != nil {
			return last, err
		}
		if len(events) == 0 {
			return last, nil
		}
		for _, event := range events {
			if to != 0 && event.Seq > to {
				return last, nil
			}
			if err := sink(event); err != nil {
				return last, err
			}
			last = event.Seq
		}
		next := events[len(events)-1].Seq + 1
		if next <= cursor {
			return last, proto.Err(proto.CodeInternal, "event pagination did not advance")
		}
		cursor = next
		if len(events) < 1000 {
			return last, nil
		}
	}
}

func newExportS3Store(destination string) (*s3.Store, error) {
	parsed, err := url.Parse(destination)
	if err != nil || parsed.Scheme != "s3" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("events export: S3 destination must be s3://bucket/prefix without credentials or query")
	}
	region := firstNonEmpty(os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), "us-east-1")
	endpoint := firstNonEmpty(os.Getenv("AWS_ENDPOINT_URL_S3"), os.Getenv("AWS_ENDPOINT_URL"))
	if endpoint == "" {
		if region == "us-east-1" {
			endpoint = "https://s3.amazonaws.com"
		} else {
			endpoint = "https://s3." + region + ".amazonaws.com"
		}
	}
	pathStyle := false
	if raw := os.Getenv("AWS_S3_PATH_STYLE"); raw != "" {
		pathStyle, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("events export: AWS_S3_PATH_STYLE must be a boolean")
		}
	}
	return s3.New(s3.Config{
		Endpoint: endpoint, Region: region, Bucket: parsed.Host,
		Prefix: strings.Trim(parsed.Path, "/"), PathStyle: pathStyle,
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"), RequestTimeout: 2 * time.Minute,
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ eventlog.Exporter = clientEventExporter{client: (*client.Client)(nil)}
