package main

import (
	"context"
	"errors"
	"testing"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

type fakeEventReader struct {
	pages [][]proto.Event
	err   error
	from  []uint64
}

func (r *fakeEventReader) ReadEventPage(_ context.Context, from uint64, _ string, _ ...client.EventFilterOption) ([]proto.Event, error) {
	r.from = append(r.from, from)
	if len(r.pages) == 0 {
		return nil, r.err
	}
	page := append([]proto.Event(nil), r.pages[0]...)
	r.pages = r.pages[1:]
	return page, r.err
}

func TestClientEventExporterBoundsRangeAndPropagatesSinkError(t *testing.T) {
	reader := &fakeEventReader{pages: [][]proto.Event{{{Seq: 3}, {Seq: 4}, {Seq: 5}}}}
	exporter := clientEventExporter{client: reader}
	var got []uint64
	last, err := exporter.Export(context.Background(), 3, 4, func(event proto.Event) error {
		got = append(got, event.Seq)
		return nil
	})
	if err != nil || last != 4 || len(reader.from) != 1 || reader.from[0] != 3 || len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("Export = (%d, %v), from=%v events=%v", last, err, reader.from, got)
	}
	stop := errors.New("stop")
	reader.pages = [][]proto.Event{{{Seq: 3}, {Seq: 4}, {Seq: 5}}}
	last, err = exporter.Export(context.Background(), 3, 0, func(event proto.Event) error {
		if event.Seq == 4 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || last != 3 {
		t.Fatalf("sink failure = (%d, %v)", last, err)
	}
}

func TestClientEventExporterReadsIncrementalPages(t *testing.T) {
	first := make([]proto.Event, 1000)
	for index := range first {
		first[index].Seq = uint64(index + 1)
	}
	reader := &fakeEventReader{pages: [][]proto.Event{first, {{Seq: 1001}, {Seq: 1002}}}}
	var count int
	last, err := (clientEventExporter{client: reader}).Export(context.Background(), 1, 0, func(proto.Event) error {
		count++
		return nil
	})
	if err != nil || last != 1002 || count != 1002 || len(reader.from) != 2 || reader.from[0] != 1 || reader.from[1] != 1001 {
		t.Fatalf("Export = (%d, %v), count=%d pages=%v", last, err, count, reader.from)
	}
}

func TestNewExportS3StoreValidatesDestinationBeforeEnvironment(t *testing.T) {
	for _, invalid := range []string{"http://bucket/prefix", "s3:///prefix", "s3://user:secret@bucket/prefix", "s3://bucket/prefix?token=secret"} {
		if _, err := newExportS3Store(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://s3.example.test")
	if _, err := newExportS3Store("s3://audit/events"); err != nil {
		t.Fatal(err)
	}
}
