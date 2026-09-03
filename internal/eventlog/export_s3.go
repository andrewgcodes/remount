package eventlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

// ExportObject records one immutable hourly JSONL piece. Multiple pieces can
// exist for a busy hour because every in-memory batch remains bounded.
type ExportObject struct {
	Hour   time.Time
	First  uint64
	Last   uint64
	ID     string
	SHA256 string
	Bytes  int64
}

// ExportObjectRecorder durably indexes a content-addressed export object.
// Recording may be retried and must therefore be idempotent by ID.
type ExportObjectRecorder interface {
	RecordExportObject(ctx context.Context, object ExportObject) error
}

// S3Sink writes UTC-hour-partitioned canonical JSONL to any content-addressed
// BlobStore. An S3 store satisfies this interface without coupling the event
// package to provider configuration or credentials.
type S3Sink struct {
	Store    artifact.BlobStore
	Recorder ExportObjectRecorder
}

// Send implements BatchSink. Each UTC hour in the batch becomes one immutable
// object whose artifact ID contains the same SHA-256 as its JSONL bytes.
func (s *S3Sink) Send(ctx context.Context, events []proto.Event) error {
	if s == nil || s.Store == nil {
		return errors.New("eventlog: object store is required")
	}
	for start := 0; start < len(events); {
		if err := ctx.Err(); err != nil {
			return err
		}
		hour := time.UnixMilli(events[start].At).UTC().Truncate(time.Hour)
		end := start + 1
		for end < len(events) && time.UnixMilli(events[end].At).UTC().Truncate(time.Hour).Equal(hour) {
			end++
		}
		var body bytes.Buffer
		for _, event := range events[start:end] {
			line, err := MarshalEventJSONLine(event)
			if err != nil {
				return err
			}
			body.Write(line)
		}
		payload := body.Bytes()
		sum := sha256.Sum256(payload)
		digest := hex.EncodeToString(sum[:])
		id, size, err := s.Store.Put(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		wantID := artifact.ID(sum[:])
		if id != wantID || size != int64(len(payload)) {
			return fmt.Errorf("eventlog: object store returned unverifiable export metadata")
		}
		object := ExportObject{
			Hour: hour, First: events[start].Seq, Last: events[end-1].Seq,
			ID: id, SHA256: digest, Bytes: size,
		}
		if s.Recorder != nil {
			if err := s.Recorder.RecordExportObject(ctx, object); err != nil {
				return err
			}
		}
		start = end
	}
	return nil
}

var _ BatchSink = (*S3Sink)(nil)
