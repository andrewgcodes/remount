package eventlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"remount.dev/remount/internal/proto"
)

// JSONLSink writes canonical event records to an io.Writer. A mutex keeps one
// batch contiguous when multiple independent exporters share stdout.
type JSONLSink struct {
	Writer io.Writer
	mu     sync.Mutex
}

// Send implements BatchSink.
func (s *JSONLSink) Send(ctx context.Context, events []proto.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.Writer == nil {
		return errors.New("eventlog: JSONL writer is required")
	}
	var body bytes.Buffer
	for _, event := range events {
		line, err := MarshalEventJSONLine(event)
		if err != nil {
			return err
		}
		body.Write(line)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for body.Len() > 0 {
		written, err := s.Writer.Write(body.Bytes())
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		body.Next(written)
	}
	return nil
}

var _ BatchSink = (*JSONLSink)(nil)
