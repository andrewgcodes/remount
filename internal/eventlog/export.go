package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
)

const (
	defaultExportBatchEvents = 128
	defaultExportBatchBytes  = 1 << 20
	maximumExportBatchEvents = 4096
	maximumExportBatchBytes  = 16 << 20
)

var (
	// ErrCursorConflict means another exporter advanced the same durable cursor.
	ErrCursorConflict = errors.New("eventlog: export cursor changed")
	// ErrExportBatchTooLarge means one event cannot fit in the configured bound.
	ErrExportBatchTooLarge = errors.New("eventlog: event exceeds export batch limit")
)

// Cursor is the next canonical event sequence an export should deliver.
// Revision fences concurrent exporters that share a cursor name.
type Cursor struct {
	Name      string `json:"name"`
	Next      uint64 `json:"next"`
	Revision  uint64 `json:"revision"`
	UpdatedAt int64  `json:"updated_at"`
}

// CursorStore durably fences progress for a named, tenant-scoped export.
// Implementations must compare both Revision and Next atomically.
type CursorStore interface {
	Load(ctx context.Context, name string) (Cursor, error)
	CompareAndSwap(ctx context.Context, name string, previous Cursor, next uint64) (Cursor, error)
}

// MemoryCursorStore is the bounded in-memory cursor implementation used by
// tests and standalone exports. Production control planes persist the same
// contract as export.cursor resources.
type MemoryCursorStore struct {
	mu      sync.Mutex
	cursors map[string]Cursor
	now     func() time.Time
	max     int
}

// NewMemoryCursorStore returns a cursor store retaining at most max named
// cursors. A non-positive max is rejected because export state must be bounded.
func NewMemoryCursorStore(max int) (*MemoryCursorStore, error) {
	if max <= 0 {
		return nil, errors.New("eventlog: cursor capacity must be positive")
	}
	return &MemoryCursorStore{cursors: make(map[string]Cursor), now: time.Now, max: max}, nil
}

// Load returns an absent cursor as revision zero and next zero.
func (s *MemoryCursorStore) Load(ctx context.Context, name string) (Cursor, error) {
	if err := ctx.Err(); err != nil {
		return Cursor{}, err
	}
	if name == "" {
		return Cursor{}, errors.New("eventlog: cursor name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor, ok := s.cursors[name]; ok {
		return cursor, nil
	}
	return Cursor{Name: name}, nil
}

// CompareAndSwap advances a cursor monotonically when previous still matches.
func (s *MemoryCursorStore) CompareAndSwap(ctx context.Context, name string, previous Cursor, next uint64) (Cursor, error) {
	if err := ctx.Err(); err != nil {
		return Cursor{}, err
	}
	if name == "" || previous.Name != name {
		return Cursor{}, errors.New("eventlog: cursor name is required and must match")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.cursors[name]
	if !ok {
		current = Cursor{Name: name}
	}
	if current.Revision != previous.Revision || current.Next != previous.Next {
		return current, ErrCursorConflict
	}
	if next < current.Next {
		return current, errors.New("eventlog: export cursor cannot move backward")
	}
	if !ok && len(s.cursors) >= s.max {
		return current, errors.New("eventlog: export cursor capacity exhausted")
	}
	if current.Revision == math.MaxUint64 {
		return current, errors.New("eventlog: export cursor revision exhausted")
	}
	current.Next = next
	current.Revision++
	current.UpdatedAt = s.now().UnixMilli()
	s.cursors[name] = current
	return current, nil
}

// BatchSink atomically accepts an ordered event batch. Implementations may
// receive a batch again after a crash or cursor conflict and must tolerate it.
type BatchSink interface {
	Send(ctx context.Context, events []proto.Event) error
}

// RunOptions bound and identify one resumable export.
type RunOptions struct {
	Name         string
	From         uint64
	To           uint64
	Follow       bool
	BatchEvents  int
	BatchBytes   int
	PollInterval time.Duration
}

func (o RunOptions) normalized() (RunOptions, error) {
	if o.BatchEvents == 0 {
		o.BatchEvents = defaultExportBatchEvents
	}
	if o.BatchBytes == 0 {
		o.BatchBytes = defaultExportBatchBytes
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.BatchEvents < 1 || o.BatchEvents > maximumExportBatchEvents {
		return o, fmt.Errorf("eventlog: batch events must be between 1 and %d", maximumExportBatchEvents)
	}
	if o.BatchBytes < 1 || o.BatchBytes > maximumExportBatchBytes {
		return o, fmt.Errorf("eventlog: batch bytes must be between 1 and %d", maximumExportBatchBytes)
	}
	if o.PollInterval < time.Millisecond || o.PollInterval > time.Minute {
		return o, errors.New("eventlog: poll interval must be between 1ms and 1m")
	}
	if o.Follow && o.To != 0 {
		return o, errors.New("eventlog: a followed export cannot have an end sequence")
	}
	return o, nil
}

// RunExport copies the retained canonical range to sink in bounded batches.
// Each batch is accepted before its cursor is advanced, giving at-least-once
// recovery: an interrupted commit can duplicate a batch but cannot skip it.
func RunExport(ctx context.Context, source Exporter, sink BatchSink, cursors CursorStore, options RunOptions) (uint64, error) {
	if source == nil || sink == nil {
		return 0, errors.New("eventlog: export source and sink are required")
	}
	opts, err := options.normalized()
	if err != nil {
		return 0, err
	}
	if (cursors == nil) != (opts.Name == "") {
		return 0, errors.New("eventlog: cursor store and cursor name must be configured together")
	}
	cursor := Cursor{}
	from := opts.From
	if cursors != nil {
		cursor, err = cursors.Load(ctx, opts.Name)
		if err != nil {
			return 0, err
		}
		if cursor.Next > from {
			from = cursor.Next
		}
	}
	var last uint64
	for {
		delivered, err := exportPass(ctx, source, sink, cursors, opts, &cursor, from)
		if delivered > last {
			last = delivered
		}
		if err != nil {
			return last, err
		}
		if !opts.Follow {
			return last, nil
		}
		if delivered != 0 {
			if delivered == math.MaxUint64 {
				return delivered, errors.New("eventlog: event sequence exhausted")
			}
			from = delivered + 1
		}
		timer := time.NewTimer(opts.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return last, ctx.Err()
		case <-timer.C:
		}
	}
}

func exportPass(ctx context.Context, source Exporter, sink BatchSink, cursors CursorStore, opts RunOptions, cursor *Cursor, from uint64) (uint64, error) {
	batch := make([]proto.Event, 0, opts.BatchEvents)
	batchBytes := 0
	var last uint64
	var expected uint64
	if from != 0 {
		expected = from
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := sink.Send(ctx, batch); err != nil {
			return err
		}
		next := batch[len(batch)-1].Seq
		if next == math.MaxUint64 {
			return errors.New("eventlog: event sequence exhausted")
		}
		next++
		if cursors != nil {
			advanced, err := cursors.CompareAndSwap(ctx, opts.Name, *cursor, next)
			if errors.Is(err, ErrCursorConflict) {
				current, loadErr := cursors.Load(ctx, opts.Name)
				if loadErr != nil {
					return loadErr
				}
				if current.Next != next {
					return ErrCursorConflict
				}
				advanced, err = current, nil
			}
			if err != nil {
				return err
			}
			*cursor = advanced
		}
		last = batch[len(batch)-1].Seq
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	_, err := source.Export(ctx, from, opts.To, func(event proto.Event) error {
		if event.Seq == 0 {
			return errors.New("eventlog: exporter received an unsequenced event")
		}
		if expected == 0 {
			expected = event.Seq
		}
		if event.Seq != expected {
			return fmt.Errorf("eventlog: export sequence gap: expected %d, got %d", expected, event.Seq)
		}
		if expected == math.MaxUint64 {
			return errors.New("eventlog: event sequence exhausted")
		}
		expected++
		line, marshalErr := MarshalEventJSONLine(event)
		if marshalErr != nil {
			return marshalErr
		}
		if len(line) > opts.BatchBytes {
			return fmt.Errorf("%w: seq %d is %d bytes, limit %d", ErrExportBatchTooLarge, event.Seq, len(line), opts.BatchBytes)
		}
		if len(batch) == opts.BatchEvents || batchBytes+len(line) > opts.BatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, event)
		batchBytes += len(line)
		return nil
	})
	if err != nil {
		return last, err
	}
	if err := flush(); err != nil {
		return last, err
	}
	return last, nil
}

// MarshalEventJSONLine returns the canonical JSONL representation used by
// stdout, SIEM, and object-store exports. Payload is decoded from CBOR so the
// output never exposes implementation bytes, and the trailing newline is part
// of the content digest.
func MarshalEventJSONLine(event proto.Event) ([]byte, error) {
	var payload any
	if len(event.Payload) != 0 {
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			return nil, fmt.Errorf("eventlog: decode payload for seq %d: %w", event.Seq, err)
		}
		normalized, err := jsonValue(payload)
		if err != nil {
			return nil, fmt.Errorf("eventlog: normalize payload for seq %d: %w", event.Seq, err)
		}
		payload = normalized
	}
	record := struct {
		Seq         uint64 `json:"seq"`
		At          int64  `json:"at"`
		Stream      string `json:"stream,omitempty"`
		Principal   string `json:"principal,omitempty"`
		Node        string `json:"node,omitempty"`
		EventID     string `json:"event_id,omitempty"`
		ReceivedAt  int64  `json:"received_at,omitempty"`
		ObservedAt  int64  `json:"observed_at,omitempty"`
		Origin      string `json:"origin,omitempty"`
		Actor       string `json:"actor,omitempty"`
		Tenant      string `json:"tenant,omitempty"`
		Workspace   string `json:"workspace,omitempty"`
		Generation  uint64 `json:"generation,omitempty"`
		Session     string `json:"session,omitempty"`
		OperationID string `json:"operation_id,omitempty"`
		ProducerSeq uint64 `json:"producer_seq,omitempty"`
		Type        string `json:"type"`
		Payload     any    `json:"payload,omitempty"`
		Cause       uint64 `json:"cause,omitempty"`
	}{
		Seq: event.Seq, At: event.At, Stream: event.Stream, Principal: event.Principal,
		Node: event.Node, EventID: event.EventID, ReceivedAt: event.ReceivedAt,
		ObservedAt: event.ObservedAt, Origin: event.Origin, Actor: event.Actor,
		Tenant: event.Tenant, Workspace: event.Workspace, Generation: event.Generation,
		Session: event.Session, OperationID: event.OperationID, ProducerSeq: event.ProducerSeq,
		Type: event.Type, Payload: payload, Cause: event.Cause,
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("eventlog: encode event seq %d: %w", event.Seq, err)
	}
	return append(encoded, '\n'), nil
}

func jsonValue(value any) (any, error) {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, child := range value {
			normalized, err := jsonValue(child)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case map[any]any:
		out := make(map[string]any, len(value))
		for rawKey, child := range value {
			key, ok := rawKey.(string)
			if !ok {
				return nil, errors.New("CBOR map key is not a string")
			}
			normalized, err := jsonValue(child)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case []any:
		out := make([]any, len(value))
		for index, child := range value {
			normalized, err := jsonValue(child)
			if err != nil {
				return nil, err
			}
			out[index] = normalized
		}
		return out, nil
	default:
		return value, nil
	}
}
