// Package session implements append-only, replayable process logs.
package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/metrics"
)

const (
	// DefaultSegmentBytes is the maximum encoded size of a sealed remote segment.
	DefaultSegmentBytes int64 = 64 << 20
	// DefaultMaxSegments bounds per-session artifact reference metadata. At the
	// default segment size it represents four pebibytes of encoded output.
	DefaultMaxSegments = 65536
	logRecordVersion   = 1
	recordHeaderBytes  = 13
	indexStride        = 64
	// TierBlob and TierDisk are stable names exposed in replay failures.
	TierBlob = "blob"
	TierDisk = "disk"
)

// Chunk is one entry in a session log.
type Chunk struct {
	Seq    uint64
	Stream uint8
	Data   []byte
}

// ErrEvicted reports that a requested seq is no longer replayable.
type ErrEvicted struct {
	Requested uint64
	Oldest    uint64
}

func (e *ErrEvicted) Error() string {
	return fmt.Sprintf("session: seq %d evicted; oldest available is %d", e.Requested, e.Oldest)
}

// ErrTierUnavailable identifies the unavailable storage tier and exact range.
// It unwraps to ErrEvicted so older node code continues to emit an explicit gap.
type ErrTierUnavailable struct {
	Tier      string
	Requested uint64
	Oldest    uint64
	Cause     error
}

func (e *ErrTierUnavailable) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("session: %s tier unavailable for seqs [%d,%d): %v", e.Tier, e.Requested, e.Oldest, e.Cause)
	}
	return fmt.Sprintf("session: %s tier unavailable for seqs [%d,%d)", e.Tier, e.Requested, e.Oldest)
}

// Unwrap preserves the pre-tiered replay-gap contract.
func (e *ErrTierUnavailable) Unwrap() error {
	return &ErrEvicted{Requested: e.Requested, Oldest: e.Oldest}
}

// SegmentRef is one immutable encoded range in a BlobStore.
type SegmentRef struct {
	First    uint64 `json:"first"`
	Next     uint64 `json:"next"`
	Artifact string `json:"artifact"`
	Bytes    int64  `json:"bytes"`
}

// LogRecord is the durable portion of a session record needed for replay.
// An empty Segments list means retention released the references for GC.
type LogRecord struct {
	Version  int          `json:"version"`
	MaxChunk int          `json:"max_chunk"`
	Segments []SegmentRef `json:"segments,omitempty"`
}

// LogStats reports bounded use across all tiers.
type LogStats struct {
	MemoryBytes     int
	MemoryChunks    int
	MaxMemoryBytes  int
	DiskBytes       int64
	MaxDiskBytes    int64
	BlobBytes       int64
	BlobSegments    int
	MaxBlobSegments int
	Oldest          uint64
	Next            uint64
	Closed          bool
	UnavailableTier string
}

// ErrLogClosed is returned by Append after Close.
var ErrLogClosed = errors.New("session: log closed")

// LogOptions bound memory and disk and optionally enable immutable artifacts.
// CommitRecord must atomically replace the session record, be idempotent, and
// not call back into this Log.
type LogOptions struct {
	MemBytes   int
	SpillBytes int64
	SpillPath  string
	MaxChunk   int
	MaxChunks  int

	BlobStore    artifact.BlobStore
	SegmentBytes int64
	MaxSegments  int
	CommitRecord func(LogRecord) error
}

func (o *LogOptions) defaults() {
	if o.MemBytes <= 0 {
		o.MemBytes = 1 << 20
	}
	if o.SpillBytes == 0 && o.SpillPath != "" {
		o.SpillBytes = 64 << 20
	}
	if o.MaxChunk <= 0 {
		o.MaxChunk = 32 << 10
	}
	if o.MaxChunks <= 0 {
		o.MaxChunks = 16384
	}
	if o.BlobStore != nil && o.SegmentBytes <= 0 {
		o.SegmentBytes = DefaultSegmentBytes
		if o.SpillBytes > 0 && o.SegmentBytes > o.SpillBytes {
			o.SegmentBytes = o.SpillBytes
		}
	}
	if o.MaxSegments <= 0 {
		o.MaxSegments = DefaultMaxSegments
	}
}

func (o LogOptions) validate() error {
	if o.MemBytes <= 0 || o.SpillBytes < 0 || o.MaxChunk <= 0 || o.MaxChunks <= 0 || o.SegmentBytes < 0 {
		return errors.New("session: log limits must be positive")
	}
	if o.SpillBytes > 0 && o.SpillPath == "" {
		return errors.New("session: SpillPath required when SpillBytes > 0")
	}
	if o.SpillBytes > 0 && o.SpillBytes < int64(recordHeaderBytes+o.MaxChunk) {
		return errors.New("session: SpillBytes must hold one maximum chunk")
	}
	if o.BlobStore != nil {
		if o.SpillBytes == 0 {
			return errors.New("session: artifact tier requires a disk spill tier")
		}
		if o.CommitRecord == nil {
			return errors.New("session: artifact tier requires durable CommitRecord")
		}
		if o.SegmentBytes < int64(recordHeaderBytes+o.MaxChunk) {
			return errors.New("session: SegmentBytes must hold one maximum chunk")
		}
		if o.SegmentBytes > DefaultSegmentBytes {
			return fmt.Errorf("session: SegmentBytes exceeds %d-byte format limit", DefaultSegmentBytes)
		}
		if o.SegmentBytes > o.SpillBytes {
			return errors.New("session: SegmentBytes exceeds local spill capacity")
		}
	}
	return nil
}

// Log is an append-only sequence of chunks.
type Log struct {
	opts LogOptions

	mu     sync.Mutex
	chunks []Chunk
	bytes  int
	first  uint64
	next   uint64
	closed bool
	wake   chan struct{}

	spill      *os.File
	spillFirst uint64
	spillNext  uint64
	spillBytes int64
	spillIndex []spillEntry

	segments        []SegmentRef
	unavailableTier string
	appendErr       error
}

type spillEntry struct {
	seq uint64
	off int64
}

// NewLog creates an empty log.
func NewLog(opts LogOptions) (*Log, error) {
	opts.defaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	l := &Log{opts: opts, wake: make(chan struct{})}
	if opts.SpillBytes > 0 {
		f, err := os.OpenFile(opts.SpillPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, err
		}
		l.spill = f
	}
	return l, nil
}

// OpenArchivedLog opens a closed remote-only log from a durable record.
func OpenArchivedLog(store artifact.BlobStore, record LogRecord) (*Log, error) {
	if store == nil {
		return nil, errors.New("session: archived log requires BlobStore")
	}
	if err := validateLogRecord(record); err != nil {
		return nil, err
	}
	next := uint64(0)
	if len(record.Segments) > 0 {
		next = record.Segments[len(record.Segments)-1].Next
	}
	return &Log{
		opts: LogOptions{
			MemBytes: 1 << 20, MaxChunk: record.MaxChunk, MaxChunks: 16384,
			BlobStore: store, SegmentBytes: DefaultSegmentBytes, MaxSegments: DefaultMaxSegments,
		},
		segments: append([]SegmentRef(nil), record.Segments...), first: next, next: next,
		closed: true, wake: make(chan struct{}),
	}, nil
}

func validateLogRecord(record LogRecord) error {
	if record.Version != logRecordVersion {
		return fmt.Errorf("session: unsupported log record version %d", record.Version)
	}
	if len(record.Segments) > 0 && record.MaxChunk <= 0 {
		return errors.New("session: log record omits maximum chunk size")
	}
	if len(record.Segments) > DefaultMaxSegments {
		return errors.New("session: log record exceeds segment limit")
	}
	var previous uint64
	for i, segment := range record.Segments {
		if segment.First >= segment.Next || segment.Bytes <= 0 || segment.Bytes > DefaultSegmentBytes {
			return errors.New("session: invalid log segment range")
		}
		if _, err := artifact.Digest(segment.Artifact); err != nil {
			return fmt.Errorf("session: invalid log segment artifact: %w", err)
		}
		if i > 0 && segment.First != previous {
			return errors.New("session: non-contiguous log segments")
		}
		previous = segment.Next
	}
	return nil
}

// Append adds data and returns the seq of the first chunk written.
func (l *Log) Append(stream uint8, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(stream, data, false)
}

// appendTerminal reserves the one bounded final append after a storage
// failure. Ordinary producers are stopped at the first error, but the exit
// marker must still be observable and last.
func (l *Log) appendTerminal(stream uint8, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(stream, data, true)
}

func (l *Log) appendLocked(stream uint8, data []byte, terminal bool) (uint64, error) {
	if l.closed {
		return 0, ErrLogClosed
	}
	if l.appendErr != nil && !terminal {
		return l.next, l.appendErr
	}
	firstSeq := l.next
	for len(data) > 0 || firstSeq == l.next {
		n := len(data)
		if n > l.opts.MaxChunk {
			n = l.opts.MaxChunk
		}
		piece := append([]byte(nil), data[:n]...)
		data = data[n:]
		l.chunks = append(l.chunks, Chunk{Seq: l.next, Stream: stream, Data: piece})
		l.bytes += n
		l.next++
		metrics.ChunksEmitted.Inc()
		metrics.BytesEmitted.Add(uint64(n))
		if n == 0 {
			break
		}
	}
	err := l.evictLocked(false)
	if err != nil {
		l.appendErr = err
	}
	l.broadcastLocked()
	return firstSeq, err
}

// Close seals all remaining output before publishing completion. Failure
// leaves the lower and memory tiers intact and is returned after joining the
// synchronous BlobStore producer.
func (l *Log) Close() error {
	return l.closeWithPublish(nil)
}

// closeWithPublish runs publish after every archival producer has joined and
// while readers are still excluded from observing EOF. Session uses it to
// commit capacity and publish its exited channel as one ordered handoff.
func (l *Log) closeWithPublish(publish func()) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	var err error
	if l.opts.BlobStore != nil {
		err = l.evictLocked(true)
		if err == nil {
			err = l.sealSpillLocked()
		}
		if err == nil {
			l.appendErr = nil
		}
	}
	l.closed = true
	if publish != nil {
		publish()
	}
	l.broadcastLocked()
	return err
}

// Forget durably releases remote references, then frees local storage.
func (l *Log) Forget() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) > 0 && l.opts.CommitRecord != nil {
		if err := l.opts.CommitRecord(LogRecord{Version: logRecordVersion, MaxChunk: l.opts.MaxChunk}); err != nil {
			return fmt.Errorf("session: release log record: %w", err)
		}
	}
	if l.spill != nil {
		name := l.spill.Name()
		_ = l.spill.Close()
		_ = os.Remove(name)
		l.spill = nil
	}
	l.chunks = nil
	l.bytes = 0
	l.segments = nil
	l.first = l.next
	return nil
}

// Release frees the log after durably dropping remote references.
func (l *Log) Release() error { return l.Forget() }

// Record returns a copy suitable for durable persistence.
func (l *Log) Record() LogRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.recordLocked(l.segments)
}

func (l *Log) recordLocked(segments []SegmentRef) LogRecord {
	return LogRecord{Version: logRecordVersion, MaxChunk: l.opts.MaxChunk, Segments: append([]SegmentRef(nil), segments...)}
}

// SegmentRefs returns artifacts that retention and GC must protect.
func (l *Log) SegmentRefs() []SegmentRef { return l.Record().Segments }

// Stats returns a point-in-time usage snapshot.
func (l *Log) Stats() LogStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	var blobBytes int64
	for _, segment := range l.segments {
		blobBytes += segment.Bytes
	}
	return LogStats{
		MemoryBytes: l.bytes, MemoryChunks: len(l.chunks), DiskBytes: l.spillBytes,
		MaxMemoryBytes: l.opts.MemBytes, MaxDiskBytes: l.opts.SpillBytes,
		BlobBytes: blobBytes, BlobSegments: len(l.segments), MaxBlobSegments: l.opts.MaxSegments, Oldest: l.oldestLocked(),
		Next: l.next, Closed: l.closed, UnavailableTier: l.unavailableTier,
	}
}

// Next returns the next append sequence.
func (l *Log) Next() uint64 { l.mu.Lock(); defer l.mu.Unlock(); return l.next }

// Oldest returns the oldest replayable sequence.
func (l *Log) Oldest() uint64 { l.mu.Lock(); defer l.mu.Unlock(); return l.oldestLocked() }

func (l *Log) oldestLocked() uint64 {
	if len(l.segments) > 0 {
		return l.segments[0].First
	}
	if l.spill != nil && l.spillNext > l.spillFirst {
		return l.spillFirst
	}
	if len(l.chunks) > 0 {
		return l.first
	}
	return l.next
}

// Closed reports whether the log is complete.
func (l *Log) Closed() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.closed }

func (l *Log) broadcastLocked() { close(l.wake); l.wake = make(chan struct{}) }

func (l *Log) evictLocked(flush bool) error {
	for len(l.chunks) > 0 && (flush || ((l.bytes > l.opts.MemBytes || len(l.chunks) > l.opts.MaxChunks) && len(l.chunks) > 1)) {
		c := l.chunks[0]
		if l.spill != nil {
			if err := l.spillLocked(c); err != nil {
				return err
			}
		}
		l.chunks = l.chunks[1:]
		l.bytes -= len(c.Data)
		l.first = c.Seq + 1
		metrics.ChunksEvicted.Inc()
	}
	if len(l.chunks) == 0 {
		l.chunks = nil
		l.first = l.next
	}
	return nil
}

func (l *Log) spillLocked(c Chunk) error {
	rec := int64(recordHeaderBytes + len(c.Data))
	if l.opts.BlobStore != nil && l.spillBytes > 0 && l.spillBytes+rec > l.opts.SegmentBytes {
		if err := l.sealSpillLocked(); err != nil {
			return err
		}
	}
	if l.opts.BlobStore == nil && l.spillBytes+rec > l.opts.SpillBytes {
		if err := l.resetSpillLocked(); err != nil {
			return fmt.Errorf("session: rotate spill: %w", err)
		}
		l.unavailableTier = TierBlob
	}
	first := l.spillFirst
	if l.spillBytes == 0 {
		first = c.Seq
	}
	off := l.spillBytes
	var hdr [recordHeaderBytes]byte
	binary.BigEndian.PutUint64(hdr[0:8], c.Seq)
	hdr[8] = c.Stream
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(c.Data)))
	if err := writeAtFull(l.spill, hdr[:], off); err != nil {
		l.invalidateSpillLocked()
		return l.tierErrorLocked(TierDisk, c.Seq, c.Seq+1, fmt.Errorf("write header: %w", err))
	}
	if err := writeAtFull(l.spill, c.Data, off+recordHeaderBytes); err != nil {
		l.invalidateSpillLocked()
		return l.tierErrorLocked(TierDisk, c.Seq, c.Seq+1, fmt.Errorf("write data: %w", err))
	}
	if err := l.spill.Sync(); err != nil {
		l.invalidateSpillLocked()
		return l.tierErrorLocked(TierDisk, c.Seq, c.Seq+1, fmt.Errorf("sync: %w", err))
	}
	if off == 0 {
		l.spillFirst = first
	}
	if (c.Seq-first)%indexStride == 0 {
		l.spillIndex = append(l.spillIndex, spillEntry{seq: c.Seq, off: off})
	}
	l.spillBytes += rec
	l.spillNext = c.Seq + 1
	if l.opts.BlobStore != nil && l.spillBytes >= l.opts.SegmentBytes {
		return l.sealSpillLocked()
	}
	return nil
}

func (l *Log) sealSpillLocked() error {
	if l.spillBytes == 0 {
		return nil
	}
	first, next, size := l.spillFirst, l.spillNext, l.spillBytes
	if len(l.segments) >= l.opts.MaxSegments {
		return l.tierErrorLocked(TierBlob, first, next, errors.New("session segment reference limit reached"))
	}
	if err := l.spill.Sync(); err != nil {
		return l.tierErrorLocked(TierDisk, first, next, fmt.Errorf("sync before seal: %w", err))
	}
	id, n, err := l.opts.BlobStore.Put(io.NewSectionReader(l.spill, 0, size))
	if err != nil {
		return l.tierErrorLocked(TierBlob, first, next, fmt.Errorf("put segment: %w", err))
	}
	if n != size {
		return l.tierErrorLocked(TierBlob, first, next, fmt.Errorf("put size %d, want %d", n, size))
	}
	head, err := l.opts.BlobStore.Head(id)
	if err != nil || head != size {
		if err == nil {
			err = fmt.Errorf("head size %d, want %d", head, size)
		}
		return l.tierErrorLocked(TierBlob, first, next, fmt.Errorf("verify segment: %w", err))
	}
	candidate := append(append([]SegmentRef(nil), l.segments...), SegmentRef{First: first, Next: next, Artifact: id, Bytes: size})
	if len(l.segments) > 0 && l.segments[len(l.segments)-1].Next != first {
		return errors.New("session: refusing non-contiguous artifact segment")
	}
	if err := l.opts.CommitRecord(l.recordLocked(candidate)); err != nil {
		return l.tierErrorLocked(TierBlob, first, next, fmt.Errorf("commit segment reference: %w", err))
	}
	// The durable reference is the commit point. Only now may disk be reclaimed.
	l.segments = candidate
	l.spillBytes, l.spillFirst, l.spillNext = 0, 0, 0
	l.spillIndex = l.spillIndex[:0]
	if err := l.spill.Truncate(0); err != nil {
		return l.tierErrorLocked(TierDisk, first, next, fmt.Errorf("reclaim sealed segment: %w", err))
	}
	if _, err := l.spill.Seek(0, io.SeekStart); err != nil {
		return l.tierErrorLocked(TierDisk, first, next, fmt.Errorf("seek after seal: %w", err))
	}
	l.unavailableTier = ""
	return nil
}

func (l *Log) resetSpillLocked() error {
	if err := l.spill.Truncate(0); err != nil {
		return err
	}
	if _, err := l.spill.Seek(0, io.SeekStart); err != nil {
		return err
	}
	l.spillBytes, l.spillFirst, l.spillNext = 0, 0, 0
	l.spillIndex = l.spillIndex[:0]
	return nil
}

func writeAtFull(w io.WriterAt, p []byte, off int64) error {
	for len(p) > 0 {
		n, err := w.WriteAt(p, off)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p, off = p[n:], off+int64(n)
	}
	return nil
}

func (l *Log) invalidateSpillLocked() {
	_ = l.spill.Truncate(0)
	l.spillBytes, l.spillFirst, l.spillNext = 0, 0, 0
	l.spillIndex = l.spillIndex[:0]
	l.unavailableTier = TierDisk
}

func (l *Log) tierErrorLocked(tier string, from, to uint64, cause error) error {
	l.unavailableTier = tier
	return tierError(tier, from, to, cause)
}

func tierError(tier string, from, to uint64, cause error) error {
	return &ErrTierUnavailable{Tier: tier, Requested: from, Oldest: to, Cause: cause}
}

func (l *Log) readSpillLocked(from uint64, max int) ([]Chunk, error) {
	if l.spill == nil || from < l.spillFirst || from >= l.spillNext {
		return nil, nil
	}
	off := int64(0)
	expected := l.spillFirst
	for _, e := range l.spillIndex {
		if e.seq <= from {
			off = e.off
			expected = e.seq
		} else {
			break
		}
	}
	var out []Chunk
	var hdr [recordHeaderBytes]byte
	for off < l.spillBytes {
		if _, err := l.spill.ReadAt(hdr[:], off); err != nil {
			return nil, l.tierErrorLocked(TierDisk, from, l.first, err)
		}
		seq := binary.BigEndian.Uint64(hdr[0:8])
		n := int(binary.BigEndian.Uint32(hdr[9:13]))
		if seq != expected || n < 0 || n > l.opts.MaxChunk || off+recordHeaderBytes+int64(n) > l.spillBytes {
			return nil, l.tierErrorLocked(TierDisk, from, l.first, errors.New("corrupt spill record"))
		}
		if seq >= from {
			data := make([]byte, n)
			if _, err := l.spill.ReadAt(data, off+recordHeaderBytes); err != nil {
				return nil, l.tierErrorLocked(TierDisk, from, l.first, err)
			}
			out = append(out, Chunk{Seq: seq, Stream: hdr[8], Data: data})
			off += int64(recordHeaderBytes + n)
			expected++
			if max > 0 && len(out) >= max {
				break
			}
			continue
		}
		off += int64(recordHeaderBytes + n)
		expected++
	}
	if off >= l.spillBytes && expected != l.spillNext {
		return nil, l.tierErrorLocked(TierDisk, from, l.first, errors.New("spill ended before its committed range"))
	}
	return out, nil
}

// Read returns up to max chunks from seq (zero max means all available).
// Remote I/O does not hold the append mutex.
func (l *Log) Read(from uint64, max int) ([]Chunk, error) {
	var out []Chunk
	for max <= 0 || len(out) < max {
		l.mu.Lock()
		if from >= l.next {
			l.mu.Unlock()
			return out, nil
		}
		segment, found := l.segmentForLocked(from)
		store, maxChunk := l.opts.BlobStore, l.opts.MaxChunk
		if found {
			l.mu.Unlock()
			remaining := 0
			if max > 0 {
				remaining = max - len(out)
			}
			chunks, err := readBlobSegment(store, segment, from, remaining, maxChunk)
			if err != nil {
				l.markUnavailable(TierBlob)
				return nil, tierError(TierBlob, from, segment.Next, err)
			}
			if len(chunks) == 0 {
				return nil, tierError(TierBlob, from, segment.Next, errors.New("segment omitted requested sequence"))
			}
			out = append(out, chunks...)
			from = chunks[len(chunks)-1].Seq + 1
			continue
		}
		oldest := l.oldestLocked()
		if from < oldest {
			tier := l.unavailableTier
			if tier == "" {
				tier = TierBlob
			}
			l.mu.Unlock()
			return nil, tierError(tier, from, oldest, nil)
		}
		remaining := 0
		if max > 0 {
			remaining = max - len(out)
		}
		if from < l.first {
			chunks, err := l.readSpillLocked(from, remaining)
			if err != nil {
				l.mu.Unlock()
				return nil, err
			}
			if len(chunks) == 0 {
				first := l.first
				l.mu.Unlock()
				return nil, tierError(TierDisk, from, first, errors.New("spill range is not contiguous"))
			}
			out = append(out, chunks...)
			from = chunks[len(chunks)-1].Seq + 1
			l.mu.Unlock()
			continue
		}
		i := int(from - l.first)
		for i < len(l.chunks) && (max <= 0 || len(out) < max) {
			chunk := l.chunks[i]
			chunk.Data = append([]byte(nil), chunk.Data...)
			out = append(out, chunk)
			i++
		}
		l.mu.Unlock()
		return out, nil
	}
	return out, nil
}

func (l *Log) segmentForLocked(seq uint64) (SegmentRef, bool) {
	for _, segment := range l.segments {
		if seq >= segment.First && seq < segment.Next {
			return segment, true
		}
		if seq < segment.First {
			break
		}
	}
	return SegmentRef{}, false
}

func (l *Log) markUnavailable(tier string) {
	l.mu.Lock()
	l.unavailableTier = tier
	l.mu.Unlock()
}

func readBlobSegment(store artifact.BlobStore, segment SegmentRef, from uint64, max, maxChunk int) ([]Chunk, error) {
	if store == nil {
		return nil, errors.New("BlobStore is not configured")
	}
	r, n, err := store.Open(segment.Artifact)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if n != segment.Bytes {
		return nil, fmt.Errorf("segment size %d, want %d", n, segment.Bytes)
	}
	limited := &io.LimitedReader{R: r, N: n}
	seq := segment.First
	var out []Chunk
	var hdr [recordHeaderBytes]byte
	for limited.N > 0 {
		if limited.N < recordHeaderBytes {
			return nil, errors.New("truncated segment header")
		}
		if _, err := io.ReadFull(limited, hdr[:]); err != nil {
			return nil, err
		}
		gotSeq := binary.BigEndian.Uint64(hdr[0:8])
		length := int(binary.BigEndian.Uint32(hdr[9:13]))
		if gotSeq != seq || length < 0 || length > maxChunk || int64(length) > limited.N {
			return nil, errors.New("invalid segment record")
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(limited, data); err != nil {
			return nil, err
		}
		if gotSeq >= from && (max <= 0 || len(out) < max) {
			out = append(out, Chunk{Seq: gotSeq, Stream: hdr[8], Data: data})
		}
		seq++
	}
	if seq != segment.Next {
		return nil, fmt.Errorf("segment ended at seq %d, want %d", seq, segment.Next)
	}
	var one [1]byte
	if read, err := r.Read(one[:]); read != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("segment has trailing bytes")
		}
		return nil, err
	}
	return out, nil
}

// Cursor is a reader position over a Log.
type Cursor struct {
	log *Log
	seq uint64
}

// CursorAt returns a cursor positioned at seq.
func (l *Log) CursorAt(seq uint64) *Cursor { return &Cursor{log: l, seq: seq} }

// Seq is the next seq this cursor will return.
func (c *Cursor) Seq() uint64 { return c.seq }

// Next blocks until at least one chunk is available.
func (c *Cursor) Next(ctx context.Context, max int) ([]Chunk, error) {
	for {
		out, err := c.log.Read(c.seq, max)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 {
			c.seq = out[len(out)-1].Seq + 1
			return out, nil
		}
		c.log.mu.Lock()
		if c.seq < c.log.next {
			c.log.mu.Unlock()
			continue
		}
		if c.log.closed {
			c.log.mu.Unlock()
			return nil, io.EOF
		}
		wake := c.log.wake
		c.log.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Skip moves the cursor forward to seq.
func (c *Cursor) Skip(seq uint64) {
	if seq > c.seq {
		c.seq = seq
	}
}
