// Package session implements the node-side session: a process (exec, pty or
// TCP port forward) whose output is an append-only log of sequenced chunks.
//
// The log is the whole design (docs/adr/0003-session-is-a-log.md). A client is
// a cursor over the log, not a socket: dropping a connection loses nothing,
// and "attach from seq N" is the same code path as live tailing. The log
// keeps a bounded in-memory ring and spills evicted chunks to a file so a
// cold replay of a long-running command is still possible; only when the
// spill file itself is rotated does a seq become truly unavailable, which is
// reported as an Evicted error carrying the oldest seq that is available.
package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"remount.dev/remount/internal/metrics"
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

// ErrLogClosed is returned by Append after Close.
var ErrLogClosed = errors.New("session: log closed")

// LogOptions bound memory and disk use.
type LogOptions struct {
	MemBytes   int    // ring capacity in bytes; default 1 MiB
	SpillBytes int64  // spill file capacity; 0 disables spilling; default 64 MiB
	SpillPath  string // file to spill to; required if SpillBytes > 0
	MaxChunk   int    // split appends larger than this; default 32 KiB
	MaxChunks  int    // ring capacity in chunks (PTYs emit tiny chunks); default 16384
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
}

// Log is an append-only sequence of chunks.
type Log struct {
	opts LogOptions

	mu     sync.Mutex
	chunks []Chunk // in-memory ring, seq-ascending; chunks[0].Seq == first
	bytes  int
	first  uint64 // seq of chunks[0] (== next when empty)
	next   uint64 // next seq to assign
	closed bool
	wake   chan struct{} // closed and replaced on every append/close

	// spill
	spill      *os.File
	spillFirst uint64 // seq of the first chunk in the spill file
	spillNext  uint64 // seq after the last chunk in the spill file
	spillBytes int64
	spillIndex []spillEntry // (seq, offset) every indexStride chunks

	oldest uint64 // oldest seq available anywhere (memory or spill)
}

type spillEntry struct {
	seq uint64
	off int64
}

const indexStride = 64

// NewLog creates an empty log.
func NewLog(opts LogOptions) (*Log, error) {
	opts.defaults()
	l := &Log{opts: opts, wake: make(chan struct{})}
	if opts.SpillBytes > 0 {
		if opts.SpillPath == "" {
			return nil, errors.New("session: SpillPath required when SpillBytes > 0")
		}
		f, err := os.OpenFile(opts.SpillPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, err
		}
		l.spill = f
	}
	return l, nil
}

// Append adds data on stream and returns the seq of the first chunk written.
// Large data is split into MaxChunk pieces so a single frame stays small.
func (l *Log) Append(stream uint8, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrLogClosed
	}
	firstSeq := l.next
	for len(data) > 0 || firstSeq == l.next { // always write at least one chunk (possibly empty)
		n := len(data)
		if n > l.opts.MaxChunk {
			n = l.opts.MaxChunk
		}
		piece := make([]byte, n)
		copy(piece, data[:n])
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
	l.evictLocked()
	l.broadcastLocked()
	return firstSeq, nil
}

// Close marks the log complete: no more appends; cursors at the end return io.EOF.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.broadcastLocked()
	return nil
}

// Release frees the spill file. Call after the session is reaped.
func (l *Log) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.spill != nil {
		name := l.spill.Name()
		_ = l.spill.Close()
		_ = os.Remove(name)
		l.spill = nil
	}
	l.chunks = nil
}

// Next returns the seq that the next Append will get.
func (l *Log) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// Oldest returns the oldest seq that can still be replayed.
func (l *Log) Oldest() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.oldestLocked()
}

func (l *Log) oldestLocked() uint64 {
	if l.spill != nil && l.spillNext > l.spillFirst {
		return l.spillFirst
	}
	if len(l.chunks) > 0 {
		return l.first
	}
	return l.next
}

// Closed reports whether the log is complete.
func (l *Log) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func (l *Log) broadcastLocked() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// evictLocked moves chunks from memory to the spill file until the ring fits.
func (l *Log) evictLocked() {
	for (l.bytes > l.opts.MemBytes || len(l.chunks) > l.opts.MaxChunks) && len(l.chunks) > 1 {
		c := l.chunks[0]
		l.chunks = l.chunks[1:]
		l.bytes -= len(c.Data)
		l.first = c.Seq + 1
		metrics.ChunksEvicted.Inc()
		if l.spill != nil {
			l.spillLocked(c)
		}
	}
	// Let the backing array be reclaimed when the ring drains.
	if len(l.chunks) == 0 {
		l.chunks = nil
		l.first = l.next
	}
}

func (l *Log) spillLocked(c Chunk) {
	rec := int64(8 + 1 + 4 + len(c.Data))
	if l.spillBytes+rec > l.opts.SpillBytes {
		// Rotate: drop history. Brute force (P10); a segmented file can come later.
		_ = l.spill.Truncate(0)
		_, _ = l.spill.Seek(0, io.SeekStart)
		l.spillBytes = 0
		l.spillFirst = c.Seq
		l.spillNext = c.Seq
		l.spillIndex = l.spillIndex[:0]
	}
	if l.spillNext == l.spillFirst && l.spillBytes == 0 {
		l.spillFirst = c.Seq
	}
	if (c.Seq-l.spillFirst)%indexStride == 0 {
		l.spillIndex = append(l.spillIndex, spillEntry{seq: c.Seq, off: l.spillBytes})
	}
	var hdr [13]byte
	binary.BigEndian.PutUint64(hdr[0:8], c.Seq)
	hdr[8] = c.Stream
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(c.Data)))
	if _, err := l.spill.WriteAt(hdr[:], l.spillBytes); err != nil {
		return
	}
	if _, err := l.spill.WriteAt(c.Data, l.spillBytes+13); err != nil {
		return
	}
	l.spillBytes += rec
	l.spillNext = c.Seq + 1
}

// readSpillLocked returns chunks with seq in [from, spillNext).
func (l *Log) readSpillLocked(from uint64) ([]Chunk, error) {
	if l.spill == nil || from < l.spillFirst || from >= l.spillNext {
		return nil, nil
	}
	off := int64(0)
	for _, e := range l.spillIndex {
		if e.seq <= from {
			off = e.off
		} else {
			break
		}
	}
	var out []Chunk
	var hdr [13]byte
	for off < l.spillBytes {
		if _, err := l.spill.ReadAt(hdr[:], off); err != nil {
			return out, err
		}
		seq := binary.BigEndian.Uint64(hdr[0:8])
		n := int(binary.BigEndian.Uint32(hdr[9:13]))
		if seq >= from {
			data := make([]byte, n)
			if _, err := l.spill.ReadAt(data, off+13); err != nil {
				return out, err
			}
			out = append(out, Chunk{Seq: seq, Stream: hdr[8], Data: data})
		}
		off += int64(13 + n)
	}
	return out, nil
}

// Read returns up to max chunks starting at seq from (0 = unlimited), without
// blocking. ok=false with no error means from == next (nothing yet).
func (l *Log) Read(from uint64, max int) ([]Chunk, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readLocked(from, max)
}

func (l *Log) readLocked(from uint64, max int) ([]Chunk, error) {
	if from >= l.next {
		return nil, nil
	}
	oldest := l.oldestLocked()
	if from < oldest {
		return nil, &ErrEvicted{Requested: from, Oldest: oldest}
	}
	var out []Chunk
	if from < l.first {
		sp, err := l.readSpillLocked(from)
		if err != nil {
			return nil, err
		}
		out = append(out, sp...)
		from = l.first
	}
	if from < l.next && len(l.chunks) > 0 {
		i := int(from - l.first)
		if i < 0 {
			i = 0
		}
		for ; i < len(l.chunks); i++ {
			out = append(out, l.chunks[i])
		}
	}
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out, nil
}

// Cursor is a reader position over a Log. Cursors are cheap: the log holds
// no per-cursor state, so a dropped client costs nothing to forget.
type Cursor struct {
	log *Log
	seq uint64
}

// CursorAt returns a cursor positioned at seq.
func (l *Log) CursorAt(seq uint64) *Cursor { return &Cursor{log: l, seq: seq} }

// Seq is the next seq this cursor will return.
func (c *Cursor) Seq() uint64 { return c.seq }

// Next blocks until at least one chunk is available at the cursor and returns
// a batch (bounded by max, 0 = all available). It returns io.EOF once the log
// is closed and fully consumed, and *ErrEvicted if the position is gone; in
// the latter case the caller may Skip to the oldest available seq.
func (c *Cursor) Next(ctx context.Context, max int) ([]Chunk, error) {
	for {
		c.log.mu.Lock()
		out, err := c.log.readLocked(c.seq, max)
		if err != nil {
			c.log.mu.Unlock()
			return nil, err
		}
		if len(out) > 0 {
			c.seq = out[len(out)-1].Seq + 1
			c.log.mu.Unlock()
			return out, nil
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
