// Package chunked stores filesystem snapshots as canonical manifests that
// reference content-defined plaintext chunks in an artifact.BlobStore.
package chunked

import (
	"errors"
	"io"
)

const (
	// MinChunkSize is the minimum FastCDC chunk size.
	MinChunkSize = 16 << 10
	// AvgChunkSize is the target FastCDC chunk size.
	AvgChunkSize = 64 << 10
	// MaxChunkSize is the maximum FastCDC chunk size.
	MaxChunkSize = 256 << 10
)

var fastCDCGear = makeGearTable()

// Chunker deterministically divides a byte stream with normalized FastCDC.
// It buffers at most MaxChunkSize bytes plus one returned chunk.
type Chunker struct {
	r       io.Reader
	buf     []byte
	eof     bool
	readErr error
}

// NewChunker returns the fixed 16 KiB / 64 KiB / 256 KiB FastCDC chunker used
// by both node snapshots and client-side directory packing.
func NewChunker(r io.Reader) *Chunker {
	return &Chunker{r: r, buf: make([]byte, 0, MaxChunkSize)}
}

// Next returns the next chunk. The returned bytes are owned by the caller.
func (c *Chunker) Next() ([]byte, error) {
	for len(c.buf) < MaxChunkSize && !c.eof && c.readErr == nil {
		need := MaxChunkSize - len(c.buf)
		start := len(c.buf)
		c.buf = c.buf[:start+need]
		n, err := c.r.Read(c.buf[start:])
		c.buf = c.buf[:start+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.eof = true
			} else {
				c.readErr = err
			}
		}
		if n == 0 && err == nil {
			c.readErr = io.ErrNoProgress
		}
	}
	if len(c.buf) == 0 {
		if c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			return nil, err
		}
		return nil, io.EOF
	}
	cut := fastCDCCut(c.buf)
	out := append([]byte(nil), c.buf[:cut]...)
	copy(c.buf, c.buf[cut:])
	c.buf = c.buf[:len(c.buf)-cut]
	return out, nil
}

func fastCDCCut(buf []byte) int {
	if len(buf) <= MinChunkSize {
		return len(buf)
	}
	end := len(buf)
	if end > MaxChunkSize {
		end = MaxChunkSize
	}
	normal := AvgChunkSize
	if normal > end {
		normal = end
	}
	// Normalization level 1 uses a stricter mask before the target and a
	// looser mask afterward, reducing pathological very-small/very-large runs.
	const smallMask = uint64((1 << 17) - 1)
	const largeMask = uint64((1 << 15) - 1)
	var hash uint64
	for i := MinChunkSize; i < normal; i++ {
		hash = (hash << 1) + fastCDCGear[buf[i]]
		if hash&smallMask == 0 {
			return i + 1
		}
	}
	for i := normal; i < end; i++ {
		hash = (hash << 1) + fastCDCGear[buf[i]]
		if hash&largeMask == 0 {
			return i + 1
		}
	}
	return end
}

func makeGearTable() [256]uint64 {
	var table [256]uint64
	// SplitMix64 with a fixed seed gives a stable, well-distributed gear table
	// without an opaque 256-line literal becoming accidental format state.
	state := uint64(0x72656d6f756e7421)
	for i := range table {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}
