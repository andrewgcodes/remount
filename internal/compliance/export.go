// Package compliance creates and verifies tenant-scoped, signed audit bundles
// from the canonical event log. It owns no authentication or signing-key
// persistence; those remain control-plane integration responsibilities.
package compliance

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

const (
	bundleSchema           = "remount.audit.v1"
	signatureDomain        = "remount.audit.manifest.v1\x00"
	defaultMaxRangeEvents  = 10_000_000
	defaultMaxEventBytes   = 16 << 20
	defaultMaxPayloadBytes = int64(8 << 30)
	maximumMaxEventBytes   = 64 << 20
	maximumMaxPayloadBytes = int64(1 << 40)
)

var (
	// ErrGap means the requested canonical sequence range was unavailable or
	// ceased being contiguous while it was read.
	ErrGap = errors.New("compliance: canonical event gap")
	// ErrTampered means a bundle is malformed or disagrees with its signature,
	// content hash, range, tenant, or event summary.
	ErrTampered = errors.New("compliance: audit bundle verification failed")
	// ErrLimit means a configured event, range, or output bound was exceeded.
	ErrLimit = errors.New("compliance: export limit exceeded")
)

// GapKind distinguishes retention before export from a gap observed during a
// retention race or faulty source.
type GapKind string

const (
	// GapEvicted means retention removed the requested range before it could be
	// read.
	GapEvicted GapKind = "evicted"
	// GapIncomplete means the source ended or skipped a sequence while the
	// requested range was being read.
	GapIncomplete GapKind = "incomplete"
)

// GapError reports exactly what part of the inclusive canonical range could
// not be proven complete. Observed is zero when the range ended early.
type GapError struct {
	Kind          GapKind
	RequestedFrom uint64
	RequestedTo   uint64
	Expected      uint64
	Observed      uint64
	Oldest        uint64
}

func (e *GapError) Error() string {
	if e.Kind == GapEvicted {
		return fmt.Sprintf("compliance: requested range [%d,%d] was evicted; oldest retained sequence is %d", e.RequestedFrom, e.RequestedTo, e.Oldest)
	}
	if e.Observed == 0 {
		return fmt.Sprintf("compliance: requested range [%d,%d] ended before sequence %d", e.RequestedFrom, e.RequestedTo, e.Expected)
	}
	return fmt.Sprintf("compliance: requested range [%d,%d] has a gap at %d before %d", e.RequestedFrom, e.RequestedTo, e.Expected, e.Observed)
}

// Unwrap makes errors.Is(err, ErrGap) reliable without matching text.
func (e *GapError) Unwrap() error { return ErrGap }

// Manifest is the signed, deterministic description of one tenant export.
// RangeFrom and RangeTo are the exact requested inclusive global sequence
// range; FirstSeq and LastSeq describe the tenant events present within it.
type Manifest struct {
	Schema             string `json:"schema"`
	Tenant             string `json:"tenant"`
	RangeFrom          uint64 `json:"range_from"`
	RangeTo            uint64 `json:"range_to"`
	CreatedAt          int64  `json:"created_at"`
	EventCount         uint64 `json:"event_count"`
	FirstSeq           uint64 `json:"first_seq"`
	LastSeq            uint64 `json:"last_seq"`
	PayloadBytes       uint64 `json:"payload_bytes"`
	PayloadSHA256      string `json:"payload_sha256"`
	HashAlgorithm      string `json:"hash_algorithm"`
	SignatureAlgorithm string `json:"signature_algorithm"`
	KeyID              string `json:"key_id"`
}

// SignedManifest is the final trailer of an audit bundle.
type SignedManifest struct {
	Manifest  Manifest `json:"remount_audit_manifest"`
	Signature string   `json:"signature"`
}

// Request selects one tenant from an exact inclusive global sequence range.
type Request struct {
	Tenant string
	From   uint64
	To     uint64
}

// Options bound one Exporter. The payload may be large on disk, but memory is
// bounded to one canonical event line plus the eventlog export page.
type Options struct {
	Now             func() time.Time
	MaxRangeEvents  uint64
	MaxEventBytes   int
	MaxPayloadBytes int64
}

// BundleSink atomically publishes a complete bundle or leaves the prior
// destination untouched. The callback writes deterministic event lines and a
// final signed-manifest line.
type BundleSink interface {
	Commit(context.Context, func(io.Writer) error) error
}

// Exporter creates tenant-filtered compliance bundles from eventlog.Exporter.
type Exporter struct {
	source          eventlog.Exporter
	privateKey      ed25519.PrivateKey
	keyID           string
	now             func() time.Time
	maxRangeEvents  uint64
	maxEventBytes   int
	maxPayloadBytes int64
}

// NewExporter validates and copies the signing key. The key is retained only
// in memory and is never serialized into a bundle or error.
func NewExporter(source eventlog.Exporter, keyID string, privateKey ed25519.PrivateKey, options Options) (*Exporter, error) {
	if source == nil || !safeID(keyID, 128) || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("compliance: source, key id, and Ed25519 private key are required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxRangeEvents == 0 {
		options.MaxRangeEvents = defaultMaxRangeEvents
	}
	if options.MaxEventBytes == 0 {
		options.MaxEventBytes = defaultMaxEventBytes
	}
	if options.MaxPayloadBytes == 0 {
		options.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	if options.MaxRangeEvents < 1 || options.MaxEventBytes < 1 || options.MaxEventBytes > maximumMaxEventBytes ||
		options.MaxPayloadBytes < 1 || options.MaxPayloadBytes > maximumMaxPayloadBytes {
		return nil, errors.New("compliance: invalid export bounds")
	}
	return &Exporter{source: source, keyID: keyID, privateKey: append(ed25519.PrivateKey(nil), privateKey...), now: options.Now,
		maxRangeEvents: options.MaxRangeEvents, maxEventBytes: options.MaxEventBytes, maxPayloadBytes: options.MaxPayloadBytes}, nil
}

// Export streams a complete range to sink. Events for other tenants are
// inspected for sequence continuity but never written to the bundle.
func (e *Exporter) Export(ctx context.Context, request Request, sink BundleSink) (SignedManifest, error) {
	if e == nil || sink == nil || !safeTenant(request.Tenant) || request.From == 0 || request.To < request.From {
		return SignedManifest{}, errors.New("compliance: tenant and a valid non-zero inclusive range are required")
	}
	span := request.To - request.From
	if span == math.MaxUint64 || span+1 > e.maxRangeEvents {
		return SignedManifest{}, ErrLimit
	}
	createdAt := e.now().UTC()
	if createdAt.UnixMilli() <= 0 {
		return SignedManifest{}, errors.New("compliance: creation clock is invalid")
	}
	var signed SignedManifest
	err := sink.Commit(ctx, func(writer io.Writer) error {
		hash := sha256.New()
		var count, first, last, payloadBytes uint64
		expected := request.From
		var scanned uint64
		_, exportErr := e.source.Export(ctx, request.From, request.To, func(event proto.Event) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if event.Seq != expected || event.Seq < request.From || event.Seq > request.To {
				return &GapError{Kind: GapIncomplete, RequestedFrom: request.From, RequestedTo: request.To, Expected: expected, Observed: event.Seq}
			}
			scanned = event.Seq
			if event.Seq < request.To {
				expected = event.Seq + 1
			}
			if event.Tenant != request.Tenant {
				return nil
			}
			line, err := eventlog.MarshalEventJSONLine(event)
			if err != nil {
				return err
			}
			if len(line) > e.maxEventBytes {
				return fmt.Errorf("%w: event %d exceeds the per-event byte bound", ErrLimit, event.Seq)
			}
			lineBytes := uint64(len(line))
			if lineBytes > uint64(e.maxPayloadBytes) || payloadBytes > uint64(e.maxPayloadBytes)-lineBytes {
				return ErrLimit
			}
			if count == math.MaxUint64 {
				return ErrLimit
			}
			if err := writeFull(writer, line); err != nil {
				return err
			}
			if _, err := hash.Write(line); err != nil {
				return err
			}
			if count == 0 {
				first = event.Seq
			}
			count++
			last, payloadBytes = event.Seq, payloadBytes+lineBytes
			return nil
		})
		if exportErr != nil {
			return normalizeGap(exportErr, request, expected)
		}
		if scanned != request.To {
			next := request.From
			if scanned != 0 {
				next = scanned + 1
			}
			return &GapError{Kind: GapIncomplete, RequestedFrom: request.From, RequestedTo: request.To, Expected: next}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		manifest := Manifest{
			Schema: bundleSchema, Tenant: request.Tenant, RangeFrom: request.From, RangeTo: request.To,
			CreatedAt: createdAt.UnixMilli(), EventCount: count, FirstSeq: first, LastSeq: last,
			PayloadBytes: payloadBytes, PayloadSHA256: hex.EncodeToString(hash.Sum(nil)), HashAlgorithm: "sha256",
			SignatureAlgorithm: "ed25519", KeyID: e.keyID,
		}
		signed = SignedManifest{Manifest: manifest, Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(e.privateKey, manifestSigningBytes(manifest)))}
		trailer, err := json.Marshal(signed)
		if err != nil {
			return err
		}
		trailer = append(trailer, '\n')
		if len(trailer) > maximumManifestBytes {
			return ErrLimit
		}
		return writeFull(writer, trailer)
	})
	if err != nil {
		return SignedManifest{}, err
	}
	return signed, nil
}

func normalizeGap(err error, request Request, expected uint64) error {
	var gap *GapError
	if errors.As(err, &gap) {
		return err
	}
	var protocol *proto.Error
	if errors.As(err, &protocol) && protocol.Code == proto.CodeEvicted {
		return &GapError{Kind: GapEvicted, RequestedFrom: request.From, RequestedTo: request.To, Expected: expected, Oldest: protocol.Oldest}
	}
	return err
}

func manifestSigningBytes(manifest Manifest) []byte {
	encoded, _ := json.Marshal(manifest)
	return append([]byte(signatureDomain), encoded...)
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func safeTenant(value string) bool { return value != "*" && safeID(value, 128) }

func safeID(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:@-", character) {
			continue
		}
		return false
	}
	return true
}
