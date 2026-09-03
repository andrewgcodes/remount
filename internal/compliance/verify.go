package compliance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"
)

const maximumManifestBytes = 64 << 10

// PublicKeyResolver returns the trusted verification key for a manifest key
// ID. A bundle never self-authorizes by embedding its own public key.
type PublicKeyResolver interface {
	ResolvePublicKey(context.Context, string) (ed25519.PublicKey, error)
}

// Keyring is an immutable in-memory PublicKeyResolver.
type Keyring struct {
	mu   sync.RWMutex
	keys map[string]ed25519.PublicKey
}

// NewKeyring validates and copies trusted Ed25519 public keys.
func NewKeyring(keys map[string]ed25519.PublicKey) (*Keyring, error) {
	if len(keys) == 0 || len(keys) > 1024 {
		return nil, errors.New("compliance: keyring must contain 1 to 1024 keys")
	}
	copyKeys := make(map[string]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if !safeID(id, 128) || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("compliance: invalid verification key")
		}
		copyKeys[id] = append(ed25519.PublicKey(nil), key...)
	}
	return &Keyring{keys: copyKeys}, nil
}

// ResolvePublicKey implements PublicKeyResolver and returns a defensive copy.
func (k *Keyring) ResolvePublicKey(ctx context.Context, id string) (ed25519.PublicKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if k == nil {
		return nil, errors.New("compliance: verification key unavailable")
	}
	k.mu.RLock()
	key, ok := k.keys[id]
	k.mu.RUnlock()
	if !ok {
		return nil, errors.New("compliance: verification key unavailable")
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

// VerifyOptions bound the verifier independently of the producing process.
type VerifyOptions struct {
	ExpectedTenant string
	MaxEvents      uint64
	MaxEventBytes  int
	MaxBundleBytes int64
}

func (o VerifyOptions) normalized() (VerifyOptions, error) {
	if o.ExpectedTenant != "" && !safeTenant(o.ExpectedTenant) {
		return o, ErrTampered
	}
	if o.MaxEvents == 0 {
		o.MaxEvents = defaultMaxRangeEvents
	}
	if o.MaxEventBytes == 0 {
		o.MaxEventBytes = defaultMaxEventBytes
	}
	if o.MaxBundleBytes == 0 {
		o.MaxBundleBytes = defaultMaxPayloadBytes + maximumManifestBytes
	}
	if o.MaxEvents < 1 || o.MaxEventBytes < 1 || o.MaxEventBytes > maximumMaxEventBytes || o.MaxBundleBytes < 1 || o.MaxBundleBytes > maximumMaxFileBytes {
		return o, errors.New("compliance: invalid verifier bounds")
	}
	return o, nil
}

// Verify reads and authenticates a complete bundle without retaining its
// payload. It verifies every event's tenant/range/order and the manifest's
// count, endpoints, byte length, hash, canonical encoding, and signature.
func Verify(ctx context.Context, reader io.Reader, keys PublicKeyResolver, options VerifyOptions) (Manifest, error) {
	if reader == nil || keys == nil {
		return Manifest{}, errors.New("compliance: bundle and trusted keys are required")
	}
	opts, err := options.normalized()
	if err != nil {
		return Manifest{}, err
	}
	buffered := bufio.NewReaderSize(io.LimitReader(reader, opts.MaxBundleBytes+1), 64<<10)
	hash := sha256.New()
	var count, first, last, payloadBytes uint64
	var totalBytes int64
	var observedTenant string
	var tenantMismatch bool
	var signed SignedManifest
	manifestFound := false
	for {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		line, readErr := readLineLimited(buffered, max(opts.MaxEventBytes, maximumManifestBytes))
		if errors.Is(readErr, io.EOF) && len(line) == 0 {
			break
		}
		if readErr != nil {
			return Manifest{}, errors.Join(ErrTampered, readErr)
		}
		if len(line) == 0 || line[len(line)-1] != '\n' {
			return Manifest{}, errors.Join(ErrTampered, errors.New("unterminated bundle line"))
		}
		if int64(len(line)) > opts.MaxBundleBytes-totalBytes {
			return Manifest{}, ErrLimit
		}
		totalBytes += int64(len(line))
		if isManifestLine(line) {
			if manifestFound || len(line) > maximumManifestBytes {
				return Manifest{}, ErrTampered
			}
			if err := decodeStrict(line, &signed); err != nil {
				return Manifest{}, errors.Join(ErrTampered, err)
			}
			canonical, err := json.Marshal(signed)
			if err != nil || !bytes.Equal(append(canonical, '\n'), line) {
				return Manifest{}, errors.Join(ErrTampered, errors.New("non-canonical manifest"))
			}
			manifestFound = true
			continue
		}
		if manifestFound {
			return Manifest{}, errors.Join(ErrTampered, errors.New("data follows signed manifest"))
		}
		if len(line) > opts.MaxEventBytes || count >= opts.MaxEvents {
			return Manifest{}, ErrLimit
		}
		var event auditEvent
		if err := decodeStrict(line, &event); err != nil || event.Seq == 0 || event.Type == "" {
			return Manifest{}, errors.Join(ErrTampered, err)
		}
		canonical, err := json.Marshal(event)
		if err != nil || !bytes.Equal(append(canonical, '\n'), line) {
			return Manifest{}, errors.Join(ErrTampered, errors.New("event line is not canonical"))
		}
		if count > 0 && event.Seq <= last {
			return Manifest{}, errors.Join(ErrTampered, errors.New("event order is not strictly increasing"))
		}
		if count == 0 {
			observedTenant = event.Tenant
		} else if event.Tenant != observedTenant {
			tenantMismatch = true
		}
		if _, err := hash.Write(line); err != nil {
			return Manifest{}, err
		}
		if count == 0 {
			first = event.Seq
		}
		if count == math.MaxUint64 {
			return Manifest{}, ErrLimit
		}
		count++
		last, payloadBytes = event.Seq, payloadBytes+uint64(len(line))
	}
	if !manifestFound {
		return Manifest{}, errors.Join(ErrTampered, errors.New("signed manifest is missing"))
	}
	manifest := signed.Manifest
	if err := validateManifest(manifest, options.ExpectedTenant); err != nil {
		return Manifest{}, err
	}
	if count != manifest.EventCount || first != manifest.FirstSeq || last != manifest.LastSeq || payloadBytes != manifest.PayloadBytes ||
		hex.EncodeToString(hash.Sum(nil)) != manifest.PayloadSHA256 {
		return Manifest{}, errors.Join(ErrTampered, errors.New("manifest summary does not match payload"))
	}
	// Check event tenancy/ranges in a second bounded-memory pass is impossible
	// on a stream, so those properties are accumulated in auditEvent below and
	// finalized through the tracker populated during the first pass.
	if eventValidationErr := validateObservedEvents(manifest, observedTenant, tenantMismatch); eventValidationErr != nil {
		return Manifest{}, eventValidationErr
	}
	key, err := keys.ResolvePublicKey(ctx, manifest.KeyID)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return Manifest{}, errors.Join(ErrTampered, errors.New("trusted verification key unavailable"))
	}
	signature, err := base64.RawURLEncoding.DecodeString(signed.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(key, manifestSigningBytes(manifest), signature) {
		return Manifest{}, errors.Join(ErrTampered, errors.New("manifest signature is invalid"))
	}
	return manifest, nil
}

// auditEvent mirrors the canonical eventlog JSON fields while leaving payload
// opaque. Verification never rewrites or interprets payload content.
type auditEvent struct {
	Seq         uint64          `json:"seq"`
	At          int64           `json:"at"`
	Stream      string          `json:"stream,omitempty"`
	Principal   string          `json:"principal,omitempty"`
	Node        string          `json:"node,omitempty"`
	EventID     string          `json:"event_id,omitempty"`
	ReceivedAt  int64           `json:"received_at,omitempty"`
	ObservedAt  int64           `json:"observed_at,omitempty"`
	Origin      string          `json:"origin,omitempty"`
	Actor       string          `json:"actor,omitempty"`
	Tenant      string          `json:"tenant,omitempty"`
	Workspace   string          `json:"workspace,omitempty"`
	Generation  uint64          `json:"generation,omitempty"`
	Session     string          `json:"session,omitempty"`
	OperationID string          `json:"operation_id,omitempty"`
	ProducerSeq uint64          `json:"producer_seq,omitempty"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Cause       uint64          `json:"cause,omitempty"`
}

func validateObservedEvents(manifest Manifest, tenant string, mismatch bool) error {
	if mismatch || (manifest.EventCount > 0 && tenant != manifest.Tenant) {
		return errors.Join(ErrTampered, errors.New("payload tenant or range does not match manifest"))
	}
	return nil
}

func validateManifest(manifest Manifest, expectedTenant string) error {
	if manifest.Schema != bundleSchema || !safeTenant(manifest.Tenant) || (expectedTenant != "" && manifest.Tenant != expectedTenant) ||
		manifest.RangeFrom == 0 || manifest.RangeTo < manifest.RangeFrom || manifest.CreatedAt <= 0 || manifest.HashAlgorithm != "sha256" ||
		manifest.SignatureAlgorithm != "ed25519" || !safeID(manifest.KeyID, 128) || len(manifest.PayloadSHA256) != sha256.Size*2 {
		return errors.Join(ErrTampered, errors.New("manifest fields are invalid"))
	}
	if manifest.EventCount == 0 {
		if manifest.FirstSeq != 0 || manifest.LastSeq != 0 || manifest.PayloadBytes != 0 {
			return errors.Join(ErrTampered, errors.New("empty manifest has event metadata"))
		}
	} else if manifest.FirstSeq < manifest.RangeFrom || manifest.LastSeq > manifest.RangeTo || manifest.FirstSeq > manifest.LastSeq || manifest.PayloadBytes == 0 {
		return errors.Join(ErrTampered, errors.New("manifest event range is invalid"))
	}
	if _, err := hex.DecodeString(manifest.PayloadSHA256); err != nil {
		return errors.Join(ErrTampered, errors.New("manifest hash is invalid"))
	}
	return nil
}

func isManifestLine(line []byte) bool {
	// Canonical event lines always start with {"seq":. Requiring the canonical
	// manifest prefix avoids building an attacker-controlled probe map merely
	// to decide which strict decoder should handle the bounded line.
	return bytes.HasPrefix(line, []byte(`{"remount_audit_manifest":`))
}

func decodeStrict(line []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("line contains trailing JSON")
	}
	return nil
}

func readLineLimited(reader *bufio.Reader, maximum int) ([]byte, error) {
	line := make([]byte, 0, min(maximum, 64<<10))
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > maximum {
			return nil, ErrLimit
		}
		line = append(line, fragment...)
		if err == nil {
			return line, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

var _ PublicKeyResolver = (*Keyring)(nil)
