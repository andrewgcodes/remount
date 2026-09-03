package encrypted

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"

	"remount.dev/remount/internal/artifact"
)

const (
	formatVersion = uint16(1)
	maxChunkSize  = 16 << 20
	fixedHeader   = 8 + 2 + 2 + 4 + 8 + 2 + 2 + sha256.Size + 8 + 12 + 48
	gcmTagSize    = 16
)

var formatMagic = [8]byte{'R', 'M', 'A', 'R', 'T', 'E', 'N', 'C'}

type objectHeader struct {
	Version       uint16
	ChunkSize     uint32
	PlaintextSize uint64
	Tenant        string
	KeyVersion    string
	Digest        [sha256.Size]byte
	NoncePrefix   [8]byte
	WrapNonce     [12]byte
	WrappedKey    [48]byte
}

func newEncryptReader(plaintext io.Reader, tenant, keyVersion, id string, plaintextSize int64, chunkSize int, tenantKey [32]byte) (io.Reader, int64, error) {
	if plaintextSize < 0 || chunkSize <= 0 || chunkSize > maxChunkSize {
		return nil, 0, fmt.Errorf("%w", ErrMalformed)
	}
	digest, err := decodeID(id)
	if err != nil {
		return nil, 0, err
	}
	dataKey, err := random32()
	if err != nil {
		return nil, 0, err
	}
	defer erase32(&dataKey)
	header := objectHeader{
		Version: formatVersion, ChunkSize: uint32(chunkSize), PlaintextSize: uint64(plaintextSize),
		Tenant: tenant, KeyVersion: keyVersion, Digest: digest,
	}
	if _, err := io.ReadFull(rand.Reader, header.NoncePrefix[:]); err != nil {
		return nil, 0, err
	}
	if _, err := io.ReadFull(rand.Reader, header.WrapNonce[:]); err != nil {
		return nil, 0, err
	}
	wrapped, err := wrapDataKey(header, tenant, keyVersion, id, tenantKey, dataKey)
	if err != nil {
		return nil, 0, err
	}
	copy(header.WrappedKey[:], wrapped)
	headerBytes, err := marshalHeader(header)
	if err != nil {
		return nil, 0, err
	}
	aead, err := newGCM(dataKey[:])
	if err != nil {
		return nil, 0, err
	}
	dataAAD := immutableAAD(header, tenant, id)
	body := &encryptBodyReader{
		plaintext: plaintext, aead: aead, aad: dataAAD,
		noncePrefix: header.NoncePrefix, chunkSize: chunkSize, remaining: plaintextSize,
	}
	encryptedSize, err := objectSize(len(headerBytes), plaintextSize, int64(chunkSize))
	if err != nil {
		return nil, 0, err
	}
	return io.MultiReader(bytes.NewReader(headerBytes), body), encryptedSize, nil
}

type encryptBodyReader struct {
	plaintext   io.Reader
	aead        cipher.AEAD
	aad         []byte
	noncePrefix [8]byte
	chunkSize   int
	remaining   int64
	chunk       uint32
	pending     []byte
	done        bool
}

func (r *encryptBodyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 && !r.done {
		if r.remaining == 0 {
			var probe [1]byte
			n, err := r.plaintext.Read(probe[:])
			if n != 0 || (err != nil && !errors.Is(err, io.EOF)) {
				return 0, fmt.Errorf("encrypted artifact: plaintext size changed during encryption")
			}
			r.done = true
		} else {
			plainSize := int64(r.chunkSize)
			if plainSize > r.remaining {
				plainSize = r.remaining
			}
			plain := make([]byte, int(plainSize))
			if _, err := io.ReadFull(r.plaintext, plain); err != nil {
				return 0, fmt.Errorf("encrypted artifact: plaintext staging read failed: %w", err)
			}
			nonce := chunkNonce(r.noncePrefix, r.chunk)
			aad := chunkAAD(r.aad, r.chunk, uint32(plainSize))
			r.pending = r.aead.Seal(nil, nonce[:], plain, aad)
			clear(plain)
			r.remaining -= plainSize
			r.chunk++
		}
	}
	if len(r.pending) == 0 && r.done {
		return 0, io.EOF
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

type decryptReader struct {
	ctx         context.Context
	raw         io.ReadCloser
	aead        cipher.AEAD
	aad         []byte
	noncePrefix [8]byte
	chunkSize   int64
	remaining   int64
	chunk       uint32
	pending     []byte
	hash        hashWriter
	expected    [sha256.Size]byte
	verified    bool
	terminalErr error
	closed      bool
}

type hashWriter struct{ h hash.Hash }

func (w *hashWriter) add(p []byte) {
	if w.h == nil {
		w.h = sha256.New()
	}
	_, _ = w.h.Write(p)
}

func (w *hashWriter) sum() [sha256.Size]byte {
	if w.h == nil {
		return sha256.Sum256(nil)
	}
	var out [sha256.Size]byte
	copy(out[:], w.h.Sum(nil))
	return out
}

func newDecryptReader(ctx context.Context, raw io.ReadCloser, rawSize int64, tenant, keyVersion, id string, keys KeyProvider) (*decryptReader, int64, error) {
	header, _, err := readHeader(raw, rawSize, tenant, keyVersion, id)
	if err != nil {
		return nil, 0, err
	}
	tenantKey, err := keys.Get(ctx, tenant, keyVersion)
	if err != nil {
		return nil, 0, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	dataKey, err := unwrapDataKey(header, tenant, keyVersion, id, tenantKey)
	erase32(&tenantKey)
	if err != nil {
		return nil, 0, err
	}
	aead, err := newGCM(dataKey[:])
	erase32(&dataKey)
	if err != nil {
		return nil, 0, err
	}
	return &decryptReader{
		ctx: ctx, raw: raw, aead: aead, aad: immutableAAD(header, tenant, id),
		noncePrefix: header.NoncePrefix, chunkSize: int64(header.ChunkSize),
		remaining: int64(header.PlaintextSize), expected: header.Digest,
	}, int64(header.PlaintextSize), nil
}

func (r *decryptReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.terminalErr != nil {
		return 0, r.terminalErr
	}
	if len(r.pending) == 0 && !r.verified {
		if err := r.ctx.Err(); err != nil {
			r.terminalErr = err
			return 0, err
		}
		if r.remaining == 0 {
			var trailing [1]byte
			n, err := r.raw.Read(trailing[:])
			if n != 0 || (err != nil && !errors.Is(err, io.EOF)) {
				r.terminalErr = fmt.Errorf("%w", ErrMalformed)
				return 0, r.terminalErr
			}
			if r.hash.sum() != r.expected {
				r.terminalErr = fmt.Errorf("%w", ErrIntegrity)
				return 0, r.terminalErr
			}
			r.verified = true
			return 0, io.EOF
		}
		plainSize := r.chunkSize
		if plainSize > r.remaining {
			plainSize = r.remaining
		}
		ciphertext := make([]byte, plainSize+gcmTagSize)
		if _, err := io.ReadFull(r.raw, ciphertext); err != nil {
			r.terminalErr = fmt.Errorf("%w", ErrIntegrity)
			return 0, r.terminalErr
		}
		nonce := chunkNonce(r.noncePrefix, r.chunk)
		aad := chunkAAD(r.aad, r.chunk, uint32(plainSize))
		plain, err := r.aead.Open(nil, nonce[:], ciphertext, aad)
		clear(ciphertext)
		if err != nil {
			r.terminalErr = fmt.Errorf("%w", ErrIntegrity)
			return 0, r.terminalErr
		}
		r.hash.add(plain)
		r.pending = plain
		r.remaining -= plainSize
		r.chunk++
	}
	if len(r.pending) == 0 && r.verified {
		return 0, io.EOF
	}
	n := copy(p, r.pending)
	clear(r.pending[:n])
	r.pending = r.pending[n:]
	return n, nil
}

func (r *decryptReader) Close() error {
	if r.closed {
		return r.terminalErr
	}
	r.closed = true
	var verifyErr error
	if !r.verified && r.terminalErr == nil {
		_, verifyErr = io.Copy(io.Discard, r)
	}
	closeErr := r.raw.Close()
	if r.terminalErr != nil {
		verifyErr = errors.Join(verifyErr, r.terminalErr)
	}
	return errors.Join(verifyErr, closeErr)
}

func marshalHeader(header objectHeader) ([]byte, error) {
	if header.Version != formatVersion || header.ChunkSize == 0 || header.ChunkSize > maxChunkSize {
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	if err := validateSegment("tenant", header.Tenant); err != nil {
		return nil, err
	}
	if err := validateSegment("key version", header.KeyVersion); err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, fixedHeader+len(header.Tenant)+len(header.KeyVersion)))
	buf.Write(formatMagic[:])
	_ = binary.Write(buf, binary.BigEndian, header.Version)
	_ = binary.Write(buf, binary.BigEndian, uint16(0))
	_ = binary.Write(buf, binary.BigEndian, header.ChunkSize)
	_ = binary.Write(buf, binary.BigEndian, header.PlaintextSize)
	_ = binary.Write(buf, binary.BigEndian, uint16(len(header.Tenant)))
	_ = binary.Write(buf, binary.BigEndian, uint16(len(header.KeyVersion)))
	buf.Write(header.Digest[:])
	buf.Write(header.NoncePrefix[:])
	buf.Write(header.WrapNonce[:])
	buf.Write(header.WrappedKey[:])
	buf.WriteString(header.Tenant)
	buf.WriteString(header.KeyVersion)
	return buf.Bytes(), nil
}

func readHeader(r io.Reader, rawSize int64, tenant, keyVersion, id string) (objectHeader, []byte, error) {
	var header objectHeader
	if rawSize < fixedHeader {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	fixed := make([]byte, fixedHeader)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	if !bytes.Equal(fixed[:8], formatMagic[:]) {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	header.Version = binary.BigEndian.Uint16(fixed[8:10])
	flags := binary.BigEndian.Uint16(fixed[10:12])
	header.ChunkSize = binary.BigEndian.Uint32(fixed[12:16])
	header.PlaintextSize = binary.BigEndian.Uint64(fixed[16:24])
	tenantLen := int(binary.BigEndian.Uint16(fixed[24:26]))
	versionLen := int(binary.BigEndian.Uint16(fixed[26:28]))
	copy(header.Digest[:], fixed[28:60])
	copy(header.NoncePrefix[:], fixed[60:68])
	copy(header.WrapNonce[:], fixed[68:80])
	copy(header.WrappedKey[:], fixed[80:128])
	const maxInt64 = uint64(^uint64(0) >> 1)
	if header.Version != formatVersion || flags != 0 || header.ChunkSize == 0 || header.ChunkSize > maxChunkSize || header.PlaintextSize > maxInt64 || tenantLen == 0 || tenantLen > 128 || versionLen == 0 || versionLen > 128 {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	variable := make([]byte, tenantLen+versionLen)
	if _, err := io.ReadFull(r, variable); err != nil {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	header.Tenant = string(variable[:tenantLen])
	header.KeyVersion = string(variable[tenantLen:])
	if header.Tenant != tenant || header.KeyVersion != keyVersion {
		return header, nil, fmt.Errorf("%w", ErrIntegrity)
	}
	digest, err := decodeID(id)
	if err != nil || digest != header.Digest {
		return header, nil, fmt.Errorf("%w", ErrIntegrity)
	}
	headerBytes := append(fixed, variable...)
	expectedSize, err := objectSize(len(headerBytes), int64(header.PlaintextSize), int64(header.ChunkSize))
	if err != nil || expectedSize != rawSize {
		return header, nil, fmt.Errorf("%w", ErrMalformed)
	}
	return header, headerBytes, nil
}

func rewrapHeader(header objectHeader, tenant, newVersion, id string, tenantKey, dataKey [32]byte) ([]byte, error) {
	header.KeyVersion = newVersion
	if _, err := io.ReadFull(rand.Reader, header.WrapNonce[:]); err != nil {
		return nil, err
	}
	wrapped, err := wrapDataKey(header, tenant, newVersion, id, tenantKey, dataKey)
	if err != nil {
		return nil, err
	}
	copy(header.WrappedKey[:], wrapped)
	return marshalHeader(header)
}

func wrapDataKey(header objectHeader, tenant, keyVersion, id string, tenantKey, dataKey [32]byte) ([]byte, error) {
	aead, err := newGCM(tenantKey[:])
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, header.WrapNonce[:], dataKey[:], wrappingAAD(header, tenant, keyVersion, id)), nil
}

func unwrapDataKey(header objectHeader, tenant, keyVersion, id string, tenantKey [32]byte) ([32]byte, error) {
	var out [32]byte
	aead, err := newGCM(tenantKey[:])
	if err != nil {
		return out, err
	}
	plain, err := aead.Open(nil, header.WrapNonce[:], header.WrappedKey[:], wrappingAAD(header, tenant, keyVersion, id))
	if err != nil || len(plain) != len(out) {
		clear(plain)
		return out, fmt.Errorf("%w", ErrIntegrity)
	}
	copy(out[:], plain)
	clear(plain)
	return out, nil
}

func immutableAAD(header objectHeader, tenant, id string) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("remount/encrypted-artifact/data/v1\x00")
	_ = binary.Write(buf, binary.BigEndian, header.Version)
	_ = binary.Write(buf, binary.BigEndian, header.ChunkSize)
	_ = binary.Write(buf, binary.BigEndian, header.PlaintextSize)
	writeAADString(buf, tenant)
	writeAADString(buf, id)
	buf.Write(header.NoncePrefix[:])
	return buf.Bytes()
}

func wrappingAAD(header objectHeader, tenant, keyVersion, id string) []byte {
	buf := bytes.NewBuffer(immutableAAD(header, tenant, id))
	buf.WriteString("\x00remount/encrypted-artifact/wrap/v1\x00")
	writeAADString(buf, keyVersion)
	return buf.Bytes()
}

func chunkAAD(base []byte, chunk, plainSize uint32) []byte {
	digest := sha256.Sum256(base)
	out := make([]byte, len(digest)+8)
	copy(out, digest[:])
	binary.BigEndian.PutUint32(out[len(digest):], chunk)
	binary.BigEndian.PutUint32(out[len(digest)+4:], plainSize)
	return out
}

func chunkNonce(prefix [8]byte, chunk uint32) [12]byte {
	var nonce [12]byte
	copy(nonce[:8], prefix[:])
	binary.BigEndian.PutUint32(nonce[8:], chunk)
	return nonce
}

func objectSize(headerLen int, plaintextSize, chunkSize int64) (int64, error) {
	if headerLen < fixedHeader || plaintextSize < 0 || chunkSize <= 0 {
		return 0, fmt.Errorf("%w", ErrMalformed)
	}
	chunks := plaintextSize / chunkSize
	if plaintextSize%chunkSize != 0 {
		chunks++
	}
	if chunks > int64(^uint32(0)) {
		return 0, fmt.Errorf("%w: too many encryption chunks", ErrMalformed)
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if chunks > (maxInt64-int64(headerLen)-plaintextSize)/gcmTagSize {
		return 0, fmt.Errorf("%w", ErrMalformed)
	}
	return int64(headerLen) + plaintextSize + chunks*gcmTagSize, nil
}

func decodeID(id string) ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	digest, err := artifact.Digest(id)
	if err != nil {
		return out, err
	}
	decoded := make([]byte, sha256.Size)
	for i := 0; i < len(decoded); i++ {
		hi, ok1 := fromHex(digest[i*2])
		lo, ok2 := fromHex(digest[i*2+1])
		if !ok1 || !ok2 {
			return out, fmt.Errorf("%w", ErrMalformed)
		}
		decoded[i] = hi<<4 | lo
	}
	copy(out[:], decoded)
	return out, nil
}

func fromHex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	default:
		return 0, false
	}
}

func writeAADString(buf *bytes.Buffer, value string) {
	_ = binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.WriteString(value)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
