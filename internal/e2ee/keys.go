package e2ee

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"

	"remount.dev/remount/internal/proto"
)

// Domain separation. Every hash and every signature this package produces
// starts with one of these, so a transcript can never be read as an AAD and
// an offer signature can never be replayed as an accept signature.
const (
	transcriptDomain = "remount/e2ee/v1 transcript\x00"
	offerSigDomain   = "remount/e2ee/v1 sig offer\x00"
	acceptSigDomain  = "remount/e2ee/v1 sig accept\x00"
	aadDomain        = "remount/e2ee/v1 aad\x00"
	keyLabelO2A      = "remount/e2ee/v1 key offerer-to-accepter"
	keyLabelA2O      = "remount/e2ee/v1 key accepter-to-offerer"
	ivLabelO2A       = "remount/e2ee/v1 iv offerer-to-accepter"
	ivLabelA2O       = "remount/e2ee/v1 iv accepter-to-offerer"
)

// suiteID is the one byte that binds a ciphertext to the suite that produced
// it, so a future suite cannot be substituted for this one under a key both
// could derive.
const suiteID byte = 1

// Suite parameters.
const (
	sidLen   = 16
	nonceLen = 12
	keyLen   = 32
)

// dirO2A and dirA2O give each direction its own key and its own place in the
// AAD, so a record can never be reflected back at its sender.
const (
	dirO2A byte = 0
	dirA2O byte = 1
)

// maxRecord bounds the per-key record counter far below the 2^64 the nonce
// could express. Sealing stops here rather than wrapping, which is what makes
// nonce reuse under one key structurally impossible.
const maxRecord uint64 = 1 << 48

// errFieldTooLong guards the 16-bit length prefix used in transcripts and
// AADs. Every field is short by construction; a value that is not comes from
// a hostile relay and must fail rather than alias another encoding.
var errFieldTooLong = errors.New("e2ee: transcript field too long")

// writeField appends a length-prefixed field. Length prefixing every variable
// field is what makes the transcript and the AAD injective: no concatenation
// of one set of values can equal the concatenation of another.
func writeField(h hash.Hash, b []byte) error {
	if len(b) > 0xffff {
		return errFieldTooLong
	}
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(b)))
	h.Write(n[:])
	h.Write(b)
	return nil
}

func appendField(dst []byte, b []byte) ([]byte, error) {
	if len(b) > 0xffff {
		return nil, errFieldTooLong
	}
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(b)))
	dst = append(dst, n[:]...)
	return append(dst, b...), nil
}

func be64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

// transcript hashes the key agreement. The offer half passes nil for the
// accepter's contribution; because every field is length-prefixed, the half
// transcript is not a prefix of any full transcript with different content.
//
// Both peer ids are in it, so a captured exchange cannot be presented as an
// exchange with someone else, and the protocol version and suite are in it,
// so a downgrade changes every derived key.
func transcript(offerer, accepter string, sid, offerEph, offerNonce []byte, ts int64, acceptEph, acceptNonce []byte) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(transcriptDomain))
	for _, field := range [][]byte{
		{proto.Version, suiteID},
		[]byte(proto.E2EESuiteX25519),
		[]byte(offerer),
		[]byte(accepter),
		sid,
		offerEph,
		offerNonce,
		be64(uint64(ts)),
		acceptEph,
		acceptNonce,
	} {
		if err := writeField(h, field); err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

// signedOffer and signedAccept are what each side's long-term identity signs.
// The offerer signs its own half plus the peer it intends to reach; the
// accepter signs the whole transcript. Together they authenticate both
// ephemeral keys to both parties.
func signedOffer(t []byte) []byte  { return append([]byte(offerSigDomain), t...) }
func signedAccept(t []byte) []byte { return append([]byte(acceptSigDomain), t...) }

// directionKeys derives both record keys and both nonce prefixes from the
// agreed secret, salted with the transcript hash. Every input either side
// contributed is therefore mixed into every key.
type directionKeys struct {
	o2aKey, a2oKey []byte
	o2aIV, a2oIV   []byte
}

func deriveKeys(shared, transcriptHash []byte) (directionKeys, error) {
	prk, err := hkdf.Extract(sha256.New, shared, transcriptHash)
	if err != nil {
		return directionKeys{}, err
	}
	var out directionKeys
	for _, field := range []struct {
		dst   *[]byte
		label string
		size  int
	}{
		{&out.o2aKey, keyLabelO2A, keyLen},
		{&out.a2oKey, keyLabelA2O, keyLen},
		{&out.o2aIV, ivLabelO2A, nonceLen},
		{&out.a2oIV, ivLabelA2O, nonceLen},
	} {
		value, err := hkdf.Expand(sha256.New, prk, field.label, field.size)
		if err != nil {
			return directionKeys{}, err
		}
		*field.dst = value
	}
	return out, nil
}

// agree performs the X25519 agreement.
func agree(priv *ecdh.PrivateKey, peer []byte) ([]byte, error) {
	pub, err := ecdh.X25519().NewPublicKey(peer)
	if err != nil {
		return nil, err
	}
	return priv.ECDH(pub)
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// nonce is the per-direction prefix XORed with the record counter. Uniqueness
// comes from the counter, which is strictly increasing under one key and
// never wraps (see maxRecord); the prefix only makes nonces unpredictable.
func nonce(prefix []byte, n uint64) [nonceLen]byte {
	var out [nonceLen]byte
	copy(out[:], prefix)
	counter := be64(n)
	for i := 0; i < 8; i++ {
		out[nonceLen-8+i] ^= counter[i]
	}
	return out
}

// recordAAD is the associated data every sealed record authenticates. It is
// the whole point of the design: the ciphertext is bound to the protocol
// version, the cipher suite, the direction, the session the key came from,
// the record counter, the frame kind, the source and destination peer ids and
// the request correlation id.
//
// A record moved to another destination, reflected back at its sender,
// re-used under another correlation id, or replayed at another position fails
// authentication here rather than reaching an operation handler.
func recordAAD(dir byte, sid []byte, n uint64, kind, src, dst string, id uint64) ([]byte, error) {
	out := make([]byte, 0, len(aadDomain)+96)
	out = append(out, aadDomain...)
	out = append(out, proto.Version, suiteID, dir)
	var err error
	for _, field := range [][]byte{
		sid,
		be64(n),
		be64(id),
		[]byte(kind),
		[]byte(src),
		[]byte(dst),
	} {
		if out, err = appendField(out, field); err != nil {
			return nil, err
		}
	}
	return out, nil
}
