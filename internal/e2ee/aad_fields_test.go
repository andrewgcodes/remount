package e2ee

import (
	"bytes"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// The associated data claims to bind nine things. TestAssociatedDataBindsEvery
// RoutingField exercises the frame-level ones an attacker rewrites in transit;
// this covers the remaining four, which live in the AAD encoding itself and
// have no frame field to tamper with: the protocol version, the suite, the
// direction and the session id. Changing any one of them must change the AAD,
// because an AAD that collided under two different meanings would let a record
// be reinterpreted under the other one.
func TestAssociatedDataIsInjectiveInEveryBoundField(t *testing.T) {
	sid := []byte("0123456789abcdef")
	base, err := recordAAD(dirO2A, sid, 7, proto.KindReq, "c_a", "n_b", 42)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(base, []byte(aadDomain)) {
		t.Fatal("the AAD is not domain separated, so a transcript could be read as one")
	}
	// Version and suite are the two bytes after the domain, so a downgrade to
	// either changes every record's associated data.
	if base[len(aadDomain)] != proto.Version || base[len(aadDomain)+1] != suiteID {
		t.Fatalf("the AAD does not open with the protocol version and suite: %x", base[len(aadDomain):len(aadDomain)+2])
	}

	other := []byte("0123456789abcdeF")
	variants := map[string]func() ([]byte, error){
		"direction":      func() ([]byte, error) { return recordAAD(dirA2O, sid, 7, proto.KindReq, "c_a", "n_b", 42) },
		"session id":     func() ([]byte, error) { return recordAAD(dirO2A, other, 7, proto.KindReq, "c_a", "n_b", 42) },
		"record counter": func() ([]byte, error) { return recordAAD(dirO2A, sid, 8, proto.KindReq, "c_a", "n_b", 42) },
		"frame kind":     func() ([]byte, error) { return recordAAD(dirO2A, sid, 7, proto.KindEvent, "c_a", "n_b", 42) },
		"source":         func() ([]byte, error) { return recordAAD(dirO2A, sid, 7, proto.KindReq, "c_x", "n_b", 42) },
		"destination":    func() ([]byte, error) { return recordAAD(dirO2A, sid, 7, proto.KindReq, "c_a", "n_x", 42) },
		"correlation id": func() ([]byte, error) { return recordAAD(dirO2A, sid, 7, proto.KindReq, "c_a", "n_b", 43) },
	}
	for name, variant := range variants {
		got, err := variant()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(got, base) {
			t.Fatalf("changing the %s did not change the associated data", name)
		}
	}
	// Field boundaries are unambiguous: moving a byte between the two adjacent
	// peer ids is a different AAD, not the same one.
	shifted, err := recordAAD(dirO2A, sid, 7, proto.KindReq, "c_an", "_b", 42)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(shifted, base) {
		t.Fatal("adjacent AAD fields are not separated")
	}
	// A peer id too long to length-prefix is refused rather than truncated
	// into an aliasing encoding.
	if _, err := recordAAD(dirO2A, sid, 7, proto.KindReq, string(make([]byte, 1<<16)), "n_b", 42); err == nil {
		t.Fatal("an oversized field was accepted into the AAD")
	}
}

// The session id is checked before the AEAD and the record counter inside it.
// A record moved to a session that did not agree it is refused by name, and a
// record whose counter is rewritten fails authentication rather than being
// accepted at a different position in the stream.
func TestSealedRecordIsBoundToItsSessionAndPosition(t *testing.T) {
	sender, receiver, keys := testPair(t)
	now := time.Now()
	original := &proto.Frame{V: proto.Version, T: proto.KindReq, ID: 5, To: "n_b", From: "c_a", Op: "fs.read"}

	sealed, err := sender.Seal(original, now)
	if err != nil {
		t.Fatal(err)
	}
	var body proto.E2EESealed
	if err := sealed.Decode(&body); err != nil {
		t.Fatal(err)
	}

	// Another session, same peers, same key material, different session id.
	var otherSID [sidLen]byte
	copy(otherSID[:], receiver.SID[:])
	otherSID[0] ^= 0xff
	elsewhere, err := newSession("n_b", "c_a", otherSID, false, receiver.TS, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := elsewhere.Open(sealed, now); err != ErrUnknownSession {
		t.Fatalf("a record was offered to a session that never agreed it: %v", err)
	}

	// The counter is in the associated data and in the nonce, so rewriting it
	// is a forgery, not a reposition.
	moved := &proto.Frame{V: proto.Version, T: sealed.T, ID: sealed.ID, To: sealed.To, From: sealed.From,
		Op: proto.OpE2EESealed, Body: proto.MustMarshal(proto.E2EESealed{Session: body.Session, N: body.N + 1, CT: body.CT})}
	if _, err := receiver.Open(moved, now); err == nil {
		t.Fatal("a record with a rewritten counter was accepted")
	}
	// The untouched record still opens, so the refusals above are the AAD and
	// not the record being unopenable in general.
	if _, err := receiver.Open(sealed, now); err != nil {
		t.Fatalf("the untouched record failed to open: %v", err)
	}
}
