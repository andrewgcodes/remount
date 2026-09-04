package e2ee

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

func testKeys(t *testing.T) directionKeys {
	t.Helper()
	shared := make([]byte, 32)
	if _, err := rand.Read(shared); err != nil {
		t.Fatal(err)
	}
	keys, err := deriveKeys(shared, []byte("transcript"))
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func testPair(t *testing.T) (*Session, *Session, directionKeys) {
	t.Helper()
	keys := testKeys(t)
	var sid [sidLen]byte
	if _, err := rand.Read(sid[:]); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	offerer, err := newSession("c_a", "n_b", sid, true, now.Unix(), keys, now)
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := newSession("n_b", "c_a", sid, false, now.Unix(), keys, now)
	if err != nil {
		t.Fatal(err)
	}
	return offerer, accepter, keys
}

// Nonce reuse under one key must be impossible by construction, not unlikely.
// The counter is assigned under the same mutex that guards the AEAD, so
// concurrent sealing produces a contiguous run with no value used twice.
func TestRecordCountersAreUniqueUnderConcurrency(t *testing.T) {
	sender, _, _ := testPair(t)
	const workers, each = 8, 250
	seen := make(map[uint64]bool, workers*each)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				sealed, err := sender.Seal(&proto.Frame{V: proto.Version, T: proto.KindReq, ID: 1, To: "n_b", Op: "fs.read"}, time.Now())
				if err != nil {
					t.Error(err)
					return
				}
				var body proto.E2EESealed
				if err := sealed.Decode(&body); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if seen[body.N] {
					t.Errorf("record counter %d used twice", body.N)
				}
				seen[body.N] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != workers*each {
		t.Fatalf("%d distinct counters for %d records", len(seen), workers*each)
	}
	for n := uint64(1); n <= uint64(workers*each); n++ {
		if !seen[n] {
			t.Fatalf("counter %d was skipped; the run is not contiguous", n)
		}
	}
}

// The counter never wraps. At the bound the session refuses rather than
// reusing a nonce, which is the property that makes reuse structural.
func TestSealRefusesRatherThanReusingANonce(t *testing.T) {
	sender, _, _ := testPair(t)
	sender.sendSeq = maxRecord
	if _, err := sender.Seal(&proto.Frame{V: proto.Version, T: proto.KindReq, ID: 1, To: "n_b"}, time.Now()); !errors.Is(err, ErrRekeyRequired) {
		t.Fatalf("err = %v, want ErrRekeyRequired", err)
	}
}

// Distinct counters produce distinct nonces, so the counter's uniqueness is
// the nonce's uniqueness.
func TestNonceIsInjectiveInTheCounter(t *testing.T) {
	prefix := make([]byte, nonceLen)
	if _, err := rand.Read(prefix); err != nil {
		t.Fatal(err)
	}
	seen := map[[nonceLen]byte]uint64{}
	for _, n := range []uint64{1, 2, 3, 255, 256, 1 << 16, 1<<32 + 7, maxRecord - 1} {
		iv := nonce(prefix, n)
		if prior, dup := seen[iv]; dup {
			t.Fatalf("counters %d and %d produced the same nonce", prior, n)
		}
		seen[iv] = n
	}
}

// The replay window tolerates the reordering a relay may impose and refuses
// every duplicate it can still see, plus everything older than it remembers.
func TestReplayWindow(t *testing.T) {
	_, receiver, _ := testPair(t)
	for _, n := range []uint64{1, 2, 3} {
		if !receiver.windowAllows(n) {
			t.Fatalf("fresh record %d refused", n)
		}
		receiver.windowAccept(n)
	}
	if receiver.windowAllows(2) {
		t.Fatal("a duplicate was allowed")
	}
	// A gap arrives late: still accepted, and only once.
	receiver.windowAccept(10)
	for _, n := range []uint64{4, 5, 9} {
		if !receiver.windowAllows(n) {
			t.Fatalf("reordered record %d refused", n)
		}
		receiver.windowAccept(n)
		if receiver.windowAllows(n) {
			t.Fatalf("record %d accepted twice", n)
		}
	}
	// Beyond the window there is no way to tell a replay from a very late
	// record, so it is refused rather than guessed at.
	receiver.windowAccept(200)
	if receiver.windowAllows(100) {
		t.Fatal("a record older than the window was allowed")
	}
	if receiver.windowAllows(0) {
		t.Fatal("counter zero was allowed")
	}
}

// A record that authenticates is delivered; a record whose associated data
// disagrees with the frame it arrived in is not. This is the binding the
// whole design rests on, checked field by field.
func TestAssociatedDataBindsEveryRoutingField(t *testing.T) {
	sender, receiver, keys := testPair(t)
	now := time.Now()
	original := &proto.Frame{V: proto.Version, T: proto.KindReq, ID: 42, To: "n_b", From: "c_a", Op: "fs.write", WS: "w_1", Body: []byte("secret")}
	sealed, err := sender.Seal(original, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Open(sealed, now); err != nil {
		t.Fatalf("an untouched record failed to open: %v", err)
	}

	// Each attempt reseals, so the replay window is never what refuses it:
	// the record fails because its associated data no longer describes the
	// frame it arrived in.
	for name, tamper := range map[string]func(f *proto.Frame){
		"kind":           func(f *proto.Frame) { f.T = proto.KindEvent },
		"correlation id": func(f *proto.Frame) { f.ID = 43 },
		"ciphertext":     func(f *proto.Frame) { f.Body[len(f.Body)-1] ^= 1 },
	} {
		again, err := sender.Seal(original, now)
		if err != nil {
			t.Fatal(err)
		}
		tamper(again)
		if _, err := receiver.Open(again, now); err == nil {
			t.Fatalf("a record with a rewritten %s was accepted", name)
		}
	}

	// Source and destination live in the session, so a record opened by a
	// session between different peers fails on identical key material.
	crossed, err := newSession("n_other", "c_a", receiver.SID, false, receiver.TS, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := sender.Seal(original, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crossed.Open(again, now); err == nil {
		t.Fatal("a record was opened by a session between different peers")
	}
}

// Each direction has its own key, so a record can never be replayed back at
// the peer that sent it even by a relay holding both halves of the traffic.
func TestDirectionKeysDiffer(t *testing.T) {
	keys := testKeys(t)
	if bytes.Equal(keys.o2aKey, keys.a2oKey) || bytes.Equal(keys.o2aIV, keys.a2oIV) {
		t.Fatal("both directions derived the same key material")
	}
}

// Every field either peer contributed changes the transcript, so a rewritten
// key exchange derives different keys and a signature over it fails.
func TestTranscriptBindsEveryContribution(t *testing.T) {
	base := func() ([]byte, error) {
		return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
	}
	want, err := base()
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]func() ([]byte, error){
		"offerer": func() ([]byte, error) {
			return transcript("c_x", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
		},
		"accepter": func() ([]byte, error) {
			return transcript("c_a", "n_x", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
		},
		"session": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdeF"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
		},
		"offer key": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-x"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
		},
		"offer nonce": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-x"), 100, []byte("eph-b"), []byte("nonce-b"))
		},
		"timestamp": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 101, []byte("eph-b"), []byte("nonce-b"))
		},
		"accept key": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-x"), []byte("nonce-b"))
		},
		"accept nonce": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-x"))
		},
		"the half form": func() ([]byte, error) {
			return transcript("c_a", "n_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, nil, nil)
		},
	}
	for name, variant := range variants {
		got, err := variant()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(got, want) {
			t.Fatalf("changing the %s did not change the transcript", name)
		}
	}
	// Field boundaries are unambiguous: moving a byte between two adjacent
	// fields is a different transcript, not the same one.
	shifted, err := transcript("c_ab", "_b", []byte("0123456789abcdef"), []byte("eph-a"), []byte("nonce-a"), 100, []byte("eph-b"), []byte("nonce-b"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(shifted, want) {
		t.Fatal("adjacent transcript fields are not separated")
	}
}

// A binding is the only thing that says which key belongs to which peer, so
// every way of getting one wrong is a refusal.
func TestBindingVerificationFailsClosed(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	identity, err := NewIdentity("n_b", "t_one", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	signed := SignBinding(priv, identity.Binding)
	if err := VerifyBinding(pub, signed, "n_b", now); err != nil {
		t.Fatalf("a valid binding was refused: %v", err)
	}
	for name, check := range map[string]func() error{
		"another peer's id":   func() error { return VerifyBinding(pub, signed, "n_c", now) },
		"another control key": func() error { return VerifyBinding(otherPub, signed, "n_b", now) },
		"no control key":      func() error { return VerifyBinding(nil, signed, "n_b", now) },
		"an expired binding":  func() error { return VerifyBinding(pub, signed, "n_b", now.Add(2*time.Hour)) },
		"an unsigned binding": func() error { return VerifyBinding(pub, identity.Binding, "n_b", now) },
		"a binding with no expiry": func() error {
			return VerifyBinding(pub, SignBinding(priv, proto.PeerBinding{Peer: "n_b", Key: signed.Key}), "n_b", now)
		},
	} {
		if err := check(); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	// A rewritten field invalidates the signature even though the key is
	// unchanged, so a relay cannot re-point a binding at itself.
	tampered := signed
	tampered.Tenant = "t_other"
	if err := VerifyBinding(pub, tampered, "n_b", now); err == nil {
		t.Fatal("a rewritten binding was accepted")
	}
}
