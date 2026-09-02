// Package ids generates short, prefixed, sortable identifiers.
//
// An id is <prefix>_<26 lowercase base32 chars>. The first 10 characters encode
// milliseconds since the Unix epoch so ids sort roughly by creation time; the
// remainder is random. Prefixes: n (node), ws (workspace), s (session),
// art (artifact), b (binding), t (timer), c (client), ev (event).
package ids

import (
	"crypto/rand"
	"encoding/base32"
	"strings"
	"time"
)

var enc = base32.NewEncoding("0123456789abcdefghjkmnpqrstvwxyz").WithPadding(base32.NoPadding)

// New returns a fresh id with the given prefix.
func New(prefix string) string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	if _, err := rand.Read(b[6:]); err != nil {
		panic(err)
	}
	return prefix + "_" + strings.ToLower(enc.EncodeToString(b[:]))
}

// Prefix returns the part before the first underscore, or "".
func Prefix(id string) string {
	i := strings.IndexByte(id, '_')
	if i < 0 {
		return ""
	}
	return id[:i]
}
