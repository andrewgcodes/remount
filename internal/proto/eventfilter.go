package proto

import (
	"net"
	"strings"
)

// EventFilter narrows the canonical log to the events an audit question is
// about: which binding, against which host, of which kind.
//
// It lives here rather than in a caller because the control plane applies it
// server-side — a tenant asking "what did this binding do?" should not have
// to receive and discard the rest of its history — and the same predicate has
// to describe the follow stream and the historical page identically.
//
// Binding and Host read the payload fields the broker records on credential
// and egress events; Types are event-type prefixes, so "egress" selects every
// `egress.*` type. Every field is optional and an empty filter matches
// everything, so a peer that does not set them sees the stream it always saw.
type EventFilter struct {
	Binding string
	Host    string
	Types   []string
}

// Empty reports whether this filter narrows anything.
func (f EventFilter) Empty() bool {
	return f.Binding == "" && f.Host == "" && len(f.Types) == 0
}

// Match reports whether e satisfies every field of the filter.
func (f EventFilter) Match(e Event) bool {
	if f.Empty() {
		return true
	}
	if len(f.Types) > 0 && !hasTypePrefix(e.Type, f.Types) {
		return false
	}
	if f.Binding == "" && f.Host == "" {
		return true
	}
	// Only a payload-bearing event can name a binding or a host, and an
	// event that names neither is not an answer to the question asked.
	fields := map[string]any{}
	if len(e.Payload) == 0 || Unmarshal(e.Payload, &fields) != nil {
		return false
	}
	if f.Binding != "" {
		binding, _ := fields["binding"].(string)
		if binding != f.Binding {
			return false
		}
	}
	if f.Host != "" {
		host, _ := fields["host"].(string)
		if !eventHostMatches(host, f.Host) {
			return false
		}
	}
	return true
}

func hasTypePrefix(eventType string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if prefix != "" && strings.HasPrefix(eventType, prefix) {
			return true
		}
	}
	return false
}

// eventHostMatches compares a recorded authority against the host an operator
// typed. Audits record a canonical `host:port`, but nobody asks a question in
// those terms, so a bare host also matches its authority.
func eventHostMatches(recorded, want string) bool {
	if recorded == "" {
		return false
	}
	if strings.EqualFold(recorded, want) {
		return true
	}
	if host, _, err := net.SplitHostPort(recorded); err == nil && strings.EqualFold(host, want) {
		return true
	}
	return false
}

// Filter returns the event filter this request declares.
func (r EventsTailReq) Filter() EventFilter {
	return EventFilter{Binding: r.Binding, Host: r.Host, Types: r.Types}
}
