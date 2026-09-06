package broker

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/redact"
)

// ReasonHeader carries the stable sub-classification of a broker refusal to
// the workspace. A harness reads one header instead of parsing prose, and an
// SDK maps it to the same typed error the control plane returns for the same
// situation over the wire.
const ReasonHeader = "X-Remount-Reason"

// denial is the workspace-facing shape of every broker refusal. It carries
// classification and the two identifiers the caller already knows (its own
// binding and the destination it asked for) and nothing else: never a secret,
// never a header the workspace sent, never a byte of the request or the
// upstream response.
type denial struct {
	status  int
	code    string // a proto.Code*, so the workspace matches the same code the wire uses
	reason  string // a proto.Reason*
	binding string
	host    string
	message string // human text; scrubbed before it is written
}

// denialBody is the JSON a refused workspace request receives.
type denialBody struct {
	Error denialError `json:"error"`
}

type denialError struct {
	Code    string `json:"code"`
	Reason  string `json:"reason"`
	Binding string `json:"binding"`
	Host    string `json:"host"`
	// Message is the same text the audit recorded, scrubbed. It is
	// diagnostic only: callers match on Code and Reason, never on prose.
	Message string `json:"message,omitempty"`
}

// deny writes one refusal. Every broker rejection goes through here so the
// header, the shape and the scrubbing cannot drift apart between surfaces.
func (b *Broker) deny(w http.ResponseWriter, d denial) {
	reason := d.reason
	if reason == "" {
		reason = proto.ReasonEgressDenied
	}
	code := d.code
	if code == "" {
		code = proto.CodeDenied
	}
	w.Header().Set(ReasonHeader, reason)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	status := d.status
	if status == 0 {
		status = http.StatusForbidden
	}
	w.WriteHeader(status)
	body := denialBody{Error: denialError{
		Code: code, Reason: reason, Binding: d.binding, Host: d.host,
		Message: redact.String(d.message),
	}}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(body)
}

// denyAudit writes the refusal for a decision already recorded in an audit:
// the same reason text, the same binding and host.
func (b *Broker) denyAudit(w http.ResponseWriter, audit Audit, status int, code, reason string) {
	b.deny(w, denial{
		status: status, code: code, reason: reason,
		binding: audit.Binding, host: audit.Host, message: "remount broker: " + audit.Reason,
	})
}

// denyStatus writes a refusal whose classification is implied by its HTTP
// status. Sites that know something more specific (an approval is pending, a
// binding's lease expired) pass their own code and reason instead.
func (b *Broker) denyStatus(w http.ResponseWriter, audit Audit, status int) {
	code, reason := classifyStatus(status)
	b.denyAudit(w, audit, status, code, reason)
}

// classifyStatus maps a refusal's HTTP status to the wire code and reason a
// caller matches on.
func classifyStatus(status int) (string, string) {
	switch status {
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		return proto.CodeResourceExhausted, proto.ReasonQuotaExceeded
	case http.StatusProxyAuthRequired:
		return proto.CodeUnauthorized, proto.ReasonRevoked
	case http.StatusBadRequest:
		return proto.CodeBadRequest, proto.ReasonEgressDenied
	case http.StatusNotFound:
		return proto.CodeNotFound, proto.ReasonEgressDenied
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return proto.CodeUnreachable, proto.ReasonEgressDenied
	case http.StatusInternalServerError:
		return proto.CodeInternal, proto.ReasonEgressDenied
	default:
		return proto.CodeDenied, proto.ReasonEgressDenied
	}
}

// denyRejection writes the refusal a credential pass produced.
func (b *Broker) denyRejection(w http.ResponseWriter, host string, rejected *credentialRejection) {
	b.deny(w, denial{
		status: rejected.status, code: rejected.code, reason: rejected.denialReason,
		binding: rejected.binding, host: host, message: "remount broker: " + rejected.public,
	})
}

// encodeBasic re-encodes a rewritten Basic credential.
func encodeBasic(plain string) string {
	return base64.StdEncoding.EncodeToString([]byte(plain))
}
