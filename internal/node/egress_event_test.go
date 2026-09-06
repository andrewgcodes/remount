package node

import (
	"testing"

	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/proto"
)

// TestEveryBrokerDecisionHasAnEventType enumerates the broker's decision
// constants and proves each one is recorded under an event type that means
// what it says.
//
// The mapping's default is `egress.allowed`, so a decision nobody adds a case
// for is recorded as a success. That is exactly what happened when the
// `revoked` decision was introduced: a workspace refused for using a withdrawn
// binding was logged as an allowed egress, and an audit reading event types
// rather than payload decisions would have counted it as traffic that went
// through.
func TestEveryBrokerDecisionHasAnEventType(t *testing.T) {
	cases := map[string]string{
		broker.DecisionAllowed:         proto.EvEgressAllowed,
		broker.DecisionSubstituted:     proto.EvCredUsed,
		broker.DecisionDenied:          proto.EvEgressDenied,
		broker.DecisionLeakBlocked:     proto.EvEgressDenied,
		broker.DecisionExpired:         proto.EvEgressDenied,
		broker.DecisionUnauthenticated: proto.EvEgressDenied,
		broker.DecisionLimitExceeded:   proto.EvEgressDenied,
		broker.DecisionRevoked:         proto.EvEgressDenied,
		broker.DecisionRedacted:        proto.EvEgressRedacted,
	}
	for decision, want := range cases {
		if got := egressEventType(decision); got != want {
			t.Errorf("decision %q is recorded as %q, want %q", decision, got, want)
		}
	}
	// Every refusal reaches the denied stream. A decision that is neither an
	// allowance nor a substitution nor a redaction is a refusal by definition.
	for _, decision := range []string{
		broker.DecisionDenied, broker.DecisionLeakBlocked, broker.DecisionExpired,
		broker.DecisionUnauthenticated, broker.DecisionLimitExceeded, broker.DecisionRevoked,
	} {
		if egressEventType(decision) == proto.EvEgressAllowed {
			t.Errorf("refusal %q is recorded as an allowed egress", decision)
		}
	}
}
