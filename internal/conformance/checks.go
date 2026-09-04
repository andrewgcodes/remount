package conformance

import (
	"context"
	"fmt"
)

// CheckFunc asserts one requirement against one target.
//
// Returning nil is a pass. Returning an *Unavailable is "could not run, and
// here is what was missing". Any other error is a failure whose text is the
// failed postcondition. There is deliberately no way to say "skipped and
// therefore fine".
type CheckFunc func(ctx context.Context, s *Session) error

// checks maps a requirement id to the function that asserts it. Manifest
// validation refuses a requirement with no entry here and an entry here with
// no requirement, so the manifest and the suite cannot drift apart.
var checks = map[string]CheckFunc{
	"CONF-NEG-001": checkNegBaselineAccepted,
	"CONF-NEG-002": checkNegOrderedIntersection,
	"CONF-NEG-003": checkNegBaselineRequired,
	"CONF-NEG-004": checkNegVersionFailsClosed,
	"CONF-NEG-005": checkNegUnknownOp,
	"CONF-NEG-006": checkNegHelloFirst,
	"CONF-NEG-007": checkNegStableCodes,
	"CONF-NEG-008": checkNegHandshakeCarriesLeaseAndKey,
	"CONF-NEG-009": checkNegControllerEpochIsFenced,

	"CONF-WS-001": checkWSCreateShape,
	"CONF-WS-002": checkWSIdempotentCreate,
	"CONF-WS-003": checkWSReadyGatesClaimed,
	"CONF-WS-004": checkWSUnknownIsNotFound,
	"CONF-WS-005": checkWSSystemMountPathRefused,
	"CONF-WS-006": checkWSListAgrees,
	"CONF-WS-007": checkWSDestroyIsAbsorbing,

	"CONF-AUTH-001": checkAuthGrantBinds,
	"CONF-AUTH-002": checkAuthGrantUnknownWorkspace,
	"CONF-AUTH-003": checkAuthStaleGenerationRefused,
	"CONF-AUTH-004": checkAuthWrongWorkspaceRefused,
	"CONF-AUTH-005": checkAuthNoGrantRefused,
	"CONF-AUTH-006": checkAuthMoveInvalidatesGrant,
	"CONF-AUTH-007": checkAuthLeaseExpiry,

	"CONF-SESS-001": checkSessInfoIsSeqZero,
	"CONF-SESS-002": checkSessExitIsLast,
	"CONF-SESS-003": checkSessSeqDense,
	"CONF-SESS-004": checkSessInputDeduplicated,
	"CONF-SESS-005": checkSessReplayIsByteIdentical,
	"CONF-SESS-006": checkSessWaitReportsExit,
	"CONF-SESS-007": checkSessEvictedReplayGaps,
	"CONF-SESS-008": checkSessAttachBeyondRange,
	"CONF-SESS-009": checkSessStdoutReachesTheClient,

	"CONF-SNAP-001": checkSnapDeterministicID,
	"CONF-SNAP-002": checkSnapDigestIsIdentity,
	"CONF-SNAP-003": checkSnapRestoreServesTree,
	"CONF-SNAP-004": checkSnapLiveIsNotAuthoritative,
	"CONF-SNAP-005": checkSnapUnknownArtifact,
	"CONF-SNAP-006": checkSnapDigestMismatchRefused,
	"CONF-SNAP-007": checkSnapMoveCarriesTree,
	"CONF-SNAP-008": checkSnapAuthoritativeQuiesces,

	"CONF-BIND-001": checkBindPlaceholderNotSecret,
	"CONF-BIND-002": checkBindLeakBlocked,
	"CONF-BIND-003": checkBindUnboundDestinationDenied,
	"CONF-BIND-004": checkBindPrivateAddressRefused,
	"CONF-BIND-005": checkBindNoSecretInEvents,

	"CONF-APP-001": checkAppUnknownApproval,
	"CONF-APP-002": checkAppListIsPendingOnly,
	"CONF-APP-003": checkAppFingerprintIdempotent,
	"CONF-APP-004": checkAppSecondDecisionConflicts,
	"CONF-APP-005": checkAppDecisionSurvivesRestart,

	"CONF-AGT-001": checkAgentCreateShape,
	"CONF-AGT-002": checkAgentInboxAppends,
	"CONF-AGT-003": checkAgentInboxBounded,
	"CONF-AGT-004": checkAgentTranscriptPage,
	"CONF-AGT-005": checkAgentTranscriptGap,
	"CONF-AGT-006": checkAgentSleepAndWake,
	"CONF-AGT-007": checkAgentUnknownIsNotFound,
	"CONF-AGT-008": checkAgentDestroyIsTerminal,

	"CONF-EVT-001": checkEvtSeqIncreasing,
	"CONF-EVT-002": checkEvtSeqDense,
	"CONF-EVT-003": checkEvtStreamFilter,
	"CONF-EVT-004": checkEvtLifecycleOrder,
	"CONF-EVT-005": checkEvtCanonicalTypes,
	"CONF-EVT-006": checkEvtTailBeyondHead,
	"CONF-EVT-007": checkEvtOutOfBandAppend,
	"CONF-EVT-008": checkEvtUnknownFieldRefused,
	"CONF-EVT-009": checkEvtExportCursor,
	"CONF-EVT-010": checkEvtEvictedNamesOldest,
}

// CheckIDs returns every requirement id the suite can assert.
func CheckIDs() []string {
	out := make([]string, 0, len(checks))
	for id := range checks {
		out = append(out, id)
	}
	return out
}

// gate returns an Unavailable when any prerequisite or capability a
// requirement depends on is missing. The capability is judged against what
// the target actually echoed at hello, not against what it claims elsewhere.
func gate(s *Session, r Requirement) error {
	for _, p := range r.Prereqs {
		if reason := s.Target.Missing(p); reason != "" {
			return Unavailablef("prerequisite %q is unavailable: %s", p, reason)
		}
	}
	if r.Capability != "" {
		for _, c := range s.Control.Negotiated {
			if c == r.Capability {
				return nil
			}
		}
		return Unavailablef("the target did not negotiate capability %q", r.Capability)
	}
	return nil
}

// failf builds a failure whose text is the postcondition that did not hold.
func failf(format string, a ...any) error { return fmt.Errorf(format, a...) }
