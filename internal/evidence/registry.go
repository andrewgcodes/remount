package evidence

import (
	"fmt"
	"slices"
	"strings"
)

// Scenario is one row of the acceptance ledger: what must be true, what proves
// it, what the environment must provide, and the last outcome anyone recorded.
//
// A scenario nothing proves yet is an open row, never an absent one. The whole
// point of the registry is that the gap is visible.
type Scenario struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Layer    Layer  `json:"layer"`
	Required bool   `json:"required"`
	// Owner names the test, script or command that proves the scenario.
	// Empty means nothing in the tree proves it; Open then says why.
	Owner string `json:"owner,omitempty"`
	// Env names the environment variables the owner needs. Names only.
	Env []string `json:"env,omitempty"`
	// Open is the reason a scenario has no owning proof at this commit.
	Open string `json:"open,omitempty"`
	// Recorded is the last outcome a source document recorded, and Note carries
	// that source's own honesty limit. The aggregate runner never promotes a
	// recorded outcome into a pass; it re-earns every row or reports it open.
	Recorded Status `json:"recorded,omitempty"`
	Note     string `json:"note,omitempty"`
	// Source is the document the row and its recorded outcome come from.
	Source string `json:"source,omitempty"`
	// Argv is the command the aggregate runner executes for this scenario.
	// Empty means the scenario is not wired into the runner yet.
	Argv []string `json:"argv,omitempty"`
}

// Owned reports whether anything in the tree proves the scenario.
func (s Scenario) Owned() bool { return s.Owner != "" }

// Wired reports whether the aggregate runner can execute the scenario itself.
func (s Scenario) Wired() bool { return len(s.Argv) > 0 }

func credentialEnv(names []string) []string {
	var credentials []string
	for _, name := range names {
		for _, part := range strings.Split(name, "_") {
			switch part {
			case "CREDENTIAL", "KEY", "PASSWORD", "SECRET", "TOKEN":
				credentials = append(credentials, name)
			}
		}
	}
	return credentials
}

const (
	sourceHandoff = "docs/engineering/handoff-2026-09-03.md (outcome: docs/engineering/implementation-closure-2026-09-03.md)"
	sourcePlanB   = "docs/engineering/plan-b-repository-executable-2026-09-03.md"
	sourceLinux   = "docs/engineering/verification-2026-09.md (Linux host verification 2026-09-04)"
)

// notLanded is the only honest thing to say about a Plan B row whose ticket
// has not merged: it is open, and the ticket that will close it is named.
func notLanded(phase, ticket string) string {
	return fmt.Sprintf("Plan B phase %s has not landed; §17 ticket %s owns it", phase, ticket)
}

// scenarios is the declarative registry. E1-E25 come from the 2026-09-03
// handoff and carry the closure document's disposition as their recorded
// outcome. B1-B32 come from the Plan B phase tables and are open until their
// ticket merges.
var scenarios = []Scenario{
	{
		ID: "E1", Title: "opencode runs in a workspace with a real key held only by the broker",
		Layer: LayerLiveService, Required: false,
		Owner:    "internal/sim.TestRunOpenCodeDockerIntegration",
		Env:      []string{"REMOUNT_INTEGRATION_OPENAI_KEY", "REMOUNT_INTEGRATION_IMAGE"},
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "live, 46.9 s against a real node:22 container; the key appeared in 0 workspace files and 0 log lines. CI does not run the docker lanes.",
	},
	{
		ID: "E2", Title: "two turns, move across nodes, third turn recalls both",
		Layer: LayerLiveService, Required: false,
		Owner:    "internal/sim.TestRunOpenCodeHandoffAcrossNodesDockerIntegration",
		Env:      []string{"REMOUNT_INTEGRATION_OPENAI_KEY", "REMOUNT_INTEGRATION_IMAGE"},
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "live, 152.1 s; key in 0 log lines; opencode only",
	},
	{
		ID: "E3", Title: "two-task queue with a sleep between tasks",
		Layer: LayerLiveService, Required: false,
		Owner:    "internal/sim.TestRunOpenCodeQueueDockerIntegration",
		Env:      []string{"REMOUNT_INTEGRATION_OPENAI_KEY", "REMOUNT_INTEGRATION_IMAGE"},
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "live, 71.7 s; key in 0 log lines",
	},
	{
		ID: "E4", Title: "gVisor enforced egress: unlisted host fails at connect and RevokeNetwork kills an in-flight transfer",
		Layer: LayerHostCI, Required: true,
		Owner:    "internal/workspace/gvisor.TestE4DenialConformance, internal/workspace/gvisor.TestE4FailedSetupCleanupConformance",
		Env:      []string{"REMOUNT_GVISOR_INTEGRATION", "REMOUNT_GVISOR_ROOTFS", "REMOUNT_CHAOS_IMAGE"},
		Argv:     []string{"./scripts/gvisor-conformance.sh", "e4"},
		Recorded: StatusPassed, Source: sourceLinux,
		Note: "the exact digest-pinned candidate passed all seven denial checks, the host-veth escape assertion, synchronous in-flight revoke, successful revoke cleanup and failed-setup cleanup",
	},
	{
		ID: "E5", Title: "two mutually untrusting tenants share one gVisor node",
		Layer: LayerHostCI, Required: true,
		Owner:    "internal/workspace/gvisor.TestE5SiblingTenantsCannotReachEachOther, internal/node.TestE5TenantIsolationConformance",
		Env:      []string{"REMOUNT_GVISOR_INTEGRATION", "REMOUNT_GVISOR_ROOTFS", "REMOUNT_CHAOS_IMAGE"},
		Argv:     []string{"./scripts/gvisor-conformance.sh", "e5"},
		Recorded: StatusPassed, Source: sourceLinux,
		Note: "the backend-level host-veth proof observed zero cross-tenant frames after positive broker controls, and the node-level proof kept files disjoint while attributing each denied broker attempt only to its source tenant",
	},
	{
		ID: "E6", Title: "approve-on-first-use parks egress, a decision releases it, timeout denies",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestApproveModeParksBeforeUpstreamAndResumesAfterDecision, internal/control.TestEgressApprovalTimeoutIsAnObservableDenial",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "committed egress.pending reaches the notifier while the upstream request stays parked",
	},
	{
		ID: "E7", Title: "per-binding budget denies the first request over the limit and usage stays visible",
		Layer: LayerCode, Required: true,
		Owner:    "internal/budget",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E8", Title: "principal revoked mid-run: node calls refused, sessions closed, others continue",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestE8PrincipalRevocationComposes, integration/identity.TestE8PrincipalRevocationCoreSurvivesRestart",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E9", Title: "chunked snapshot of a 500 MB tree: second snapshot uploads under 1 MB and the move is under 3 s",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestE9ChunkedSnapshotMoveDeduplicates500MBLogicalFixture",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "literal gap: a 500 MiB logical tree measured on a local process backend, not across a network",
	},
	{
		ID: "E10", Title: "control plane killed mid-move: standby promotes, no duplicate generation, lost window recorded",
		Layer: LayerCode, Required: true,
		Owner:    "integration/failover",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E11", Title: "long PTY session replays byte-identically from any point across tiers",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "literal gap: 8 MiB over more than 128 real 128 KiB segments, not a six-hour 2 GB session",
	},
	{
		ID: "E12", Title: "published SDK packages install from a real index and script the E1 flow",
		Layer: LayerArtifact, Required: false,
		Owner:    "integration/sdks",
		Recorded: StatusUnavailable, Source: sourceHandoff,
		Note: "both packages install and build from the local tree and their generated types are drift-checked, but neither is published; a local install is not publication",
	},
	{
		ID: "E13", Title: "two real MCP harnesses drive remount without touching .remount/env",
		Layer: LayerLiveService, Required: false,
		Owner:    "integration/mcp/e13-headless.sh",
		Env:      []string{"REMOUNT_E13_BINARY", "REMOUNT_E13_MODEL_KEY"},
		Recorded: StatusUnavailable, Source: sourceHandoff,
		Note: "no harness keys present; the lane skips explicitly rather than passing",
	},
	{
		ID: "E14", Title: "clone and push through a real GitHub App installation token, token in 0 workspace files",
		Layer: LayerLiveService, Required: false,
		Owner:    "internal/sim.TestRepoClonedAtMaterializeWithoutTokenInWorkspace",
		Env:      []string{"GITHUB_APP_PRIVATE_KEY"},
		Recorded: StatusUnavailable, Source: sourceHandoff,
		Note: "exercised against a local Git HTTP/token fake; no GitHub App is registered",
	},
	{
		ID: "E15", Title: "console: fleet, live PTY, scrubback, credential uses, blocked leaks, approve a pending egress",
		Layer: LayerCode, Required: true,
		Owner:    "web/tests/e15",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "browser-to-real-server; cred.used substitution, production-mode PTY under a role, and wss: remain uncovered",
	},
	{
		ID: "E16", Title: "event export streams OTLP spans and hourly JSONL with the same content hash",
		Layer: LayerCode, Required: true,
		Owner:    "internal/eventlog.TestE16OTLPHTTPJSONStreamsSessionsAsTraces, internal/eventlog.TestE16S3SinkWritesHourlyContentHashedJSONL",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E17", Title: "pool scales 0 to 1 on first claim and back to 0 when idle, every action an event",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestE17PoolClaimScalesUpAndIdleScalesDown",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "verified against a fake provisioner; real vendor scaling is externally gated",
	},
	{
		ID: "E18", Title: "signed release from a tag verifies through install.sh, Homebrew, image and go install",
		Layer: LayerArtifact, Required: false,
		Open:     "not performed, by decision: release authority was withheld on 2026-09-03 and no implementation depends on it",
		Recorded: StatusUnavailable, Source: sourceHandoff,
		Note: "no tag, image, formula or go install path was created or verified",
	},
	{
		ID: "E19", Title: "agent create prints {id, url} and a client sees transcript, tool_call, terminal and diff",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentEndToEndTurnsAndTranscript",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "fake ACP agent in sim; the real-harness docker variants remain",
	},
	{
		ID: "E20", Title: "sleep on client cut, a message wakes the agent on any node and session/load replays context",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentSurvivesNodeLoss",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "fake ACP agent in sim; real opencode and goose variants remain",
	},
	{
		ID: "E21", Title: "approval parks the harness and approval.decided precedes the tool result",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentEndToEndApprovalRoundTrip, internal/sim.TestAgentHTTPApprovalsAndFork",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E22", Title: "a parent agent creates a child through MCP; the child's key is in 0 files of either workspace",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentChildReportsToParent",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "covered via CLI/API --parent; the MCP-from-inside-the-workspace path is not the exercised one",
	},
	{
		ID: "E23", Title: "fork mid-conversation: the copy loads the same history and both continue independently",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentHTTPApprovalsAndFork",
		Recorded: StatusPassed, Source: sourceHandoff,
	},
	{
		ID: "E24", Title: "the same task across every recipe yields identical API responses and event shapes",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentEndToEndTurnsAndTranscript",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "recipes exist for every listed harness; they are not verified against real binaries in the docker lane",
	},
	{
		ID: "E25", Title: "a sleeping agent's port wakes on preview, proxies HTTP and WebSocket, and is refused without auth",
		Layer: LayerSim, Required: true,
		Owner:    "internal/sim.TestAgentHTTPAuthAndLifecycle",
		Recorded: StatusPassed, Source: sourceHandoff,
		Note: "the real harness web UI is not exercised",
	},

	{
		ID: "B1", Title: "OpenCode edits GREETING.txt through the local fake model and exits zero",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeDeterministicModelLane",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeDeterministicModelLane$", "./internal/sim/"},
		Note:  "a real pinned OpenCode runs in a docker workspace against internal/modelfake through the b_openai broker binding; keyless, but not offline: it needs a docker daemon and registry.npmjs.org, and either absence is unavailable rather than green",
	},
	{
		ID: "B2", Title: "the ACP transcript contains the expected tool call and terminal result",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeAgentTranscriptApprovalAndResume",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeAgentTranscriptApprovalAndResume$", "./internal/sim/"},
		Note:  "drives the real ACP agent path; keyless, but not offline: it needs a docker daemon and registry.npmjs.org, and either absence is unavailable rather than green",
	},
	{
		ID: "B3", Title: "client disconnect and reattach produce byte-identical session output",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeDeterministicModelLane",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeDeterministicModelLane$", "./internal/sim/"},
		Note:  "the client is cut mid-stream three times and a replay from seq 0 is byte-identical with no gap; the generic reattach contract is separately proven by internal/sim.TestReconnectMidStreamIsLossless, and this row requires it through the OpenCode lane",
	},
	{
		ID: "B4", Title: "sleep, restore and session/load retain the prior OpenCode conversation",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeAgentTranscriptApprovalAndResume",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeAgentTranscriptApprovalAndResume$", "./internal/sim/"},
		Note:  "sleep releases the workspace, a message restores it, and a post-restore upstream request carries the prior conversation",
	},
	{
		ID: "B5", Title: "an automated approval commits before the approved tool result",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeAgentTranscriptApprovalAndResume",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeAgentTranscriptApprovalAndResume$", "./internal/sim/"},
		Note:  "the approved tool result is absent from the transcript and every upstream request at decision time, and approval.decided commits at a lower event seq than the first request carrying it",
	},
	{
		ID: "B6", Title: "the synthetic reusable value occurs in zero workspace or durable evidence bytes",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBOpenCodeDeterministicModelLane",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBOpenCodeDeterministicModelLane$", "./internal/sim/"},
		Note:  "scans the workspace tree, OpenCode state, the expanded snapshot, events and both diagnostics; every scan carries a positive control it must find first, so a scan that cannot see anything fails rather than reporting clean",
	},
	{
		ID: "B7", Title: "a live OpenAI run uses the broker and passes the same leak scan",
		Layer: LayerLiveService, Required: false, Source: sourcePlanB,
		Env:  []string{"OPENAI_API_KEY"},
		Open: notLanded("B1 (§9)", "3"),
	},
	{
		ID: "B8", Title: "the fake E2B contract covers create, paginated list, filter and idempotent destroy",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/provision/e2b.TestB8ContractCoversCreatePaginatedListFilterAndIdempotentDestroy",
		Argv:  []string{"go", "test", "-count=1", "-run", "^TestB8ContractCoversCreatePaginatedListFilterAndIdempotentDestroy$", "./internal/provision/e2b/"},
		Note:  "the fake serves shapes captured from the real api.e2b.app on 2026-09-03; contract/create_response.json is deliberately sparser than contract/list_item.json because the real create returns no metadata, startedAt or state",
	},
	{
		ID: "B9", Title: "an ambiguous create is recovered by unique name without duplicate machines",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/provision/e2b.TestB9AmbiguousCreateIsRecoveredByNameWithoutDuplicates",
		Argv:  []string{"go", "test", "-count=1", "-run", "^TestB9AmbiguousCreateIsRecoveredByNameWithoutDuplicates$|^TestAmbiguousCreateWithUnreadableInventoryStaysAmbiguous$", "./internal/provision/e2b/"},
		Note:  "covers both halves: the driver resolves the ambiguity in-call by unique name, and when inventory is also unreadable the outcome stays honestly unknown rather than reported as success",
	},
	{
		ID: "B10", Title: "pool reconciliation never holds control authority while calling E2B",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/control.TestPoolReconcileNeverHoldsControlAuthorityAcrossAProviderCall",
		Argv:  []string{"go", "test", "-count=1", "-timeout=3m", "-run", "^TestPoolReconcileNeverHoldsControlAuthorityAcrossAProviderCall$", "./internal/control/"},
		Note:  "the provider stalls mid-call and ordinary control-plane work must still complete; verified to hang and fail when the control mutex is held across the call, so the forbidden result is observable rather than assumed",
	},
	{
		ID: "B11", Title: "a live E2B candidate enrolls and reaches ws.ready",
		Layer: LayerLiveService, Required: false, Source: sourcePlanB,
		Env:  []string{"E2B_API_KEY"},
		Open: notLanded("B2 (§10)", "5"),
	},
	{
		ID: "B12", Title: "a live E2B workspace runs the keyless OpenCode scenario through the broker",
		Layer: LayerLiveService, Required: false, Source: sourcePlanB,
		Env:  []string{"E2B_API_KEY"},
		Open: notLanded("B2 (§10)", "5"),
	},
	{
		ID: "B13", Title: "every live sandbox and candidate template is absent after cleanup",
		Layer: LayerLiveService, Required: false, Source: sourcePlanB,
		Env:  []string{"E2B_API_KEY"},
		Open: notLanded("B2 (§10)", "5"),
	},
	{
		ID: "B14", Title: "cut the client mid-command; reattach is byte-identical or carries an explicit gap",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBContinuityClientCutReattachIsLosslessOrExplicitlyGapped",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBContinuityClientCutReattachIsLosslessOrExplicitlyGapped$", "./internal/sim/"},
		Note:  "proves both branches of the disjunction, not just the lossless one: seeded chaos for byte-identical reattach, and a dropped spill for an explicit gap whose range is asserted. internal/sim.TestReconnectMidStreamIsLossless covers the generic reattach contract separately; the row inside the E2B pool failure matrix remains open until that lane runs.",
	},
	{
		ID: "B15", Title: "lose the node before checkpoint commit; the source stays retained and fenced",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBContinuityNodeLostBeforeCheckpointCommitRetainsAndFencesSource",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBContinuityNodeLostBeforeCheckpointCommitRetainsAndFencesSource$", "./internal/sim/"},
		Note:  "the checkpoint is archived but its result never arrives; the row settles failed at the same generation, no snapshot is published, the second node never takes it, and the source tree keeps its pre-move content",
	},
	{
		ID: "B16", Title: "lose the node after commit; the replacement claims only the new generation",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBContinuityNodeLostAfterCommitYieldsOnlyTheNewGeneration",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBContinuityNodeLostAfterCommitYieldsOnlyTheNewGeneration$", "./internal/sim/"},
		Note:  "generation advances exactly twice with each bump named in the event log, and the lost node restarted with its original identity is refused re-adoption while its diverged tree keeps the uncommitted write",
	},
	{
		ID: "B17", Title: "restart control; stale grants, nodes and ready messages are refused",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBContinuityControlRestartRefusesStaleAuthority",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBContinuityControlRestartRefusesStaleAuthority$", "./internal/sim/"},
		Note:  "durable authority survives the restart, which is the honest boundary; a raw node peer is refused on ws.ready at three generations and on ws.claim, and ws.renew answers fence naming the authoritative generation",
	},
	{
		ID: "B18", Title: "an OpenCode queue resumes on another node without repeating a completed item",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBContinuityQueueResumesWithoutRepeatingACompletedItem",
		Argv:  []string{"go", "test", "-count=1", "-timeout=30m", "-run", "^TestPlanBContinuityQueueResumesWithoutRepeatingACompletedItem$", "./internal/sim/"},
		Note:  "the interrupted item carries no outcome at all rather than an ambiguous one, the completed item's effect is published exactly once, and the uncommitted partial write is not",
	},
	{
		ID: "B19", Title: "kill the live E2B worker; the replacement resumes the agent and cleanup is complete",
		Layer: LayerLiveService, Required: false, Source: sourcePlanB,
		Env:  []string{"E2B_API_KEY"},
		Open: notLanded("B3 (§11)", "6"),
	},
	{
		ID: "B20", Title: "the reference binary passes the complete required conformance manifest",
		Layer: LayerArtifact, Required: true, Source: sourcePlanB,
		Owner: "cmd/conformance --build .",
		Argv:  []string{"go", "run", "./cmd/conformance", "--build", "."},
		Note:  "runs the versioned black-box manifest against a freshly built binary; 53 required rows, 0 failed, 8 honestly unavailable with printed reasons",
	},
	{
		ID: "B21", Title: "a deliberately broken test implementation fails each semantic category",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/conformance.TestB21BrokenImplementationFailsEachSemanticCategory",
		Argv:  []string{"go", "test", "-count=1", "-run", "^TestB21", "./internal/conformance/"},
		Note:  "a deliberately broken shim with twelve switchable defects; the control asserts the undefective shim passes all 53 required rows first, so a defect cannot be caught by failing everything",
	},
	{
		ID: "B22", Title: "unknown extensions stay ignorable while unknown required capabilities fail",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/conformance.TestB22UnknownExtensionsStayIgnorableAndUnknownRequiredCapabilitiesFail",
		Argv:  []string{"go", "test", "-count=1", "-run", "^TestB22", "./internal/conformance/"},
		Note:  "an unknown extension is accepted and never echoed; a hello without the v1 baseline is refused; a capability-gated row absent its gate is unavailable, never passed",
	},
	{
		ID: "B23", Title: "a protocol-version mismatch fails before any lifecycle mutation",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/conformance.TestB23ProtocolVersionMismatchFailsBeforeAnyLifecycleMutation",
		Argv:  []string{"go", "test", "-count=1", "-run", "^TestB23", "./internal/conformance/"},
		Note:  "the ordering claim is load-bearing: a shim that refuses the connection but mutates first must fail, so the row rests on nothing having changed rather than on an error arriving",
	},
	{
		ID: "B24", Title: "a relay capture contains no plaintext operation payload or secret-shaped canary",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBRelayCaptureHasNoPlaintextPayload",
		Argv:  []string{"go", "test", "-count=1", "-timeout=20m", "-run", "^TestPlanBRelayCaptureHasNoPlaintextPayload$", "./internal/sim/"},
		Note:  "positive control first: the same workload unsealed puts the canary in the capture six times, so the absence assertion is made by a scanner proven able to see",
	},
	{
		ID: "B25", Title: "mutation, replay and cross-destination substitution are rejected",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBRelayRejectsMutationReplayAndSubstitution",
		Argv:  []string{"go", "test", "-count=1", "-timeout=20m", "-run", "^TestPlanBRelayRejectsMutationReplayAndSubstitution$", "./internal/sim/"},
		Note:  "ciphertext mutation, duplicate delivery and re-addressing to a second node, all through the real relay",
	},
	{
		ID: "B26", Title: "reconnect rekeys without losing cursor or idempotency semantics",
		Layer: LayerSim, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency",
		Argv:  []string{"go", "test", "-count=1", "-timeout=20m", "-run", "^TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency$", "./internal/sim/"},
		Note:  "rekeying may not disturb the cursor or idempotency semantics reconnect depends on; four distinct agreements, byte-identical output, no gap",
	},
	{
		ID: "B27", Title: "required encryption with an incompatible peer fails before mutation",
		Layer: LayerCode, Required: true, Source: sourcePlanB,
		Owner: "internal/sim.TestPlanBRelayRequiredEncryptionFailsBeforeMutation",
		Argv:  []string{"go", "test", "-count=1", "-timeout=20m", "-run", "^TestPlanBRelayRequiredEncryptionFailsBeforeMutation$", "./internal/sim/"},
		Note:  "asserted as ordering: zero sealed and zero plaintext operation frames on the wire, and the far peer handed nothing",
	},
	{
		ID: "B28", Title: "the exact gVisor candidate passes isolation and enforced-gateway conformance",
		Layer: LayerHostCI, Required: true, Source: sourceLinux,
		Env:      []string{"REMOUNT_GVISOR_INTEGRATION", "REMOUNT_GVISOR_ROOTFS", "REMOUNT_CHAOS_IMAGE"},
		Owner:    "scripts/gvisor-conformance.sh",
		Argv:     []string{"./scripts/gvisor-conformance.sh", "all"},
		Recorded: StatusPassed,
		Note:     "the aggregate host lane probes Docker/runsc, runs E4 denial/revoke/cleanup, backend-level host-veth sibling isolation and node-level tenant/event isolation against the exact digest-pinned candidate",
	},
	{
		ID: "B29", Title: "Firecracker reports either an exact-host pass or unavailable with a reason",
		Layer: LayerHostCI, Required: true, Source: sourceLinux,
		Env: []string{
			"REMOUNT_FIRECRACKER_DATA_ROOT", "REMOUNT_FIRECRACKER_BINARY",
			"REMOUNT_FIRECRACKER_JAILER", "REMOUNT_FIRECRACKER_CGROUP_PARENT",
			"REMOUNT_FIRECRACKER_KERNEL", "REMOUNT_FIRECRACKER_ROOTFS",
			"REMOUNT_FIRECRACKER_GUEST_MANIFEST",
		},
		Owner:    "scripts/firecracker-conformance.sh",
		Argv:     []string{"./scripts/firecracker-conformance.sh"},
		Recorded: StatusPassed,
		Note:     "the exact Linux/KVM candidate passed real jailer/vsock guest operations, enforced egress, synchronous revoke, full-VM prepare/abort/commit/restore with exact-once continuation, failure-path rejection and cleanup",
	},
	{
		ID: "B30", Title: "reconnect and pool bursts stay within declared resource ceilings",
		Layer: LayerCode, Required: true, Source: sourceLinux,
		Owner:    "internal/sim.TestPlanBScale*, scripts/planb-resource-ceilings.sh",
		Argv:     []string{"./scripts/planb-resource-ceilings.sh"},
		Recorded: StatusPassed,
		Note:     "three cycles of 10,000 simultaneous reconnecting cursors returned to the resource floor; pool, spill, artifact, event, subscription and quota workloads remained bounded and counted every rejection or loss",
	},
	{
		ID: "B31", Title: "two clean builds produce the declared reproducible artifacts",
		Layer: LayerArtifact, Required: true, Source: sourceLinux,
		Owner:    "integration/reproducible.TestB31BinariesAreAFunctionOfTheSourceAlone, scripts/reproducible-container-builds.sh",
		Argv:     []string{"./scripts/reproducible-container-builds.sh"},
		Recorded: StatusPassed,
		Note:     "two clean digest-pinned Go containers with cold caches and different TMPDIR values produce byte-identical binaries for every supported platform",
	},
	{
		ID: "B32", Title: "clean installs pass the black-box smoke without source-tree imports",
		Layer: LayerArtifact, Required: true, Source: sourceLinux,
		Owner:    "integration/installs.TestB32*",
		Argv:     []string{"go", "test", "-count=1", "-timeout=20m", "./integration/installs/"},
		Recorded: StatusPassed,
		Note:     "the dist binary and both release images passed manifest 1.1.0 black-box conformance; the wheel, npm tarball and external Go module drove an installed server without a source-tree dependency, and the checksums, static-link, inventory and SPDX SBOM lanes passed",
	},
}

// Scenarios returns the registry. It returns a fresh slice because the caller
// sorts and filters it; the registry itself is never handed out live.
func Scenarios() []Scenario {
	out := make([]Scenario, len(scenarios))
	copy(out, scenarios)
	for i := range out {
		out[i].Env = slices.Clone(out[i].Env)
		out[i].Argv = slices.Clone(out[i].Argv)
	}
	return out
}

// ScenarioByID returns the registered scenario with this id.
func ScenarioByID(id string) (Scenario, bool) {
	for _, s := range Scenarios() {
		if strings.EqualFold(s.ID, id) {
			return s, true
		}
	}
	return Scenario{}, false
}

// ValidateRegistry proves the registry itself cannot lie: ids are unique, every
// row is either owned or explicitly open, environment entries are names, and
// nothing claims an outcome it has no source for.
func ValidateRegistry() error {
	seen := map[string]bool{}
	for _, s := range scenarios {
		if s.ID == "" || s.Title == "" {
			return fmt.Errorf("evidence: registry row %q has no id or title", s.ID)
		}
		if seen[s.ID] {
			return fmt.Errorf("evidence: registry has two rows for %s", s.ID)
		}
		seen[s.ID] = true
		if !contains(Layers, s.Layer) {
			return fmt.Errorf("evidence: %s: layer %q is not one of %v", s.ID, s.Layer, Layers)
		}
		if s.Owned() == (s.Open != "") {
			return fmt.Errorf("evidence: %s: a row is owned or open, never both and never neither", s.ID)
		}
		if s.Recorded != "" && !contains(Statuses, s.Recorded) {
			return fmt.Errorf("evidence: %s: recorded outcome %q is not one of %v", s.ID, s.Recorded, Statuses)
		}
		if s.Recorded != "" && s.Source == "" {
			return fmt.Errorf("evidence: %s: a recorded outcome must name its source", s.ID)
		}
		if s.Recorded == StatusPassed && !s.Owned() {
			return fmt.Errorf("evidence: %s: an unowned row cannot record a pass", s.ID)
		}
		for _, name := range s.Env {
			if !envNamePattern.MatchString(name) {
				return fmt.Errorf("evidence: %s: %q is not an environment variable name", s.ID, name)
			}
		}
	}
	return nil
}
