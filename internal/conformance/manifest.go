package conformance

import (
	"fmt"
	"sort"
	"strings"
)

// ManifestVersion is the version of the whole manifest document. It changes
// when a requirement is added, retired or has its tier changed, so a report
// names exactly which contract an implementation was judged against.
const ManifestVersion = "1.0.0"

// ProtocolVersion is the wire version this manifest describes. A target that
// declares any other version is not judged by it (§13).
const ProtocolVersion = "1"

// Category is one of the nine semantic areas Plan B §12.1 names.
type Category string

// The nine categories, in the order §12.1 lists them.
const (
	CategoryNegotiation Category = "negotiation"
	CategoryWorkspace   Category = "workspace"
	CategoryAuthority   Category = "authority"
	CategorySession     Category = "session"
	CategorySnapshot    Category = "snapshot"
	CategoryBinding     Category = "binding"
	CategoryApproval    Category = "approval"
	CategoryAgent       Category = "agent"
	CategoryEvent       Category = "event"
)

// Categories is the canonical order.
var Categories = []Category{
	CategoryNegotiation, CategoryWorkspace, CategoryAuthority,
	CategorySession, CategorySnapshot, CategoryBinding,
	CategoryApproval, CategoryAgent, CategoryEvent,
}

// Tier is Plan B §12.5's three-way declaration.
type Tier string

const (
	// TierRequired must hold in every conforming implementation. A required
	// requirement that cannot be observed is a failure of the run, not a
	// pass: see Runner.Run.
	TierRequired Tier = "required"
	// TierCapabilityGated holds only when the target provides the named
	// capability or prerequisite. Absent it, the row is unavailable and says
	// what was missing.
	TierCapabilityGated Tier = "capability-gated"
	// TierExtension describes behaviour outside the baseline contract. An
	// implementation may omit it entirely and still conform.
	TierExtension Tier = "extension"
)

// Tiers is the canonical order.
var Tiers = []Tier{TierRequired, TierCapabilityGated, TierExtension}

// Prerequisite names something the *runner* must be able to arrange, as
// opposed to a capability the peer negotiates. Keeping the two apart matters:
// "this build does not implement approvals" and "this run had no credential
// to test approvals with" are different sentences and a report that conflates
// them is lying about one of them.
type Prerequisite string

const (
	// PrereqBindings: the target has at least one egress binding configured,
	// so placeholder substitution and leak blocking can be exercised.
	PrereqBindings Prerequisite = "bindings"
	// PrereqCLI: the target ships a command-line client the runner may call.
	PrereqCLI Prerequisite = "cli"
	// PrereqSessionEviction: the target's session retention bound is small
	// enough that the runner can overrun it inside its time budget.
	PrereqSessionEviction Prerequisite = "session-eviction"
	// PrereqEventEviction: the runner can drive the event log past its
	// retention watermark.
	PrereqEventEviction Prerequisite = "event-eviction"
	// PrereqTranscriptEviction: the runner can drive an agent transcript past
	// its mirror bound.
	PrereqTranscriptEviction Prerequisite = "transcript-eviction"
	// PrereqNodeFault: the runner may stop a node and let its lease expire.
	PrereqNodeFault Prerequisite = "node-fault"
	// PrereqEgressApproval: the target has an approve-mode egress rule, so an
	// approval row can be created from a real request.
	PrereqEgressApproval Prerequisite = "egress-approval"
	// PrereqRestart: the runner may restart the control plane in place.
	PrereqRestart Prerequisite = "restart"
	// PrereqQuiescedSnapshot: the target's backend can quiesce a workspace
	// for an authoritative checkpoint.
	PrereqQuiescedSnapshot Prerequisite = "quiesced-snapshot"
	// PrereqNotifier: the target has an outbound notification destination
	// configured, so export-cursor behaviour is observable.
	PrereqNotifier Prerequisite = "notifier"
)

// Requirement is one versioned row of the manifest.
type Requirement struct {
	// ID is stable forever. A retired requirement keeps its id.
	ID string `json:"id"`
	// Version is the requirement's own semantic version. It changes when the
	// assertion changes meaning, so a report can say an implementation passed
	// CONF-SESS-004 v1.0.0 and not the stricter v1.1.0.
	Version string `json:"version"`
	// Since is the manifest version that introduced it.
	Since string `json:"since"`
	// Category is the semantic area of §12.1.
	Category Category `json:"category"`
	// Tier is §12.5's declaration.
	Tier Tier `json:"tier"`
	// Title states the obligation in one sentence, in the imperative of the
	// spec rather than the vocabulary of this implementation.
	Title string `json:"title"`
	// Spec cites the normative section.
	Spec string `json:"spec"`
	// Capability names the negotiated capability a capability-gated row needs,
	// empty when the gate is a prerequisite instead.
	Capability string `json:"capability,omitempty"`
	// Prereqs are the runner-arranged conditions the row needs.
	Prereqs []Prerequisite `json:"prereqs,omitempty"`
	// Extension names the extension a TierExtension row belongs to.
	Extension string `json:"extension,omitempty"`
}

// Manifest is the versioned conformance contract.
type Manifest struct {
	Version         string        `json:"version"`
	ProtocolVersion string        `json:"protocol_version"`
	Requirements    []Requirement `json:"requirements"`
}

// Standard returns the manifest this release publishes.
func Standard() Manifest {
	return Manifest{Version: ManifestVersion, ProtocolVersion: ProtocolVersion, Requirements: requirements}
}

// Find returns the requirement with the given id.
func (m Manifest) Find(id string) (Requirement, bool) {
	for _, r := range m.Requirements {
		if r.ID == id {
			return r, true
		}
	}
	return Requirement{}, false
}

// ByCategory groups the requirements in canonical category order.
func (m Manifest) ByCategory() map[Category][]Requirement {
	out := map[Category][]Requirement{}
	for _, r := range m.Requirements {
		out[r.Category] = append(out[r.Category], r)
	}
	return out
}

// Counts returns the number of requirements per tier.
func (m Manifest) Counts() map[Tier]int {
	out := map[Tier]int{}
	for _, r := range m.Requirements {
		out[r.Tier]++
	}
	return out
}

// Validate enforces the manifest's own invariants. A manifest that violates
// one of them can produce a report that is quietly wrong, which is worse than
// no report.
func (m Manifest) Validate() error {
	if m.Version == "" {
		return fmt.Errorf("conformance: manifest has no version")
	}
	if m.ProtocolVersion == "" {
		return fmt.Errorf("conformance: manifest names no protocol version")
	}
	seen := map[string]bool{}
	covered := map[Category]bool{}
	for _, r := range m.Requirements {
		switch {
		case r.ID == "":
			return fmt.Errorf("conformance: a requirement has no id")
		case seen[r.ID]:
			return fmt.Errorf("conformance: duplicate requirement id %q", r.ID)
		case r.Version == "":
			return fmt.Errorf("conformance: %s has no version", r.ID)
		case r.Since == "":
			return fmt.Errorf("conformance: %s does not say which manifest version introduced it", r.ID)
		case r.Title == "":
			return fmt.Errorf("conformance: %s has no title", r.ID)
		case r.Spec == "":
			return fmt.Errorf("conformance: %s cites no normative section", r.ID)
		}
		seen[r.ID] = true
		if !validCategory(r.Category) {
			return fmt.Errorf("conformance: %s has category %q, not one of %v", r.ID, r.Category, Categories)
		}
		covered[r.Category] = true
		if !validTier(r.Tier) {
			return fmt.Errorf("conformance: %s has tier %q, not one of %v", r.ID, r.Tier, Tiers)
		}
		if err := validateGate(r); err != nil {
			return err
		}
		if _, ok := checks[r.ID]; !ok {
			return fmt.Errorf("conformance: %s has no registered check; an unasserted requirement is decoration", r.ID)
		}
	}
	for _, c := range Categories {
		if !covered[c] {
			return fmt.Errorf("conformance: category %q has no requirements; §12.1 names it as part of the semantic core", c)
		}
	}
	for id := range checks {
		if !seen[id] {
			return fmt.Errorf("conformance: check %q asserts a requirement the manifest does not declare", id)
		}
	}
	return nil
}

// validateGate keeps the three tiers honest about each other: only a
// capability-gated row may name a gate, and it must name exactly one kind.
func validateGate(r Requirement) error {
	switch r.Tier {
	case TierRequired:
		if r.Capability != "" || len(r.Prereqs) > 0 {
			return fmt.Errorf("conformance: %s is required but names a gate; a gated obligation is capability-gated by definition", r.ID)
		}
		if r.Extension != "" {
			return fmt.Errorf("conformance: %s is required but names extension %q", r.ID, r.Extension)
		}
	case TierCapabilityGated:
		if r.Capability == "" && len(r.Prereqs) == 0 {
			return fmt.Errorf("conformance: %s is capability-gated but names no capability or prerequisite, so nothing says when it applies", r.ID)
		}
		if r.Capability != "" && !knownCapability(r.Capability) {
			return fmt.Errorf("conformance: %s gates on unknown capability %q", r.ID, r.Capability)
		}
	case TierExtension:
		if r.Extension == "" {
			return fmt.Errorf("conformance: %s is extension-specific but names no extension", r.ID)
		}
	}
	return nil
}

func validCategory(c Category) bool {
	for _, k := range Categories {
		if k == c {
			return true
		}
	}
	return false
}

func validTier(t Tier) bool {
	for _, k := range Tiers {
		if k == t {
			return true
		}
	}
	return false
}

func knownCapability(c string) bool {
	for _, k := range KnownCapabilities {
		if k == c {
			return true
		}
	}
	return false
}

// SortedIDs returns every requirement id in lexical order.
func (m Manifest) SortedIDs() []string {
	out := make([]string, 0, len(m.Requirements))
	for _, r := range m.Requirements {
		out = append(out, r.ID)
	}
	sort.Strings(out)
	return out
}

// Gates renders a requirement's gate for a report.
func (r Requirement) Gates() string {
	var parts []string
	if r.Capability != "" {
		parts = append(parts, "capability "+r.Capability)
	}
	for _, p := range r.Prereqs {
		parts = append(parts, "prerequisite "+string(p))
	}
	if r.Extension != "" {
		parts = append(parts, "extension "+r.Extension)
	}
	return strings.Join(parts, ", ")
}

const v1 = "1.0.0"

// requirements is the manifest itself. Each row is a sentence from
// spec/PROTOCOL.md turned into an obligation, cited back to the section that
// says it, and classified by §12.5.
var requirements = []Requirement{
	// ---- protocol negotiation and stable errors -------------------------
	{
		ID: "CONF-NEG-001", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "a hello offering the v1 baseline is accepted and the response echoes v1",
		Spec:  "PROTOCOL.md §3, §3.1",
	},
	{
		ID: "CONF-NEG-002", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "the negotiated capability list is the ordered intersection, v1 first, and never echoes an identifier the implementation does not implement",
		Spec:  "PROTOCOL.md §3.1",
	},
	{
		ID: "CONF-NEG-003", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "a hello without the v1 baseline is refused with the stable code unsupported, and an empty capability list is not an implicit wildcard",
		Spec:  "PROTOCOL.md §2 rule 1, §3.1",
	},
	{
		ID: "CONF-NEG-004", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "a frame whose version was never negotiated fails closed before its kind or body is interpreted",
		Spec:  "PROTOCOL.md §2 rule 1, §13",
	},
	{
		ID: "CONF-NEG-005", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "an unknown operation is answered with a res carrying the stable code unsupported, not a dropped request",
		Spec:  "PROTOCOL.md §2 rule 2",
	},
	{
		ID: "CONF-NEG-006", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "no frame may precede the hello; a connection that opens with anything else is refused",
		Spec:  "PROTOCOL.md §3",
	},
	{
		ID: "CONF-NEG-007", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "every protocol failure carries one of the twelve stable error codes, so a caller can match on the code and never on the message",
		Spec:  "PROTOCOL.md §2",
	},
	{
		ID: "CONF-NEG-008", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "the handshake response names the lease interval and the control plane's grant-signing key, so a node can renew and verify offline",
		Spec:  "PROTOCOL.md §3",
	},
	{
		ID: "CONF-NEG-009", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierCapabilityGated,
		Capability: "controller-epoch",
		Title:      "a peer that negotiated controller-epoch is told a nonzero epoch and persists it on every event, so a superseded controller's decisions are distinguishable",
		Spec:       "PROTOCOL.md §3.2",
	},

	// ---- workspace generation and readiness -----------------------------
	{
		ID: "CONF-WS-001", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "ws.create returns a workspace with an id, a generation and a state drawn from the lifecycle table",
		Spec:  "PROTOCOL.md §5, §5.1",
	},
	{
		ID: "CONF-WS-002", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "replaying a mutating request with the same idempotency key is a no-op that returns the original result",
		Spec:  "PROTOCOL.md §6",
	},
	{
		ID: "CONF-WS-003", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "ws.ready gates claimed: a workspace reported claimed names the node holding it and a nonzero generation",
		Spec:  "PROTOCOL.md §5, §12 item 6",
	},
	{
		ID: "CONF-WS-004", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "ws.get of an id that does not exist is not_found, never an empty success",
		Spec:  "PROTOCOL.md §6",
	},
	{
		ID: "CONF-WS-005", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "a mount path inside a system directory is refused with bad_request at create time",
		Spec:  "PROTOCOL.md §5.2",
	},
	{
		ID: "CONF-WS-006", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "ws.list reports a created workspace at the same generation ws.get reports",
		Spec:  "PROTOCOL.md §6",
	},
	{
		ID: "CONF-WS-007", Version: v1, Since: v1,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: "destroyed is absorbing: after ws.destroy the workspace is gone or terminal and never returns to a serving state",
		Spec:  "PROTOCOL.md §5.1",
	},

	// ---- claims, leases and stale-authority rejection --------------------
	{
		ID: "CONF-AUTH-001", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a grant is issued only for a claimed workspace and binds client, workspace, node and generation",
		Spec:  "PROTOCOL.md §4, §5",
	},
	{
		ID: "CONF-AUTH-002", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a grant request for a workspace that does not exist is refused, not answered with a grant for nothing",
		Spec:  "PROTOCOL.md §4",
	},
	{
		ID: "CONF-AUTH-003", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a node refuses a grant whose generation is not the workspace's current one",
		Spec:  "PROTOCOL.md §4, §12 item 4",
	},
	{
		ID: "CONF-AUTH-004", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a node refuses a grant whose workspace does not match the request it accompanies",
		Spec:  "PROTOCOL.md §4",
	},
	{
		ID: "CONF-AUTH-005", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a node operation presented with no grant at all is refused",
		Spec:  "PROTOCOL.md §7",
	},
	{
		ID: "CONF-AUTH-006", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierRequired,
		Title: "a move increments the generation, and every grant minted under the previous one stops working",
		Spec:  "PROTOCOL.md §4, §5",
	},
	{
		ID: "CONF-AUTH-007", Version: v1, Since: v1,
		Category: CategoryAuthority, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqNodeFault},
		Title:   "an expired lease returns the workspace to pending with its last snapshot, and a node dying is the same event as a node moving",
		Spec:    "PROTOCOL.md §5, §5.1",
	},

	// ---- cursor output, input deduplication and gaps ---------------------
	{
		ID: "CONF-SESS-001", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "seq 0 of every session is the info chunk, so a replay from 0 reconstructs the session header",
		Spec:  "PROTOCOL.md §8 guarantee 1, §12 item 1",
	},
	{
		ID: "CONF-SESS-002", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "the exit chunk is the last chunk and the log is closed after it",
		Spec:  "PROTOCOL.md §8 guarantee 2, §12 item 1",
	},
	{
		ID: "CONF-SESS-003", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "chunk sequence numbers start at 0 and increase by exactly one, with no reordering and no hole",
		Spec:  "PROTOCOL.md §8 guarantee 1",
	},
	{
		ID: "CONF-SESS-004", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "input is deduplicated by iseq: a retried keystroke at or below the last applied sequence is dropped, never typed twice",
		Spec:  "PROTOCOL.md §8, §12 item 3",
	},
	{
		ID: "CONF-SESS-005", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "s.attach from 0 replays the retained log byte-identically through the same path as the live tail",
		Spec:  "PROTOCOL.md §8 guarantee 3",
	},
	{
		ID: "CONF-SESS-006", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "s.wait reports the exit status the exit chunk carried, so a detached client learns the outcome",
		Spec:  "PROTOCOL.md §7, §8",
	},
	{
		ID: "CONF-SESS-007", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqSessionEviction},
		Title:   "a replay from an evicted seq emits a gap chunk naming the lost range, continues from the oldest retained chunk, and does not kill the session",
		Spec:    "PROTOCOL.md §8 guarantee 4, §12 item 2",
	},
	{
		ID: "CONF-SESS-008", Version: v1, Since: v1,
		Category: CategorySession, Tier: TierRequired,
		Title: "an attach past the produced range never fabricates output: the node reports where live begins and emits nothing before it",
		Spec:  "PROTOCOL.md §8 guarantee 3",
	},

	// ---- snapshot identity, restore, move and source retention -----------
	{
		ID: "CONF-SNAP-001", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "the same tree always produces the same artifact id: a snapshot is deterministic, not merely reproducible in principle",
		Spec:  "PROTOCOL.md §10",
	},
	{
		ID: "CONF-SNAP-002", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "an artifact id is the digest of its bytes and GET returns exactly those bytes",
		Spec:  "PROTOCOL.md §10",
	},
	{
		ID: "CONF-SNAP-003", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "a workspace restored from a snapshot serves the files the source held",
		Spec:  "PROTOCOL.md §10",
	},
	{
		ID: "CONF-SNAP-004", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "an ordinary snapshot is labelled live and is never reported authoritative, because it may have observed a changing tree",
		Spec:  "PROTOCOL.md §7, §12 item 12",
	},
	{
		ID: "CONF-SNAP-005", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "an unknown artifact is not found, and a digest held by another tenant is indistinguishable from one that does not exist",
		Spec:  "PROTOCOL.md §10",
	},
	{
		ID: "CONF-SNAP-006", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "an upload is published only after its digest matches the id it was stored under",
		Spec:  "PROTOCOL.md §10",
	},
	{
		ID: "CONF-SNAP-007", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierRequired,
		Title: "a move advances the generation and the destination serves the source tree: the filesystem, identity and policy travel",
		Spec:  "PROTOCOL.md §5, §10",
	},
	{
		ID: "CONF-SNAP-008", Version: v1, Since: v1,
		Category: CategorySnapshot, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqQuiescedSnapshot},
		Title:   "an authoritative snapshot quiesces the workspace, reports consistency quiesced, and only then becomes committable failover state",
		Spec:    "PROTOCOL.md §7, §12 item 12",
	},

	// ---- binding placeholders, destination scoping and leak blocking -----
	{
		ID: "CONF-BIND-001", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings},
		Title:   "the workspace holds a placeholder and never the secret: the environment the node writes carries a reference, and the real value appears nowhere in the tree",
		Spec:    "PROTOCOL.md §9",
	},
	{
		ID: "CONF-BIND-002", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings},
		Title:   "a placeholder aimed at a host its binding does not cover blocks the request and records the decision as leak_blocked, rather than forwarding it",
		Spec:    "PROTOCOL.md §9 rule 1, §12 item 5",
	},
	{
		ID: "CONF-BIND-003", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings},
		Title:   "a destination no rule and no binding covers is denied before any byte is sent upstream",
		Spec:    "PROTOCOL.md §9 rules 2 and 4",
	},
	{
		ID: "CONF-BIND-004", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings},
		Title:   "a destination resolving to a loopback, private, link-local or multicast address is refused unless explicitly allowed, which closes the metadata endpoint by default",
		Spec:    "PROTOCOL.md §9 rule 5",
	},
	{
		ID: "CONF-BIND-005", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings},
		Title:   "no event payload carries the secret a binding holds; the audit record names the decision, not the credential",
		Spec:    "PROTOCOL.md §9, §11",
	},

	// ---- approval durability and request fingerprinting ------------------
	{
		ID: "CONF-APP-001", Version: v1, Since: v1,
		Category: CategoryApproval, Tier: TierRequired,
		Title: "approval.get of an id that does not exist is not_found",
		Spec:  "PROTOCOL.md §6, §6.1",
	},
	{
		ID: "CONF-APP-002", Version: v1, Since: v1,
		Category: CategoryApproval, Tier: TierRequired,
		Title: "approval.list returns pending rows unless a status is named, so a caller polling for work never sees a decided row as outstanding",
		Spec:  "PROTOCOL.md §6",
	},
	{
		ID: "CONF-APP-003", Version: v1, Since: v1,
		Category: CategoryApproval, Tier: TierCapabilityGated,
		Capability: "approvals",
		Prereqs:    []Prerequisite{PrereqBindings, PrereqEgressApproval},
		Title:      "an approve-mode request releases no upstream byte before a durable decision, and the row is idempotent on the request fingerprint",
		Spec:       "PROTOCOL.md §6.1, §9",
	},
	{
		ID: "CONF-APP-004", Version: v1, Since: v1,
		Category: CategoryApproval, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings, PrereqEgressApproval},
		Title:   "a second decision on a decided row is conflict, while a replay carrying the original idempotency key returns the row as decided",
		Spec:    "PROTOCOL.md §6.1",
	},
	{
		ID: "CONF-APP-005", Version: v1, Since: v1,
		Category: CategoryApproval, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqBindings, PrereqEgressApproval, PrereqRestart},
		Title:   "a decision is durable: it survives a control-plane restart and is still the answer the broker gets",
		Spec:    "PROTOCOL.md §6.1",
	},

	// ---- agent inbox, transcript, wake and resume ------------------------
	{
		ID: "CONF-AGT-001", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "agent.create returns a durable agent with a workspace, a derived status and an inbox, so nothing about the conversation lives only in a process",
		Spec:  "PROTOCOL.md §6.1",
	},
	{
		ID: "CONF-AGT-002", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "agent.message appends to the inbox and the message stays there until a turn consumes it",
		Spec:  "PROTOCOL.md §6.1",
	},
	{
		ID: "CONF-AGT-003", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "the inbox is bounded and refuses the excess with resource_exhausted rather than growing without limit",
		Spec:  "PROTOCOL.md §6.1",
	},
	{
		ID: "CONF-AGT-004", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "agent.transcript answers from the durable mirror with a page and a continuation cursor, and never wakes the workspace to do it",
		Spec:  "PROTOCOL.md §6.2",
	},
	{
		ID: "CONF-AGT-005", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqTranscriptEviction},
		Title:   "a transcript read below the oldest retained index names the evicted range as a gap, so a reader never sees a shorter log without being told",
		Spec:    "PROTOCOL.md §6.2",
	},
	{
		ID: "CONF-AGT-006", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "agent.sleep pauses the workspace and a later message wakes it, reporting that it did",
		Spec:  "PROTOCOL.md §6.1",
	},
	{
		ID: "CONF-AGT-007", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "agent.get of an id that does not exist is not_found",
		Spec:  "PROTOCOL.md §6",
	},
	{
		ID: "CONF-AGT-008", Version: v1, Since: v1,
		Category: CategoryAgent, Tier: TierRequired,
		Title: "a destroyed agent is terminal and is never resurrected by a later message, wake or read",
		Spec:  "PROTOCOL.md §6.1",
	},

	// ---- canonical event ordering and export cursor behaviour ------------
	{
		ID: "CONF-EVT-001", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "seq is assigned by the control plane and is the only total order: a tail is strictly increasing in seq",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-002", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "the canonical log is dense: a tail from the oldest retained sequence has no hole",
		Spec:  "PROTOCOL.md §11, §11.1",
	},
	{
		ID: "CONF-EVT-003", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "a stream filter returns one workspace's whole history and nothing else",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-004", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "a state change that emits no event is a bug: creating and claiming a workspace emits ws.created, then ws.claiming, then ws.claimed, in that order",
		Spec:  "PROTOCOL.md §5, §11",
	},
	{
		ID: "CONF-EVT-005", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "lifecycle events use the canonical type names, so a reader of another implementation's log recognises them",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-006", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "a tail from a sequence beyond the head returns nothing and no error, rather than blocking or inventing a page",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-007", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "an out-of-band append is assigned a sequence by the control plane and becomes visible in the canonical order",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-008", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierRequired,
		Title: "an out-of-band append carrying a field the implementation does not know is refused, rather than silently half-applied",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-009", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierExtension, Extension: "notifier",
		Title: "an export cursor advances only after its destination accepts the bounded batch, so retries may duplicate but never skip",
		Spec:  "PROTOCOL.md §11",
	},
	{
		ID: "CONF-EVT-010", Version: v1, Since: v1,
		Category: CategoryEvent, Tier: TierCapabilityGated,
		Prereqs: []Prerequisite{PrereqEventEviction},
		Title:   "a request older than the retention watermark fails with evicted and names the oldest retained sequence; it never silently starts at a newer one",
		Spec:    "PROTOCOL.md §11, §11.1",
	},
}
