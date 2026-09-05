// Package metrics is the low-level counter surface: cheap atomic counters and
// gauges, rendered as Prometheus text.
//
// Deliberately dependency-free. A metrics library would be a bigger import than
// the thing it measures, and the whole surface here is "add one" and "set".
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds counters and gauges by name.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*Counter
	gauges   map[string]*Gauge
	funcs    map[string]func() float64
	help     map[string]string
}

// Counter only ever increases.
type Counter struct{ v atomic.Uint64 }

// Add increments by n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Inc increments by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value reads it.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge goes up and down.
type Gauge struct{ v atomic.Int64 }

// Set replaces the value.
func (g *Gauge) Set(n int64) { g.v.Store(n) }

// Add adds a delta, which may be negative.
func (g *Gauge) Add(n int64) { g.v.Add(n) }

// Value reads it.
func (g *Gauge) Value() int64 { return g.v.Load() }

// New creates an empty registry.
func New() *Registry {
	return &Registry{
		counters: map[string]*Counter{},
		gauges:   map[string]*Gauge{},
		funcs:    map[string]func() float64{},
		help:     map[string]string{},
	}
}

// Default is the process-wide registry.
var Default = New()

// Counter returns (creating if needed) a named counter.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.RLock()
	c := r.counters[name]
	r.mu.RUnlock()
	if c != nil {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c = r.counters[name]; c == nil {
		c = &Counter{}
		r.counters[name] = c
		r.help[name] = help
	}
	return c
}

// Gauge returns (creating if needed) a named gauge.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.RLock()
	g := r.gauges[name]
	r.mu.RUnlock()
	if g != nil {
		return g
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if g = r.gauges[name]; g == nil {
		g = &Gauge{}
		r.gauges[name] = g
		r.help[name] = help
	}
	return g
}

// GaugeFunc registers a gauge computed on scrape, for things that already have
// a source of truth elsewhere (open sessions, connected peers).
func (r *Registry) GaugeFunc(name, help string, f func() float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.funcs[name] = f
	r.help[name] = help
}

// Counter and Gauge on the default registry.
func Count(name, help string) *Counter { return Default.Counter(name, help) }
func Measure(name, help string) *Gauge { return Default.Gauge(name, help) }

// Write renders the registry as Prometheus text format.
func (r *Registry) Write(sb *strings.Builder) {
	r.mu.RLock()
	type sample struct {
		name string
		typ  string
		val  float64
	}
	var out []sample
	for n, c := range r.counters {
		out = append(out, sample{n, "counter", float64(c.Value())})
	}
	for n, g := range r.gauges {
		out = append(out, sample{n, "gauge", float64(g.Value())})
	}
	for n, f := range r.funcs {
		out = append(out, sample{n, "gauge", f()})
	}
	help := make(map[string]string, len(r.help))
	for k, v := range r.help {
		help[k] = v
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	for _, s := range out {
		if h := help[s.name]; h != "" {
			fmt.Fprintf(sb, "# HELP %s %s\n", s.name, h)
		}
		fmt.Fprintf(sb, "# TYPE %s %s\n", s.name, s.typ)
		fmt.Fprintf(sb, "%s %g\n", s.name, s.val)
	}
}

// Snapshot returns every metric as a map, for the CLI and for tests.
func (r *Registry) Snapshot() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]float64{}
	for n, c := range r.counters {
		out[n] = float64(c.Value())
	}
	for n, g := range r.gauges {
		out[n] = float64(g.Value())
	}
	for n, f := range r.funcs {
		out[n] = f()
	}
	return out
}

// ---------------------------------------------------------------------------
// The metrics Remount actually keeps. Named here so they are one list rather
// than string literals scattered through the code.
// ---------------------------------------------------------------------------

var (
	FramesRouted            = Count("remount_frames_routed_total", "frames forwarded between peers by the relay")
	FramesDropped           = Count("remount_frames_dropped_total", "frames discarded because the destination was gone")
	BytesRouted             = Count("remount_relay_bytes_total", "approximate frame bytes forwarded by the relay")
	PeersConnected          = Measure("remount_peers_connected", "peers currently connected to the relay")
	ControlRequestsActive   = Measure("remount_control_requests_active", "control-plane requests currently executing")
	ControlRequestsRejected = Count("remount_control_requests_rejected_total", "control-plane requests rejected by admission control")
	NodeRequestsActive      = Measure("remount_node_requests_active", "node requests currently executing")
	NodeRequestsRejected    = Count("remount_node_requests_rejected_total", "node requests rejected by admission control")

	WSCreated              = Count("remount_workspaces_created_total", "workspaces created")
	WorkspaceQuotaRejected = Count("remount_workspace_quota_rejections_total", "workspace creates rejected by tenant or subject capacity limits")
	BaseQuotaRejected      = Count("remount_base_quota_rejections_total", "base creates rejected by the per-tenant pinned-base limit")
	VolumeQuotaRejected    = Count("remount_volume_quota_rejections_total", "volume creates rejected by the per-tenant shared-volume limit")
	QueueQuotaRejected     = Count("remount_queue_quota_rejections_total", "queue creates rejected by the per-tenant queue limit")
	AgentsCreated          = Count("remount_agents_created_total", "agents created, including forks")
	AgentsForked           = Count("remount_agents_forked_total", "agents created by fork")
	AgentsDestroyed        = Count("remount_agents_destroyed_total", "agents destroyed")
	AgentsFailed           = Count("remount_agents_failed_total", "agents that reached failed")
	AgentChildrenFinished  = Count("remount_agent_children_finished_total", "finished child agents reported to their parent")
	AgentsFinished         = Count("remount_agents_finished_total", "agents that reached finished by policy")
	AgentsSlept            = Count("remount_agents_slept_total", "agent workspaces put to sleep by request or policy")
	AgentsWoken            = Count("remount_agents_woken_total", "agent workspaces woken by a message, approval or preview")
	AgentMessages          = Count("remount_agent_messages_total", "messages accepted into agent inboxes")
	AgentInboxRejected     = Count("remount_agent_inbox_rejections_total", "messages refused because the inbox was full")
	AgentQuotaRejected     = Count("remount_agent_quota_rejections_total", "agent creates rejected by the per-tenant agent limit")
	AgentRunsStarted       = Count("remount_agent_runs_started_total", "harness launches acknowledged by a node")
	AgentRunsFinished      = Count("remount_agent_runs_finished_total", "harness runs that ended for any reason")
	AgentTranscriptRecords = Count("remount_agent_transcript_records_total", "transcript records mirrored into the control plane")
	AgentTranscriptBytes   = Count("remount_agent_transcript_bytes_total", "transcript bytes mirrored into the control plane")
	AgentTranscriptEvicted = Count("remount_agent_transcript_evicted_total", "transcript records evicted from an agent's mirror by its byte budget")
	// AgentTranscriptReportsDropped counts transcript reports a node dropped
	// from a run's bounded report queue while the control plane was
	// unreachable; each drop is announced to the mirror as a gap chunk.
	AgentTranscriptReportsDropped  = Count("remount_agent_transcript_reports_dropped_total", "transcript reports dropped from a node's bounded report queue (reported to the mirror as a gap)")
	HTTPClients                    = Measure("remount_http_api_clients", "in-process SDK clients held for HTTP API credentials")
	HTTPClientsRejected            = Count("remount_http_api_clients_rejected_total", "HTTP API requests refused because the credential pool was full")
	HTTPRequests                   = Count("remount_http_api_requests_total", "HTTP API requests served")
	WebhookAccepted                = Count("remount_webhooks_accepted_total", "webhook events appended")
	WebhookRejected                = Count("remount_webhooks_rejected_total", "webhook requests refused for a bad credential or signature")
	NotificationDeliveries         = Count("remount_notification_deliveries_total", "outbound notification batches accepted by their destination")
	NotificationDeliveryFailures   = Count("remount_notification_delivery_failures_total", "outbound notification batches that exhausted delivery attempts")
	NotificationDeadLetters        = Count("remount_notification_dead_letters_total", "failed outbound notification batches durably dead-lettered")
	NotificationDeadLettersPruned  = Count("remount_notification_dead_letters_pruned_total", "expired outbound notification dead letters removed")
	NotificationUnavailable        = Count("remount_notification_unavailable_total", "outbound notification workers or durable fallbacks reported unavailable")
	HTTPProxyRequests              = Count("remount_http_api_proxy_requests_total", "requests forwarded to a port inside a workspace")
	HTTPTerminalAttaches           = Count("remount_http_api_terminal_attaches_total", "terminal WebSocket attaches")
	HTTPTranscriptStreams          = Count("remount_http_api_transcript_streams_total", "transcript SSE and WebSocket streams opened")
	ApprovalsPending               = Count("remount_approvals_pending_total", "approvals parked for a human")
	ApprovalsDecided               = Count("remount_approvals_decided_total", "approvals answered")
	ApprovalsExpired               = Count("remount_approvals_expired_total", "approvals whose run ended before a decision")
	ApprovalQuotaRejected          = Count("remount_approval_quota_rejections_total", "approvals refused by the per-agent pending limit")
	BudgetReservations             = Count("remount_budget_reservations_total", "authoritative budget reservations durably admitted")
	BudgetSettlements              = Count("remount_budget_settlements_total", "authoritative budget reservations durably settled")
	BudgetDenied                   = Count("remount_budget_denials_total", "broker requests denied by an authoritative budget")
	BudgetExpired                  = Count("remount_budget_expired_total", "ambiguous budget reservations conservatively expired")
	BudgetUnmetered                = Count("remount_budget_unmetered_total", "admitted requests whose token or price usage is explicitly unavailable")
	PoolScaleActions               = Count("remount_pool_scale_actions_total", "provider-backed node pool creates and destroys")
	PoolProvisionFailures          = Count("remount_pool_provision_failures_total", "provider-backed pool inventory or mutation failures")
	PoolMachines                   = Measure("remount_pool_machines", "last durably observed provider-backed pool machine count")
	WSClaims                       = Count("remount_workspace_claims_total", "successful claims, including re-adoptions")
	WSClaimDenied                  = Count("remount_workspace_claims_denied_total", "claims refused as ineligible or already held")
	TenantPlacementDenied          = Count("remount_tenant_placement_denials_total", "node claims refused because authoritative node and workspace tenants differ")
	WSLeaseExpired                 = Count("remount_workspace_lease_expired_total", "leases that expired, returning a workspace to pending")
	WSMoved                        = Count("remount_workspaces_moved_total", "explicit moves")
	WSDestroyed                    = Count("remount_workspaces_destroyed_total", "workspaces destroyed")
	FleetOperations                = Count("remount_fleet_operations_total", "durable fleet containment operations requested")
	FleetOperationsCompleted       = Count("remount_fleet_operations_completed_total", "fleet containment operations reaching a terminal state")
	FleetTargetsFenced             = Count("remount_fleet_targets_fenced_total", "workspace targets authoritatively fenced by fleet operations")
	ReleaseJournalQuotaRejected    = Count("remount_release_journal_quota_rejections_total", "workspace releases rejected by the durable lifecycle-journal limit")
	QuarantineJournalQuotaRejected = Count("remount_quarantine_journal_quota_rejections_total", "fleet quarantines rejected by the durable lifecycle-journal limit")
	ReleaseAbortsPublished         = Count("remount_release_aborts_published_total", "restored release sources published after durable control authorization")
	QuarantineProofsPrepared       = Count("remount_quarantine_proofs_prepared_total", "durable fleet quarantine phase-one proofs prepared")

	SessionsOpened       = Count("remount_sessions_opened_total", "sessions opened on this node")
	SessionsExited       = Count("remount_sessions_exited_total", "sessions that finished")
	SessionQuotaRejected = Count("remount_session_quota_rejections_total", "session opens rejected by node or workspace capacity limits")
	ChunksEmitted        = Count("remount_session_chunks_total", "output chunks appended to session logs")
	BytesEmitted         = Count("remount_session_bytes_total", "output bytes appended to session logs")
	ChunksEvicted        = Count("remount_session_chunks_evicted_total", "chunks dropped from memory into spill or discarded")
	GapsReported         = Count("remount_session_gaps_total", "replay gaps reported to clients; each is data a client could not get")
	InputsDropped        = Count("remount_session_inputs_deduped_total", "duplicate inputs dropped by sequence")

	SnapshotsTaken        = Count("remount_snapshots_total", "snapshots created")
	SnapshotBytes         = Count("remount_snapshot_bytes_total", "bytes written into snapshots")
	SnapshotBytesUploaded = Count("remount_snapshot_bytes_uploaded_total", "plaintext chunk and manifest bytes uploaded after deduplication")
	SnapshotLogicalBytes  = Count("remount_snapshot_logical_bytes_total", "logical plaintext bytes considered for chunked snapshots")
	SnapshotQuotaRejected = Count("remount_snapshot_quota_rejections_total", "explicit snapshots rejected by node concurrency or frequency admission")
	RestoresDone          = Count("remount_restores_total", "workspaces restored from a snapshot")
	ArtifactMiss          = Count("remount_artifact_digest_mismatch_total", "artifacts rejected because the digest did not match; each one is corruption")
	ArtifactQuotaRejected = Count("remount_artifact_quota_rejections_total", "artifact uploads rejected before store byte or object limits could be exceeded")
	ArtifactGCRuns        = Count("remount_artifact_gc_runs_total", "reference-aware artifact garbage-collection passes")
	ArtifactGCErrors      = Count("remount_artifact_gc_errors_total", "artifact garbage-collection passes that encountered an error")
	ArtifactGCObjects     = Count("remount_artifact_gc_objects_total", "unreferenced artifacts removed by reference-aware garbage collection")
	ArtifactGCBytes       = Count("remount_artifact_gc_bytes_total", "bytes reclaimed by reference-aware artifact garbage collection")
	OrphanSpillsRemoved   = Count("remount_session_orphan_spills_removed_total", "orphaned session spill files removed during node startup")

	CredUsed                      = Count("remount_credentials_substituted_total", "credential substitutions at the egress broker")
	EgressAllow                   = Count("remount_egress_allowed_total", "requests allowed without a credential")
	EgressDeny                    = Count("remount_egress_denied_total", "requests denied by policy")
	LeakBlocked                   = Count("remount_egress_leak_blocked_total", "placeholders sent to an unbound host; each is an exfiltration attempt")
	LeaseExpired                  = Count("remount_binding_lease_expired_total", "requests refused because a binding lease had expired")
	PackageUpstreamRequests       = Count("remount_connector_package_upstream_requests_total", "managed package requests sent to registries")
	PackageCacheHits              = Count("remount_connector_package_cache_hits_total", "managed package requests served from a workspace-authorized immutable reference")
	PackageResponseBytes          = Count("remount_connector_package_response_bytes_total", "bytes staged by the managed package connector")
	PackageFailures               = Count("remount_connector_package_failures_total", "managed package requests rejected after connector dispatch")
	PackageQuotaRejected          = Count("remount_connector_package_quota_rejections_total", "managed package requests rejected by cache capacity limits")
	PackageCacheIntegrityFailures = Count("remount_connector_package_cache_integrity_failures_total", "workspace cache references whose bytes or metadata failed digest verification and were invalidated before release")
	PackageStagingReclaimed       = Count("remount_connector_package_staging_reclaimed_total", "abandoned package staging files removed during connector store startup")
	GitUpstreamRequests           = Count("remount_connector_git_upstream_requests_total", "git smart-HTTP requests sent upstream by the managed git connector")
	GitFailures                   = Count("remount_connector_git_failures_total", "git smart-HTTP requests rejected after connector dispatch")
	RepoClones                    = Count("remount_repo_clones_total", "repositories cloned during materialization")
	RepoCloneFailures             = Count("remount_repo_clone_failures_total", "materializations that failed because the clone failed")

	EventsAppended             = Count("remount_events_total", "events appended to the canonical log")
	EventsPruned               = Count("remount_events_pruned_total", "events removed from the retained sequence prefix")
	NodeEventPostFailures      = Count("remount_node_event_post_failures_total", "attempts to forward a node's events to control that failed; each is then retried, re-issued, or dropped, and the last two are counted below")
	NodeEventsResequenced      = Count("remount_node_events_resequenced_total", "node events re-issued under new producer sequences after control reported that a sequence already named a different event")
	NodeEventsDropped          = Count("remount_node_events_dropped_total", "node events control rejected outright, which the node stopped retrying so the events behind them could be delivered")
	EventGCRuns                = Count("remount_event_gc_runs_total", "completed event-retention passes")
	EventGCErrors              = Count("remount_event_gc_errors_total", "event-retention passes that failed")
	TimersFired                = Count("remount_timers_fired_total", "durable timers that fired")
	TimerQuotaRejected         = Count("remount_timer_quota_rejections_total", "wake timers rejected by durable control-record limits")
	MutationQuotaRejected      = Count("remount_mutation_quota_rejections_total", "idempotent mutations rejected by durable control-record limits")
	TimersPruned               = Count("remount_timers_pruned_total", "expired fired timers removed by control-record retention")
	MutationsPruned            = Count("remount_mutations_pruned_total", "expired idempotency results removed by control-record retention")
	WorkspacesPruned           = Count("remount_workspace_tombstones_pruned_total", "destroyed workspace records removed by control-record retention")
	FleetOperationsPruned      = Count("remount_fleet_operations_pruned_total", "terminal fleet operations removed by control-record retention")
	AssignmentsPruned          = Count("remount_assignment_records_pruned_total", "historical assignment records removed by control-record retention")
	EventSubscribersDropped    = Count("remount_event_subscribers_dropped_total", "events dropped for a subscriber that fell behind; the subscription stays open so the loss shows as a sequence gap")
	EventTailQuotaRejected     = Count("remount_event_tail_quota_rejections_total", "follow subscriptions refused because one peer reached its concurrent tail limit")
	SessionLogsPruned          = Count("remount_session_logs_pruned_total", "expired session log records removed by retention, each with a session.log.deleted event")
	SessionLogQuotaRejected    = Count("remount_session_log_quota_rejections_total", "session log commits refused because the tenant retained-record limit was reached, counting reserved in-flight publications")
	RecordGCRuns               = Count("remount_control_record_gc_runs_total", "completed control-record retention passes")
	RecordGCErrors             = Count("remount_control_record_gc_errors_total", "control-record retention passes that failed")
	ControllerFenced           = Count("remount_controller_fenced_total", "control decisions refused because the writer lease or epoch was stale")
	ControllerShipFailures     = Count("remount_controller_ship_failures_total", "control recovery-point publications that failed")
	ControllerEpoch            = Measure("remount_controller_epoch", "current fenced controller writer epoch")
	ControllerReplicationLagMS = Measure("remount_controller_replication_lag_ms", "milliseconds since the last committed recovery point")
	ControllerReconciling      = Measure("remount_controller_reconciling", "1 while a promoted controller reconciles node-authoritative state")

	TenantQuotaExceeded       = Count("remount_tenant_quota_exceeded_total", "tenant admissions refused by a durable quota or reservation limit")
	TenantResidencyDenied     = Count("remount_tenant_residency_denied_total", "placements and admissions refused because a node was outside the tenant residency policy")
	TenantRetentionEnforced   = Count("remount_tenant_retention_enforced_total", "completed per-tenant retention passes that removed or redacted something")
	TenantRetentionFailed     = Count("remount_tenant_retention_failed_total", "per-tenant retention passes that could not complete")
	TenantEventsRedacted      = Count("remount_tenant_events_redacted_total", "canonical events whose payload was removed by per-tenant event retention")
	TenantArtifactsCollected  = Count("remount_tenant_artifacts_collected_total", "unreferenced artifacts removed early by a per-tenant artifact retention policy")
	TenantRetentionUnenforced = Measure("remount_tenant_retention_unenforced", "tenants whose retention policy cannot currently be enforced")
	AuditExports              = Count("remount_audit_exports_total", "signed compliance bundles produced by audit export")
	AuditExportDenied         = Count("remount_audit_exports_denied_total", "audit exports refused by tenant isolation, authorization or range validation")
	AuditExportGaps           = Count("remount_audit_export_gaps_total", "audit exports refused because the requested range is no longer complete")
)

func init() {
	Default.GaugeFunc("remount_snapshot_dedupe_ratio", "fraction of logical chunked snapshot bytes avoided by content deduplication", func() float64 {
		logical := SnapshotLogicalBytes.Value()
		if logical == 0 {
			return 0
		}
		uploaded := SnapshotBytesUploaded.Value()
		if uploaded >= logical {
			return 0
		}
		return 1 - float64(uploaded)/float64(logical)
	})
}
