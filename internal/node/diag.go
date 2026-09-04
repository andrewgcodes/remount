package node

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"time"

	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// Diag reports this machine's deep state: what it is holding, how big each
// workspace is on disk, every session's log position, and anything it can see
// that looks wrong. It is the bottom of the three inspection levels, the one
// that touches the actual filesystem.
func (n *Node) Diag(ctx context.Context) *proto.NodeDiag {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	info := n.status().Info
	d := &proto.NodeDiag{
		Node:            n.id,
		ControllerEpoch: n.currentControllerEpoch(),
		Info:            info,
		Now:             time.Now().UnixMilli(),
		Uptime:          int64(time.Since(n.started).Seconds()),
		DataDir:         n.opts.DataDir,
		Goroutines:      runtime.NumGoroutine(),
		HeapBytes:       ms.HeapAlloc,
		Metrics:         metrics.Default.Snapshot(),
	}
	d.DiskFree, d.DiskTotal = diskSpace(n.opts.DataDir)
	artifactStats := n.store.Stats()
	d.Artifacts = artifactStats.Objects
	d.ArtifactBytes = artifactStats.Bytes
	d.ArtifactReservedBytes = artifactStats.ReservedBytes
	d.ArtifactReservedObjects = artifactStats.ReservedObjects
	d.ArtifactMaxBytes = artifactStats.MaxBytes
	d.ArtifactMaxObjects = artifactStats.MaxObjects
	sessionStats := n.sessions.Stats()
	d.SessionsRetained = sessionStats.Sessions
	d.SessionsActive = sessionStats.Active
	d.SessionsMax = sessionStats.MaxSessions
	d.SessionsActiveMax = sessionStats.MaxActive
	d.SessionsPerWorkspaceMax = sessionStats.MaxSessionsPerWorkspace
	d.SessionsPerPrincipalMax = sessionStats.MaxSessionsPerPrincipal
	d.SessionMemoryBytes = n.opts.SessionMemoryBytes
	d.SessionSpillBytes = n.opts.SessionSpillBytes
	d.SessionMemoryChunks = n.opts.SessionMaxMemoryChunks
	d.SessionChunkBytes = n.opts.SessionMaxChunkBytes
	d.RequestsActiveMax = n.opts.MaxConcurrentRequests
	d.ConnectorMaxBytes = n.opts.MaxConnectorCacheBytes
	d.ConnectorScopeMaxBytes = n.opts.MaxConnectorWorkspaceBytes
	d.ConnectorObjectMaxBytes = n.opts.MaxConnectorObjectBytes
	d.ConnectorMaxObjects = n.opts.MaxConnectorObjects
	d.ConnectorScopeMaxObjects = n.opts.MaxConnectorWorkspaceObjects
	n.mutationMu.Lock()
	d.MutationRecords = len(n.mutations)
	n.mutationMu.Unlock()
	d.MutationRecordsMax = n.opts.MaxMutationRecords
	d.SnapshotsActive = len(n.snapshotSlots)
	d.SnapshotsActiveMax = cap(n.snapshotSlots)
	if n.volumes != nil {
		stats := n.volumes.Stats()
		d.Volumes, d.VolumeAttachments, d.VolumeQuotaRejections = stats.Volumes, stats.Attachments, stats.QuotaRejections
		d.VolumeSourceBytes, d.VolumeSourceEntries, _ = volumeSourceUsage(filepath.Join(n.opts.DataDir, "volumes", "sources"))
		d.VolumeSourceMaxBytes = n.opts.MaxVolumeSourceBytes
		d.VolumeSourceMaxEntries = n.opts.MaxVolumeSourceEntries
	}

	// Snapshot each workspace's mutable fields under the lock rather than
	// carrying live *ws pointers past the unlock. The renew loop writes
	// LeaseUntil and the authorization fields on exactly these rows, so
	// reading them afterwards is a data race — the "live pointer escaping the
	// mutex" shape AGENTS.md records as one of the three bugs only the race
	// detector found. The handle and broker are set once at claim time and are
	// carried deliberately, because the work below them does I/O and must not
	// hold the lock.
	n.mu.Lock()
	held := make([]wsDiagView, 0, len(n.workspaces))
	for _, w := range n.workspaces {
		held = append(held, wsDiagView{
			ID:         w.ID,
			Generation: w.Generation,
			Tenant:     w.Tenant,
			LeaseUntil: w.LeaseUntil,
			Volumes:    append([]proto.VolumeMount(nil), w.Spec.Volumes...),
			leases:     append([]proto.BindingLease(nil), w.leases...),
			handle:     w.handle,
			broker:     w.broker,
		})
	}
	materializing := make([]string, 0, len(n.materializing))
	for id := range n.materializing {
		if _, done := n.workspaces[id]; !done {
			materializing = append(materializing, id)
		}
	}
	n.mu.Unlock()

	for _, w := range held {
		wd := proto.WSDiag{
			ID:        w.ID,
			Gen:       w.Generation,
			Backend:   w.handle.Backend(),
			Root:      workspace.MountPathOf(w.handle),
			LeaseEnds: w.LeaseUntil,
			Volumes:   append([]proto.VolumeMount(nil), w.Volumes...),
		}
		for _, mount := range w.Volumes {
			if n.volumes == nil {
				d.Findings = append(d.Findings, proto.Finding{Severity: "error", Check: "node.diag_unavailable", Subject: w.ID, Detail: "volume mount verification is unavailable"})
				continue
			}
			detail, err := n.volumes.Inspect(ctx, w.Tenant, mount.ID)
			if err != nil {
				d.Findings = append(d.Findings, proto.Finding{Severity: "error", Check: "volume.inspect", Subject: w.ID, Detail: err.Error()})
				continue
			}
			verified := false
			for _, attachment := range detail.Attachments {
				if attachment.Workspace == w.ID && attachment.Generation == w.Generation && attachment.Path == mount.Path && attachment.VolumeVersion == mount.Version && attachment.Artifact == mount.Artifact && attachment.ReadOnly && attachment.MountPresent && attachment.MountVerified && attachment.State == "attached" {
					verified = true
					break
				}
			}
			if !verified {
				d.Findings = append(d.Findings, proto.Finding{Severity: "error", Check: "volume.mount", Subject: w.ID, Detail: fmt.Sprintf("%s at %s is not verified read-only for generation %d", mount.ID, mount.Path, w.Generation)})
			}
		}
		if host, ok := workspace.HostFileSystemOf(w.handle); ok {
			wd.Bytes, wd.Files = dirUsage(host.Root())
		}
		if w.broker != nil {
			wd.Broker = w.broker.BaseURL()
		}
		for _, l := range w.leases {
			wd.Bindings = append(wd.Bindings, l.ID)
			if l.ExpiresAt != 0 && time.Now().UnixMilli() > l.ExpiresAt {
				d.Findings = append(d.Findings, proto.Finding{
					Severity: "warn", Check: "binding.lease_expired", Subject: w.ID,
					Detail: fmt.Sprintf("lease for %s expired at %s; the broker is failing closed",
						l.ID, time.UnixMilli(l.ExpiresAt).Format(time.RFC3339)),
					Hint: "the node renews on a timer; persistent expiry means it cannot reach the control plane",
				})
			}
		}
		for _, s := range n.sessions.List(w.ID) {
			st := nodeSessionStatus(s)
			wd.Sessions = append(wd.Sessions, st)
			// A non-zero oldest means output has already been evicted, so a
			// client that reconnects far enough behind will see a gap.
			if st.Oldest > 0 {
				d.Findings = append(d.Findings, proto.Finding{
					Severity: "info", Check: "session.evicted_output", Subject: s.ID,
					Detail: fmt.Sprintf("output below seq %d is gone; a replay from earlier gets a gap chunk", st.Oldest),
					Hint:   "raise the session log limits if clients reconnect after long gaps",
				})
			}
			if st.UnavailableTier != "" {
				d.Findings = append(d.Findings, proto.Finding{
					Severity: "error", Check: "session.tier_unavailable", Subject: s.ID,
					Detail: fmt.Sprintf("%s log tier is unavailable; affected replay returns a named gap", st.UnavailableTier),
				})
			}
		}
		d.Workspaces = append(d.Workspaces, wd)
	}

	for _, id := range materializing {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "info", Check: "workspace.materializing", Subject: id,
			Detail: "claimed and currently restoring; not serving requests yet",
		})
	}
	if d.DiskTotal > 0 {
		if pct := float64(d.DiskFree) / float64(d.DiskTotal) * 100; pct < 10 {
			d.Findings = append(d.Findings, proto.Finding{
				Severity: "error", Check: "node.disk_low",
				Detail: fmt.Sprintf("%.1f%% of %s free", pct, humanBytes(d.DiskTotal)),
				Hint:   "snapshots and session spill both write here; a full disk fails restores",
			})
		}
	}
	return d
}

// VerifyArtifacts re-hashes every artifact this node has cached and reports
// the damaged ones. This reads every byte, so it is the honest check rather
// than the fast one.
func (n *Node) VerifyArtifacts() []proto.Finding {
	ids, err := n.store.List()
	if err != nil {
		return []proto.Finding{{Severity: "error", Check: "artifact.list", Detail: err.Error()}}
	}
	var out []proto.Finding
	bad := 0
	for _, id := range ids {
		if err := n.store.Verify(id); err != nil {
			bad++
			out = append(out, proto.Finding{
				Severity: "error", Check: "artifact.digest", Subject: id, Detail: err.Error(),
				Hint: "the cached blob no longer matches its content address; delete it and refetch",
			})
		}
	}
	out = append(out, proto.Finding{
		Severity: "info", Check: "artifact.verified",
		Detail: fmt.Sprintf("node re-hashed %d cached artifacts, %d damaged", len(ids), bad),
	})
	return out
}

// dirUsage totals a tree. It is deliberately a plain walk: a workspace is
// usually small, and an approximate number that is always available beats an
// exact one that needs a background job.
func dirUsage(root string) (bytes int64, files int) {
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			bytes += info.Size()
			files++
		}
		return nil
	})
	return bytes, files
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// wsDiagView is one workspace's diagnostic state, copied while the node lock is
// held. It exists so the reporting below — which does filesystem and volume
// I/O and therefore must run unlocked — reads a consistent snapshot instead of
// fields another goroutine is still writing.
type wsDiagView struct {
	ID         string
	Generation uint64
	Tenant     string
	LeaseUntil int64
	Volumes    []proto.VolumeMount
	leases     []proto.BindingLease
	handle     workspace.Handle
	broker     *broker.Broker
}
