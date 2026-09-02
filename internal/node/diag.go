package node

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"time"

	"golang.org/x/sys/unix"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
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
		Node:       n.id,
		Info:       info,
		Now:        time.Now().UnixMilli(),
		Uptime:     int64(time.Since(n.started).Seconds()),
		DataDir:    n.opts.DataDir,
		Goroutines: runtime.NumGoroutine(),
		HeapBytes:  ms.HeapAlloc,
		Metrics:    metrics.Default.Snapshot(),
	}
	d.DiskFree, d.DiskTotal = diskSpace(n.opts.DataDir)
	if ids, err := n.store.List(); err == nil {
		d.Artifacts = len(ids)
	}

	n.mu.Lock()
	held := make([]*ws, 0, len(n.workspaces))
	for _, w := range n.workspaces {
		held = append(held, w)
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
			Root:      w.handle.FS().Root(),
			LeaseEnds: w.LeaseUntil,
		}
		wd.Bytes, wd.Files = dirUsage(w.handle.FS().Root())
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
			st := proto.SessionStatus{
				Info: s.Info, Exited: s.Exited(), Exit: s.ExitInfo(),
				Next: s.Log.Next(), Oldest: s.Log.Oldest(),
			}
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

func diskSpace(path string) (free, total int64) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize)
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
