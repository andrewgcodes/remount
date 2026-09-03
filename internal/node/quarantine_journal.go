package node

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	quarantinePreparing         = "preparing"
	quarantineQuiesced          = "quiesced"
	quarantineCheckpointed      = "checkpointed"
	quarantineDetached          = "detached"
	quarantinePrepared          = "prepared"
	quarantineDestroyAuthorized = "destroy-authorized"
	quarantineCommitted         = "committed"
	quarantineSuperseded        = "superseded"
)

// durableQuarantine is the non-prunable authority and progress proof for a
// fleet quarantine. Nonterminal records retain the exact request, immutable
// volume pins and checkpoint response needed to resume after restart.
type durableQuarantine struct {
	Request     proto.WSQuarantineReq `cbor:"request"`
	Response    proto.WSQuarantineRes `cbor:"response"`
	State       string                `cbor:"state"`
	CompletedAt int64                 `cbor:"completed_at,omitempty"`
}

func quarantineKey(operation, workspace string) string { return operation + "|" + workspace }

func loadQuarantines(path string) (map[string]durableQuarantine, error) {
	out := map[string]durableQuarantine{}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("node: corrupt quarantine journal: %w", err)
	}
	for key, record := range out {
		if record.Request.OperationID == "" || record.Request.WS == "" || record.Request.Gen == 0 ||
			key != quarantineKey(record.Request.OperationID, record.Request.WS) {
			return nil, fmt.Errorf("node: corrupt quarantine record %q", key)
		}
		switch record.State {
		case quarantinePreparing, quarantineQuiesced, quarantineCheckpointed, quarantineDetached,
			quarantinePrepared, quarantineDestroyAuthorized, quarantineCommitted, quarantineSuperseded:
		default:
			return nil, fmt.Errorf("node: corrupt quarantine state %q for %q", record.State, key)
		}
	}
	return out, nil
}

func sameQuarantineRequest(a, b proto.WSQuarantineReq) bool {
	return string(proto.MustMarshal(a)) == string(proto.MustMarshal(b))
}

func (n *Node) persistQuarantinesLocked() error {
	b, err := proto.Marshal(n.quarantines)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(n.quarantinePath), ".quarantines-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0o600); err == nil {
		var written int
		written, err = tmp.Write(b)
		if err == nil && written != len(b) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, n.quarantinePath)
	}
	if err == nil {
		err = syncParentDir(filepath.Dir(n.quarantinePath))
	}
	return err
}

func (n *Node) pruneCommittedQuarantinesLocked(now time.Time) {
	retention := n.opts.MutationRetention
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	cutoff := now.Add(-retention).UnixMilli()
	for key, record := range n.quarantines {
		if (record.State == quarantineCommitted || record.State == quarantineSuperseded) && record.CompletedAt > 0 && record.CompletedAt < cutoff {
			delete(n.quarantines, key)
		}
	}
}

func (n *Node) beginQuarantine(req proto.WSQuarantineReq) (durableQuarantine, bool, error) {
	key := quarantineKey(req.OperationID, req.WS)
	n.quarantineMu.Lock()
	defer n.quarantineMu.Unlock()
	if existing, ok := n.quarantines[key]; ok {
		if !sameQuarantineRequest(existing.Request, req) {
			return durableQuarantine{}, true, proto.Err(proto.CodeConflict, "quarantine retry does not match durable operation")
		}
		return existing, true, nil
	}
	n.pruneCommittedQuarantinesLocked(time.Now())
	previous := make(map[string]durableQuarantine)
	for oldKey, old := range n.quarantines {
		if old.Request.WS != req.WS || old.State == quarantineCommitted || old.State == quarantineSuperseded {
			continue
		}
		if old.State != quarantinePrepared || (old.Response.Action == proto.FleetActionDestroy && old.Response.Fenced) {
			return durableQuarantine{}, false, proto.Err(proto.CodeConflict,
				"workspace has another active quarantine operation")
		}
		previous[oldKey] = old
		old.State = quarantineSuperseded
		old.CompletedAt = time.Now().UnixMilli()
		n.quarantines[oldKey] = old
	}
	limit := n.opts.MaxMutationRecords
	if limit <= 0 {
		limit = 10_000
	}
	if len(n.quarantines) >= limit {
		metrics.QuarantineJournalQuotaRejected.Inc()
		return durableQuarantine{}, false, proto.Err(proto.CodeResourceExhausted,
			"node quarantine journal contains %d records", limit)
	}
	record := durableQuarantine{Request: req, State: quarantinePreparing}
	n.quarantines[key] = record
	if err := n.persistQuarantinesLocked(); err != nil {
		delete(n.quarantines, key)
		for oldKey, old := range previous {
			n.quarantines[oldKey] = old
		}
		return durableQuarantine{}, false, proto.Err(proto.CodeInternal, "persist quarantine intent before fencing: %v", err)
	}
	return record, false, nil
}

func (n *Node) quarantineRecord(operation, workspace string) (durableQuarantine, bool) {
	n.quarantineMu.Lock()
	defer n.quarantineMu.Unlock()
	record, ok := n.quarantines[quarantineKey(operation, workspace)]
	return record, ok
}

func (n *Node) advanceQuarantine(operation, workspace, from, to string, response proto.WSQuarantineRes) error {
	key := quarantineKey(operation, workspace)
	n.quarantineMu.Lock()
	defer n.quarantineMu.Unlock()
	record, ok := n.quarantines[key]
	if !ok {
		return proto.Err(proto.CodeConflict, "quarantine operation has no durable intent")
	}
	if record.State == to {
		return nil
	}
	if record.State != from {
		return proto.Err(proto.CodeConflict, "quarantine operation changed from %s", from)
	}
	previous := record
	record.State = to
	record.Response = response
	if to == quarantineCommitted {
		record.CompletedAt = time.Now().UnixMilli()
	}
	n.quarantines[key] = record
	if err := n.persistQuarantinesLocked(); err != nil {
		n.quarantines[key] = previous
		return proto.Err(proto.CodeInternal, "persist quarantine state %s: %v", to, err)
	}
	if to == quarantinePrepared {
		metrics.QuarantineProofsPrepared.Inc()
	}
	return nil
}
