package control

import (
	"context"
	"regexp"
	"sort"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

var sleepUntilPattern = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

func queueResource(q *proto.Queue) Resource {
	return Resource{Kind: "queue", ID: q.ID, Tenant: q.Tenant, Owner: q.Owner}
}

type mutationQueueResult struct {
	ID string `cbor:"id"`
}

// queueEvent is attributed to the workspace the queue drives so it lands in
// the workspace's stream next to run.started/run.finished.
func (c *Control) queueEvent(typ string, q *proto.Queue, ws *proto.Workspace, principal string, payload map[string]any) *proto.Event {
	payload["queue"] = q.ID
	e := c.newEvent(typ, q.WS, principal, "", payload)
	if ws != nil {
		return stampWS(e, ws)
	}
	e.Tenant, e.Workspace = q.Tenant, q.WS
	return e
}

// queueCreate records a task list against a workspace the subject may write.
// Exactly one unfinished queue may exist per workspace: two drivers on one
// workspace would race for its single working tree.
func (c *Control) queueCreate(ctx context.Context, subject Subject, req *proto.QueueCreateReq) (*proto.Queue, error) {
	if req.WS == "" {
		return nil, proto.Err(proto.CodeBadRequest, "workspace id is required")
	}
	if err := proto.ValidateQueueTasks(req.Tasks); err != nil {
		return nil, err
	}
	if req.SleepAfterSec < 0 {
		return nil, proto.Err(proto.CodeBadRequest, "sleep_after_sec must not be negative")
	}
	if req.SleepUntil != "" && !sleepUntilPattern.MatchString(req.SleepUntil) {
		return nil, proto.Err(proto.CodeBadRequest, "sleep_until %q must be HH:MM", req.SleepUntil)
	}
	if req.SleepUntil != "" && req.SleepAfterSec != 0 {
		return nil, proto.Err(proto.CodeBadRequest, "sleep_until and sleep_after_sec are exclusive")
	}
	ws, err := c.wsGet(req.WS)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, subject, ActionWrite, workspaceResource(ws)); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|queue.create"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationQueueResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpQueueCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.queueCopy(prior.ID)
	}
	items := make([]proto.QueueItem, len(req.Tasks))
	for i, t := range req.Tasks {
		items[i] = proto.QueueItem{Task: t}
	}
	now := c.now().UnixMilli()
	q := &proto.Queue{
		ID: ids.New("q"), WS: ws.ID, Tenant: ws.Tenant, Owner: subject.ID, Recipe: req.Recipe,
		Items: items, Status: proto.QueueRunning, SleepAfterSec: req.SleepAfterSec, SleepUntil: req.SleepUntil,
		CreatedAt: now, UpdatedAt: now,
	}
	c.mu.Lock()
	current := c.workspaces[ws.ID]
	if current == nil || current.State == proto.WSDestroyed || current.State == proto.WSDestroying {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is gone", req.WS)
	}
	tenantQueues := 0
	for _, other := range c.queues {
		if other.Tenant == q.Tenant {
			tenantQueues++
		}
		if other.WS == q.WS && other.Status != proto.QueueDone {
			otherID := other.ID
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "workspace %s already has unfinished queue %s", req.WS, otherID)
		}
	}
	if tenantQueues >= c.opts.MaxQueuesPerTenant {
		limit := c.opts.MaxQueuesPerTenant
		c.mu.Unlock()
		metrics.QueueQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant queue limit %d reached", limit)
	}
	wsCopy := *current
	err = c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO queues(id, data) VALUES(?,?)`, q.ID, proto.MustMarshal(q)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpQueueCreate, req, mutationQueueResult{ID: q.ID})
	}, []*proto.Event{c.queueEvent(proto.EvQueueCreated, q, &wsCopy, subject.ID, map[string]any{"ws": q.WS, "items": len(q.Items)})})
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.queues[q.ID] = q
	cp := copyQueue(q)
	c.mu.Unlock()
	return cp, nil
}

func copyQueue(q *proto.Queue) *proto.Queue {
	cp := *q
	cp.Items = append([]proto.QueueItem(nil), q.Items...)
	return &cp
}

func (c *Control) queueCopy(id string) (*proto.Queue, error) {
	c.mu.Lock()
	q := c.queues[id]
	var cp *proto.Queue
	if q != nil {
		cp = copyQueue(q)
	}
	c.mu.Unlock()
	if cp == nil {
		return nil, proto.Err(proto.CodeNotFound, "queue %s not found", id)
	}
	return cp, nil
}

// queueGet returns a queue the subject may read: its owner, or anyone who may
// read the workspace it drives.
func (c *Control) queueGet(ctx context.Context, subject Subject, id string) (*proto.Queue, error) {
	q, err := c.queueCopy(id)
	if err != nil {
		return nil, err
	}
	if err := c.queueAuthorize(ctx, subject, q, ActionRead); err != nil {
		return nil, err
	}
	return q, nil
}

func (c *Control) queueAuthorize(ctx context.Context, subject Subject, q *proto.Queue, action string) error {
	if err := c.check(ctx, subject, action, queueResource(q)); err == nil {
		return nil
	}
	ws, wsErr := c.wsGet(q.WS)
	if wsErr != nil {
		return proto.Err(proto.CodeDenied, "subject %s may not %s queue %s", subject.ID, action, q.ID)
	}
	return c.check(ctx, subject, action, workspaceResource(ws))
}

func (c *Control) queueList(ctx context.Context, subject Subject, req *proto.QueueListReq) (*proto.QueueListRes, error) {
	c.mu.Lock()
	all := make([]*proto.Queue, 0, len(c.queues))
	for _, q := range c.queues {
		if req.WS == "" || q.WS == req.WS {
			all = append(all, copyQueue(q))
		}
	}
	c.mu.Unlock()
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt != all[j].CreatedAt {
			return all[i].CreatedAt < all[j].CreatedAt
		}
		return all[i].ID < all[j].ID
	})
	out := &proto.QueueListRes{Queues: []proto.Queue{}}
	for _, q := range all {
		if c.queueAuthorize(ctx, subject, q, ActionRead) == nil {
			out.Queues = append(out.Queues, *q)
		}
	}
	return out, nil
}

// queueAdvance commits one task's outcome and the cursor in one transaction
// with queue.advanced, so a driver that crashes right after sees, on
// restart, either the task still due or its result recorded, never a
// half-recorded run.
func (c *Control) queueAdvance(ctx context.Context, subject Subject, req *proto.QueueAdvanceReq) (*proto.Queue, error) {
	q, err := c.queueCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.queueAuthorize(ctx, subject, q, ActionWrite); err != nil {
		return nil, err
	}
	scope := q.Tenant + "|" + q.ID + "|queue.advance"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationQueueResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpQueueAdvance, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.queueCopy(prior.ID)
	}
	c.mu.Lock()
	live := c.queues[req.ID]
	if live == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "queue %s not found", req.ID)
	}
	if live.Status == proto.QueueDone {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "queue %s is done", req.ID)
	}
	if req.Index != live.Cursor {
		cursor := live.Cursor
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "queue %s cursor is at %d, not %d", req.ID, cursor, req.Index)
	}
	next := copyQueue(live)
	item := &next.Items[req.Index]
	item.Session, item.Exit, item.Signal = req.Session, req.Exit, req.Signal
	item.Attempts++
	item.FinishedAt = c.now().UnixMilli()
	if req.Exit == 0 && req.Signal == "" {
		next.Cursor++
		next.Status = proto.QueueRunning
		if next.Cursor == len(next.Items) {
			next.Status = proto.QueueDone
		}
	} else {
		next.Status = proto.QueueFailed
	}
	next.UpdatedAt = item.FinishedAt
	var wsCopy *proto.Workspace
	if ws := c.workspaces[next.WS]; ws != nil {
		cp := *ws
		wsCopy = &cp
	}
	err = c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO queues(id, data) VALUES(?,?)`, next.ID, proto.MustMarshal(next)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpQueueAdvance, req, mutationQueueResult{ID: next.ID})
	}, []*proto.Event{c.queueEvent(proto.EvQueueAdvanced, next, wsCopy, subject.ID, map[string]any{
		"index": req.Index, "exit": req.Exit, "signal": req.Signal, "status": next.Status, "cursor": next.Cursor,
	})})
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.queues[next.ID] = next
	cp := copyQueue(next)
	c.mu.Unlock()
	return cp, nil
}
