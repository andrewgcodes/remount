package launch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// QueueOptions drive several tasks through one workspace in order
// (ADR 0041). Progress lives in a control-plane Queue, so a driver that dies
// mid-way is continued with Continue and the workspace's own files never
// carry the cursor.
type QueueOptions struct {
	// Tasks are the prompts, in order. Ignored when Continue is set.
	Tasks []string
	// SleepAfter puts the workspace to sleep for this long between tasks;
	// SleepUntil (HH:MM, local) sleeps until the next such wall-clock time.
	// They are exclusive; with neither, the tree is checkpointed between
	// tasks and the next one starts at once.
	SleepAfter time.Duration
	SleepUntil string
	// Continue names an existing queue whose remaining tasks are run in the
	// workspace it belongs to.
	Continue string
	// Run is the launch every task shares. Run.Task and Run.BeforeOpen must
	// be empty; Run.WS may name an existing workspace.
	Run Options
	// Output receives every task's stdout and stderr; nil discards.
	Output io.Writer
	// Now is the clock for SleepUntil; nil means time.Now.
	Now func() time.Time
}

// QueueResult is the queue as last read back plus the workspace it ran in.
type QueueResult struct {
	Queue     *proto.Queue
	Workspace *proto.Workspace
	// Sessions are the run sessions opened by this driver, in order.
	Sessions []string
}

// ParseQueueFile reads a task list: one task per line, blank lines and
// lines starting with # dropped, and a line ending in a backslash continued
// on the next.
func ParseQueueFile(r io.Reader) ([]string, error) {
	var tasks []string
	var pending strings.Builder
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), proto.MaxQueueTaskSize+1024)
	for sc.Scan() {
		line := sc.Text()
		if pending.Len() == 0 {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
		}
		if strings.HasSuffix(line, "\\") {
			pending.WriteString(strings.TrimSuffix(line, "\\"))
			pending.WriteByte('\n')
			continue
		}
		pending.WriteString(line)
		tasks = append(tasks, strings.TrimSpace(pending.String()))
		pending.Reset()
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if pending.Len() > 0 {
		tasks = append(tasks, strings.TrimSpace(pending.String()))
	}
	if err := proto.ValidateQueueTasks(tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// NextWallClock returns the first instant strictly after now whose local
// time of day is hhmm.
func NextWallClock(now time.Time, hhmm string) (time.Time, error) {
	t, err := time.ParseInLocation("15:04", hhmm, now.Location())
	if err != nil {
		return time.Time{}, fmt.Errorf("--sleep-until %q must be HH:MM", hhmm)
	}
	at := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, now.Location())
	if !at.After(now) {
		at = at.AddDate(0, 0, 1)
	}
	return at, nil
}

// RunQueue runs the queue's tasks one after another in one workspace,
// recording each outcome with queue.advance before starting the next, and
// sleeping or checkpointing the workspace in between. A non-zero exit stops
// the queue with the cursor on the failed task.
func RunQueue(ctx context.Context, cl *client.Client, o QueueOptions) (*QueueResult, error) {
	if o.Run.Task != "" || o.Run.BeforeOpen != nil {
		return nil, errors.New("queue tasks come from the queue; --task and BeforeOpen must be empty")
	}
	if o.SleepAfter < 0 {
		return nil, errors.New("--sleep-after must not be negative")
	}
	if o.SleepAfter > 0 && o.SleepUntil != "" {
		return nil, errors.New("--sleep-after and --sleep-until are exclusive")
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	if o.SleepUntil != "" {
		if _, err := NextWallClock(now(), o.SleepUntil); err != nil {
			return nil, err
		}
	}
	stderr := o.Run.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	out := o.Output
	if out == nil {
		out = io.Discard
	}
	res := &QueueResult{}

	var q *proto.Queue
	if o.Continue != "" {
		existing, err := cl.GetQueue(ctx, o.Continue)
		if err != nil {
			return nil, err
		}
		if existing.Status == proto.QueueDone {
			return nil, fmt.Errorf("queue %s is done", existing.ID)
		}
		if o.Run.WS != "" && o.Run.WS != existing.WS {
			return nil, fmt.Errorf("queue %s belongs to workspace %s, not %s", existing.ID, existing.WS, o.Run.WS)
		}
		if o.Run.Recipe != nil && existing.Recipe != "" && existing.Recipe != o.Run.Recipe.Name {
			return nil, fmt.Errorf("queue %s was created for recipe %s, not %s", existing.ID, existing.Recipe, o.Run.Recipe.Name)
		}
		o.Run.WS = existing.WS
		q = existing
		fmt.Fprintf(stderr, "continuing queue %s at task %d of %d\n", q.ID, q.Cursor+1, len(q.Items))
	} else {
		if err := proto.ValidateQueueTasks(o.Tasks); err != nil {
			return nil, err
		}
		if o.Run.Recipe == nil {
			return nil, errors.New("a recipe is required")
		}
	}
	if q != nil && o.Run.Recipe == nil {
		if q.Recipe == "" {
			return nil, fmt.Errorf("queue %s records no recipe; pass one", q.ID)
		}
		r, err := Load(q.Recipe)
		if err != nil {
			return nil, err
		}
		o.Run.Recipe = r
	}
	// Validate needs a task; probe with the first one due so flag errors
	// surface before a workspace or queue exists.
	probe := o.Run
	if q != nil {
		probe.Task = q.Items[q.Cursor].Task
	} else {
		probe.Task = o.Tasks[0]
	}
	if _, err := probe.Validate(); err != nil {
		return nil, err
	}

	// Each task's launch starts from the shared Options; the first one may
	// also create the workspace and, through BeforeOpen, the queue.
	launch := func(ctx context.Context, i int, task string, before func(context.Context, *proto.Workspace) error) (*Result, *proto.ExitInfo, error) {
		run := o.Run
		run.Task = task
		run.BeforeOpen = before
		run.Stderr = stderr
		if run.WaitClaimed <= 0 {
			run.WaitClaimed = 60 * time.Second
		}
		fmt.Fprintf(stderr, "queue task %d: starting\n", i+1)
		r, err := Start(ctx, cl, run)
		if r != nil && r.Workspace != nil {
			res.Workspace = r.Workspace
		}
		if err != nil {
			return r, nil, err
		}
		res.Sessions = append(res.Sessions, r.Session.ID)
		for ch := range r.Session.Chunks() {
			if ch.Stream == proto.StreamStdout || ch.Stream == proto.StreamStderr {
				_, _ = out.Write(ch.Data)
			}
		}
		if err := r.Session.Err(); err != nil {
			return r, nil, fmt.Errorf("queue task %d: %w", i+1, err)
		}
		exit := r.Session.Exit()
		if exit == nil {
			return r, nil, fmt.Errorf("queue task %d: session ended without an exit record", i+1)
		}
		return r, exit, nil
	}
	advance := func(ctx context.Context, i int, r *Result, exit *proto.ExitInfo) error {
		// The key is derived from the session so a retried call after a
		// network failure replays rather than double-counting the attempt.
		key := "queue|" + q.ID + "|" + r.Session.ID
		next, err := cl.AdvanceQueue(ctx, proto.QueueAdvanceReq{
			ID: q.ID, Index: i, Session: r.Session.ID, Exit: exit.Code, Signal: exit.Signal,
		}, client.WithIdempotencyKey(key))
		if err != nil {
			return fmt.Errorf("queue task %d finished (exit %d) but its outcome was not recorded: %w", i+1, exit.Code, err)
		}
		q = next
		res.Queue = q
		if exit.Code != 0 || exit.Signal != "" {
			return fmt.Errorf("queue task %d exited %d%s; queue %s stopped at task %d (continue it with --queue-continue %s)",
				i+1, exit.Code, signalSuffix(exit.Signal), q.ID, i+1, q.ID)
		}
		fmt.Fprintf(stderr, "queue task %d: done\n", i+1)
		return nil
	}

	start := 0
	if q == nil {
		var created *proto.Queue
		before := func(ctx context.Context, ws *proto.Workspace) error {
			var err error
			created, err = cl.CreateQueue(ctx, proto.QueueCreateReq{
				WS: ws.ID, Recipe: o.Run.Recipe.Name, Tasks: o.Tasks,
				SleepAfterSec: int64(o.SleepAfter / time.Second), SleepUntil: o.SleepUntil,
			})
			if err != nil {
				return fmt.Errorf("create queue: %w", err)
			}
			fmt.Fprintf(stderr, "queue %s created with %d tasks\n", created.ID, len(created.Items))
			return nil
		}
		r, exit, err := launch(ctx, 0, o.Tasks[0], before)
		if created != nil {
			q, res.Queue = created, created
		}
		if err != nil {
			return res, err
		}
		if err := advance(ctx, 0, r, exit); err != nil {
			return res, err
		}
		start = 1
		// Every later task runs in the workspace the queue was pinned to.
		o.Run.WS, o.Run.Name, o.Run.RestoreFrom = q.WS, "", ""
	} else {
		start = q.Cursor
		if o.SleepAfter == 0 && o.SleepUntil == "" {
			o.SleepAfter = time.Duration(q.SleepAfterSec) * time.Second
			o.SleepUntil = q.SleepUntil
		}
		ws, err := cl.GetWorkspace(ctx, q.WS)
		if err != nil {
			return res, err
		}
		if ws.State == proto.WSPaused {
			if _, err := cl.WakeWorkspace(ctx, ws.ID); err != nil {
				return res, err
			}
		}
		if ws.State != proto.WSClaimed {
			wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			ws, err = cl.WaitClaimed(wctx, ws.ID)
			cancel()
			if err != nil {
				return res, fmt.Errorf("workspace %s not claimed: %w", q.WS, err)
			}
		}
		res.Workspace = ws
	}

	if o.Run.WS != q.WS {
		return res, fmt.Errorf("queue %s is pinned to workspace %s", q.ID, q.WS)
	}
	for i := start; i < len(q.Items); i++ {
		if err := pauseBetween(ctx, cl, q.WS, o, now, stderr); err != nil {
			return res, err
		}
		r, exit, err := launch(ctx, i, q.Items[i].Task, nil)
		if err != nil {
			return res, err
		}
		if err := advance(ctx, i, r, exit); err != nil {
			return res, err
		}
	}
	fmt.Fprintf(stderr, "queue %s done: %d tasks\n", q.ID, len(q.Items))
	return res, nil
}

// pauseBetween checkpoints the workspace, or puts it to sleep on the
// control plane's timer and waits for it to be claimed again, before the
// next task. Sleep already checkpoints; both leave the tree durable.
func pauseBetween(ctx context.Context, cl *client.Client, wsID string, o QueueOptions, now func() time.Time, stderr io.Writer) error {
	switch {
	case o.SleepAfter > 0 || o.SleepUntil != "":
		req := proto.WSSleepReq{ID: wsID}
		if o.SleepUntil != "" {
			at, err := NextWallClock(now(), o.SleepUntil)
			if err != nil {
				return err
			}
			req.AtMillis = at.UnixMilli()
			fmt.Fprintf(stderr, "sleeping %s until %s\n", wsID, at.Format(time.RFC3339))
		} else {
			req.AfterSec = int64(o.SleepAfter / time.Second)
			if req.AfterSec < 1 {
				req.AfterSec = 1
			}
			fmt.Fprintf(stderr, "sleeping %s for %s\n", wsID, o.SleepAfter)
		}
		if _, err := cl.SleepWorkspace(ctx, req); err != nil {
			return fmt.Errorf("sleep %s: %w", wsID, err)
		}
		if _, err := cl.WaitClaimed(ctx, wsID); err != nil {
			return fmt.Errorf("%s did not wake: %w", wsID, err)
		}
		fmt.Fprintf(stderr, "%s awake\n", wsID)
	default:
		if _, err := cl.Checkpoint(ctx, wsID); err != nil {
			return fmt.Errorf("checkpoint %s: %w", wsID, err)
		}
	}
	return nil
}
