// Command long-running-autosleep shows the durable half of workspace
// lifecycle: a deadline that lives in Remount's control plane instead of in
// this process (ADR 0090).
//
// The mistake this exists to replace is a timer in the caller. A background
// job starts, the API worker arms an `asyncio.sleep`/`setTimeout`/`time.Sleep`
// to clean up afterwards, and the cleanup is then only as durable as the
// worker: a deploy, a scale-down, an OOM kill or a partition takes the timer
// with it and the workspace runs until somebody notices the bill.
//
// `ws.lease` moves the deadline to the control plane. This program takes a
// twenty-second hold, starts a `sleep 300` that will outlive it, renews once
// to prove a renewal moves the deadline, and then stops renewing and watches
// the control plane act on its own: the session ends with the exit reason
// "lifecycle_deadline_expired", the workspace publishes `paused`, and
// `ws.lifecycle.expired` says what happened and why. Waking it afterwards
// shows what a hold does and does not preserve — the filesystem survives, the
// processes do not.
//
// It imports only the supported public packages (remount.dev/remount/api and
// remount.dev/remount/client), needs no credentials, and reaches no network
// but the Remount server itself.
//
// Run a whole system on a laptop and point this at it:
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/long-running-autosleep
//
// REMOUNT_TOKEN is read when set, so the same program works against a server
// that requires one. Standalone mode has no token.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

const (
	// holdSeconds is the first hard deadline. Twenty seconds is short enough
	// to watch and long enough that the renewal below is not a race.
	holdSeconds = 20
	// renewSeconds is the deadline the single renewal installs, measured from
	// the moment of the renewal rather than from the original grant.
	renewSeconds = 15
	// marker proves the filesystem, and only the filesystem, survived.
	marker = "state-before-the-deadline.txt"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "long-running-autosleep:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	c, err := client.New(client.Options{
		Server:    envOr("REMOUNT_SERVER", "http://127.0.0.1:7443"),
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_long_running_autosleep",
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{Name: "long-running-autosleep"})
	if err != nil {
		return err
	}
	if ws, err = c.WaitClaimed(ctx, ws.ID); err != nil {
		return err
	}
	fmt.Fprintf(out, "workspace %s claimed by node %s\n", ws.ID, ws.Node)
	defer func() {
		stop, cancelStop := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer cancelStop()
		if err := c.DestroyWorkspace(stop, ws.ID); err != nil {
			fmt.Fprintf(out, "destroy %s: %v\n", ws.ID, err)
			return
		}
		fmt.Fprintf(out, "workspace %s destroyed\n", ws.ID)
	}()

	// Something worth keeping, written before the deadline so its survival
	// afterwards is a claim about the checkpoint rather than about timing.
	if err := c.WriteFile(ctx, ws.ID, marker, []byte("written before the hold expired\n"), 0o644); err != nil {
		return err
	}

	// The hold. min_alive_sec is a floor for the idle policy; max_alive_sec is
	// the hard deadline. on_expiry "sleep" releases with a filesystem
	// checkpoint and publishes paused; "destroy" ends the workspace instead.
	lease, err := c.LeaseWorkspace(ctx, api.LeaseRequest{
		ID: ws.ID, MaxAliveSec: holdSeconds, OnExpiry: api.LeaseExpirySleep,
		Reason: "background_job",
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "held %s until %s (lease %s, on_expiry %s)\n",
		ws.ID, stamp(lease.MaxAliveUntil), lease.ID, lease.OnExpiry)

	// Work that will outlive its own deadline. Nothing about this session
	// extends the hold: activity is explicit, so a workspace with a running
	// process and no renewal still sleeps on time. That is the contract, not
	// a bug — a deadline nobody has to renew is a deadline that never fires.
	session, err := c.Exec(ctx, api.SessionOpenRequest{
		WS: ws.ID, Kind: api.SessionExec,
		Program: []string{"sh", "-c", "echo background job started; sleep 300"},
	})
	if err != nil {
		return err
	}
	exits := make(chan *api.ExitInfo, 1)
	go func() {
		// Copy drains the session log to completion. The exit chunk it ends on
		// is the one the control plane's expiry produced.
		exit, copyErr := client.Copy(ctx, session, out, out)
		if copyErr != nil {
			fmt.Fprintf(out, "session %s stream ended: %v\n", session.ID, copyErr)
		}
		exits <- exit
	}()

	// A renewal is the activity signal for a hold. It moves the hard deadline
	// to renewSeconds from now, not from the original grant.
	renewed, err := c.RenewLease(ctx, ws.ID, lease.ID, renewSeconds)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "renewed %s until %s (renewals %d)\n", renewed.ID, stamp(renewed.MaxAliveUntil), renewed.Renewals)

	// A fresh reader learns the schedule from the workspace, not from this
	// process. ws.lifecycle_deadline is the derived view: what happens next,
	// when, and which rule asked for it.
	status, err := c.GetLease(ctx, ws.ID)
	if err != nil {
		return err
	}
	if status.Deadline == nil {
		return errors.New("workspace reports no pending lifecycle deadline while held")
	}
	fmt.Fprintf(out, "pending deadline: %s at %s (source %s)\n",
		status.Deadline.Action, stamp(status.Deadline.At), status.Deadline.Source)

	// Stop renewing. From here the control plane owns the outcome, and this
	// process is only a spectator: killing it now would change nothing.
	paused, err := waitForState(ctx, c, ws.ID, api.WorkspacePaused, 3*time.Minute)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "deadline fired: workspace %s is %s, checkpoint %s\n",
		paused.ID, paused.State, paused.LastSnapshot)

	// The session ended because a policy decision ended it. The exit reason
	// says so, so a reader of the log can tell an expiry from a crash without
	// correlating timestamps against another stream.
	var exit *api.ExitInfo
	select {
	case exit = <-exits:
	case <-ctx.Done():
		return ctx.Err()
	}
	if exit == nil {
		return fmt.Errorf("session %s ended without an exit chunk", session.ID)
	}
	fmt.Fprintf(out, "session %s exit code %d reason %q\n", session.ID, exit.Code, exit.Reason)
	if exit.Reason != "lifecycle_deadline_expired" {
		return fmt.Errorf("session %s ended with reason %q, want lifecycle_deadline_expired", session.ID, exit.Reason)
	}

	// The event log is the durable explanation. ws.lifecycle.expired names the
	// action, the source and the timer; ws.lease.expired retires the hold.
	events, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		return err
	}
	for _, want := range []string{"ws.lease.granted", "ws.lease.renewed", "ws.lifecycle.expired", "ws.lease.expired", "ws.paused"} {
		event := findEvent(events, want)
		if event == nil {
			return fmt.Errorf("workspace %s never emitted %s", ws.ID, want)
		}
		fmt.Fprintf(out, "event %s %s\n", event.Type, payload(event))
	}

	// Waking says what a hold preserves. The filesystem came back; the
	// `sleep 300` did not, because a hold is not a memory checkpoint.
	woken, err := c.WakeWorkspace(ctx, ws.ID)
	if err != nil {
		return err
	}
	if woken, err = c.WaitClaimed(ctx, woken.ID); err != nil {
		return err
	}
	fmt.Fprintf(out, "woken: node %s gen %d\n", woken.Node, woken.Generation)
	data, err := c.ReadFile(ctx, ws.ID, marker)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "filesystem survived: %s", data)

	running, _, _, err := c.Run(ctx, ws.ID, "sh", "-c", "pgrep -f '[s]leep 300' >/dev/null && echo still-running || echo processes-gone")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "processes after wake: %s", running)
	return nil
}

// waitForState polls the workspace until it reaches want. Clients read
// lifecycle position from the workspace row, never from a timer of their own:
// that is the whole point of the deadline living in the control plane.
func waitForState(ctx context.Context, c *client.Client, id, want string, within time.Duration) (*api.Workspace, error) {
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		ws, err := c.GetWorkspace(ctx, id)
		if err != nil {
			return nil, err
		}
		if ws.State == want {
			return ws, nil
		}
		last = ws.State
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("workspace %s is %s after %s, want %s", id, last, within, want)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func findEvent(events []api.Event, kind string) *api.Event {
	for i := range events {
		if events[i].Type == kind {
			return &events[i]
		}
	}
	return nil
}

// payload renders the handful of event fields this program is making a claim
// about, in a fixed order so the output is stable to compare. An event payload
// is deterministic CBOR on the wire; api.DecodeEventPayload is the supported
// way to read one.
func payload(event *api.Event) string {
	var fields map[string]any
	if err := api.DecodeEventPayload(*event, &fields); err != nil {
		return "(undecodable payload: " + err.Error() + ")"
	}
	var parts []string
	for _, key := range []string{"action", "source", "lease", "reason", "on_expiry", "renewals"} {
		if value, ok := fields[key]; ok && value != nil && value != "" {
			parts = append(parts, fmt.Sprintf("%s=%v", key, value))
		}
	}
	return strings.Join(parts, " ")
}

func stamp(at int64) string {
	if at == 0 {
		return "-"
	}
	return time.UnixMilli(at).UTC().Format(time.RFC3339)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
