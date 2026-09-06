package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// Durable holds and idle policy on the command line (ADR 0090).
//
// The point of these subcommands is that the deadline does not live here. A
// shell loop that sleeps and then calls `ws sleep` is the same mistake as an
// `asyncio.sleep` in an API worker: kill the terminal and the workspace runs
// until somebody notices the bill. `ws lease` hands the deadline to the
// control plane, which is why the live proof for this feature kills the CLI
// process between taking a hold and observing it fire.

const (
	leaseUsage        = "ws lease WS --max 20m [--min 30s] [--on-expiry sleep|destroy] [--reason R] [--idem K]"
	leaseRenewUsage   = "ws lease renew WS LEASE --extend 10m [--idem K]"
	leaseCancelUsage  = "ws lease cancel WS LEASE [--idem K]"
	leaseGetUsage     = "ws lease get WS"
	idlePolicyUsage   = "ws idle-policy WS --sleep-after 10m [--destroy-after 2h] [--idem K]"
	markIdleUsage     = "ws mark-idle WS [--reason R] [--idem K]"
	markActiveUsage   = "ws mark-active WS [--reason R] [--idem K]"
	lifecycleHelpText = leaseUsage + "\n  " + leaseRenewUsage + "\n  " + leaseCancelUsage +
		"\n  " + leaseGetUsage + "\n  " + idlePolicyUsage + "\n  " + markIdleUsage + "\n  " + markActiveUsage
)

// cmdWSLifecycle dispatches the `ws lease*`, `ws idle-policy`, `ws mark-idle`
// and `ws mark-active` subcommands. It builds its own flag set rather than
// reusing the caller's, because `ws lease renew` takes a different set of
// flags from `ws lease` and Go's flag package has no notion of a sub-verb.
func cmdWSLifecycle(ctx context.Context, sub string, args []string) error {
	verb := sub
	rest := args
	if sub == "lease" && len(args) > 0 {
		switch args[0] {
		case "renew", "cancel", "get":
			verb, rest = "lease "+args[0], args[1:]
		}
	}
	if len(rest) > 0 && isHelp(rest[0]) {
		return errors.New(lifecycleHelpText)
	}
	fs := flag.NewFlagSet("ws "+verb, flag.ExitOnError)
	var c common
	c.flags(fs)
	switch verb {
	case "lease":
		return leaseCommand(ctx, &c, fs, rest)
	case "lease renew":
		return leaseRenewCommand(ctx, &c, fs, rest)
	case "lease cancel":
		return leaseCancelCommand(ctx, &c, fs, rest)
	case "lease get":
		return leaseGetCommand(ctx, &c, fs, rest)
	case "idle-policy":
		return idlePolicyCommand(ctx, &c, fs, rest)
	case "mark-idle":
		return markIdleCommand(ctx, &c, fs, rest, true, markIdleUsage)
	case "mark-active":
		return markIdleCommand(ctx, &c, fs, rest, false, markActiveUsage)
	default:
		return fmt.Errorf("unknown ws subcommand %q", sub)
	}
}

func leaseCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string) error {
	minAlive := fs.Duration("min", 0, "how long the idle policy is forbidden from acting")
	maxAlive := fs.Duration("max", 0, "hard deadline: sleep or destroy the workspace after this long unless renewed")
	onExpiry := fs.String("on-expiry", "", "sleep (default) or destroy")
	reason := fs.String("reason", "", "why the workspace is held; recorded on the event")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, leaseUsage); err != nil {
		return err
	}
	maxSec, err := leaseSeconds("--max", *maxAlive, true)
	if err != nil {
		return err
	}
	minSec, err := leaseSeconds("--min", *minAlive, false)
	if err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	lease, err := cl.LeaseWorkspace(ctx, proto.WSLeaseReq{
		ID: fs.Arg(0), MinAliveSec: minSec, MaxAliveSec: maxSec,
		OnExpiry: *onExpiry, Reason: *reason,
	}, idemOptions(*idem)...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(lease)
		return nil
	}
	fmt.Println(lease.ID)
	fmt.Fprintf(os.Stderr, "held: ws=%s gen=%d on_expiry=%s max_alive_until=%s%s\n",
		lease.WS, lease.Generation, lease.OnExpiry, millis(lease.MaxAliveUntil), minAliveNote(lease))
	return nil
}

func leaseRenewCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string) error {
	extend := fs.Duration("extend", 0, "extend the hard deadline to this long from now")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 2, 2, leaseRenewUsage); err != nil {
		return err
	}
	extendSec, err := leaseSeconds("--extend", *extend, true)
	if err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	lease, err := cl.RenewLease(ctx, fs.Arg(0), fs.Arg(1), extendSec, idemOptions(*idem)...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(lease)
		return nil
	}
	fmt.Fprintf(os.Stderr, "renewed: lease=%s renewals=%d max_alive_until=%s%s\n",
		lease.ID, lease.Renewals, millis(lease.MaxAliveUntil), minAliveNote(lease))
	return nil
}

func leaseCancelCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string) error {
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 2, 2, leaseCancelUsage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	ws, err := cl.CancelLease(ctx, fs.Arg(0), fs.Arg(1), idemOptions(*idem)...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(ws)
		return nil
	}
	fmt.Fprintf(os.Stderr, "hold released: ws=%s state=%s\n", ws.ID, ws.State)
	printLifecycleDeadline(os.Stderr, ws.LifecycleDeadline)
	return nil
}

func leaseGetCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string) error {
	parse(fs, args)
	if err := arity(fs, 1, 1, leaseGetUsage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	res, err := cl.GetLease(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	printJSON(res)
	if !c.json {
		if res.Lease != nil {
			fmt.Fprintf(os.Stderr, "lease=%s live=%t on_expiry=%s max_alive_until=%s%s\n",
				res.Lease.ID, res.Lease.Live(), res.Lease.OnExpiry, millis(res.Lease.MaxAliveUntil), minAliveNote(res.Lease))
			if !res.Lease.Live() {
				fmt.Fprintf(os.Stderr, "ended: reason=%s at=%s\n", res.Lease.EndedReason, millis(res.Lease.EndedAt))
			}
		}
		printLifecycleDeadline(os.Stderr, res.Deadline)
	}
	return nil
}

func idlePolicyCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string) error {
	sleepAfter := fs.Duration("sleep-after", 0, "sleep the workspace once it has been marked idle this long")
	destroyAfter := fs.Duration("destroy-after", 0, "destroy the workspace once it has been marked idle this long")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, idlePolicyUsage); err != nil {
		return err
	}
	sleepSec, err := leaseSeconds("--sleep-after", *sleepAfter, false)
	if err != nil {
		return err
	}
	destroySec, err := leaseSeconds("--destroy-after", *destroyAfter, false)
	if err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	ws, err := cl.SetIdlePolicy(ctx, fs.Arg(0), sleepSec, destroySec, idemOptions(*idem)...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(ws)
		return nil
	}
	if sleepSec == 0 && destroySec == 0 {
		fmt.Fprintf(os.Stderr, "idle policy removed: ws=%s\n", ws.ID)
	} else {
		fmt.Fprintf(os.Stderr, "idle policy set: ws=%s sleep_after=%ds destroy_after=%ds\n", ws.ID, sleepSec, destroySec)
		fmt.Fprintln(os.Stderr, "the clock only runs while the workspace is marked idle; run `remount ws mark-idle` when a turn settles")
	}
	printLifecycleDeadline(os.Stderr, ws.LifecycleDeadline)
	return nil
}

func markIdleCommand(ctx context.Context, c *common, fs *flag.FlagSet, args []string, idle bool, usage string) error {
	reason := fs.String("reason", "", "why the workspace changed activity; recorded on the event")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, usage); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	options := idemOptions(*idem)
	mark := cl.MarkActive
	if idle {
		mark = cl.MarkIdle
	}
	ws, err := mark(ctx, fs.Arg(0), *reason, options...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(ws)
		return nil
	}
	if idle {
		fmt.Fprintf(os.Stderr, "marked idle: ws=%s idle_since=%s\n", ws.ID, millis(ws.IdleSince))
	} else {
		fmt.Fprintf(os.Stderr, "marked active: ws=%s last_activity_at=%s\n", ws.ID, millis(ws.LastActivityAt))
	}
	printLifecycleDeadline(os.Stderr, ws.LifecycleDeadline)
	return nil
}

// printLifecycleDeadline renders the derived view a client reads instead of
// looking at timers. It prints nothing when the workspace has no schedule,
// because "no deadline" is the ordinary case and a line saying so on every
// `ws get` would be noise.
func printLifecycleDeadline(w io.Writer, d *proto.LifecycleDeadline) {
	if d == nil {
		return
	}
	switch {
	case d.Failed:
		fmt.Fprintf(w, "lifecycle deadline FAILED: action=%s source=%s attempts=%d err=%s\n",
			d.Action, d.Source, d.Attempts, d.Error)
		fmt.Fprintln(w, "the workspace is degraded and still holding compute; it needs an operator")
	case d.Fired:
		fmt.Fprintf(w, "lifecycle deadline fired at %s: %s is running (source=%s)\n",
			millis(d.FiredAt), d.Action, d.Source)
	default:
		fmt.Fprintf(w, "lifecycle deadline: %s at %s (in %s, source=%s)\n",
			d.Action, millis(d.At), time.Until(time.UnixMilli(d.At)).Round(time.Second), d.Source)
	}
}

// leaseSeconds converts a duration flag to the whole seconds the protocol
// carries. A sub-second value is refused rather than silently truncated to
// zero: `--max 500ms` would otherwise read as "no deadline asked for" and the
// control plane would reject it with a message about a field the caller never
// typed.
func leaseSeconds(flagName string, d time.Duration, required bool) (int64, error) {
	if d == 0 {
		if required {
			return 0, fmt.Errorf("%s is required and must be at least 1s", flagName)
		}
		return 0, nil
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative", flagName)
	}
	if d < time.Second {
		return 0, fmt.Errorf("%s must be at least 1s, got %s", flagName, d)
	}
	return int64(d / time.Second), nil
}

func idemOptions(key string) []client.OperationOption {
	if key == "" {
		return nil
	}
	return []client.OperationOption{client.WithIdempotencyKey(key)}
}

func minAliveNote(l *proto.WorkspaceLease) string {
	if l == nil || l.MinAliveUntil == 0 {
		return ""
	}
	return " min_alive_until=" + millis(l.MinAliveUntil)
}

func millis(at int64) string {
	if at == 0 {
		return "-"
	}
	return time.UnixMilli(at).Format(time.RFC3339)
}
