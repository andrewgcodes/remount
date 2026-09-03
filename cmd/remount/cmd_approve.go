package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"remount.dev/remount/internal/proto"
)

// cmdApprovals lists approvals, pending ones by default. It is what a human
// runs to find out why an Agent is waiting.
func cmdApprovals(ctx context.Context, args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "ls":
			args = args[1:]
		case "approve":
			return cmdApprove(ctx, args[1:])
		case "deny":
			return cmdApprove(ctx, append(args[1:], "--deny"))
		}
	}
	fs := flag.NewFlagSet("approvals", flag.ExitOnError)
	var c common
	c.flags(fs)
	agent := fs.String("agent", "", "only this Agent's approvals")
	kind := fs.String("kind", "", "tool_call | elicitation | egress")
	status := fs.String("status", proto.ApprovalPending, "pending | decided | expired | all")
	parse(fs, args)
	if err := arity(fs, 0, 0, "approvals ls [--agent ID] [--kind K] [--status S]"); err != nil {
		return err
	}
	if *status == "all" {
		*status = ""
	}
	cl := c.client()
	defer cl.Close()
	list, err := cl.ListApprovals(ctx, proto.ApprovalListReq{Agent: *agent, Kind: *kind, Status: *status})
	if err != nil {
		return err
	}
	if c.json {
		printJSON(list)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "ID\tAGENT\tKIND\tSTATUS\tTITLE\tOPTIONS\tAGE")
	for _, ap := range list {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", ap.ID, ap.Agent, ap.Kind, ap.Status, short(ap.Title),
			approvalOptions(ap.Options), time.Since(time.UnixMilli(ap.CreatedAt)).Truncate(time.Second))
	}
	tw.Flush()
	return nil
}

// cmdApprove decides one approval: an option the harness offered, a denial,
// or an elicitation's content. With no flag the first allow option wins,
// which is the common "yes, go ahead".
func cmdApprove(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("approve", flag.ExitOnError)
	var c common
	c.flags(fs)
	option := fs.String("option", "", "option id the harness offered (tool_call), or allow|deny (egress)")
	deny := fs.Bool("deny", false, "reject the request")
	content := fs.String("content", "", "JSON object answering an elicitation")
	remember := fs.String("remember", "", "egress decision scope: none | host | rule")
	parse(fs, args)
	if err := arity(fs, 1, 1, "approve ID [--option X | --deny | --content JSON]"); err != nil {
		return err
	}
	set := 0
	for _, on := range []bool{*option != "", *deny, *content != ""} {
		if on {
			set++
		}
	}
	if set > 1 {
		return errors.New("approve: --option, --deny and --content are exclusive")
	}
	req := proto.ApprovalDecideReq{ID: fs.Arg(0), Option: *option, Denied: *deny, Remember: *remember}
	if *content != "" {
		var probe map[string]json.RawMessage
		if err := json.Unmarshal([]byte(*content), &probe); err != nil {
			return fmt.Errorf("approve: --content must be a JSON object: %w", err)
		}
		req.Content = json.RawMessage(*content)
	}
	cl := c.client()
	defer cl.Close()
	ap, err := cl.DecideApproval(ctx, req)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(ap)
		return nil
	}
	fmt.Printf("%s %s %s\n", ap.ID, ap.Status, approvalDecisionText(ap))
	return nil
}

func approvalOptions(opts []proto.ApprovalOption) string {
	if len(opts) == 0 {
		return "-"
	}
	ids := make([]string, 0, len(opts))
	for _, o := range opts {
		ids = append(ids, o.ID)
	}
	return strings.Join(ids, ",")
}

func approvalDecisionText(ap *proto.Approval) string {
	d := ap.Decision
	if d == nil {
		return ""
	}
	switch {
	case d.Denied:
		return "denied"
	case d.Option != "":
		return "option " + d.Option
	case len(d.Content) > 0:
		return "answered"
	}
	return "decided"
}
