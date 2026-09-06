package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// eventFilter narrows an event stream on the client. The workspace filter is
// applied by the control plane; session attribution is a per-event field the
// server does not index, so it is matched here.
type eventFilter struct {
	Session string
}

func (f eventFilter) match(e proto.Event) bool {
	return f.Session == "" || e.Session == f.Session
}

// splitEventTypes turns a comma-separated --type value into prefixes,
// dropping empty entries so a trailing comma is not a filter that matches
// everything.
func splitEventTypes(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// eventJSON is the --json shape of one event: authoritative metadata plus the
// decoded payload, never the raw CBOR.
func eventJSON(e proto.Event) map[string]any {
	var payload any
	_ = proto.Unmarshal(e.Payload, &payload)
	out := map[string]any{
		"seq": e.Seq, "at": time.UnixMilli(e.At).Format(time.RFC3339Nano), "type": e.Type,
		"stream": e.Stream, "principal": e.Principal, "node": e.Node, "payload": payload,
	}
	if e.Workspace != "" {
		out["workspace"] = e.Workspace
	}
	if e.Generation != 0 {
		out["generation"] = e.Generation
	}
	if e.Session != "" {
		out["session"] = e.Session
	}
	if e.Tenant != "" {
		out["tenant"] = e.Tenant
	}
	if e.Origin != "" {
		out["origin"] = e.Origin
	}
	if e.Cause != 0 {
		out["cause"] = e.Cause
	}
	return out
}

func cmdEvents(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "export" {
		return cmdEventsExport(ctx, args[1:])
	}
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	var c common
	c.flags(fs)
	follow := fs.Bool("follow", false, "stream new events")
	ws := fs.String("ws", "", "filter by workspace")
	session := fs.String("session", "", "filter by session id (s.opened, s.exited, …)")
	binding := fs.String("binding", "", "filter by credential binding id (server-side)")
	host := fs.String("host", "", "filter by egress destination host (server-side)")
	types := fs.String("type", "", "filter by comma-separated event-type prefixes, e.g. egress,cred (server-side)")
	from := fs.Uint64("from", 1, "first seq")
	parse(fs, args)
	if err := arity(fs, 0, 0, "events [--follow] [--ws WS] [--session SID] [--binding B] [--host H] [--type PREFIX,…] [--from N]"); err != nil {
		return err
	}
	filter := eventFilter{Session: *session}
	// Binding, host and type narrow the stream in the control plane; session
	// stays here because it is a per-event field the server does not index.
	var serverFilters []client.EventFilterOption
	if *binding != "" {
		serverFilters = append(serverFilters, client.WithEventBinding(*binding))
	}
	if *host != "" {
		serverFilters = append(serverFilters, client.WithEventHost(*host))
	}
	if prefixes := splitEventTypes(*types); len(prefixes) > 0 {
		serverFilters = append(serverFilters, client.WithEventTypes(prefixes...))
	}
	cl := c.client()
	defer cl.Close()
	print := func(e proto.Event) {
		if !filter.match(e) {
			return
		}
		if c.json {
			printJSON(eventJSON(e))
			return
		}
		var payload any
		_ = proto.Unmarshal(e.Payload, &payload)
		pj, _ := json.Marshal(payload)
		fmt.Printf("%6d %s %-18s %-30s %-14s %s\n", e.Seq, time.UnixMilli(e.At).Format("15:04:05.000"), e.Type, e.Stream, e.Principal, string(pj))
	}
	if !*follow {
		evs, err := cl.ReadEvents(ctx, *from, *ws, serverFilters...)
		if err != nil {
			return err
		}
		for _, e := range evs {
			print(e)
		}
		return nil
	}
	ch, err := cl.TailEvents(ctx, *from, *ws, serverFilters...)
	if err != nil {
		return err
	}
	for e := range ch {
		print(e)
	}
	return nil
}
