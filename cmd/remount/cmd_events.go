package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"remount.dev/remount/internal/proto"
)

func cmdEvents(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	var c common
	c.flags(fs)
	follow := fs.Bool("follow", false, "stream new events")
	ws := fs.String("ws", "", "filter by workspace")
	from := fs.Uint64("from", 1, "first seq")
	parse(fs, args)
	if err := arity(fs, 0, 0, "events [--follow] [--ws WS] [--from N]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	print := func(e proto.Event) {
		if c.json {
			var payload any
			_ = proto.Unmarshal(e.Payload, &payload)
			printJSON(map[string]any{"seq": e.Seq, "at": time.UnixMilli(e.At).Format(time.RFC3339Nano), "type": e.Type, "stream": e.Stream, "principal": e.Principal, "node": e.Node, "payload": payload})
			return
		}
		var payload any
		_ = proto.Unmarshal(e.Payload, &payload)
		pj, _ := json.Marshal(payload)
		fmt.Printf("%6d %s %-18s %-30s %-14s %s\n", e.Seq, time.UnixMilli(e.At).Format("15:04:05.000"), e.Type, e.Stream, e.Principal, string(pj))
	}
	if !*follow {
		evs, err := cl.ReadEvents(ctx, *from, *ws)
		if err != nil {
			return err
		}
		for _, e := range evs {
			print(e)
		}
		return nil
	}
	ch, err := cl.TailEvents(ctx, *from, *ws)
	if err != nil {
		return err
	}
	for e := range ch {
		print(e)
	}
	return nil
}
