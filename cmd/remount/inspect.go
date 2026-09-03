package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// The three inspection depths:
//
//	remount status         the fleet in one screen
//	remount inspect WS     one workspace, down to session log positions
//	remount doctor         every check that can find damage or loss
//
// Each takes --json so an agent can parse it instead of reading a table.

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

func cmdStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	var c common
	c.flags(fs)
	watch := fs.Duration("watch", 0, "repeat every interval until interrupted")
	parse(fs, args)
	cl := c.client()
	defer cl.Close()
	for {
		if err := printStatus(ctx, cl, c.json); err != nil {
			return err
		}
		if *watch <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*watch):
		}
		if !c.json {
			fmt.Println()
		}
	}
}

type statusReport struct {
	Control   *proto.ControlDiag `json:"control"`
	Nodes     []proto.NodeStatus `json:"nodes"`
	Workspace []proto.Workspace  `json:"workspaces"`
	Timers    []proto.Timer      `json:"timers,omitempty"`
	Findings  []proto.Finding    `json:"findings,omitempty"`
}

func printStatus(ctx context.Context, cl *client.Client, asJSON bool) error {
	d, err := cl.Diag(ctx, false)
	if err != nil {
		return err
	}
	nodes, err := cl.ListNodes(ctx)
	if err != nil {
		return err
	}
	wss, err := cl.ListWorkspaces(ctx)
	if err != nil {
		return err
	}
	timers, _ := cl.ListTimers(ctx)
	rep := statusReport{Control: d, Nodes: nodes, Workspace: wss, Findings: d.Findings}
	for _, t := range timers {
		if !t.Fired {
			rep.Timers = append(rep.Timers, t)
		}
	}
	if asJSON {
		printJSON(rep)
		return nil
	}

	online := 0
	for _, n := range nodes {
		if n.Online {
			online++
		}
	}
	fmt.Printf("control   up %s   events %d   lease %ds   bindings %d\n",
		dur(d.Uptime), d.EventSeq, d.LeaseSec, len(d.Bindings))
	fmt.Printf("fleet     %d/%d nodes online   %d workspaces   %d timers pending\n",
		online, len(nodes), len(wss), len(rep.Timers))

	states := make([]string, 0, len(d.WorkspaceState))
	for k := range d.WorkspaceState {
		states = append(states, k)
	}
	sort.Strings(states)
	var parts []string
	for _, s := range states {
		parts = append(parts, fmt.Sprintf("%s %d", s, d.WorkspaceState[s]))
	}
	if len(parts) > 0 {
		fmt.Printf("state     %s\n", strings.Join(parts, "   "))
	}

	m := d.Metrics
	fmt.Printf("egress    %.0f substituted   %.0f allowed   %.0f denied   %.0f leaks blocked\n",
		m["remount_credentials_substituted_total"], m["remount_egress_allowed_total"],
		m["remount_egress_denied_total"], m["remount_egress_leak_blocked_total"])
	fmt.Printf("sessions  %.0f opened   %.0f exited   %.0f chunks   %s streamed   %.0f gaps\n",
		m["remount_sessions_opened_total"], m["remount_sessions_exited_total"],
		m["remount_session_chunks_total"], humanBytes(int64(m["remount_session_bytes_total"])),
		m["remount_session_gaps_total"])
	fmt.Printf("moves     %.0f claims   %.0f lease expiries   %.0f moves   %.0f snapshots   %.0f restores\n",
		m["remount_workspace_claims_total"], m["remount_workspace_lease_expired_total"],
		m["remount_workspaces_moved_total"], m["remount_snapshots_total"], m["remount_restores_total"])

	if len(nodes) > 0 {
		fmt.Println()
		tw := tabWriter()
		fmt.Fprintln(tw, "NODE\tONLINE\tOS/ARCH\tCPU\tMEM\tLABELS\tWS")
		for _, n := range nodes {
			fmt.Fprintf(tw, "%s\t%v\t%s/%s\t%d\t%s\t%s\t%d\n", n.ID, n.Online, n.Info.OS, n.Info.Arch,
				n.Info.CPU, humanBytes(int64(n.Info.MemMiB)*1024*1024), kvFlag(n.Labels).String(), len(n.Workspaces))
		}
		tw.Flush()
	}
	printFindings(d.Findings)
	return nil
}

func printFindings(fs []proto.Finding) {
	if len(fs) == 0 {
		return
	}
	fmt.Println()
	for _, f := range fs {
		mark := map[string]string{"error": "ERROR", "warn": " WARN", "info": " INFO"}[f.Severity]
		if mark == "" {
			mark = " INFO"
		}
		subj := f.Subject
		if subj != "" {
			subj = " " + subj
		}
		fmt.Printf("%s  %s%s: %s\n", mark, f.Check, subj, f.Detail)
		if f.Hint != "" {
			fmt.Printf("        %s\n", f.Hint)
		}
	}
}

// ---------------------------------------------------------------------------
// inspect
// ---------------------------------------------------------------------------

type inspectReport struct {
	Workspace *proto.Workspace `json:"workspace"`
	Node      *proto.NodeDiag  `json:"node,omitempty"`
	OnNode    *proto.WSDiag    `json:"on_node,omitempty"`
	Placement []proto.Finding  `json:"placement,omitempty"`
	Events    []proto.Event    `json:"recent_events,omitempty"`
	Findings  []proto.Finding  `json:"findings,omitempty"`
}

func cmdInspect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	var c common
	c.flags(fs)
	events := fs.Int("events", 15, "how many recent events to show")
	parse(fs, args)
	if fs.NArg() < 1 {
		return errors.New("inspect WS")
	}
	id := fs.Arg(0)
	cl := c.client()
	defer cl.Close()

	ws, err := cl.GetWorkspace(ctx, id)
	if err != nil {
		return err
	}
	rep := inspectReport{Workspace: ws}

	// Ask the node holding it for the deep view.
	if ws.Node != "" {
		if nd, err := cl.NodeDiag(ctx, ws.Node, id, false); err == nil {
			rep.Node = nd
			for i := range nd.Workspaces {
				if nd.Workspaces[i].ID == id {
					rep.OnNode = &nd.Workspaces[i]
				}
			}
			rep.Findings = append(rep.Findings, nd.Findings...)
		} else {
			rep.Findings = append(rep.Findings, proto.Finding{
				Severity: "warn", Check: "node.unreachable", Subject: ws.Node,
				Detail: err.Error(), Hint: "the control plane believes this node holds the workspace",
			})
		}
	}
	// The control plane's own view is the only thing that explains "pending".
	if ws.State == proto.WSPending {
		rep.Findings = append(rep.Findings, proto.Finding{
			Severity: "warn", Check: "workspace.pending", Subject: id,
			Detail: "queued, waiting for an eligible node to claim it",
		})
	}
	if evs, err := cl.ReadEvents(ctx, 1, id); err == nil {
		if len(evs) > *events {
			evs = evs[len(evs)-*events:]
		}
		rep.Events = evs
	}
	// Consistency: control says claimed, node says it has it.
	if ws.State == proto.WSClaimed && rep.OnNode == nil && ws.Node != "" && rep.Node != nil {
		rep.Findings = append(rep.Findings, proto.Finding{
			Severity: "error", Check: "consistency.workspace_missing_on_node", Subject: id,
			Detail: fmt.Sprintf("control plane says %s holds it; that node does not have it", ws.Node),
			Hint:   "the lease will expire and it will be re-queued from its last snapshot",
		})
	}
	if rep.OnNode != nil && rep.OnNode.Gen != ws.Generation {
		rep.Findings = append(rep.Findings, proto.Finding{
			Severity: "error", Check: "consistency.generation_mismatch", Subject: id,
			Detail: fmt.Sprintf("control plane generation %d, node generation %d", ws.Generation, rep.OnNode.Gen),
		})
	}

	if c.json {
		printJSON(rep)
		return nil
	}
	fmt.Printf("workspace %s  %q\n", ws.ID, ws.Spec.Name)
	fmt.Printf("  state       %s (generation %d)\n", ws.State, ws.Generation)
	fmt.Printf("  node        %s\n", orDash(ws.Node))
	if ws.LeaseUntil > 0 {
		fmt.Printf("  lease       %s remaining\n", time.Until(time.UnixMilli(ws.LeaseUntil)).Round(time.Second))
	}
	fmt.Printf("  snapshot    %s\n", orDash(ws.LastSnapshot))
	fmt.Printf("  principal   %s\n", orDash(ws.Spec.Principal))
	if len(ws.Spec.Bindings) > 0 {
		fmt.Printf("  bindings    %s\n", strings.Join(ws.Spec.Bindings, ", "))
	}
	if r := ws.Spec.Requires; r.CPU > 0 || r.MemMiB > 0 || r.Backend != "" || len(r.Caps) > 0 {
		fmt.Printf("  requires    %+v\n", r)
	}
	if p := ws.Spec.Placement; p.Node != "" || len(p.Allow) > 0 {
		fmt.Printf("  placement   %+v\n", p)
	}
	if d := rep.OnNode; d != nil {
		fmt.Printf("\non node %s (%s/%s)\n", rep.Node.Node, rep.Node.Info.OS, rep.Node.Info.Arch)
		fmt.Printf("  backend     %s\n", d.Backend)
		fmt.Printf("  root        %s\n", d.Root)
		fmt.Printf("  disk        %s in %d files   node free %s\n", humanBytes(d.Bytes), d.Files, humanBytes(rep.Node.DiskFree))
		fmt.Printf("  broker      %s\n", orDash(d.Broker))
		if len(d.Sessions) > 0 {
			fmt.Println("\n  sessions")
			tw := tabWriter()
			fmt.Fprintln(tw, "  ID\tKIND\tSTATE\tSEQ\tOLDEST\tPROGRAM")
			for _, s := range d.Sessions {
				state := "running"
				if s.Exited && s.Exit != nil {
					state = fmt.Sprintf("exit %d", s.Exit.Code)
					if s.Exit.Signal != "" {
						state = s.Exit.Signal
					}
				}
				fmt.Fprintf(tw, "  %s\t%s\t%s\t%d\t%d\t%s\n", s.Info.ID, s.Info.Kind, state,
					s.Next, s.Oldest, truncate(strings.Join(s.Info.Program, " "), 40))
			}
			tw.Flush()
		}
	}
	if len(rep.Events) > 0 {
		fmt.Println("\nrecent events")
		for _, e := range rep.Events {
			fmt.Printf("  %6d %s %-18s %s\n", e.Seq, time.UnixMilli(e.At).Format("15:04:05.000"), e.Type, payloadSummary(e))
		}
	}
	printFindings(rep.Findings)
	return nil
}

func payloadSummary(e proto.Event) string {
	var v any
	if err := proto.Unmarshal(e.Payload, &v); err != nil || v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return truncate(string(b), 90)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func dur(sec int64) string {
	return (time.Duration(sec) * time.Second).Round(time.Second).String()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

type doctorReport struct {
	OK       bool            `json:"ok"`
	Checked  []string        `json:"checked"`
	Findings []proto.Finding `json:"findings"`
}

func cmdDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	var c common
	c.flags(fs)
	deep := fs.Bool("deep", false, "re-hash every artifact; reads every byte")
	parse(fs, args)
	cl := c.client()
	defer cl.Close()
	rep := doctorReport{OK: true}

	add := func(f ...proto.Finding) {
		rep.Findings = append(rep.Findings, f...)
		for _, x := range f {
			if x.Severity == "error" {
				rep.OK = false
			}
		}
	}
	check := func(name string) { rep.Checked = append(rep.Checked, name) }

	// 1. Control plane reachable, and its own findings.
	check("control.reachable")
	d, err := cl.Diag(ctx, *deep)
	if err != nil {
		add(proto.Finding{Severity: "error", Check: "control.reachable", Detail: err.Error(),
			Hint: "check --server and --token"})
		return finish(rep, c.json)
	}
	add(d.Findings...)

	// 2. The database's own integrity check. This is the deepest statement
	// the control plane can make about whether its state is intact.
	check("control.db_integrity")
	if d.DBIntegrity != "" && d.DBIntegrity != "ok" {
		add(proto.Finding{Severity: "error", Check: "control.db_integrity", Detail: d.DBIntegrity,
			Hint: "sqlite reports corruption; restore control.db from a backup"})
	}

	// 3. Every node: reachable, and agreeing with the control plane about
	// what it holds.
	nodes, err := cl.ListNodes(ctx)
	if err != nil {
		add(proto.Finding{Severity: "error", Check: "node.list", Detail: err.Error()})
		return finish(rep, c.json)
	}
	wss, _ := cl.ListWorkspaces(ctx)
	expected := map[string][]string{}
	for _, ws := range wss {
		if ws.Node != "" && (ws.State == proto.WSClaimed || ws.State == proto.WSClaiming) {
			expected[ws.Node] = append(expected[ws.Node], ws.ID)
		}
	}
	check("node.consistency")
	for _, n := range nodes {
		if !n.Online {
			if len(expected[n.ID]) > 0 {
				add(proto.Finding{Severity: "warn", Check: "node.offline_holding", Subject: n.ID,
					Detail: fmt.Sprintf("offline while holding %d workspaces", len(expected[n.ID])),
					Hint:   "their leases will expire and they will be re-queued"})
			}
			continue
		}
		// Ask about a workspace the control plane says this node holds, so
		// the grant is one we are entitled to.
		var probe string
		if len(expected[n.ID]) > 0 {
			probe = expected[n.ID][0]
		}
		nd, err := cl.NodeDiag(ctx, n.ID, probe, *deep)
		if err != nil {
			// Never skip a check quietly. A consistency check that cannot run
			// is not a passing consistency check.
			add(proto.Finding{Severity: "warn", Check: "node.diag_unavailable", Subject: n.ID,
				Detail: "could not read this node's deep state: " + err.Error(),
				Hint:   "its workspaces were not checked for consistency"})
			continue
		}
		add(nd.Findings...)
		have := map[string]bool{}
		for _, w := range nd.Workspaces {
			have[w.ID] = true
		}
		for _, id := range expected[n.ID] {
			if !have[id] {
				add(proto.Finding{Severity: "error", Check: "consistency.missing_on_node", Subject: id,
					Detail: fmt.Sprintf("control plane says %s holds it; the node does not", n.ID),
					Hint:   "data may be lost if there is no snapshot; check `remount inspect " + id + "`"})
			}
		}
		for _, w := range nd.Workspaces {
			found := false
			for _, id := range expected[n.ID] {
				if id == w.ID {
					found = true
				}
			}
			if !found {
				add(proto.Finding{Severity: "warn", Check: "consistency.orphan_on_node", Subject: w.ID,
					Detail: fmt.Sprintf("%s is holding a workspace the control plane does not assign to it", n.ID),
					Hint:   "it will be dropped on the node's next resync"})
			}
		}
	}

	// 4. Workspaces that can never be placed.
	check("workspace.placeable")
	for _, ws := range wss {
		if ws.State != proto.WSPending {
			continue
		}
		placeable := false
		for _, n := range nodes {
			if n.Online {
				placeable = true // refined by the control plane's own finding
			}
		}
		if !placeable {
			add(proto.Finding{Severity: "error", Check: "workspace.no_nodes", Subject: ws.ID,
				Detail: "pending with no node online at all"})
		}
	}

	// 5. Tenant policy. The control plane already returned its own retention
	// and residency findings above; naming the checks here is what tells an
	// operator which properties were actually examined, so an unavailable
	// finding is legible as "not checked" rather than as silence.
	check("tenant.retention")
	check("tenant.residency")

	// 6. Loss signals from the counters. A deep pass answers three separate
	// questions, so it registers three names: the blob still matches its
	// content address, the chunk manifest still decodes canonically, and every
	// object that manifest names is still present at its declared size.
	if *deep {
		check("artifact.deep_verify")
		check("artifact.manifest_canonical")
		check("artifact.closure_complete")
	}
	check("data.loss_signals")
	if v := d.Metrics["remount_artifact_digest_mismatch_total"]; v > 0 {
		add(proto.Finding{Severity: "error", Check: "artifact.corruption",
			Detail: fmt.Sprintf("%g artifacts failed digest verification", v)})
	}
	if v := d.Metrics["remount_frames_dropped_total"]; v > 0 {
		add(proto.Finding{Severity: "info", Check: "relay.frames_dropped",
			Detail: fmt.Sprintf("%g frames were addressed to a peer that had gone", v),
			Hint:   "expected during reconnects; a rising rate means peers are flapping"})
	}
	if v := d.Metrics["remount_session_gaps_total"]; v > 0 {
		add(proto.Finding{Severity: "warn", Check: "session.gaps",
			Detail: fmt.Sprintf("%g replay gaps were reported; that much output no client could retrieve", v)})
	}
	return finish(rep, c.json)
}

func finish(rep doctorReport, asJSON bool) error {
	sort.SliceStable(rep.Findings, func(i, j int) bool {
		rank := map[string]int{"error": 0, "warn": 1, "info": 2}
		return rank[rep.Findings[i].Severity] < rank[rep.Findings[j].Severity]
	})
	if asJSON {
		printJSON(rep)
	} else {
		fmt.Printf("checked: %s\n", strings.Join(rep.Checked, ", "))
		if len(rep.Findings) == 0 {
			fmt.Println("no problems found")
		} else {
			printFindings(rep.Findings)
		}
		fmt.Println()
		if rep.OK {
			fmt.Println("healthy")
		} else {
			fmt.Println("PROBLEMS FOUND")
		}
	}
	if !rep.OK {
		return exitError(1)
	}
	return nil
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

func cmdMetrics(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("metrics", flag.ExitOnError)
	var c common
	c.flags(fs)
	node := fs.String("node", "", "read a node's counters instead of the control plane's")
	parse(fs, args)
	cl := c.client()
	defer cl.Close()
	var m map[string]float64
	if *node != "" {
		ws := ""
		if list, err := cl.ListWorkspaces(ctx); err == nil {
			for _, w := range list {
				if w.Node == *node {
					ws = w.ID
					break
				}
			}
		}
		d, err := cl.NodeDiag(ctx, *node, ws, false)
		if err != nil {
			return err
		}
		m = d.Metrics
	} else {
		d, err := cl.Diag(ctx, false)
		if err != nil {
			return err
		}
		m = d.Metrics
	}
	if c.json {
		printJSON(m)
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if m[k] == 0 {
			continue
		}
		fmt.Printf("%-46s %g\n", k, m[k])
	}
	_ = os.Stdout.Sync()
	return nil
}
