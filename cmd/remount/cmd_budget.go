package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"remount.dev/remount/internal/budget"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

const budgetUsage = "budget create ID --attach tenant|workspace|principal|binding:ID [--window 1h|1d|30d] [--max-requests N] [--max-tokens N] [--max-estimated-cost-micros N] | budget ls | budget rm ID"

func cmdBudget(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New(budgetUsage)
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("budget "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "create":
		attach := fs.String("attach", "", "attachment kind:id")
		window := fs.String("window", string(budget.WindowDay), "rolling window: 1h, 1d, or 30d")
		maxRequests := fs.Int64("max-requests", 0, "hard request limit")
		maxTokens := fs.Int64("max-tokens", 0, "hard token limit")
		var maxCost int64
		fs.Int64Var(&maxCost, "max-estimated-cost-micros", 0, "approximate cost limit in micro-dollars")
		fs.Int64Var(&maxCost, "max-cost-micros", 0, "alias for --max-estimated-cost-micros")
		idem := fs.String("idem", "", "stable idempotency key")
		parse(fs, rest)
		if err := arity(fs, 1, 1, budgetUsage); err != nil {
			return err
		}
		kind, id, ok := strings.Cut(*attach, ":")
		if !ok || kind == "" || id == "" {
			return errors.New("--attach must be kind:id")
		}
		value := proto.Budget{
			ID: fs.Arg(0), AttachTo: kind, AttachID: id, Window: *window,
			MaxRequests: *maxRequests, MaxTokens: *maxTokens, MaxEstimatedCostMicros: maxCost,
		}
		validationTenant := "tenant-validation"
		if kind == string(budget.AttachTenant) {
			validationTenant = id
		}
		if err := budget.ValidateBudget(budget.Budget{
			ID: value.ID, Tenant: validationTenant, AttachTo: budget.AttachmentKind(kind), AttachID: id, Window: budget.Window(*window),
			MaxRequests: value.MaxRequests, MaxTokens: value.MaxTokens, MaxEstimatedCostMicros: value.MaxEstimatedCostMicros,
		}); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var options []client.OperationOption
		if *idem != "" {
			options = append(options, client.WithIdempotencyKey(*idem))
		}
		created, err := cl.CreateBudget(ctx, value, options...)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(created)
		} else {
			fmt.Printf("%s\t%s:%s\t%s\n", created.ID, created.AttachTo, created.AttachID, created.Window)
		}
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, budgetUsage); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		values, err := cl.ListBudgets(ctx)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(values)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "ID\tATTACHED TO\tWINDOW\tREQUESTS\tTOKENS\tEST. COST MICROS")
		for _, value := range values {
			fmt.Fprintf(tw, "%s\t%s:%s\t%s\t%d\t%d\t%d\n", value.ID, value.AttachTo, value.AttachID, value.Window, value.MaxRequests, value.MaxTokens, value.MaxEstimatedCostMicros)
		}
		tw.Flush()
		return nil
	case "rm":
		idem := fs.String("idem", "", "stable idempotency key")
		parse(fs, rest)
		if err := arity(fs, 1, 1, budgetUsage); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var options []client.OperationOption
		if *idem != "" {
			options = append(options, client.WithIdempotencyKey(*idem))
		}
		if err := cl.RemoveBudget(ctx, fs.Arg(0), options...); err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(map[string]string{"removed": fs.Arg(0)})
		}
	default:
		return fmt.Errorf("unknown budget subcommand %q", sub)
	}
	return nil
}

func cmdUsage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("usage", flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	tenant := fs.String("tenant", "", "tenant filter")
	workspace := fs.String("ws", "", "workspace filter")
	principal := fs.String("principal", "", "principal filter")
	binding := fs.String("binding", "", "binding filter")
	window := fs.String("window", "", "window filter: 1h, 1d, or 30d")
	parse(fs, args)
	if err := arity(fs, 0, 0, "usage [--tenant T] [--ws WS] [--principal P] [--binding B] [--window 1h|1d|30d]"); err != nil {
		return err
	}
	if *window != "" {
		if _, ok := budget.Window(*window).Duration(); !ok {
			return fmt.Errorf("invalid usage window %q", *window)
		}
	}
	cl := commonFlags.client()
	defer cl.Close()
	values, err := cl.Usage(ctx, proto.UsageReq{Tenant: *tenant, WS: *workspace, Principal: *principal, Binding: *binding, Window: *window})
	if err != nil {
		return err
	}
	if commonFlags.json {
		printJSON(proto.UsageRes{Usage: values})
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "BUDGET\tWINDOW\tREQUESTS\tTOKENS\tEST. COST MICROS\tACTIVE\tUNMETERED\tINCOMPLETE")
	for _, value := range values {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\n", value.BudgetID, value.Window, value.Requests, value.Tokens, value.EstimatedCostMicros, value.ActiveReservations, value.UnmeteredRequests, value.IncompleteRequests)
	}
	tw.Flush()
	return nil
}
